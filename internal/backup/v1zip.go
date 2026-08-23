// v1 self-host dump import. V1's backup pipeline (server/src/instant/
// backup.clj + restore.clj) produces a ZIP archive with exactly this layout:
//
//	config.json            — first entry; plain JSON (zstd is applied only to
//	                         the separate S3 object, never inside the zip):
//	  {"title": "...",
//	   "rules": <jsonb rules code or null>,
//	   "schema": {
//	     "blobs": {"<etype>": {"<label>": {"valueType": "string|number|
//	                           boolean|date|any|null",
//	                           "config": {"unique": bool, "indexed": bool}}}},
//	     "refs":  {"<linkName>": {"forward":  {"on": E, "label": L, "has":
//	                               "one"|"many", ...},
//	                              "reverse": {...}}},
//	   ... plus counts/tripleCount/webhooks/emailTemplates/etc., all ignored}
//	entities/<etype>.jsonl — one JSON object per line:
//	  {"entity": {"id": "<uuid>", "<label>": <value>, ...},
//	   "createdAt": <epoch millis>}
//	files/<location-id>    — raw file blobs; SKIPPED by this importer
//	                         ($files metadata triples are still imported when
//	                         the schema declares the $files attrs).
//
// Restore semantics mirror restore.clj: config.json must be the first entry;
// every entity field must resolve to a schema attr (else the import aborts,
// like v1's "Missing attr" exception); an implicit unique+indexed `id` attr is
// created per etype; refs arrive as uuid strings and are stored as such.
// The transactions journal does not exist in v1 dumps, so it imports empty.
package backup

import (
	"archive/zip"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"strings"
	"time"
)

const (
	v1ConfigEntry  = "config.json"
	v1EntitiesDir  = "entities/"
	v1EntitiesSufx = ".jsonl"
	v1FilesDir     = "files/"
	v1BatchTriples = 2000
)

// RestoreV1Zip ingests a v1 export zip (see format docs above) into appID.
// The app row is created if absent; attrs are created from the schema defs
// with fresh uuids exactly like v1's schema-model plan/apply path. Entity
// values become triples via the same Postgres flag/md5 derivation as Import.
func RestoreV1Zip(ctx context.Context, pool *pgxpool.Pool, zr io.ReaderAt, size int64, appID [16]byte) (Counts, error) {
	var counts Counts
	reader, err := zip.NewReader(zr, size)
	if err != nil {
		return counts, fmt.Errorf("backup: open v1 zip: %w", err)
	}
	if len(reader.File) == 0 || reader.File[0].Name != v1ConfigEntry {
		got := "none"
		if len(reader.File) > 0 {
			got = reader.File[0].Name
		}
		return counts, fmt.Errorf("backup: expected first v1 zip entry to be %s, got %s", v1ConfigEntry, got)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return counts, fmt.Errorf("backup: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	cfgBody, err := readEntry(reader.File[0])
	if err != nil {
		return counts, err
	}
	var cfg struct {
		Title  string          `json:"title"`
		Rules  json.RawMessage `json:"rules"`
		Schema struct {
			Blobs map[string]map[string]struct {
				ValueType string `json:"valueType"`
				Config    struct {
					Unique  bool `json:"unique"`
					Indexed bool `json:"indexed"`
				} `json:"config"`
			} `json:"blobs"`
			Refs map[string]struct {
				Forward v1Endpoint `json:"forward"`
				Reverse v1Endpoint `json:"reverse"`
			} `json:"refs"`
		} `json:"schema"`
	}
	if err := json.Unmarshal(cfgBody, &cfg); err != nil {
		return counts, fmt.Errorf("backup: malformed v1 %s: %v", v1ConfigEntry, err)
	}
	if cfg.Schema.Blobs == nil && cfg.Schema.Refs == nil {
		return counts, fmt.Errorf("backup: v1 %s missing schema.blobs/schema.refs", v1ConfigEntry)
	}

	title := cfg.Title
	if title == "" {
		title = "Restored v1 app"
	}
	if err := ensureAppRow(ctx, tx, appID, newRandomUUID(), title); err != nil {
		return counts, err
	}

	attrs := map[string][16]byte{} // "etype\x00label" -> attr id

	insertAttr := func(etype, label, valueType, cardinality string, cdt *string, uniq, indexed bool) error {
		key := etype + "\x00" + label
		if _, ok := attrs[key]; ok {
			return nil
		}
		attrID := newRandomUUID()
		fwd := newRandomUUID()
		rev := newRandomUUID()
		if _, err := tx.Exec(ctx, `
			INSERT INTO attrs (id, app_id, etype, label, reverse_etype, reverse_label,
			                   value_type, cardinality, is_unique, is_indexed,
			                   forward_ident, reverse_ident, checked_data_type)
			VALUES ($1,$2,$3,$4,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
			attrID, appID, etype, label, valueType, cardinality, uniq, indexed, fwd, rev, cdt); err != nil {
			return fmt.Errorf("backup: insert v1 attr %s.%s: %w", etype, label, err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO idents (id, app_id, attr_id, etype, label)
			VALUES ($1,$2,$3,$4,$5)
			ON CONFLICT DO NOTHING`, newRandomUUID(), appID, attrID, etype, label); err != nil {
			return err
		}
		attrs[key] = attrID
		counts.Attrs++
		return nil
	}

	// Blobs: explicit defs + implicit id attr per etype (v1 semantics).
	for etype, labels := range cfg.Schema.Blobs {
		for label, def := range labels {
			cdt := v1CheckedDataType(def.ValueType)
			if err := insertAttr(etype, label, "blob", "one", cdt, def.Config.Unique, def.Config.Indexed); err != nil {
				return counts, err
			}
		}
		if _, ok := attrs[etype+"\x00id"]; !ok {
			if err := insertAttr(etype, "id", "blob", "one", nil, true, true); err != nil {
				return counts, err
			}
		}
	}
	// Refs: forward + reverse attr pair, reverse names mirrored like v1.
	for _, link := range cfg.Schema.Refs {
		fwd, rev := link.Forward, link.Reverse
		if fwd.On == "" || fwd.Label == "" || rev.On == "" || rev.Label == "" {
			return counts, fmt.Errorf("backup: v1 ref link missing endpoint names")
		}
		cardF, cardR := fwd.Has, rev.Has
		if cardF == "" {
			cardF = "many"
		}
		if cardR == "" {
			cardR = "many"
		}
		if err := insertAttr(fwd.On, fwd.Label, "ref", cardF, nil, false, true); err != nil {
			return counts, err
		}
		if err := insertAttr(rev.On, rev.Label, "ref", cardR, nil, false, true); err != nil {
			return counts, err
		}
		// Mirror reverse naming onto the pair (v1 add-attr shape).
		if _, err := tx.Exec(ctx, `
			UPDATE attrs SET reverse_etype=$2, reverse_label=$3
			 WHERE app_id=$1 AND etype=$4 AND label=$5`,
			appID, rev.On, rev.Label, fwd.On, fwd.Label); err != nil {
			return counts, err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE attrs SET reverse_etype=$2, reverse_label=$3
			 WHERE app_id=$1 AND etype=$4 AND label=$5`,
			appID, fwd.On, fwd.Label, rev.On, rev.Label); err != nil {
			return counts, err
		}
	}

	// Rules code (jsonb).
	if len(cfg.Rules) > 0 && string(cfg.Rules) != "null" {
		if _, err := tx.Exec(ctx, `
			INSERT INTO rules (app_id, code, version) VALUES ($1,$2,0)
			ON CONFLICT (app_id) DO UPDATE SET code=EXCLUDED.code`,
			appID, []byte(cfg.Rules)); err != nil {
			return counts, fmt.Errorf("backup: import v1 rules: %w", err)
		}
		counts.Rules++
	}

	batch := newTripleBatch()
	flush := func() error { return batch.flush(ctx, tx, appID, &counts.Triples) }

	for _, entry := range reader.File[1:] {
		name := entry.Name
		switch {
		case strings.HasPrefix(name, v1FilesDir):
			// File blobs stay in the archive; $files metadata triples below
			// reference them but bytes upload is out of scope for DB restore.
			continue
		case strings.HasPrefix(name, v1EntitiesDir) && strings.HasSuffix(name, v1EntitiesSufx):
			etype := name[len(v1EntitiesDir) : len(name)-len(v1EntitiesSufx)]
			body, rerr := readEntry(entry)
			if rerr != nil {
				return counts, rerr
			}
			if err := importV1Entities(ctx, batch, attrs, etype, body); err != nil {
				return counts, err
			}
			if batch.len() >= tripleBatchSize {
				if err := flush(); err != nil {
					return counts, err
				}
			}
		default:
			return counts, fmt.Errorf("backup: unexpected v1 zip entry %q (want entities/* or files/*)", name)
		}
	}
	if err := flush(); err != nil {
		return counts, err
	}
	if err := tx.Commit(ctx); err != nil {
		return counts, fmt.Errorf("backup: commit v1 restore: %w", err)
	}
	return counts, nil
}

type v1Endpoint struct {
	On    string `json:"on"`
	Label string `json:"label"`
	Has   string `json:"has"`
}

func importV1Entities(ctx context.Context, batch *tripleBatch, attrs map[string][16]byte, etype string, body []byte) error {
	sc := newLineScanner(strings.NewReader(string(body)))
	lineNo := 0
	for sc.Next() {
		lineNo++
		line := sc.line()
		if strings.TrimSpace(line) == "" {
			continue
		}
		var row struct {
			Entity    map[string]json.RawMessage `json:"entity"`
			CreatedAt float64                    `json:"createdAt"`
		}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return fmt.Errorf("backup: v1 entities/%s line %d: %v", etype, lineNo, err)
		}
		idRaw, ok := row.Entity["id"]
		if !ok {
			return fmt.Errorf("backup: v1 entities/%s line %d: entity missing id", etype, lineNo)
		}
		var entityID string
		if err := json.Unmarshal(idRaw, &entityID); err != nil {
			return fmt.Errorf("backup: v1 entities/%s line %d: bad entity id: %v", etype, lineNo, err)
		}
		hasCreated := row.CreatedAt > 0
		created := time.Time{}
		if hasCreated {
			created = time.UnixMilli(int64(row.CreatedAt))
		}
		for label, value := range row.Entity {
			if label == "id" {
				continue
			}
			attrID, ok := attrs[etype+"\x00"+label]
			if !ok {
				return fmt.Errorf("backup: v1 entities/%s line %d: missing attr for %s.%s (schema/config.json incomplete?)", etype, lineNo, etype, label)
			}
			var decoded any
			if err := json.Unmarshal(value, &decoded); err != nil {
				return fmt.Errorf("backup: v1 entities/%s line %d: bad value for %s.%s: %v", etype, lineNo, etype, label, err)
			}
			emit := func(v json.RawMessage) error {
				return batch.addTriple(entityID, uuidOf(attrID), v, created, hasCreated)
			}
			// many-cardinality fields arrive as arrays of values.
			if arr, isArr := decoded.([]any); isArr {
				for _, item := range arr {
					itemJSON, err := json.Marshal(item)
					if err != nil {
						return err
					}
					if err := emit(itemJSON); err != nil {
						return err
					}
				}
			} else if err := emit(value); err != nil {
				return err
			}
		}
	}
	return sc.err(nil)
}

// v1CheckedDataType maps a v1 schema valueType to the checked_data_type enum
// text; any/null map to SQL NULL (unchecked), matching v1 migration 36.
func v1CheckedDataType(valueType string) *string {
	switch valueType {
	case "string", "number", "boolean", "date":
		s := valueType
		return &s
	default:
		return nil
	}
}

func readEntry(f *zip.File) ([]byte, error) {
	rc, err := f.Open()
	if err != nil {
		return nil, fmt.Errorf("backup: open zip entry %s: %v", f.Name, err)
	}
	defer func() { _ = rc.Close() }() //nolint:errcheck // read already completed
	body, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("backup: read zip entry %s: %v", f.Name, err)
	}
	return body, nil
}

func newRandomUUID() [16]byte {
	var out [16]byte
	if _, err := rand.Read(out[:]); err != nil {
		panic("crypto/rand failure: " + err.Error())
	}
	out[6] = (out[6] & 0x0f) | 0x40
	out[8] = (out[8] & 0x3f) | 0x80
	return out
}

func uuidOf(u [16]byte) string { return platform.UUIDToStr(u) }
