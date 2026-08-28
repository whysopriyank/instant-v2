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
		Schema v1Schema        `json:"schema"`
	}
	if err := json.Unmarshal(cfgBody, &cfg); err != nil {
		return counts, fmt.Errorf("backup: malformed v1 %s: %v", v1ConfigEntry, err)
	}
	// Older self-host exports call these sections blobs/refs; current v1
	// exports use entities/links. Normalize both shapes before materializing
	// attrs so restore remains compatible with either producer.
	blobs := cfg.Schema.Blobs
	if blobs == nil {
		blobs = make(map[string]map[string]v1BlobDef)
	}
	for etype, entity := range cfg.Schema.Entities {
		if blobs[etype] == nil {
			blobs[etype] = make(map[string]v1BlobDef)
		}
		for label, def := range entity.Attrs {
			blobs[etype][label] = def
		}
	}
	refs := cfg.Schema.Refs
	if refs == nil {
		refs = make(map[string]v1Link)
	}
	for name, link := range cfg.Schema.Links {
		refs[name] = link
	}
	if len(blobs) == 0 && len(refs) == 0 {
		return counts, fmt.Errorf("backup: v1 %s missing schema.blobs/schema.refs", v1ConfigEntry)
	}

	title := cfg.Title
	if title == "" {
		title = "Restored v1 app"
	}
	if err := ensureAppRow(ctx, tx, appID, newRandomUUID(), title); err != nil {
		return counts, err
	}

	attrs := map[string]v1AttrRef{} // "etype\x00label" -> attr metadata

	insertAttr := func(etype, label, reverseEtype, reverseLabel, valueType, cardinality string, cdt *string, uniq, indexed, required bool) error {
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
			                   forward_ident, reverse_ident, checked_data_type, is_required)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
			attrID, appID, etype, label, reverseEtype, reverseLabel, valueType, cardinality, uniq, indexed, fwd, rev, cdt, required); err != nil {
			return fmt.Errorf("backup: insert v1 attr %s.%s: %w", etype, label, err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO idents (id, app_id, attr_id, etype, label)
			VALUES ($1,$2,$3,$4,$5)
			ON CONFLICT DO NOTHING`, newRandomUUID(), appID, attrID, etype, label); err != nil {
			return err
		}
		attrs[key] = v1AttrRef{ID: attrID, Cardinality: cardinality}
		counts.Attrs++
		return nil
	}

	// Blobs: explicit defs + implicit id attr per etype (v1 semantics).
	for etype, labels := range blobs {
		for label, def := range labels {
			cdt := v1CheckedDataType(def.ValueType)
			if err := insertAttr(etype, label, etype, label, "blob", "one", cdt, def.Config.Unique, def.Config.Indexed, v1AttrRequired(def)); err != nil {
				return counts, err
			}
		}
		if _, ok := attrs[etype+"\x00id"]; !ok {
			if err := insertAttr(etype, "id", etype, "id", "blob", "one", nil, true, true, true); err != nil {
				return counts, err
			}
		}
	}
	// Entity files can exist for an etype that has no blob declaration (for
	// example, a ref-only endpoint). Ensure its implicit id attr still exists.
	for _, entry := range reader.File[1:] {
		name := entry.Name
		if !strings.HasPrefix(name, v1EntitiesDir) || !strings.HasSuffix(name, v1EntitiesSufx) {
			continue
		}
		etype := name[len(v1EntitiesDir) : len(name)-len(v1EntitiesSufx)]
		if etype != "" {
			if err := insertAttr(etype, "id", etype, "id", "blob", "one", nil, true, true, true); err != nil {
				return counts, err
			}
		}
	}
	// Refs: forward + reverse attr pair, reverse names mirrored like v1.
	for _, link := range refs {
		fwd, rev := link.Forward, link.Reverse
		if fwd.On == "" || fwd.Label == "" || rev.On == "" || rev.Label == "" {
			return counts, fmt.Errorf("backup: v1 ref link missing endpoint names")
		}
		cardF := fwd.Has
		if cardF == "" {
			cardF = "many"
		}
		// In the v1 model, reverse `has: one` means the forward ref is unique
		// (at most one source can point at a target). Preserve that constraint;
		// reverse cardinality is metadata, not a second attrs row.
		reverseUnique := rev.Has == "one"
		// A v1 link is represented by one ref attr. Its reverse endpoint is
		// metadata on that attr, not a second row (the attrs name-collision
		// trigger intentionally rejects materializing both directions).
		if err := insertAttr(fwd.On, fwd.Label, rev.On, rev.Label, "ref", cardF, nil, reverseUnique, true, v1EndpointRequired(fwd)); err != nil {
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

type v1AttrRef struct {
	ID          [16]byte
	Cardinality string
}

type v1Endpoint struct {
	On        string `json:"on"`
	Label     string `json:"label"`
	Has       string `json:"has"`
	Required  *bool  `json:"required"`
	RequiredQ *bool  `json:"required?"`
	Config    struct {
		Required  *bool `json:"required"`
		RequiredQ *bool `json:"required?"`
	} `json:"config"`
}

type v1Link struct {
	Forward v1Endpoint `json:"forward"`
	Reverse v1Endpoint `json:"reverse"`
}

type v1EntityDef struct {
	Attrs map[string]v1BlobDef `json:"attrs"`
}

type v1Schema struct {
	// Legacy shape used by early self-host exports.
	Blobs map[string]map[string]v1BlobDef `json:"blobs"`
	Refs  map[string]v1Link               `json:"refs"`
	// Current v1 shape emitted by schema->defs in config.json.
	Entities map[string]v1EntityDef `json:"entities"`
	Links    map[string]v1Link      `json:"links"`
}

// v1BlobDef accepts both spellings emitted by historical Instant exports.
// Older exports omitted requiredness entirely; those attrs remain optional,
// except the implicit id attr which is normalized below.
type v1BlobDef struct {
	ValueType string `json:"valueType"`
	Required  *bool  `json:"required"`
	RequiredQ *bool  `json:"required?"`
	Config    struct {
		Unique    bool  `json:"unique"`
		Indexed   bool  `json:"indexed"`
		Required  *bool `json:"required"`
		RequiredQ *bool `json:"required?"`
	} `json:"config"`
}

func v1AttrRequired(def v1BlobDef) bool {
	for _, p := range []*bool{def.Required, def.RequiredQ, def.Config.Required, def.Config.RequiredQ} {
		if p != nil {
			return *p
		}
	}
	return false
}

func v1EndpointRequired(ep v1Endpoint) bool {
	for _, p := range []*bool{ep.Required, ep.RequiredQ, ep.Config.Required, ep.Config.RequiredQ} {
		if p != nil {
			return *p
		}
	}
	return false
}

func importV1Entities(ctx context.Context, batch *tripleBatch, attrs map[string]v1AttrRef, etype string, body []byte) error {
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
		// The id field is a real, implicit required attr in Instant's entity
		// model. Materialize it so restored queries and required checks see the
		// same representation as native imports.
		idAttr, ok := attrs[etype+"\x00id"]
		if !ok {
			return fmt.Errorf("backup: v1 entities/%s line %d: missing implicit id attr", etype, lineNo)
		}
		idJSON, err := json.Marshal(entityID)
		if err != nil {
			return err
		}
		if err := batch.addTriple(entityID, uuidOf(idAttr.ID), idJSON, created, hasCreated); err != nil {
			return err
		}
		for label, value := range row.Entity {
			if label == "id" {
				continue
			}
			attr, ok := attrs[etype+"\x00"+label]
			if !ok {
				return fmt.Errorf("backup: v1 entities/%s line %d: missing attr for %s.%s (schema/config.json incomplete?)", etype, lineNo, etype, label)
			}
			var decoded any
			if err := json.Unmarshal(value, &decoded); err != nil {
				return fmt.Errorf("backup: v1 entities/%s line %d: bad value for %s.%s: %v", etype, lineNo, etype, label, err)
			}
			emit := func(v json.RawMessage) error {
				return batch.addTriple(entityID, uuidOf(attr.ID), v, created, hasCreated)
			}
			// Only many-cardinality fields use an array as a transport wrapper.
			// For one-cardinality attrs an array is the value itself and must stay
			// intact (not be silently expanded into multiple triples).
			if attr.Cardinality == "many" {
				arr, isArr := decoded.([]any)
				if !isArr {
					if err := emit(value); err != nil {
						return err
					}
					continue
				}
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

// maxEntryBytes bounds one zip entry's uncompressed size. A hostile archive
// can declare tiny compressed bytes and expand to gigabytes (decompression
// bomb); imports stream into memory per entry, so the bound is load-bearing.
const maxEntryBytes = 1 << 30 // 1 GiB

func readEntry(f *zip.File) ([]byte, error) {
	rc, err := f.Open()
	if err != nil {
		return nil, fmt.Errorf("backup: open zip entry %s: %v", f.Name, err)
	}
	defer func() { _ = rc.Close() }() //nolint:errcheck // read already completed
	body, err := io.ReadAll(io.LimitReader(rc, maxEntryBytes+1))
	if err != nil {
		return nil, fmt.Errorf("backup: read zip entry %s: %v", f.Name, err)
	}
	if len(body) > maxEntryBytes {
		return nil, fmt.Errorf("backup: zip entry %s exceeds %d bytes", f.Name, maxEntryBytes)
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
