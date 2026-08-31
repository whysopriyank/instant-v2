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
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
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
	schema, err := cfg.Schema.normalized()
	if err != nil {
		return counts, err
	}

	title := cfg.Title
	if title == "" {
		title = "Restored v1 app"
	}
	if err := ensureAppRow(ctx, tx, appID, newRandomUUID(), title); err != nil {
		return counts, err
	}

	attrs, err := importV1Schema(ctx, tx, appID, schema, reader.File[1:], &counts)
	if err != nil {
		return counts, err
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
			if err := importV1Entities(batch, attrs, etype, body); err != nil {
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
