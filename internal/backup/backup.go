// Package backup implements streaming export/import of one app's data —
// workstream 6B, a port of the semantics of v1 instant.backup +
// instant.restore (server/src/instant/backup.clj, restore.clj) without their
// Java stream / S3 machinery.
//
// # v2 dump wire format (NDJSON)
//
// A dump is UTF-8 text, one JSON object per line, each terminated by '\n'.
// Every line carries a leading "kind" discriminator:
//
//	{"kind":"header","format":"instant-v2-backup","version":2,
//	       "app_id":"<uuid>","title":"...","creator_id":"<uuid>"}
//	{"kind":"attr","id":...,"etype":...,"label":...,            (attrs in id order)
//	                ...full attrs row, nullable fields as null}
//	{"kind":"triple","entity_id":"<uuid>","attr_id":"<uuid>",   (triples ordered by
//	                 "value":<json>,"created_at":"<RFC3339>"}    entity/attr/md5)
//	{"kind":"rule","code":<jsonb>,"version":N}                  (0 or 1 lines)
//	{"kind":"transaction","id":N,"created_at":"<RFC3339>"}      (journal tail)
//	{"kind":"checksum","sha256":"<hex>","records":N}            (always last)
//
// The sha256 is computed over EVERY byte of the stream that precedes the
// checksum line (header and all record lines, including their newlines). The
// "records" field counts logical records (attr+triple+rule+transaction lines;
// header and checksum are not counted).
//
// Resume: ExportOptions.FromOffset skips the first N logical records in
// canonical section order (attrs, then triples, then rule, then transactions).
// The emitted stream still carries header and checksum; the checksum covers
// only what was actually emitted. This is a documented simplification versus
// byte-range resume: a partial dump verifies against itself, not against the
// prefix of a full dump.
//
// Values are exported exactly as stored (jsonb), so md5(value::text) — the
// value_md5 PK component — recomputes deterministically on import; dumps are
// therefore stable across export/import cycles.
package backup

import (
	"errors"
)

const (
	dumpFormat = "instant-v2-backup"
	// Version 2 adds the requiredness bit to attr records. Import retains
	// compatibility with v1 dumps, which predate that field.
	dumpVer = 2
	// JSONNullMD5 mirrors triple.JSONNullMD5 (kept local to avoid a dependency
	// cycle risk; the constant is frozen by v1 semantics: md5("null")).
	jsonNullMD5 = "37a6259cc0c1dae299a7866489dff0bd"

	tripleBatchSize = 2000
)

// ErrAppNotFound is returned when exporting an unknown app.
var ErrAppNotFound = errors.New("backup: app not found")

// Counts summarizes an export or import. On import they reflect rows written
// or verified against an existing DB (idempotent re-import of identical rows
// still counts them).
type Counts struct {
	Attrs        int64 `json:"attrs"`
	Triples      int64 `json:"triples"`
	Rules        int64 `json:"rules"`
	Transactions int64 `json:"transactions"`
}

// ExportOptions tunes Export. Zero value exports everything.
type ExportOptions struct {
	// FromOffset skips the first N logical records (see package docs).
	FromOffset int
}
