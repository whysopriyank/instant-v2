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
//	{"kind":"header","format":"instant-v2-backup","version":1,
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
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/instant-v2/instant-v2/internal/platform"
)

const (
	dumpFormat = "instant-v2-backup"
	dumpVer    = 1
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

// hashWriter tees every dumped byte into the checksum before the trailer.
type hashWriter struct {
	w io.Writer
	h interface {
		io.Writer
		Sum(b []byte) []byte
	}
}

func (hw hashWriter) writeLine(line string) error {
	if _, err := io.WriteString(hw.w, line); err != nil {
		return err
	}
	if _, err := io.WriteString(hw.w, "\n"); err != nil {
		return err
	}
	if _, err := io.WriteString(hw.h, line+"\n"); err != nil {
		return err
	}
	return nil
}

// recordWriter emits COPY/query output lines as logical records, honoring the
// resume offset and counting what it emits.
type recordWriter struct {
	hw    hashWriter
	skip  int
	count int64
}

func (r *recordWriter) emit(line string) error {
	if r.skip > 0 {
		r.skip--
		return nil
	}
	r.count++
	return r.hw.writeLine(line)
}

// Export streams appID as a v2 NDJSON dump to w. Memory use is bounded: rows
// stream out of Postgres one at a time via pgx cursors; nothing accumulates.
func Export(ctx context.Context, w io.Writer, pool *pgxpool.Pool, appID [16]byte, opts ExportOptions) (Counts, error) {
	var counts Counts

	var title string
	var creator [16]byte
	err := pool.QueryRow(ctx, `SELECT title, creator_id FROM apps WHERE id=$1`, appID).Scan(&title, &creator)
	if errors.Is(err, pgx.ErrNoRows) {
		return counts, ErrAppNotFound
	}
	if err != nil {
		return counts, fmt.Errorf("backup: app lookup: %w", err)
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return counts, fmt.Errorf("backup: acquire conn: %w", err)
	}
	defer conn.Release()

	hw := hashWriter{w: w, h: sha256.New()}
	header := map[string]any{
		"kind":       "header",
		"format":     dumpFormat,
		"version":    dumpVer,
		"app_id":     platform.UUIDToStr(appID),
		"title":      title,
		"creator_id": platform.UUIDToStr(creator),
	}
	hb, err := json.Marshal(header)
	if err != nil {
		return counts, err
	}
	if err := hw.writeLine(string(hb)); err != nil {
		return counts, err
	}

	rw := &recordWriter{hw: hw, skip: opts.FromOffset}

	const attrSQL = `SELECT json_build_object(
		'kind', 'attr',
		'id', id, 'etype', etype, 'label', label,
		'reverse_etype', reverse_etype, 'reverse_label', reverse_label,
		'value_type', value_type, 'cardinality', cardinality,
		'is_unique', is_unique, 'is_indexed', is_indexed,
		'forward_ident', forward_ident, 'reverse_ident', reverse_ident,
		'checked_data_type', checked_data_type::text,
		'checking_data_type', checking_data_type,
		'deletion_marked_at', to_json(deletion_marked_at))::text
	FROM attrs WHERE app_id=$1 ORDER BY id`
	if counts.Attrs, err = streamSection(ctx, conn, rw, attrSQL, appID); err != nil {
		return counts, fmt.Errorf("backup: export attrs: %w", err)
	}

	const tripleSQL = `SELECT json_build_object(
		'kind', 'triple',
		'entity_id', entity_id, 'attr_id', attr_id,
		'value', value,
		'created_at', to_json(created_at))::text
	FROM triples WHERE app_id=$1 ORDER BY entity_id, attr_id, value_md5`
	if counts.Triples, err = streamSection(ctx, conn, rw, tripleSQL, appID); err != nil {
		return counts, fmt.Errorf("backup: export triples: %w", err)
	}

	const ruleSQL = `SELECT json_build_object(
		'kind', 'rule', 'code', code, 'version', version)::text
	FROM rules WHERE app_id=$1`
	if counts.Rules, err = streamSection(ctx, conn, rw, ruleSQL, appID); err != nil {
		return counts, fmt.Errorf("backup: export rules: %w", err)
	}

	const txSQL = `SELECT json_build_object(
		'kind', 'transaction', 'id', id, 'created_at', to_json(created_at))::text
	FROM transactions WHERE app_id=$1 ORDER BY id`
	if counts.Transactions, err = streamSection(ctx, conn, rw, txSQL, appID); err != nil {
		return counts, fmt.Errorf("backup: export transactions: %w", err)
	}

	sum := hw.h.Sum(nil)
	total := counts.Attrs + counts.Triples + counts.Rules + counts.Transactions
	trailer := fmt.Sprintf(`{"kind":"checksum","sha256":"%x","records":%d}`, sum, total)
	if _, err := io.WriteString(w, trailer+"\n"); err != nil {
		return counts, err
	}
	return counts, nil
}

func streamSection(ctx context.Context, conn *pgxpool.Conn, rw *recordWriter, sqlStr string, appID [16]byte) (int64, error) {
	rows, err := conn.Query(ctx, sqlStr, appID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return 0, err
		}
		if err := rw.emit(line); err != nil {
			return 0, err
		}
	}
	return rw.count, rows.Err()
}

// ---- Import ----

var errBadHeader = errors.New("backup: invalid dump header")

// Import consumes a v2 NDJSON dump from r into the database inside a single
// transaction. It is idempotent: re-importing a dump over a DB already
// holding its rows is a no-op (uuid-PK upserts / ON CONFLICT DO NOTHING).
// The trailing checksum is verified against everything consumed before it;
// any mismatch aborts the transaction. Unknown apps are created on the fly
// (with a synthetic creator user when needed), so a fresh schema accepts a
// dump directly.
func Import(ctx context.Context, pool *pgxpool.Pool, r io.Reader) (Counts, error) {
	var counts Counts
	tx, err := pool.Begin(ctx)
	if err != nil {
		return counts, fmt.Errorf("backup: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	sc := newLineScanner(r)
	hash := sha256.New()

	if !sc.Next() {
		return counts, sc.err(errBadHeader)
	}
	line := sc.line()
	hash.Write([]byte(line + "\n"))
	var hdr struct {
		Kind      string `json:"kind"`
		Format    string `json:"format"`
		Version   int    `json:"version"`
		AppID     string `json:"app_id"`
		CreatorID string `json:"creator_id"`
		Title     string `json:"title"`
	}
	if err := json.Unmarshal([]byte(line), &hdr); err != nil {
		return counts, fmt.Errorf("%w: %v", errBadHeader, err)
	}
	if hdr.Kind != "header" || hdr.Format != dumpFormat || hdr.Version != dumpVer || hdr.AppID == "" {
		return counts, fmt.Errorf("%w: kind=%q format=%q version=%d", errBadHeader, hdr.Kind, hdr.Format, hdr.Version)
	}
	appID, err := platform.ScanUUIDErr(hdr.AppID)
	if err != nil {
		return counts, fmt.Errorf("%w: bad app_id: %v", errBadHeader, err)
	}
	creator := appID // deterministic fallback when header lacks creator_id
	if hdr.CreatorID != "" {
		if c, cerr := platform.ScanUUIDErr(hdr.CreatorID); cerr == nil {
			creator = c
		}
	}
	if err := ensureAppRow(ctx, tx, appID, creator, hdr.Title); err != nil {
		return counts, err
	}

	batch := newTripleBatch()
	flushCounts := func() error { return batch.flush(ctx, tx, appID, &counts.Triples) }

	for sc.Next() {
		line := sc.line()
		kind, body, err := decodeKinded(line)
		if err != nil {
			return counts, err
		}
		if kind == "checksum" {
			var ck struct {
				SHA256  string `json:"sha256"`
				Records int64  `json:"records"`
			}
			if err := json.Unmarshal(body, &ck); err != nil {
				return counts, fmt.Errorf("backup: bad checksum line: %v", err)
			}
			logical := counts.Attrs + counts.Triples + counts.Rules + counts.Transactions
			if got := fmt.Sprintf("%x", hash.Sum(nil)); got != ck.SHA256 {
				return counts, fmt.Errorf("backup: checksum mismatch: expected %s, computed %s", ck.SHA256, got)
			}
			if logical != ck.Records {
				return counts, fmt.Errorf("backup: checksum mismatch: expected %d records, saw %d", ck.Records, logical)
			}
			if err := finishImport(ctx, tx, flushCounts, &counts); err != nil {
				return counts, err
			}
			if sc.Next() {
				return counts, fmt.Errorf("backup: unexpected content after checksum line")
			}
			if err := tx.Commit(ctx); err != nil {
				return counts, fmt.Errorf("backup: commit: %w", err)
			}
			return counts, nil
		}

		hash.Write([]byte(line + "\n"))
		switch kind {
		case "attr":
			if err := importAttr(ctx, tx, appID, body); err != nil {
				return counts, fmt.Errorf("backup: import attr: %w", err)
			}
			counts.Attrs++
		case "triple":
			if err := batch.add(body); err != nil {
				return counts, fmt.Errorf("backup: import triple: %w", err)
			}
			if batch.len() >= tripleBatchSize {
				if err := flushCounts(); err != nil {
					return counts, err
				}
			}
		case "rule":
			if err := importRule(ctx, tx, appID, body); err != nil {
				return counts, fmt.Errorf("backup: import rule: %w", err)
			}
			counts.Rules++
		case "transaction":
			if err := importTransaction(ctx, tx, appID, body); err != nil {
				return counts, fmt.Errorf("backup: import transaction: %w", err)
			}
			counts.Transactions++
		default:
			return counts, fmt.Errorf("backup: unknown record kind %q", kind)
		}
	}
	if e := sc.err(nil); e != nil {
		return counts, e
	}
	return counts, errors.New("backup: dump ended without checksum line")
}

func finishImport(ctx context.Context, tx pgx.Tx, flush func() error, counts *Counts) error {
	if err := flush(); err != nil {
		return err
	}
	// Preserve imported journal ids above the identity sequence.
	if _, err := tx.Exec(ctx, `
		SELECT setval(pg_get_serial_sequence('transactions','id'),
		              GREATEST((SELECT COALESCE(MAX(id),0) FROM transactions), 1))`); err != nil {
		return fmt.Errorf("backup: reset transactions sequence: %w", err)
	}
	return nil
}

// lineScanner wraps bufio.Scanner with generous caps (values can be large).
type lineScanner struct {
	sc *bufio.Scanner
	l  string
}

func newLineScanner(r io.Reader) *lineScanner {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 64<<20)
	return &lineScanner{sc: sc}
}

func (l *lineScanner) Next() bool {
	if !l.sc.Scan() {
		return false
	}
	l.l = l.sc.Text()
	return true
}

func (l *lineScanner) line() string { return l.l }

func (l *lineScanner) err(def error) error {
	if e := l.sc.Err(); e != nil {
		return fmt.Errorf("backup: read dump: %v", e)
	}
	return def
}

// decodeKinded parses one record line into kind + raw body.
func decodeKinded(line string) (string, json.RawMessage, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		return "", nil, fmt.Errorf("backup: malformed record line: %v", err)
	}
	kb, ok := m["kind"]
	if !ok {
		return "", nil, errors.New(`backup: record missing "kind"`)
	}
	var kind string
	if err := json.Unmarshal(kb, &kind); err != nil {
		return "", nil, fmt.Errorf("backup: bad kind field: %v", err)
	}
	body, err := json.Marshal(m)
	if err != nil {
		return "", nil, err
	}
	return kind, body, nil
}

// ensureAppRow creates the placeholder creator user (if any) and upserts the
// apps row — the FK anchor every other imported row needs.
func ensureAppRow(ctx context.Context, tx pgx.Tx, appID, creator [16]byte, title string) error {
	email := "restore+" + platform.UUIDToStr(creator) + "@backup.local"
	if _, err := tx.Exec(ctx, `
		INSERT INTO instant_users (id, email) VALUES ($1, $2)
		ON CONFLICT (id) DO NOTHING`, creator, email); err != nil {
		return fmt.Errorf("backup: ensure creator user: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO apps (id, creator_id, title) VALUES ($1,$2,$3)
		ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title`,
		appID, creator, title); err != nil {
		return fmt.Errorf("backup: upsert app row: %w", err)
	}
	return nil
}

func importAttr(ctx context.Context, tx pgx.Tx, appID [16]byte, body json.RawMessage) error {
	var a struct {
		ID               string  `json:"id"`
		Etype            *string `json:"etype"`
		Label            *string `json:"label"`
		ReverseEtype     *string `json:"reverse_etype"`
		ReverseLabel     *string `json:"reverse_label"`
		ValueType        string  `json:"value_type"`
		Cardinality      string  `json:"cardinality"`
		IsUnique         bool    `json:"is_unique"`
		IsIndexed        bool    `json:"is_indexed"`
		ForwardIdent     string  `json:"forward_ident"`
		ReverseIdent     *string `json:"reverse_ident"`
		CheckedDataType  *string `json:"checked_data_type"`
		CheckingDataType *bool   `json:"checking_data_type"`
		DeletionMarkedAt *string `json:"deletion_marked_at"`
	}
	if err := strictUnmarshal(body, &a); err != nil {
		return err
	}
	id, err := platform.ScanUUIDErr(a.ID)
	if err != nil {
		return fmt.Errorf("bad attr id %q: %v", a.ID, err)
	}
	fwd, err := platform.ScanUUIDErr(a.ForwardIdent)
	if err != nil {
		return fmt.Errorf("bad forward_ident %q: %v", a.ForwardIdent, err)
	}
	var rev *[16]byte
	if a.ReverseIdent != nil {
		u, uerr := platform.ScanUUIDErr(*a.ReverseIdent)
		if uerr != nil {
			return fmt.Errorf("bad reverse_ident %q: %v", *a.ReverseIdent, uerr)
		}
		rev = &u
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO attrs (id, app_id, etype, label, reverse_etype, reverse_label,
		                   value_type, cardinality, is_unique, is_indexed,
		                   forward_ident, reverse_ident,
		                   checked_data_type, checking_data_type, deletion_marked_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
		ON CONFLICT (id) DO UPDATE SET
			etype=EXCLUDED.etype, label=EXCLUDED.label,
			reverse_etype=EXCLUDED.reverse_etype, reverse_label=EXCLUDED.reverse_label,
			value_type=EXCLUDED.value_type, cardinality=EXCLUDED.cardinality,
			is_unique=EXCLUDED.is_unique, is_indexed=EXCLUDED.is_indexed,
			forward_ident=EXCLUDED.forward_ident, reverse_ident=EXCLUDED.reverse_ident,
			checked_data_type=EXCLUDED.checked_data_type,
			checking_data_type=EXCLUDED.checking_data_type,
			deletion_marked_at=EXCLUDED.deletion_marked_at`,
		id, appID, a.Etype, a.Label, a.ReverseEtype, a.ReverseLabel,
		a.ValueType, a.Cardinality, a.IsUnique, a.IsIndexed,
		fwd, rev, a.CheckedDataType, a.CheckingDataType, a.DeletionMarkedAt); err != nil {
		return err
	}
	// Mirror the idents row like platform.GetOrCreateAttr does on creation.
	if a.Etype != nil && a.Label != nil {
		if _, err := tx.Exec(ctx, `
			INSERT INTO idents (id, app_id, attr_id, etype, label)
			VALUES ($1,$2,$3,$4,$5)
			ON CONFLICT DO NOTHING`, fwd, appID, id, *a.Etype, *a.Label); err != nil {
			return err
		}
	}
	return nil
}

func importRule(ctx context.Context, tx pgx.Tx, appID [16]byte, body json.RawMessage) error {
	var ru struct {
		Code    json.RawMessage `json:"code"`
		Version *int            `json:"version"`
	}
	if err := strictUnmarshal(body, &ru); err != nil {
		return err
	}
	if len(ru.Code) == 0 {
		return errors.New("rule missing code")
	}
	version := 0
	if ru.Version != nil {
		version = *ru.Version
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO rules (app_id, code, version) VALUES ($1,$2,$3)
		ON CONFLICT (app_id) DO UPDATE SET code=EXCLUDED.code, version=EXCLUDED.version`,
		appID, []byte(ru.Code), version)
	return err
}

func importTransaction(ctx context.Context, tx pgx.Tx, appID [16]byte, body json.RawMessage) error {
	var tr struct {
		ID        int64   `json:"id"`
		CreatedAt *string `json:"created_at"`
	}
	if err := strictUnmarshal(body, &tr); err != nil {
		return err
	}
	if tr.CreatedAt == nil {
		_, err := tx.Exec(ctx, `
			INSERT INTO transactions (id, app_id) OVERRIDING SYSTEM VALUE VALUES ($1,$2)
			ON CONFLICT (id) DO NOTHING`, tr.ID, appID)
		return err
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO transactions (id, app_id, created_at) OVERRIDING SYSTEM VALUE VALUES ($1,$2,$3)
		ON CONFLICT (id) DO NOTHING`, tr.ID, appID, *tr.CreatedAt)
	return err
}

// ---- Triple batching (shared by v2 Import and the v1 zip importer) ----

// tripleBatch accumulates decoded triples and flushes them in one statement
// whose flags/md5 are derived by Postgres exactly like storage.InsertTriples'
// enhanced-triples CTE. JSON-null values store as SQL NULL with JSONNullMD5.
type tripleBatch struct {
	entityIDs []string
	attrIDs   []string
	values    []any // string(raw json) | nil
	createdAt []any // time.Time | nil
}

func newTripleBatch() *tripleBatch { return &tripleBatch{} }

func (b *tripleBatch) len() int { return len(b.entityIDs) }

func (b *tripleBatch) addTriple(entityID, attrID string, value json.RawMessage, createdAt time.Time, hasCreated bool) error {
	e, err := platform.ScanUUIDErr(entityID)
	if err != nil {
		return fmt.Errorf("bad entity_id %q: %v", entityID, err)
	}
	a, err := platform.ScanUUIDErr(attrID)
	if err != nil {
		return fmt.Errorf("bad attr_id %q: %v", attrID, err)
	}
	b.entityIDs = append(b.entityIDs, platform.UUIDToStr(e))
	b.attrIDs = append(b.attrIDs, platform.UUIDToStr(a))
	if len(value) == 0 || string(value) == "null" {
		b.values = append(b.values, nil)
	} else {
		b.values = append(b.values, string(value))
	}
	if hasCreated && !createdAt.IsZero() {
		b.createdAt = append(b.createdAt, createdAt)
	} else {
		b.createdAt = append(b.createdAt, nil)
	}
	return nil
}

func (b *tripleBatch) add(body json.RawMessage) error {
	var t struct {
		EntityID  string          `json:"entity_id"`
		AttrID    string          `json:"attr_id"`
		Value     json.RawMessage `json:"value"`
		CreatedAt string          `json:"created_at"`
	}
	if err := strictUnmarshal(body, &t); err != nil {
		return err
	}
	var ts time.Time
	if t.CreatedAt != "" {
		parsed, err := parsePGTime(t.CreatedAt)
		if err != nil {
			return fmt.Errorf("bad created_at %q: %v", t.CreatedAt, err)
		}
		ts = parsed
	}
	return b.addTriple(t.EntityID, t.AttrID, t.Value, ts, t.CreatedAt != "")
}

func (b *tripleBatch) reset() {
	b.entityIDs = b.entityIDs[:0]
	b.attrIDs = b.attrIDs[:0]
	b.values = b.values[:0]
	b.createdAt = b.createdAt[:0]
}

func (b *tripleBatch) flush(ctx context.Context, tx pgx.Tx, appID [16]byte, counter *int64) error {
	if b.len() == 0 {
		return nil
	}
	appStr := platform.UUIDToStr(appID)
	// Reject triples referencing unknown/deleted attrs instead of silently
	// dropping them in the join below.
	var dangling int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM unnest($1::uuid[]) AS u(aid)
		WHERE NOT EXISTS (
			SELECT 1 FROM attrs WHERE id = u.aid AND deletion_marked_at IS NULL)`,
		b.attrIDs).Scan(&dangling); err != nil {
		return err
	}
	if dangling > 0 {
		return fmt.Errorf("%d triple(s) reference unknown attrs (import a matching attr set first)", dangling)
	}
	ct, err := tx.Exec(ctx, `
		WITH input AS (
			SELECT * FROM unnest($2::uuid[], $3::uuid[], $4::jsonb[], $5::timestamptz[])
			WITH ORDINALITY AS t(entity_id, attr_id, value, created_at, ord)
		),
		enhanced AS (
			SELECT i.entity_id, i.attr_id,
			       CASE WHEN i.value IS NULL THEN NULL ELSE i.value END AS value,
			       COALESCE(md5(i.value::text), '`+jsonNullMD5+`')       AS value_md5,
			       (a.cardinality = 'one')                              AS ea,
			       (a.value_type  = 'ref')                              AS eav,
			       a.is_unique                                          AS av,
			       a.is_indexed                                         AS ave,
			       (a.value_type  = 'ref')                              AS vae,
			       a.checked_data_type                                  AS checked_data_type
			  FROM input i
			  JOIN attrs a ON a.id = i.attr_id AND a.deletion_marked_at IS NULL
		)
		INSERT INTO triples (app_id, entity_id, attr_id, value, value_md5,
		                     ea, eav, av, ave, vae, checked_data_type, created_at)
		SELECT $1, entity_id, attr_id, value, value_md5,
		       ea, eav, av, ave, vae, checked_data_type, COALESCE(created_at, NOW())
		FROM enhanced
		ON CONFLICT DO NOTHING`,
		appStr, b.entityIDs, b.attrIDs, b.values, b.createdAt)
	if err != nil {
		return err
	}
	*counter += ct.RowsAffected()
	b.reset()
	return nil
}

// ---- shared helpers ----

func strictUnmarshal(body json.RawMessage, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("malformed record: %v", err)
	}
	return nil
}

// parsePGTime parses Postgres to_json(timestamptz) output (RFC3339-ish ISO).
func parsePGTime(s string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.999999999Z07:00"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unparseable timestamp")
}
