// Package storage owns all Postgres access for triples: the two-path upsert
// (cardinality-one overwrite vs many-cardinality dedup), pattern fetch,
// delete-by-triple, and the per-app transaction journal. Port of v1's
// db/model/triple.clj insert-multi!/delete-multi!/fetch + jdbc/sql layer.
//
// Invariants (docs/02-architecture.md §5):
//   - value_md5 is computed by Postgres (md5(value::text)) — never client-side.
//   - JSON-null values are stored as SQL NULL with triple.JSONNullMD5.
package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/triple"
)

// DB wraps a pgx pool.
type DB struct {
	Pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *DB { return &DB{Pool: pool} }

// ErrUnknownAttr mirrors v1's hsql-attr-id-or-raise behaviour.
var ErrUnknownAttr = errors.New("storage: unknown attr id")

// InsertResult summarizes one InsertTriples call.
type InsertResult struct {
	Upserted int64 // cardinality-one rows written (overwrite path)
	Inserted int64 // remaining rows newly inserted
}

// enhancedRowsCTE mirrors v1's enhanced-triples CTE: join attrs for flag
// derivation and let PG compute md5 over the jsonb text. JSON null arrives as
// jsonb 'null'; md5('null'::text) == triple.JSONNullMD5, so one expression
// covers every case.
const enhancedRowsCTE = `
	WITH input(app_id, entity_id, attr_id, value, idx) AS (
		SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::jsonb[], $5::int[])
	),
	enhanced AS (
		SELECT i.app_id, i.entity_id, i.attr_id, i.value, i.idx,
		       md5(i.value::text)                 AS value_md5,
		       (a.cardinality = 'one')            AS ea,
		       (a.value_type  = 'ref')            AS eav,
		       a.is_unique                        AS av,
		       a.is_indexed                       AS ave,
		       (a.value_type  = 'ref')            AS vae,
		       a.checked_data_type                AS checked_data_type
		  FROM input i
		  JOIN attrs a ON a.id = i.attr_id AND a.deletion_marked_at IS NULL
	)`

// ErrUniqueViolation wraps Postgres 23505 raised by triple writes — e.g. a
// second entity claiming the same value on an is_unique attr via the
// av_ignore_nulls_index expression index. Callers surface it as a clean 4xx
// ("unique constraint violated") instead of raw PG boilerplate; match with
// errors.Is.
var ErrUniqueViolation = errors.New("unique constraint violated")

func wrapUnique(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		// Chain the ORIGINAL error too: callers map violations by
		// errors.As(*pgconn.PgError) (storageapi 409 path), which would
		// break if only the sentinel were wrapped.
		return fmt.Errorf("%w (%s) [%w]", ErrUniqueViolation, pgErr.ConstraintName, err)
	}
	return err
}

const insertCols = `app_id, entity_id, attr_id, value, value_md5,
	ea, eav, av, ave, vae, checked_data_type`

// InsertTriples writes triples in one transaction:
//
//   - cardinality-one attrs (ea=true): DISTINCT ON (entity_id, attr_id) keeping
//     the LAST occurrence in slice order, then upsert-on-(app,e,a)-where-ea that
//     overwrites the stored value (v1 ea-conflict semantics).
//   - everything else: INSERT ... ON CONFLICT (app,e,a,value_md5) DO NOTHING
//     (refs and many-cardinality values accumulate; duplicates no-op).
//
// Unknown attr ids abort the whole call with ErrUnknownAttr, mirroring
// hsql-attr-id-or-raise. Lookup-ref resolution and indexed-null synthesis are
// Phase 2 transactor concerns and intentionally out of scope here.
func (d *DB) InsertTriples(ctx context.Context, appID [16]byte, cat *platform.AttrCatalog, ts []triple.Triple, overwriteT bool) (InsertResult, error) {
	if len(ts) == 0 {
		return InsertResult{}, nil
	}
	var res InsertResult
	err := d.WithTx(ctx, func(tx pgx.Tx) error {
		r, err := insertBatch(ctx, tx, appID, cat, ts, overwriteT)
		if err != nil {
			return err
		}
		res = r
		return nil
	})
	return res, err
}

func insertBatch(ctx context.Context, tx pgx.Tx, appID [16]byte, cat *platform.AttrCatalog, ts []triple.Triple, overwriteT bool) (InsertResult, error) {
	for _, t := range ts {
		if _, ok := cat.ByID(t.A); !ok {
			return InsertResult{}, fmt.Errorf("%w: %x", ErrUnknownAttr, t.A)
		}
	}
	apps := make([][16]byte, 0, len(ts))
	ents := make([][16]byte, 0, len(ts))
	attrs := make([][16]byte, 0, len(ts))
	idxs := make([]int, 0, len(ts))
	vs := make([]any, len(ts))
	for i, t := range ts {
		apps = append(apps, appID)
		ents = append(ents, t.E)
		attrs = append(attrs, t.A)
		vs[i] = t.V
		idxs = append(idxs, i)
	}
	// Batched value encoding: one shared encoder/buffer for the whole
	// batch instead of a fresh Buffer+Encoder per triple (audit backlog).
	// encodeOrNull semantics fold in: nil → JSON 'null' literal.
	vals, err := triple.EncodeValues(vs)
	if err != nil {
		return InsertResult{}, err
	}

	var res InsertResult

	setClause := "SET value = EXCLUDED.value, value_md5 = EXCLUDED.value_md5"
	if overwriteT {
		setClause += ", created_at = now()"
	}

	// --- cardinality-one overwrite path -------------------------------------
	eaSQL := enhancedRowsCTE + `,
	ea_distinct AS (
		SELECT DISTINCT ON (entity_id, attr_id) *
		  FROM enhanced WHERE ea ORDER BY entity_id, attr_id, idx DESC
	)
	INSERT INTO triples (` + insertCols + `)
	SELECT ` + insertCols + ` FROM ea_distinct
ON CONFLICT (app_id, entity_id, attr_id) WHERE ea DO UPDATE ` + setClause

	eaTag, err := tx.Exec(ctx, eaSQL, apps, ents, attrs, vals, idxs)
	if err != nil {
		return res, fmt.Errorf("ea upsert: %w", wrapUnique(err))
	}
	res.Upserted = eaTag.RowsAffected()

	// --- remaining (refs / many-cardinality) path ----------------------------
	const remSQL = enhancedRowsCTE + `
	INSERT INTO triples (app_id, entity_id, attr_id, value, value_md5,
	                     ea, eav, av, ave, vae, checked_data_type)
	SELECT app_id, entity_id, attr_id, value, value_md5,
	       ea, eav, av, ave, vae, checked_data_type
	  FROM enhanced WHERE NOT ea
	ON CONFLICT (app_id, entity_id, attr_id, value_md5) DO NOTHING`

	remTag, err := tx.Exec(ctx, remSQL, apps, ents, attrs, vals, idxs)
	if err != nil {
		return res, fmt.Errorf("remaining insert: %w", wrapUnique(err))
	}
	res.Inserted = remTag.RowsAffected()
	return res, nil
}

// Enhanced is a fetched triple plus its storage metadata.
type Enhanced struct {
	Triple          triple.Triple
	MD5             string
	Flags           platform.Flags
	CheckedDataType *string
}

// FetchFilter constrains a fetch; zero fields mean unconstrained. At least one
// must be set to avoid full-table scans.
type FetchFilter struct {
	EntityIDs [][16]byte
	AttrIDs   [][16]byte
	Value     any // exact-match on canonical JSON encoding
}

func (f FetchFilter) empty() bool {
	return len(f.EntityIDs) == 0 && len(f.AttrIDs) == 0 && f.Value == nil
}

// FetchTx fetches within a transaction/connection.
func FetchTx(ctx context.Context, tx pgx.Tx, appID [16]byte, f FetchFilter) ([]Enhanced, error) {
	return fetch(ctx, tx, appID, f)
}

// EAPair is one exact (entity, attr) probe for pair-scoped reads.
type EAPair struct {
	E [16]byte
	A [16]byte
}

// FetchPairs fetches triples for exact (entity, attr) pairs in ONE query —
// the batched deep-merge probe (audit backlog). FetchFilter's EntityIDs ×
// AttrIDs is a cross-product and cannot express per-row pairs; the unnest
// join here binds entity and attr together. Multiple stored rows per pair
// (many-cardinality attrs) all come back — callers pick the first, exactly
// like the per-step fetch they replaced.
func FetchPairs(ctx context.Context, tx pgx.Tx, appID [16]byte, pairs []EAPair) ([]Enhanced, error) {
	if len(pairs) == 0 {
		return nil, nil
	}
	ents := make([][16]byte, len(pairs))
	attrs := make([][16]byte, len(pairs))
	for i, p := range pairs {
		ents[i] = p.E
		attrs[i] = p.A
	}
	rows, err := tx.Query(ctx, `
		SELECT t.entity_id, t.attr_id, t.value, t.value_md5,
		       t.ea, t.eav, t.av, t.ave, t.vae, t.checked_data_type
		  FROM triples t
		  JOIN attrs a ON a.id = t.attr_id AND a.deletion_marked_at IS NULL
		  JOIN unnest($2::uuid[], $3::uuid[]) AS p(entity_id, attr_id)
		    ON t.entity_id = p.entity_id AND t.attr_id = p.attr_id
		 WHERE t.app_id = $1`, appID, ents, attrs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Enhanced
	for rows.Next() {
		var e Enhanced
		var val []byte
		if err := rows.Scan(&e.Triple.E, &e.Triple.A, &val, &e.MD5,
			&e.Flags.EA, &e.Flags.EAV, &e.Flags.AV, &e.Flags.AVE, &e.Flags.VAE,
			&e.CheckedDataType); err != nil {
			return nil, err
		}
		v, err := decodeValue(val, e.Flags.EAV)
		if err != nil {
			return nil, err
		}
		e.Triple.V = v
		out = append(out, e)
	}
	return out, rows.Err()
}

// FetchTriples reads triples for appID joined against non-deleted attrs,
// mirroring v1 db.model.triple/fetch.
func (d *DB) FetchTriples(ctx context.Context, appID [16]byte, f FetchFilter) ([]Enhanced, error) {
	return fetch(ctx, d.Pool, appID, f)
}

func fetch(ctx context.Context, q platform.Queryer, appID [16]byte, f FetchFilter) ([]Enhanced, error) {
	if f.empty() {
		return nil, errors.New("storage: fetch filter must constrain at least one dimension")
	}
	sqlStr := `
		SELECT t.entity_id, t.attr_id, t.value, t.value_md5,
		       t.ea, t.eav, t.av, t.ave, t.vae, t.checked_data_type
		  FROM triples t
		  JOIN attrs a ON a.id = t.attr_id AND a.deletion_marked_at IS NULL
		 WHERE t.app_id = $1`
	args := []any{appID}
	n := 2
	if len(f.EntityIDs) > 0 {
		sqlStr += fmt.Sprintf(` AND t.entity_id = ANY($%d::uuid[])`, n)
		args = append(args, f.EntityIDs)
		n++
	}
	if len(f.AttrIDs) > 0 {
		sqlStr += fmt.Sprintf(` AND t.attr_id = ANY($%d::uuid[])`, n)
		args = append(args, f.AttrIDs)
		n++
	}
	if f.Value != nil {
		enc, err := triple.EncodeValue(f.Value)
		if err != nil {
			return nil, err
		}
		sqlStr += fmt.Sprintf(` AND t.value = $%d::jsonb`, n)
		args = append(args, string(enc))
	}
	rows, err := q.Query(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Enhanced
	for rows.Next() {
		var e Enhanced
		var val []byte
		if err := rows.Scan(&e.Triple.E, &e.Triple.A, &val, &e.MD5,
			&e.Flags.EA, &e.Flags.EAV, &e.Flags.AV, &e.Flags.AVE, &e.Flags.VAE,
			&e.CheckedDataType); err != nil {
			return nil, err
		}
		v, err := decodeValue(val, e.Flags.EAV)
		if err != nil {
			return nil, err
		}
		e.Triple.V = v
		out = append(out, e)
	}
	return out, rows.Err()
}

// decodeValue converts stored jsonb back into the Go value; refs surface as
// uuid strings (parity with v1 row->enhanced-triple UUID/fromString when eav).
func decodeValue(raw []byte, isRef bool) (any, error) {
	if raw == nil {
		return nil, nil
	}
	var v any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	_ = isRef
	return v, nil
}

// DeleteTriples removes exact (e,a,value) matches using SQL-computed md5 —
// port of v1 delete-multi! without lookup-ref expansion (Phase 2 concern).
// Runs on the pool (autocommit): only for standalone cleanups that are not
// part of a larger transaction. The transact pipeline must use DeleteTx.
func (d *DB) DeleteTriples(ctx context.Context, appID [16]byte, ts []triple.Triple) (int64, error) {
	return deleteTriplesExec(ctx, d.Pool, appID, ts)
}

// DeleteTx is DeleteTriples scoped to a caller-owned transaction so retract
// effects commit or roll back together with the rest of the Transact batch
// ("journal first" invariant: every visible effect has its transactions row).
func (d *DB) DeleteTx(ctx context.Context, tx pgx.Tx, appID [16]byte, ts []triple.Triple) (int64, error) {
	return deleteTriplesExec(ctx, tx, appID, ts)
}

// deleteExecutor is satisfied by both pgx.Tx and *pgxpool.Pool.
type deleteExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// deleteTriplesExec deletes exactly one row per supplied (e,a,value) tuple.
// Dimensions are correlated PER ROW — never as set-membership across the
// batch. A batch {(e1,a1,v1),(e2,a2,v2)} must not touch a pre-existing
// (e1,a1,v2): entity/attr/value_md5 all bind to the same input row, unlike a
// cross-product of IN-lists. Duplicate tuples delete the row once.
//
// Matching is EXACT-md5 (v1 delete-multi! parity): the md5 binds to the
// canonical jsonb text of the stored value, so a value written with a
// different numeric scale (1 vs 1.0) has a different fingerprint and a
// retract of one form will not match the other. Writes share this pipeline;
// only externally-restored rows can diverge.
func deleteTriplesExec(ctx context.Context, exec deleteExecutor, appID [16]byte, ts []triple.Triple) (int64, error) {
	if len(ts) == 0 {
		return 0, nil
	}
	ents := make([][16]byte, len(ts))
	attrs := make([][16]byte, len(ts))
	vs := make([]any, len(ts))
	for i, t := range ts {
		ents[i] = t.E
		attrs[i] = t.A
		vs[i] = t.V
	}
	// Batched value encoding (audit backlog): shared encoder, exact
	// encodeOrNull parity (nil → JSON 'null' literal).
	vals, err := triple.EncodeValues(vs)
	if err != nil {
		return 0, err
	}
	tag, err := exec.Exec(ctx, `
		WITH input(entity_id, attr_id, value) AS (
			SELECT * FROM unnest($2::uuid[], $3::uuid[], $4::jsonb[])
		)
		DELETE FROM triples t
		 USING input i
		 WHERE t.app_id = $1
		   AND t.entity_id = i.entity_id
		   AND t.attr_id = i.attr_id
		   AND t.value_md5 = md5(i.value::text)`,
		appID, ents, attrs, vals)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// ApplyStatementLimits installs per-connection Postgres ceilings on a pool
// config via RuntimeParams: one pathological query must not pin a pooled
// connection forever (writer budget is shared by transacts and refreshes).
// Zero-valued durations are skipped, leaving the server default. Migrations
// run on their own database/sql connection and are intentionally exempt.
func ApplyStatementLimits(cfg *pgxpool.Config, statementTimeout, lockTimeout, idleInTxTimeout time.Duration) {
	if cfg == nil || cfg.ConnConfig == nil {
		return
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	rp := cfg.ConnConfig.RuntimeParams
	if statementTimeout > 0 {
		rp["statement_timeout"] = statementTimeout.String()
	}
	if lockTimeout > 0 {
		rp["lock_timeout"] = lockTimeout.String()
	}
	if idleInTxTimeout > 0 {
		rp["idle_in_transaction_session_timeout"] = idleInTxTimeout.String()
	}
}

// RecordTransaction appends a journal row and returns its identity id — the
// per-app monotonic sequence later phases expose as tx-id/isn.
func RecordTransaction(ctx context.Context, tx pgx.Tx, appID [16]byte) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx,
		`INSERT INTO transactions (app_id) VALUES ($1) RETURNING id`, appID).Scan(&id)
	return id, err
}

// WithTx runs fn inside one transaction.
func (d *DB) WithTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// SetTx inserts triples inside an existing transaction with the supplied
// catalog, preserving the single-tx invariant the transact pipeline needs.
func (d *DB) SetTx(ctx context.Context, tx pgx.Tx, appID [16]byte, cat *platform.AttrCatalog, ts []triple.Triple, overwriteT bool) error {
	_, err := insertBatch(ctx, tx, appID, cat, ts, overwriteT)
	return err
}
