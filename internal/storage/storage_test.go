package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/triple"
)

// testDB resets the schema and applies migrations. Set DATABASE_URL to enable;
// CI provides it via testcontainers (docs/05-conformance.md). Locally:
//
//	DATABASE_URL='postgres://instant@localhost:54329/instant_v2_test?sslmode=disable' \
//	  go test ./internal/storage -count=1
func testDB(t *testing.T) *DB {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping live-PG storage tests")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if _, err := pool.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public;`); err != nil {
		t.Fatalf("reset schema: %v", err)
	}
	sqlDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer sqlDB.Close()
	if err := platform.Migrate(ctx, sqlDB); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return New(pool)
}

func seedCatalog(t *testing.T, db *DB) ([16]byte, *platform.AttrCatalog, attrIDs) {
	t.Helper()
	ctx := context.Background()
	appID := rand16()
	creator := rand16()
	err := db.WithTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO instant_users (id, email) VALUES ($1,$2)`,
			creator, fmt.Sprintf("%x@test.local", creator[:4])); err != nil {
			return err
		}
		return platform.CreateApp(ctx, tx, creator, appID, "test-app")
	})
	if err != nil {
		t.Fatalf("seed app: %v", err)
	}
	var nameAttr, linkAttr, handleAttr platform.Attr
	err = db.WithTx(ctx, func(tx pgx.Tx) error {
		var e1, e2, e3 error
		nameAttr, e1 = platform.GetOrCreateAttr(ctx, tx, appID, "todos", "name", "blob", "one", false, true)
		linkAttr, e2 = platform.GetOrCreateAttr(ctx, tx, appID, "todos", "posts", "ref", "many", false, false)
		handleAttr, e3 = platform.GetOrCreateAttr(ctx, tx, appID, "users", "handle", "blob", "one", true, true)
		return joinErr(e1, e2, e3)
	})
	if err != nil {
		t.Fatalf("seed attrs: %v", err)
	}
	cat, err := platform.LoadAttrCatalog(ctx, db.Pool, appID)
	if err != nil {
		t.Fatalf("LoadAttrCatalog: %v", err)
	}
	return appID, cat, attrIDs{name: nameAttr.ID, link: linkAttr.ID, handle: handleAttr.ID}
}

type attrIDs struct {
	name   [16]byte
	link   [16]byte
	handle [16]byte
}

func TestInsertObjectOverwrite(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seedCatalog(t, db)
	e := rand16()

	res, err := db.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: e, A: ids.name, V: "first"},
	}, false)
	if err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	if res.Upserted != 1 || res.Inserted != 0 {
		t.Fatalf("res1: %+v", res)
	}
	res, err = db.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: e, A: ids.name, V: "second"},
	}, false)
	if err != nil {
		t.Fatalf("insert 2: %v", err)
	}
	if res.Upserted != 1 {
		t.Fatalf("overwrite should upsert 1: %+v", res)
	}

	rows, err := db.FetchTriples(ctx, appID, FetchFilter{EntityIDs: [][16]byte{e}})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want exactly 1 row after overwrite, got %+v", rows)
	}
	if rows[0].Triple.V != "second" {
		t.Fatalf("value %v want second", rows[0].Triple.V)
	}
	// Flag parity: name is blob/one/indexed → ea+ave.
	if !rows[0].Flags.EA || !rows[0].Flags.AVE || rows[0].Flags.EAV || rows[0].Flags.VAE || rows[0].Flags.AV {
		t.Fatalf("flags %+v want ea+ave only", rows[0].Flags)
	}
}

func TestInsertManyCardinalityDedup(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seedCatalog(t, db)
	e, target1, target2 := rand16(), rand16(), rand16()

	res, err := db.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: e, A: ids.link, V: uuidStr(target1)},
		{E: e, A: ids.link, V: uuidStr(target1)}, // duplicate → no-op
		{E: e, A: ids.link, V: uuidStr(target2)},
	}, false)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if res.Inserted != 2 {
		t.Fatalf("Inserted %d want 2 (dup no-op)", res.Inserted)
	}
	if res.Upserted != 0 {
		t.Fatalf("refs never take ea path: %+v", res)
	}
	rows, err := db.FetchTriples(ctx, appID, FetchFilter{EntityIDs: [][16]byte{e}, AttrIDs: [][16]byte{ids.link}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("want 2 link rows, got %d", len(rows))
	}
	for _, r := range rows {
		if !r.Flags.EAV || !r.Flags.VAE || r.Flags.EA {
			t.Fatalf("link flags %+v want eav+vae", r.Flags)
		}
	}
}

func TestUnknownAttrAborts(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seedCatalog(t, db)
	ghost := rand16()
	_, err := db.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: rand16(), A: ids.name, V: "ok"},
		{E: rand16(), A: ghost, V: "bad"},
	}, false)
	if !isUnknownAttr(err) {
		t.Fatalf("want ErrUnknownAttr, got %v", err)
	}
	// Whole batch rolled back: the ok triple must not exist.
	rows, _ := db.FetchTriples(ctx, appID, FetchFilter{AttrIDs: [][16]byte{ids.name}})
	if len(rows) != 0 {
		t.Fatalf("batch not rolled back: %d rows", len(rows))
	}
}

func TestNullValueStoresJSONNullMD5(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seedCatalog(t, db)
	e := rand16()
	if _, err := db.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: e, A: ids.name, V: nil},
	}, false); err != nil {
		t.Fatalf("insert null: %v", err)
	}
	rows, err := db.FetchTriples(ctx, appID, FetchFilter{EntityIDs: [][16]byte{e}})
	if err != nil || len(rows) != 1 {
		t.Fatalf("fetch: %v rows=%d", err, len(rows))
	}
	if rows[0].MD5 != triple.JSONNullMD5 {
		t.Fatalf("md5 %q want JSONNullMD5", rows[0].MD5)
	}
	if rows[0].Triple.V != nil {
		t.Fatalf("value %v want nil", rows[0].Triple.V)
	}
}

func TestRefCheckRejectsNonUUID(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seedCatalog(t, db)
	_, err := db.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: rand16(), A: ids.link, V: "not-a-uuid"},
	}, false)
	if err == nil {
		t.Fatal("ref CHECK should reject non-uuid string")
	}
}

func TestIndexedValueCapEnforced(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seedCatalog(t, db)
	big := make([]byte, 2000)
	for i := range big {
		big[i] = 'x'
	}
	_, err := db.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: rand16(), A: ids.handle, V: string(big)}, // unique+indexed → cap applies
	}, false)
	if err == nil {
		t.Fatal("indexed-value cap should reject >1024 bytes")
	}
}

func TestDeleteExactMatch(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seedCatalog(t, db)
	e, t1, t2 := rand16(), rand16(), rand16()
	if _, err := db.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: e, A: ids.link, V: uuidStr(t1)},
		{E: e, A: ids.link, V: uuidStr(t2)},
	}, false); err != nil {
		t.Fatal(err)
	}
	n, err := db.DeleteTriples(ctx, appID, []triple.Triple{{E: e, A: ids.link, V: uuidStr(t1)}})
	if err != nil || n != 1 {
		t.Fatalf("delete n=%d err=%v want 1", n, err)
	}
	rows, _ := db.FetchTriples(ctx, appID, FetchFilter{EntityIDs: [][16]byte{e}})
	if len(rows) != 1 || rows[0].Triple.V != uuidStr(t2) {
		t.Fatalf("remaining rows wrong: %+v", rows)
	}
}

// TestDeleteExactMatchMultiTuple pins per-row correlation for batched
// deletes: dimensions must never decouple into a cross-product. With rows
// (e1,A,t1),(e1,A,t2),(e2,A,t2) present, deleting [(e1,A,t1),(e2,A,t2)] must
// remove exactly those two tuples and leave (e1,A,t2) intact.
func TestDeleteExactMatchMultiTuple(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seedCatalog(t, db)
	e1, e2, t1, t2 := rand16(), rand16(), rand16(), rand16()
	A := ids.link // ref/many: same attr can hold several values per entity
	if _, err := db.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: e1, A: A, V: uuidStr(t1)},
		{E: e1, A: A, V: uuidStr(t2)}, // survivor under the cross-product bug
		{E: e2, A: A, V: uuidStr(t2)},
	}, false); err != nil {
		t.Fatal(err)
	}

	n, err := db.DeleteTriples(ctx, appID, []triple.Triple{
		{E: e1, A: A, V: uuidStr(t1)},
		{E: e2, A: A, V: uuidStr(t2)},
	})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if n != 2 {
		t.Fatalf("deleted %d rows, want exactly 2 (cross-product over-deletes)", n)
	}
	rows, err := db.FetchTriples(ctx, appID, FetchFilter{EntityIDs: [][16]byte{e1, e2}})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want exactly 1 surviving row, got %+v", rows)
	}
	if rows[0].Triple.E != e1 || rows[0].Triple.A != A || rows[0].Triple.V != uuidStr(t2) {
		t.Fatalf("survivor mismatch: got (%v,%v,%v) want (e1,A,t2)",
			rows[0].Triple.E, rows[0].Triple.A, rows[0].Triple.V)
	}
}

// TestDeleteExactMatchJSONNil pins the null canonicalization on the delete
// path: retracting a nil value must match the stored jsonb 'null' via
// md5('null') and remove exactly that row. (Nulls are only legal on blob
// attrs — refs enforce uuid values via ref_values_are_uuid.)
func TestDeleteExactMatchJSONNil(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seedCatalog(t, db)
	eNil, eReal := rand16(), rand16()
	if _, err := db.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: eNil, A: ids.name, V: nil},
		{E: eReal, A: ids.name, V: "real"},
	}, false); err != nil {
		t.Fatal(err)
	}
	tx := mustTx(t, db, ctx)
	n, err := db.DeleteTx(ctx, tx, appID, []triple.Triple{{E: eNil, A: ids.name, V: nil}})
	if err != nil {
		t.Fatalf("nil delete: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if n != 1 {
		t.Fatalf("nil delete n=%d want 1", n)
	}
	rows, err := db.FetchTriples(ctx, appID, FetchFilter{EntityIDs: [][16]byte{eNil, eReal}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Triple.V != "real" {
		t.Fatalf("want only the real value to survive: %+v", rows)
	}
}

func mustTx(t *testing.T, db *DB, ctx context.Context) pgx.Tx {
	t.Helper()
	tx, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	return tx
}

// TestApplyStatementLimits unit-pins RuntimeParams installation semantics.
func TestApplyStatementLimits(t *testing.T) {
	cfg := &pgxpool.Config{ConnConfig: &pgx.ConnConfig{}}
	ApplyStatementLimits(cfg, 30*time.Second, 5*time.Second, 45*time.Second)
	want := map[string]string{
		"statement_timeout":                   "30s",
		"lock_timeout":                        "5s",
		"idle_in_transaction_session_timeout": "45s",
	}
	rp := cfg.ConnConfig.Config.RuntimeParams
	for k, v := range want {
		if rp[k] != v {
			t.Fatalf("RuntimeParams[%s]=%q want %q", k, rp[k], v)
		}
	}

	// Zero durations must be skipped (server default stands), not set to 0s.
	cfg2 := &pgxpool.Config{ConnConfig: &pgx.ConnConfig{}}
	ApplyStatementLimits(cfg2, 0, 5*time.Second, 0)
	rp2 := cfg2.ConnConfig.Config.RuntimeParams
	if _, ok := rp2["statement_timeout"]; ok {
		t.Fatal("zero statement_timeout must be skipped, not installed")
	}
	if _, ok := rp2["idle_in_transaction_session_timeout"]; ok {
		t.Fatal("zero idle_in_transaction timeout must be skipped")
	}
	if rp2["lock_timeout"] != "5s" {
		t.Fatalf("lock_timeout=%q want 5s", rp2["lock_timeout"])
	}
}

// TestStatementTimeoutEnforcedLive proves the ceiling is real: a pooled
// connection reports the configured statement_timeout and pg_sleep beyond it
// is cancelled by the server.
func TestStatementTimeoutEnforcedLive(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping live statement-timeout probe")
	}
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	ApplyStatementLimits(cfg, 750*time.Millisecond, 5*time.Second, 30*time.Second)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	var got string
	if err := pool.QueryRow(ctx, `SHOW statement_timeout`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "750ms" {
		t.Fatalf("SHOW statement_timeout = %q, want 750ms", got)
	}
	start := time.Now()
	err = pool.QueryRow(ctx, `SELECT pg_sleep(5)`).Scan(&got)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("pg_sleep(5) must fail under a 750ms statement_timeout")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "57014" {
		t.Fatalf("want query_canceled(57014), got %v", err)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("cancellation took %s; ceiling not enforced server-side", elapsed)
	}
}

func TestCopyTriplesBulk(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seedCatalog(t, db)
	e, target := rand16(), rand16()
	n, err := db.CopyTriples(ctx, appID, cat, []triple.Triple{
		{E: e, A: ids.name, V: "copied"},
		{E: e, A: ids.link, V: uuidStr(target)},
	})
	if err != nil {
		t.Fatalf("CopyTriples: %v", err)
	}
	if n != 2 {
		t.Fatalf("n=%d want 2", n)
	}
	rows, _ := db.FetchTriples(ctx, appID, FetchFilter{EntityIDs: [][16]byte{e}})
	if len(rows) != 2 {
		t.Fatalf("rows %d want 2", len(rows))
	}
	// Re-copy same data: ea row overwrites (still 1), ref dedups (still 1).
	n2, err := db.CopyTriples(ctx, appID, cat, []triple.Triple{
		{E: e, A: ids.name, V: "copied"},
		{E: e, A: ids.link, V: uuidStr(target)},
	})
	if err != nil {
		t.Fatalf("re-copy: %v", err)
	}
	_ = n2
	rows2, _ := db.FetchTriples(ctx, appID, FetchFilter{EntityIDs: [][16]byte{e}})
	if len(rows2) != 2 {
		t.Fatalf("dedup broken after re-copy: %d rows", len(rows2))
	}
}

func TestRecordTransactionMonotonic(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, _, _ := seedCatalog(t, db)
	var first int64
	for i := range 3 {
		var id int64
		err := db.WithTx(ctx, func(tx pgx.Tx) error {
			var err error
			id, err = RecordTransaction(ctx, tx, appID)
			return err
		})
		if err != nil {
			t.Fatalf("record: %v", err)
		}
		if i == 0 {
			first = id
		} else if id != first+int64(i) {
			t.Fatalf("ids not monotonic: %d then %d", first, id)
		}
	}
}

var _ = fmt.Sprintf // retained for fixture debugging
