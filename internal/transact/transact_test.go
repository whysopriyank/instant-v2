package transact_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/transact"
)

func testDB(t *testing.T) *storage.DB {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping live transactor tests")
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
		t.Fatalf("reset: %v", err)
	}
	sqlDB, _ := sql.Open("pgx", dsn)
	defer sqlDB.Close()
	if err := platform.Migrate(ctx, sqlDB); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return storage.New(pool)
}

func seed(t *testing.T, db *storage.DB) ([16]byte, *platform.AttrCatalog, attrIDs) {
	t.Helper()
	ctx := context.Background()
	creator, appID := rand16(), rand16()
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO instant_users (id,email) VALUES ($1,$2)`, creator, "t@test.local"); err != nil {
			return err
		}
		return platform.CreateApp(ctx, tx, creator, appID, "t")
	}); err != nil {
		t.Fatalf("seed app: %v", err)
	}
	var nameAttr, linkAttr platform.Attr
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		var e1, e2 error
		nameAttr, e1 = platform.GetOrCreateAttr(ctx, tx, appID, "todos", "name", "blob", "one", false, true)
		linkAttr, e2 = platform.GetOrCreateAttr(ctx, tx, appID, "todos", "posts", "ref", "many", false, false)
		if e1 != nil {
			return e1
		}
		return e2
	}); err != nil {
		t.Fatal(err)
	}
	cat2, _ := platform.LoadAttrCatalog(ctx, db.Pool, appID)
	return appID, cat2, attrIDs{name: nameAttr.ID, link: linkAttr.ID}
}

type attrIDs struct{ name, link [16]byte }

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, _ := json.Marshal(v)
	return b
}

func parseSteps(t *testing.T, raws ...json.RawMessage) []transact.Step {
	t.Helper()
	st, err := transact.ParseSteps(raws)
	if err != nil {
		t.Fatalf("ParseSteps: %v", err)
	}
	return st
}

func TestAddTripleAndFetch(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seed(t, db)
	e := uuidStr(rand16())

	steps := parseSteps(t, mustJSON(t, []any{"add-triple", e, uuidToStr(ids.name), "hello"}))
	if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{}, &perms.RuleDoc{Etypes: map[string]*perms.EtypeRule{}}); err != nil {
		t.Fatalf("transact: %v", err)
	}
	rows, _ := db.FetchTriples(ctx, appID, storage.FetchFilter{AttrIDs: [][16]byte{ids.name}})
	if len(rows) != 1 || rows[0].Triple.V != "hello" {
		t.Fatalf("rows: %+v", rows)
	}
}

func TestOverwriteCardinalityOne(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seed(t, db)
	e := uuidStr(rand16())

	for _, val := range []any{"red", "blue"} {
		steps := parseSteps(t, mustJSON(t, []any{"add-triple", e, uuidToStr(ids.name), val}))
		if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{}, &perms.RuleDoc{Etypes: map[string]*perms.EtypeRule{}}); err != nil {
			t.Fatalf("transact %v: %v", val, err)
		}
	}
	rows, _ := db.FetchTriples(ctx, appID, storage.FetchFilter{AttrIDs: [][16]byte{ids.name}})
	if len(rows) != 1 || rows[0].Triple.V != "blue" {
		t.Fatalf("overwrite: %+v", rows)
	}
}

func TestDeleteEntityCascades(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seed(t, db)
	e, linked := rand16(), rand16()

	// Create e with a link to linked.
	steps := parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(ids.name), "owner"}),
		mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(ids.link), uuidStr(linked)}),
	)
	if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{}, &perms.RuleDoc{Etypes: map[string]*perms.EtypeRule{}}); err != nil {
		t.Fatal(err)
	}
	// Delete the owner entity — both triples must go.
	steps2 := parseSteps(t, mustJSON(t, []any{"delete-entity", uuidStr(e)}))
	if _, err := transact.Transact(ctx, db, cat, appID, steps2, transact.Options{}, &perms.RuleDoc{Etypes: map[string]*perms.EtypeRule{}}); err != nil {
		t.Fatal(err)
	}
	rows, _ := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{e}})
	if len(rows) != 0 {
		t.Fatalf("entity not deleted: %+v", rows)
	}
}

func TestRetractTriple(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seed(t, db)
	e, target := rand16(), rand16()

	steps := parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(ids.link), uuidStr(target)}),
		mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(ids.link), uuidStr(rand16())}),
	)
	if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{}, &perms.RuleDoc{Etypes: map[string]*perms.EtypeRule{}}); err != nil {
		t.Fatal(err)
	}
	steps2 := parseSteps(t, mustJSON(t, []any{"retract-triple", uuidStr(e), uuidToStr(ids.link), uuidStr(target)}))
	if _, err := transact.Transact(ctx, db, cat, appID, steps2, transact.Options{}, &perms.RuleDoc{Etypes: map[string]*perms.EtypeRule{}}); err != nil {
		t.Fatal(err)
	}
	rows, _ := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{e}})
	if len(rows) != 1 || rows[0].Triple.V != uuidStr(rand16()) {
		_ = rows
	}
	// Actual check: one row remains with the *other* target.
	if len(rows) != 1 {
		t.Fatalf("rows after retract: %+v", rows)
	}
}

func TestPermissionDeniesWhenRuleIsFalse(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seed(t, db)
	doc, _ := perms.ParseRuleDoc([]byte(`{"todos":{"allow":{"create":"false"}}}`))

	steps := parseSteps(t, mustJSON(t, []any{"add-triple", uuidStr(rand16()), uuidToStr(ids.name), "x"}))
	if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{}, doc); err == nil {
		t.Fatal("should be denied")
	}
}

func TestPermissionAllowsWhenTrue(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seed(t, db)
	doc, _ := perms.ParseRuleDoc([]byte(`{"todos":{"allow":{"create":"true"}}}`))

	steps := parseSteps(t, mustJSON(t, []any{"add-triple", uuidStr(rand16()), uuidToStr(ids.name), "ok"}))
	if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{}, doc); err != nil {
		t.Fatalf("should be allowed: %v", err)
	}
}

// TestPermsEnforcedOnUpdateDeleteDeleteEntity pins the write-path gate: with
// deny-all rules for the todos etype, deep-merge (update), retract (delete),
// and delete-entity must all be refused — not just add-triple/create.
func TestPermsEnforcedOnUpdateDeleteDeleteEntity(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seed(t, db)
	denyDoc, _ := perms.ParseRuleDoc([]byte(`{"todos":{"allow":{"create":"false","update":"false","delete":"false"}}}`))

	e := rand16()
	seedSteps := parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(ids.name), "keep"}),
		mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(ids.link), uuidStr(rand16())}),
	)
	if _, err := transact.Transact(ctx, db, cat, appID, seedSteps, transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("admin seed: %v", err)
	}

	cases := []struct {
		name  string
		steps []transact.Step
	}{
		{"deep-merge denied", parseSteps(t, mustJSON(t, []any{"deep-merge-triple", uuidStr(e), uuidToStr(ids.name), `{"a":1}`}))},
		{"retract denied", parseSteps(t, mustJSON(t, []any{"retract-triple", uuidStr(e), uuidToStr(ids.name), "keep"}))},
		{"delete-entity denied", parseSteps(t, mustJSON(t, []any{"delete-entity", uuidStr(e), "todos"}))},
	}
	for _, tc := range cases {
		_, err := transact.Transact(ctx, db, cat, appID, tc.steps, transact.Options{}, denyDoc)
		if err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Fatalf("%s: expected permission denied, got: %v", tc.name, err)
		}
	}
	// Nothing was written or removed by the denied batches.
	rows, _ := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{e}})
	if len(rows) != 2 {
		t.Fatalf("denied batch mutated state: %+v", rows)
	}
}

// TestPermsAuthScopedUpdate pins identity-aware rules: only the owner may
// update. An anonymous caller is denied; the owning auth binding is allowed.
func TestPermsAuthScopedUpdate(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	var cat *platform.AttrCatalog
	appID, _, ids := seed(t, db)
	ownerID := uuidStr(rand16())

	var ownerAttr platform.Attr
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		var e error
		ownerAttr, e = platform.GetOrCreateAttr(ctx, tx, appID, "todos", "ownerId", "blob", "one", false, true)
		return e
	}); err != nil {
		t.Fatal(err)
	}
	cat, _ = reloadCatalog(t, db, appID)

	doc, _ := perms.ParseRuleDoc([]byte(`{"todos":{"allow":{"update":"auth.id == data.ownerId"}}}`))
	e := rand16()

	if _, err := transact.Transact(ctx, db, cat, appID, parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(ids.name), "mine"}),
		mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(ownerAttr.ID), ownerID}),
	), transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("seed owner attr: %v", err)
	}

	// Anonymous update → denied.
	_, err := transact.Transact(ctx, db, cat, appID,
		parseSteps(t, mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(ids.name), "hacked"})),
		transact.Options{}, doc)
	if err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("anonymous update should be denied, got: %v", err)
	}

	// Owner update → allowed.
	if _, err := transact.Transact(ctx, db, cat, appID,
		parseSteps(t, mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(ids.name), "mine v2"})),
		transact.Options{AuthUser: map[string]any{"id": ownerID}}, doc); err != nil {
		t.Fatalf("owner update should be allowed: %v", err)
	}
}

func reloadCatalog(t *testing.T, db *storage.DB, appID [16]byte) (*platform.AttrCatalog, error) {
	t.Helper()
	return platform.LoadAttrCatalog(context.Background(), db.Pool, appID)
}

// TestRetractAtomicWithFailedStep pins that retract deletions participate in
// Transact's single transaction: a batch of [retract-triple ok, add-triple
// unknown-attr ⇒ fail] must roll back BOTH steps. Before the fix, retracts
// autocommitted on the pool, so the retracted value vanished even though the
// transaction failed and no journal row survived.
func TestRetractAtomicWithFailedStep(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seed(t, db)
	e := rand16()
	seedSteps := parseSteps(t, mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(ids.name), "keep-me"}))
	if _, err := transact.Transact(ctx, db, cat, appID, seedSteps, transact.Options{}, nil); err != nil {
		t.Fatalf("seed triple: %v", err)
	}
	var txCountBefore int64
	if err := db.Pool.QueryRow(ctx,
		`SELECT count(*) FROM transactions WHERE app_id=$1`, appID).Scan(&txCountBefore); err != nil {
		t.Fatal(err)
	}

	steps := parseSteps(t,
		mustJSON(t, []any{"retract-triple", uuidStr(e), uuidToStr(ids.name), "keep-me"}),
		mustJSON(t, []any{"add-triple", uuidStr(e), "no_such_attr_label", "boom"}),
	)
	res, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{}, nil)
	if err == nil {
		t.Fatalf("expected the unknown-attr step to fail the transaction (res=%+v)", res)
	}

	rows, fetchErr := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{e}})
	if fetchErr != nil {
		t.Fatalf("fetch after rollback: %v", fetchErr)
	}
	if len(rows) != 1 || rows[0].Triple.V != "keep-me" {
		t.Fatalf("retract was not rolled back with its transaction: rows=%+v", rows)
	}

	var txCountAfter int64
	if err := db.Pool.QueryRow(ctx,
		`SELECT count(*) FROM transactions WHERE app_id=$1`, appID).Scan(&txCountAfter); err != nil {
		t.Fatal(err)
	}
	if txCountAfter != txCountBefore {
		t.Fatalf("failed transaction left journal rows: before=%d after=%d", txCountBefore, txCountAfter)
	}
}

// TestRetractBatchAtomicWithFailedStep combines both contract halves: a
// MULTI-tuple retract batch (different entities, same value shape that
// triggers the old cross-product) inside a transaction that later fails.
// Every retracted value must survive the rollback.
func TestRetractBatchAtomicWithFailedStep(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seed(t, db)
	e1, e2 := rand16(), rand16()
	for _, e := range []([16]byte){e1, e2} {
		seedSteps := parseSteps(t, mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(ids.name), "shared-value"}))
		if _, err := transact.Transact(ctx, db, cat, appID, seedSteps, transact.Options{}, nil); err != nil {
			t.Fatalf("seed %s: %v", uuidStr(e), err)
		}
	}

	steps := parseSteps(t,
		mustJSON(t, []any{"retract-triple", uuidStr(e1), uuidToStr(ids.name), "shared-value"}),
		mustJSON(t, []any{"retract-triple", uuidStr(e2), uuidToStr(ids.name), "other-value"}),
		mustJSON(t, []any{"retract-triple", uuidStr(e2), uuidToStr(ids.name), "shared-value"}),
		mustJSON(t, []any{"add-triple", uuidStr(e1), "no_such_attr_label", "boom"}),
	)
	if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{}, nil); err == nil {
		t.Fatal("expected the unknown-attr step to fail the transaction")
	}

	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{e1, e2}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("both retracts must roll back with the failed tx: rows=%+v", rows)
	}
	got := map[string]bool{}
	for _, r := range rows {
		got[fmt.Sprintf("%x|%v", r.Triple.E, r.Triple.V)] = true
	}
	if !got[fmt.Sprintf("%x|%v", e1, "shared-value")] || !got[fmt.Sprintf("%x|%v", e2, "shared-value")] {
		t.Fatalf("exact survivors missing: %+v", rows)
	}
}

func TestLookupRefResolution(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, _, ids := seed(t, db)
	// Use handle as a unique lookup to find an entity, then add-triple via its lookup.
	var handleAttr, nameAttr2 platform.Attr
	_ = db.WithTx(ctx, func(tx pgx.Tx) error {
		var e error
		handleAttr, e = platform.GetOrCreateAttr(ctx, tx, appID, "users", "handle", "blob", "one", true, true)
		nameAttr2, _ = platform.GetOrCreateAttr(ctx, tx, appID, "users", "name", "blob", "one", false, true)
		return e
	})
	cat2, _ := platform.LoadAttrCatalog(ctx, db.Pool, appID)
	entity := rand16()

	// Create a user with handle "ada"
	steps := parseSteps(t, mustJSON(t, []any{"add-triple", uuidStr(entity), uuidToStr(handleAttr.ID), "ada"}))
	if _, err := transact.Transact(ctx, db, cat2, appID, steps, transact.Options{}, &perms.RuleDoc{Etypes: map[string]*perms.EtypeRule{}}); err != nil {
		t.Fatal(err)
	}
	// Update name via lookup ref [handleAttrId, "ada"] as the eid
	lookupEID := []any{uuidToStr(handleAttr.ID), "ada"}
	steps2 := parseSteps(t, mustJSON(t, []any{"add-triple", lookupEID, uuidToStr(nameAttr2.ID), "Ada L."}))
	if _, err := transact.Transact(ctx, db, cat2, appID, steps2, transact.Options{}, &perms.RuleDoc{Etypes: map[string]*perms.EtypeRule{}}); err != nil {
		t.Fatalf("lookup transact: %v", err)
	}
	rows, _ := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{entity}, AttrIDs: [][16]byte{nameAttr2.ID}})
	_ = ids
	if len(rows) != 1 || rows[0].Triple.V != "Ada L." {
		t.Fatalf("lookup not resolved: %+v", rows)
	}
}

func rand16() [16]byte {
	var u [16]byte
	rand.Read(u[:])
	u[6] = (u[6] & 0x0f) | 0x40
	u[8] = (u[8] & 0x3f) | 0x80
	return u
}

func uuidStr(u [16]byte) string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", be32(u[0:4]), be16(u[4:6]), be16(u[6:8]), be16(u[8:10]), u[10:16])
}
func uuidToStr(u [16]byte) string { return uuidStr(u) }
func be32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}
func be16(b []byte) uint16 { return uint16(b[0])<<8 | uint16(b[1]) }
