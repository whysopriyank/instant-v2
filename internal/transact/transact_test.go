package transact_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/testkit"
	"github.com/instant-v2/instant-v2/internal/transact"
)

func testDB(t *testing.T) *storage.DB {
	t.Helper()
	fixture := testkit.NewPostgres(t, testkit.PostgresOptions{})
	ctx := context.Background()
	sqlDB, err := sql.Open("pgx", fixture.DSN)
	if err != nil {
		t.Fatalf("open migrations database: %v", err)
	}
	defer sqlDB.Close()
	if err := platform.Migrate(ctx, sqlDB); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return storage.New(fixture.Pool)
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
	e, target, survivor := rand16(), rand16(), rand16()

	steps := parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(ids.link), uuidStr(target)}),
		mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(ids.link), uuidStr(survivor)}),
	)
	if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{}, &perms.RuleDoc{Etypes: map[string]*perms.EtypeRule{}}); err != nil {
		t.Fatal(err)
	}
	steps2 := parseSteps(t, mustJSON(t, []any{"retract-triple", uuidStr(e), uuidToStr(ids.link), uuidStr(target)}))
	if _, err := transact.Transact(ctx, db, cat, appID, steps2, transact.Options{}, &perms.RuleDoc{Etypes: map[string]*perms.EtypeRule{}}); err != nil {
		t.Fatal(err)
	}
	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{e}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Triple.E != e || rows[0].Triple.A != ids.link || rows[0].Triple.V != uuidStr(survivor) {
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
// deny-all rules for the todos etype, deep-merge (update), retract (update),
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

// TestPermissionEntityLevelBindingsUseFullImages proves that permission checks
// are entity-level: a new entity's create check sees the full final image for
// both data and newData, while an existing entity's update check sees the
// original image in data and the full final image in newData.
func TestPermissionEntityLevelBindingsUseFullImages(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
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
	cat, err := reloadCatalog(t, db, appID)
	if err != nil {
		t.Fatal(err)
	}

	// Create requires both fields in both bindings. A per-attribute projection
	// would fail the first permission check because it would omit one field.
	doc, err := perms.ParseRuleDoc([]byte(`{"todos":{"allow":{
		"create":"data.id == newData.id && data.name == 'created' && data.ownerId == auth.id && newData.name == 'created' && newData.ownerId == auth.id",
		"update":"data.id == newData.id && data.name == 'initial' && !has(data.ownerId) && newData.name == 'updated' && newData.ownerId == auth.id"
	}}}`))
	if err != nil {
		t.Fatal(err)
	}

	createEID := rand16()
	createSteps := parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(createEID), uuidStr(ids.name), "created"}),
		mustJSON(t, []any{"add-triple", uuidStr(createEID), uuidStr(ownerAttr.ID), ownerID}),
	)
	if _, err := transact.Transact(ctx, db, cat, appID, createSteps,
		transact.Options{AuthUser: map[string]any{"id": ownerID}}, doc); err != nil {
		t.Fatalf("create must see the full final entity image: %v", err)
	}

	updateEID := rand16()
	if _, err := transact.Transact(ctx, db, cat, appID, parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(updateEID), uuidToStr(ids.name), "initial"}),
	), transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("seed update entity: %v", err)
	}
	updateSteps := parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(updateEID), uuidToStr(ownerAttr.ID), ownerID}),
		mustJSON(t, []any{"add-triple", uuidStr(updateEID), uuidStr(ids.name), "updated"}),
	)
	if _, err := transact.Transact(ctx, db, cat, appID, updateSteps,
		transact.Options{AuthUser: map[string]any{"id": ownerID}}, doc); err != nil {
		t.Fatalf("update must see original data and full final newData: %v", err)
	}
}

// TestPermissionProjectionBindsExactDataNewDataAndAction checks create/update
// bindings against the actual Transact path and verifies retract follows the
// update fallback. The rules reject operations with the wrong images/action.
func TestPermissionProjectionBindsExactDataNewDataAndAction(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seed(t, db)
	doc, err := perms.ParseRuleDoc([]byte(`{
		"todos":{"allow":{
			"create":"has(data.name) && data.name == 'created' && newData.name == 'created'",
			"update":"(has(data.name) && data.name == 'old' && newData.name == 'updated') || (has(data.name) && data.name == 'updated' && !has(newData.name))"
		}}
	}`))
	if err != nil {
		t.Fatal(err)
	}

	createEID := rand16()
	if _, err := transact.Transact(ctx, db, cat, appID, parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(createEID), uuidStr(ids.name), "created"}),
	), transact.Options{}, doc); err != nil {
		t.Fatalf("create binding: %v", err)
	}

	updateEID := rand16()
	if _, err := transact.Transact(ctx, db, cat, appID, parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(updateEID), uuidStr(ids.name), "old"}),
	), transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("seed update binding: %v", err)
	}
	if _, err := transact.Transact(ctx, db, cat, appID, parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(updateEID), uuidStr(ids.name), "updated"}),
	), transact.Options{}, doc); err != nil {
		t.Fatalf("update binding: %v", err)
	}

	if _, err := transact.Transact(ctx, db, cat, appID, parseSteps(t,
		mustJSON(t, []any{"retract-triple", uuidStr(updateEID), uuidStr(ids.name), "updated"}),
	), transact.Options{}, doc); err != nil {
		t.Fatalf("retract update-fallback binding: %v", err)
	}
}

// A JSON null has the same exact fingerprint in permission projection and in
// storage. If the retract misses, the following add is an update and this
// rule denies it; if it matches, the following add is a create and succeeds.
func TestPermissionSecurityNullManyRetractMatchesStorageIdentity(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, _, _ := seed(t, db)

	var valuesAttr platform.Attr
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		var err error
		valuesAttr, err = platform.GetOrCreateAttr(ctx, tx, appID, "todos", "values", "blob", "many", false, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	cat, err := reloadCatalog(t, db, appID)
	if err != nil {
		t.Fatal(err)
	}
	eid := rand16()
	if _, err := transact.Transact(ctx, db, cat, appID, parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(valuesAttr.ID), nil}),
	), transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("seed null: %v", err)
	}

	doc, err := perms.ParseRuleDoc([]byte(`{"todos":{"allow":{"create":"true","update":"true","delete":"true"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transact.Transact(ctx, db, cat, appID, parseSteps(t,
		mustJSON(t, []any{"retract-triple", uuidStr(eid), uuidStr(valuesAttr.ID), nil}),
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(valuesAttr.ID), "after-null"}),
	), transact.Options{}, doc); err != nil {
		t.Fatalf("null retract/add: %v", err)
	}

	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{
		EntityIDs: [][16]byte{eid}, AttrIDs: [][16]byte{valuesAttr.ID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Triple.V != "after-null" {
		t.Fatalf("null identity mismatch: got %+v", rows)
	}
}

func TestPermissionSecurityDeepMergeNestedNullDeletesKey(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seed(t, db)
	eid := rand16()
	if _, err := transact.Transact(ctx, db, cat, appID, parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(ids.name), map[string]any{"a": 1, "b": 2}}),
	), transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("seed merge value: %v", err)
	}

	doc, err := perms.ParseRuleDoc([]byte(`{"todos":{"allow":{"update":"!has(newData.name.a) && newData.name.b == 2"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transact.Transact(ctx, db, cat, appID, parseSteps(t,
		mustJSON(t, []any{"deep-merge-triple", uuidStr(eid), uuidStr(ids.name), map[string]any{"a": nil}}),
	), transact.Options{}, doc); err != nil {
		t.Fatalf("nested null merge: %v", err)
	}

	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{
		EntityIDs: [][16]byte{eid}, AttrIDs: [][16]byte{ids.name},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || canonical(t, rows[0].Triple.V) != `{"b":2}` {
		t.Fatalf("nested null merge mismatch: got %+v", rows)
	}
}

// delete-entity is namespace-scoped. Supplying an etype for another
// namespace may be a harmless no-op, but it must never delete this entity's
// triples from the actual namespace.
func TestDeleteEntityEtypeCannotDeleteAnotherNamespace(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, _, ids := seed(t, db)
	var userName platform.Attr
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		var err error
		userName, err = platform.GetOrCreateAttr(ctx, tx, appID, "users", "name", "blob", "one", false, true)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	cat, err := reloadCatalog(t, db, appID)
	if err != nil {
		t.Fatal(err)
	}
	eid := rand16()
	if _, err := transact.Transact(ctx, db, cat, appID, parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(ids.name), "todo"}),
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(userName.ID), "user"}),
	), transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("seed namespaces: %v", err)
	}

	if _, err := transact.Transact(ctx, db, cat, appID, parseSteps(t,
		mustJSON(t, []any{"delete-entity", uuidStr(eid), "users"}),
	), transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("delete users namespace: %v", err)
	}
	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{eid}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Triple.A != ids.name || rows[0].Triple.V != "todo" {
		t.Fatalf("namespace-spoofed delete removed wrong rows: %+v", rows)
	}
}

// Permission denial happens before operation dispatch. It must therefore
// leave both the journal and all committed triples unchanged.
func TestPermissionDenialLeavesJournalAndTriplesUnchanged(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seed(t, db)
	eid := rand16()
	if _, err := transact.Transact(ctx, db, cat, appID, parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(ids.name), "keep"}),
	), transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var before int64
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE app_id=$1`, appID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	doc, err := perms.ParseRuleDoc([]byte(`{"todos":{"allow":{"update":"false"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transact.Transact(ctx, db, cat, appID, parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(ids.name), "must-not-commit"}),
	), transact.Options{}, doc); err == nil {
		t.Fatal("expected permission denial")
	}
	var after int64
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE app_id=$1`, appID).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("denied transaction left journal row: before=%d after=%d", before, after)
	}
	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{eid}, AttrIDs: [][16]byte{ids.name}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Triple.V != "keep" {
		t.Fatalf("denied transaction changed triples: %+v", rows)
	}
}

func TestPermissionCELFailureRollsBackJournalAndTriples(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seed(t, db)
	eid := rand16()
	if _, err := transact.Transact(ctx, db, cat, appID, parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(ids.name), "keep"}),
	), transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var before int64
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE app_id=$1`, appID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	doc, err := perms.ParseRuleDoc([]byte(`{"todos":{"allow":{"update":"data.!!!"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	_, err = transact.Transact(ctx, db, cat, appID, parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(ids.name), "must-not-commit"}),
	), transact.Options{}, doc)
	if err == nil || !strings.Contains(err.Error(), "CEL compile") {
		t.Fatalf("expected fail-closed CEL compilation error, got: %v", err)
	}
	var after int64
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE app_id=$1`, appID).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("CEL failure left journal row: before=%d after=%d", before, after)
	}
	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{eid}, AttrIDs: [][16]byte{ids.name}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Triple.V != "keep" {
		t.Fatalf("CEL failure changed triples: %+v", rows)
	}
}

// 1. Stored v1; rules allow delete/create but deny update; batch retract(v2) -> add(v3) must be denied and leave v1 unchanged.
func TestPermissionSecurityCardinalityOneNonmatchingRetractLeavesUpdateDenied(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seed(t, db)
	e := rand16()

	// Seed eid with v1
	seedSteps := parseSteps(t, mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(ids.name), "v1"}))
	if _, err := transact.Transact(ctx, db, cat, appID, seedSteps, transact.Options{Admin: true}, nil); err != nil {
		t.Fatal(err)
	}

	doc, err := perms.ParseRuleDoc([]byte(`{"todos":{"allow":{"delete":"true","create":"true","update":"false"}}}`))
	if err != nil {
		t.Fatal(err)
	}

	// Batch: retract(v2) -> add(v3)
	// Because retract(v2) does not match v1, it is a storage no-op and v1 remains present.
	// Therefore add(v3) is an update, which is denied by the rule!
	batch := parseSteps(t,
		mustJSON(t, []any{"retract-triple", uuidStr(e), uuidToStr(ids.name), "v2"}),
		mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(ids.name), "v3"}),
	)
	_, err = transact.Transact(ctx, db, cat, appID, batch, transact.Options{}, doc)
	if err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("expected permission denied (update todos), got: %v", err)
	}

	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{e}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Triple.V != "v1" {
		t.Fatalf("expected v1 to remain unchanged, got: %+v", rows)
	}
}

// 2. A many attribute containing one value; rules allow delete/update but deny create; exact retract followed by add must deny the add.
func TestPermissionSecurityCardinalityManyRetractAllDeniesSubsequentCreate(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, _, _ := seed(t, db)

	var tagsAttr platform.Attr
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		var e error
		tagsAttr, e = platform.GetOrCreateAttr(ctx, tx, appID, "todos", "tags", "blob", "many", false, false)
		return e
	}); err != nil {
		t.Fatal(err)
	}
	cat, err := reloadCatalog(t, db, appID)
	if err != nil {
		t.Fatal(err)
	}

	e := rand16()
	// Seed with one tag "tech"
	seedSteps := parseSteps(t, mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(tagsAttr.ID), "tech"}))
	if _, err := transact.Transact(ctx, db, cat, appID, seedSteps, transact.Options{Admin: true}, nil); err != nil {
		t.Fatal(err)
	}

	// Rule: delete and update are allowed, create is denied
	doc, err := perms.ParseRuleDoc([]byte(`{"todos":{"allow":{"delete":"true","update":"true","create":"false"}}}`))
	if err != nil {
		t.Fatal(err)
	}

	// Batch: exact retract of "tech" followed by add of "science"
	// Exact retract removes the final value and clears attrExists.
	// The subsequent add is classified as create, which is denied!
	batch := parseSteps(t,
		mustJSON(t, []any{"retract-triple", uuidStr(e), uuidToStr(tagsAttr.ID), "tech"}),
		mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(tagsAttr.ID), "science"}),
	)
	_, err = transact.Transact(ctx, db, cat, appID, batch, transact.Options{}, doc)
	if err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("expected permission denied (create todos), got: %v", err)
	}

	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{e}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Triple.V != "tech" {
		t.Fatalf("expected tech to remain in storage, got: %+v", rows)
	}
}

// 3. Duplicate many-value add followed by a rule based on collection size.
func TestPermissionSecurityDuplicateManyAddCollectionSize(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, _, _ := seed(t, db)

	var tagsAttr platform.Attr
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		var e error
		tagsAttr, e = platform.GetOrCreateAttr(ctx, tx, appID, "todos", "tags", "blob", "many", false, false)
		return e
	}); err != nil {
		t.Fatal(err)
	}
	cat, err := reloadCatalog(t, db, appID)
	if err != nil {
		t.Fatal(err)
	}

	e := rand16()
	seedSteps := parseSteps(t, mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(tagsAttr.ID), "existing"}))
	if _, err := transact.Transact(ctx, db, cat, appID, seedSteps, transact.Options{Admin: true}, nil); err != nil {
		t.Fatal(err)
	}

	// Rule allows updates only if tags size <= 2
	doc, err := perms.ParseRuleDoc([]byte(`{"todos":{"allow":{"create":"true","update":"newData.tags.size() <= 2"}}}`))
	if err != nil {
		t.Fatal(err)
	}

	// Batch: add "dup" three times. Because storage uses ON CONFLICT DO NOTHING,
	// the tags collection size should remain 2 (existing + dup).
	batch := parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(tagsAttr.ID), "dup"}),
		mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(tagsAttr.ID), "dup"}),
		mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(tagsAttr.ID), "dup"}),
	)
	if _, err := transact.Transact(ctx, db, cat, appID, batch, transact.Options{}, doc); err != nil {
		t.Fatalf("expected duplicate adds to not inflate collection size: %v", err)
	}

	// Now try adding a third distinct tag "third" -> size would be 3 -> denied!
	batchExceed := parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(tagsAttr.ID), "third"}),
	)
	_, err = transact.Transact(ctx, db, cat, appID, batchExceed, transact.Options{}, doc)
	if err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("expected size > 2 to be denied, got: %v", err)
	}
}

// 4. Many-value deep merge followed by a rule based on collection membership/shape.
func TestPermissionSecurityManyDeepMergeCollectionMembershipAndShape(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, _, _ := seed(t, db)

	var metaAttr platform.Attr
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		var e error
		metaAttr, e = platform.GetOrCreateAttr(ctx, tx, appID, "todos", "meta", "blob", "many", false, false)
		return e
	}); err != nil {
		t.Fatal(err)
	}
	cat, err := reloadCatalog(t, db, appID)
	if err != nil {
		t.Fatal(err)
	}

	e := rand16()
	seedSteps := parseSteps(t, mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(metaAttr.ID), map[string]any{"tag": "v1", "count": 1}}))
	if _, err := transact.Transact(ctx, db, cat, appID, seedSteps, transact.Options{Admin: true}, nil); err != nil {
		t.Fatal(err)
	}

	// Rule: verifies that newData.meta is a list with 2 elements and the merged second item has count 2 and tag "v1"
	doc, err := perms.ParseRuleDoc([]byte(`{"todos":{"allow":{"create":"true","update":"newData.meta.size() == 2 && newData.meta[1].count == 2 && newData.meta[1].tag == 'v1'"}}}`))
	if err != nil {
		t.Fatal(err)
	}

	batch := parseSteps(t,
		mustJSON(t, []any{"deep-merge-triple", uuidStr(e), uuidToStr(metaAttr.ID), map[string]any{"count": 2}}),
	)
	if _, err := transact.Transact(ctx, db, cat, appID, batch, transact.Options{}, doc); err != nil {
		t.Fatalf("expected many deep-merge to produce consistent collection shape: %v", err)
	}
}

// 5. Numeric many value 1; retract 1; a later rule must not observe the removed value.
func TestPermissionSecurityNumericManyRetractUnobservesValue(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, _, _ := seed(t, db)

	var numAttr platform.Attr
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		var e error
		numAttr, e = platform.GetOrCreateAttr(ctx, tx, appID, "todos", "numbers", "blob", "many", false, false)
		return e
	}); err != nil {
		t.Fatal(err)
	}
	cat, err := reloadCatalog(t, db, appID)
	if err != nil {
		t.Fatal(err)
	}

	e := rand16()
	seedSteps := parseSteps(t, mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(numAttr.ID), 1}))
	if _, err := transact.Transact(ctx, db, cat, appID, seedSteps, transact.Options{Admin: true}, nil); err != nil {
		t.Fatal(err)
	}

	// Permission checks use the update fallback for retract; the final storage
	// assertion below verifies that the numeric value was actually removed.
	doc, err := perms.ParseRuleDoc([]byte(`{"todos":{"allow":{"delete":"true","create":"true","update":"true"}}}`))
	if err != nil {
		t.Fatal(err)
	}

	// Batch: retract 1 (as integer 1) then add 2
	batch := parseSteps(t,
		mustJSON(t, []any{"retract-triple", uuidStr(e), uuidToStr(numAttr.ID), 1}),
		mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(numAttr.ID), 2}),
	)
	if _, err := transact.Transact(ctx, db, cat, appID, batch, transact.Options{}, doc); err != nil {
		t.Fatalf("expected numeric retract 1 to unobserve value in later rule: %v", err)
	}

	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{e}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || fmt.Sprint(rows[0].Triple.V) != "2" {
		t.Fatalf("expected only 2 to remain, got: %+v", rows)
	}
}

// Restored JSONB rows can retain numeric scale in value_md5 even though JSONB
// equality treats 1 and 1.0 as equal. Permission projection must use the same
// exact identity as DeleteTx or it can authorize a later create while the
// stored value remains present.
func TestPermissionSecurityRestoredNumericScaleRetractRemainsUpdate(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, _, _ := seed(t, db)

	var numAttr platform.Attr
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		var err error
		numAttr, err = platform.GetOrCreateAttr(ctx, tx, appID, "todos", "numbers", "blob", "many", false, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	cat, err := reloadCatalog(t, db, appID)
	if err != nil {
		t.Fatal(err)
	}

	e := rand16()
	if _, err := db.Pool.Exec(ctx, `
		INSERT INTO triples (app_id, entity_id, attr_id, value, value_md5,
		                     ea, eav, av, ave, vae, checked_data_type)
		VALUES ($1, $2, $3, '1.0'::jsonb, md5(('1.0'::jsonb)::text),
		        false, false, false, false, false, NULL)`, appID, e, numAttr.ID); err != nil {
		t.Fatal(err)
	}

	doc, err := perms.ParseRuleDoc([]byte(`{"todos":{"allow":{"delete":"true","create":"true","update":"false"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	batch := parseSteps(t,
		mustJSON(t, []any{"retract-triple", uuidStr(e), uuidToStr(numAttr.ID), 1}),
		mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(numAttr.ID), 2}),
	)
	_, err = transact.Transact(ctx, db, cat, appID, batch, transact.Options{}, doc)
	if err == nil || !strings.Contains(err.Error(), "permission denied (update todos)") {
		t.Fatalf("expected exact-md5 no-op retract to use update authorization, got: %v", err)
	}

	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{e}, AttrIDs: [][16]byte{numAttr.ID}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].MD5 != "e4c2e8edac362acab7123654b9e73432" {
		t.Fatalf("expected restored 1.0 row to remain unchanged, got: %+v", rows)
	}
}

func TestPermissionSecurityExponentRetractUsesPostgresFingerprint(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, _, _ := seed(t, db)

	var numAttr platform.Attr
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		var err error
		numAttr, err = platform.GetOrCreateAttr(ctx, tx, appID, "todos", "numbers", "blob", "many", false, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	cat, err := reloadCatalog(t, db, appID)
	if err != nil {
		t.Fatal(err)
	}

	e := rand16()
	if _, err := db.Pool.Exec(ctx, `
		INSERT INTO triples (app_id, entity_id, attr_id, value, value_md5,
		                     ea, eav, av, ave, vae, checked_data_type)
		VALUES ($1, $2, $3, '1e21'::jsonb, md5(('1e21'::jsonb)::text),
		        false, false, false, false, false, NULL)`, appID, e, numAttr.ID); err != nil {
		t.Fatal(err)
	}

	doc, err := perms.ParseRuleDoc([]byte(`{"todos":{"allow":{"delete":"true","create":"false","update":"true"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	batch := parseSteps(t,
		mustJSON(t, []any{"retract-triple", uuidStr(e), uuidToStr(numAttr.ID), float64(1e21)}),
		mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(numAttr.ID), 2}),
	)
	_, err = transact.Transact(ctx, db, cat, appID, batch, transact.Options{}, doc)
	if err == nil || !strings.Contains(err.Error(), "permission denied (create todos)") {
		t.Fatalf("expected PostgreSQL-normalized retract to make the add a denied create, got: %v", err)
	}

	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{e}, AttrIDs: [][16]byte{numAttr.ID}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || fmt.Sprint(rows[0].Triple.V) != "1000000000000000000000" {
		t.Fatalf("expected transaction rollback to preserve the exponent value, got: %+v", rows)
	}
}

func TestPermissionSecurityManyDeepMergeUsesDeterministicValueMD5Base(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, _, _ := seed(t, db)

	var metaAttr platform.Attr
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		var err error
		metaAttr, err = platform.GetOrCreateAttr(ctx, tx, appID, "todos", "meta", "blob", "many", false, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	cat, err := reloadCatalog(t, db, appID)
	if err != nil {
		t.Fatal(err)
	}

	e := rand16()
	seedSteps := parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(metaAttr.ID), map[string]any{"base": "z"}}),
		mustJSON(t, []any{"add-triple", uuidStr(e), uuidToStr(metaAttr.ID), map[string]any{"base": "a"}}),
	)
	if _, err := transact.Transact(ctx, db, cat, appID, seedSteps, transact.Options{Admin: true}, nil); err != nil {
		t.Fatal(err)
	}

	doc, err := perms.ParseRuleDoc([]byte(`{"todos":{"allow":{"update":"newData.meta.exists(v, v.base == 'a' && v.merged == true)"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	merge := parseSteps(t, mustJSON(t, []any{
		"deep-merge-triple", uuidStr(e), uuidToStr(metaAttr.ID), map[string]any{"merged": true},
	}))
	if _, err := transact.Transact(ctx, db, cat, appID, merge, transact.Options{}, doc); err != nil {
		t.Fatalf("expected permission and storage to select the same canonical base: %v", err)
	}

	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{e}, AttrIDs: [][16]byte{metaAttr.ID}})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range rows {
		if m, ok := row.Triple.V.(map[string]any); ok && m["base"] == "a" && m["merged"] == true {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected merged value to use base a, got: %+v", rows)
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

func TestLookupRefResolutionPreservesLargeIntegers(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, _, _ := seed(t, db)

	var numberAttr, markerAttr platform.Attr
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		var err error
		numberAttr, err = platform.GetOrCreateAttr(ctx, tx, appID, "users", "accountNumber", "blob", "one", true, true)
		if err != nil {
			return err
		}
		markerAttr, err = platform.GetOrCreateAttr(ctx, tx, appID, "users", "lookupMarker", "blob", "one", false, true)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	cat, err := platform.LoadAttrCatalog(ctx, db.Pool, appID)
	if err != nil {
		t.Fatal(err)
	}

	values := []string{"9007199254740992", "9007199254740993"}
	entities := [][16]byte{rand16(), rand16()}
	for i, value := range values {
		steps := parseSteps(t, mustJSON(t, []any{
			"add-triple", uuidStr(entities[i]), uuidToStr(numberAttr.ID), json.RawMessage(value),
		}))
		if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{}, nil); err != nil {
			t.Fatalf("seed exact numeric value %s: %v", value, err)
		}
	}

	for i, value := range values {
		lookupEID := []any{uuidToStr(numberAttr.ID), json.RawMessage(value)}
		steps := parseSteps(t, mustJSON(t, []any{
			"add-triple", lookupEID, uuidToStr(markerAttr.ID), fmt.Sprintf("matched-%d", i),
		}))
		if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{}, nil); err != nil {
			t.Fatalf("lookup exact numeric value %s: %v", value, err)
		}
	}

	for i, value := range values {
		var stored string
		if err := db.Pool.QueryRow(ctx, `
			SELECT value::text FROM triples
			 WHERE app_id=$1 AND entity_id=$2 AND attr_id=$3`,
			appID, entities[i], numberAttr.ID).Scan(&stored); err != nil {
			t.Fatalf("read exact numeric value %s: %v", value, err)
		}
		if stored != value {
			t.Fatalf("stored numeric value for entity %d: got %q, want %q", i, stored, value)
		}

		rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{
			EntityIDs: [][16]byte{entities[i]}, AttrIDs: [][16]byte{markerAttr.ID},
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0].Triple.V != fmt.Sprintf("matched-%d", i) {
			t.Fatalf("lookup for %s mutated wrong entity: rows=%+v", value, rows)
		}
	}
}

func TestHighLevelSameBatchEquivalentNumericLookupsReuseEntity(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, _, _ := seed(t, db)

	var numberAttr, idAttr platform.Attr
	markers := make([]platform.Attr, 3)
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		var err error
		numberAttr, err = platform.GetOrCreateAttr(ctx, tx, appID, "users", "accountNumber", "blob", "one", true, true)
		if err != nil {
			return err
		}
		idAttr, err = platform.GetOrCreateAttr(ctx, tx, appID, "users", "id", "blob", "one", true, true)
		if err != nil {
			return err
		}
		for i := range markers {
			markers[i], err = platform.GetOrCreateAttr(ctx, tx, appID, "users", fmt.Sprintf("marker%d", i), "blob", "one", false, true)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	cat, err := platform.LoadAttrCatalog(ctx, db.Pool, appID)
	if err != nil {
		t.Fatal(err)
	}

	hooks := transact.LowerHooks{
		ResolveUnique: func(ctx context.Context, appID, attrID [16]byte, value json.RawMessage) ([16]byte, bool, error) {
			var eid [16]byte
			err := db.Pool.QueryRow(ctx, `
				SELECT entity_id FROM triples
				 WHERE app_id=$1 AND attr_id=$2 AND av AND value=$3::jsonb
				 LIMIT 1`, appID, attrID, string(value)).Scan(&eid)
			if err == pgx.ErrNoRows {
				return [16]byte{}, false, nil
			}
			return eid, true, err
		},
	}
	spellings := []string{"1", "1.0", "1e0"}
	raw := make([]json.RawMessage, 0, len(spellings))
	for i, spelling := range spellings {
		raw = append(raw, mustJSON(t, []any{
			"update", "users", []any{"accountNumber", json.RawMessage(spelling)},
			map[string]any{fmt.Sprintf("marker%d", i): fmt.Sprintf("matched-%d", i)},
		}))
	}
	lowered, err := transact.LowerAdminSteps(ctx, appID, cat, raw, hooks, false)
	if err != nil {
		t.Fatalf("lower high-level same-batch lookups: %v", err)
	}
	steps := parseSteps(t, lowered...)
	if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{}, nil); err != nil {
		t.Fatalf("transact high-level same-batch lookups: %v", err)
	}

	var entity [16]byte
	var stored string
	if err := db.Pool.QueryRow(ctx, `
		SELECT entity_id, value::text FROM triples
		 WHERE app_id=$1 AND attr_id=$2`, appID, numberAttr.ID).Scan(&entity, &stored); err != nil {
		t.Fatalf("read minted lookup entity: %v", err)
	}
	if stored != "1" {
		t.Fatalf("canonical lookup value: got %q, want %q", stored, "1")
	}
	for i, marker := range markers {
		rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{
			EntityIDs: [][16]byte{entity}, AttrIDs: [][16]byte{marker.ID},
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0].Triple.V != fmt.Sprintf("matched-%d", i) {
			t.Fatalf("same-batch lookup marker %d was not attached to one entity: rows=%+v", i, rows)
		}
	}

	var entityCount int
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM triples WHERE app_id=$1 AND attr_id=$2`, appID, idAttr.ID).Scan(&entityCount); err != nil {
		t.Fatal(err)
	}
	if entityCount != 1 {
		t.Fatalf("equivalent lookup spellings minted %d entities, want 1", entityCount)
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
