package transact_test

import (
	"context"

	"github.com/jackc/pgx/v5"
	"strings"
	"testing"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/transact"
)

// TestAddAttrRejectsReservedNamespaces pins the tenant guard on client
// schema minting: system namespaces are server-provisioned, and a runtime
// session must not be able to create attrs inside them (e.g. $users fields
// that would surface in auth/user projections).
func TestAddAttrRejectsReservedNamespaces(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	appID, cat, _ := seed(t, db)

	for _, etype := range []string{"$users", "$files", "$default", "$streams", "$rateLimits"} {
		attrID := rand16()
		steps := parseSteps(t,
			mustJSON(t, []any{"add-attr", map[string]any{
				"id":               uuidStr(attrID),
				"forward-identity": []string{uuidStr(attrID), etype, "sneaky"},
				"value-type":       "blob",
				"cardinality":      "one",
			}}),
		)
		if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{}, nil); err == nil {
			t.Fatalf("%s: reserved namespace add-attr must be rejected", etype)
		}
	}
}

// The frozen protocol lets clients mint attr ids (instaml add-attr) and
// reference them in the SAME batch. Regression: add-attr was a stub, so every
// real SDK write died with "unknown attr <id>".
func TestAddAttrThenTripleSameBatch(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)

	appID, _, _ := seed(t, db)

	attrID := rand16()
	eid := rand16()
	steps := parseSteps(t,
		mustJSON(t, []any{"add-attr", map[string]any{
			"id":               uuidStr(attrID),
			"forward-identity": []string{uuidStr(attrID), "wireattr", "name"},
			"value-type":       "blob",
			"cardinality":      "one",
		}}),
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(attrID), "hello"}),
	)
	res, err := transact.Transact(ctx, db, mustCatalog(t, db, appID), appID, steps, transact.Options{Admin: true}, nil)
	if err != nil {
		t.Fatalf("Transact: %v", err)
	}
	if res.TxID <= 0 {
		t.Fatalf("bad tx id %d", res.TxID)
	}

	cat2, err := platform.LoadAttrCatalog(ctx, db.Pool, appID)
	if err != nil {
		t.Fatal(err)
	}
	a, ok := cat2.ByID(attrID)
	if !ok {
		t.Fatalf("attr %s missing after commit", uuidStr(attrID))
	}
	if a.ValueType != "blob" || *a.Etype != "wireattr" || *a.Label != "name" {
		t.Fatalf("stored attr wrong: %+v", a)
	}
}

func TestAddAttrReplayIsNoop(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)

	appID, _, _ := seed(t, db)

	payload := map[string]any{
		"id":               uuidStr(rand16()),
		"forward-identity": []string{uuidStr(rand16()), "wireattr", "count"},
		"value-type":       "number",
		"cardinality":      "one",
	}
	step := mustJSON(t, []any{"add-attr", payload})
	steps := parseSteps(t, step, step)
	if _, err := transact.Transact(ctx, db, mustCatalog(t, db, appID), appID, steps, transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("replayed add-attr must be a no-op, got: %v", err)
	}
}

func mustCatalog(t *testing.T, db *storage.DB, appID [16]byte) *platform.AttrCatalog {
	t.Helper()
	cat, err := platform.LoadAttrCatalog(context.Background(), db.Pool, appID)
	if err != nil {
		t.Fatal(err)
	}
	return cat
}

// The TS client always provisions the implicit <etype>/id attr. When the
// server already stores it under a different id, add-attr must ADOPT the
// stored row and rewrite same-batch triple references (client _rewriteMutations
// equivalent server-side).
func TestAddAttrAdoptsExistingIdent(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)

	appID, _, ids := seed(t, db)
	existingID := ids.name // seeded todos/name attr under a server id
	clientMinted := rand16()
	eid := rand16()

	steps := parseSteps(t,
		mustJSON(t, []any{"add-attr", map[string]any{
			"id":               uuidStr(clientMinted),
			"forward-identity": []string{uuidStr(rand16()), "todos", "name"},
			"value-type":       "blob",
			"cardinality":      "one",
		}}),
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(clientMinted), "via-alias"}),
	)
	if _, err := transact.Transact(ctx, db, mustCatalog(t, db, appID), appID, steps, transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("Transact with adopted attr: %v", err)
	}

	// The triple must be stored against the SERVER's attr id.
	rows, _ := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{eid}})
	if len(rows) == 0 {
		t.Fatalf("no triples for entity")
	}
	found := false
	for _, r := range rows {
		if r.Triple.A == existingID {
			found = true
		}
	}
	if !found {
		t.Fatalf("triple not stored under adopted attr id %s", existingID)
	}
}

// --- Security wave S2: attr metadata validation ---

func TestAddAttrRejectsUnsafeIdentNames(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	appID, cat, _ := seed(t, db)

	cases := []struct{ name, etype, label string }{
		{"oversized label", "todos", strings.Repeat("x", 300)},
		{"space in label", "todos", "my label"},
		{"newline injection", "todos", "a\nb"},
		{"unicode label", "todos", "ラベル"},
		{"oversized etype", strings.Repeat("e", 300), "ok"},
		{"space in etype", "my todos", "ok"},
	}
	for _, tc := range cases {
		attrID := rand16()
		steps := parseSteps(t,
			mustJSON(t, []any{"add-attr", map[string]any{
				"id":               uuidStr(attrID),
				"forward-identity": []string{uuidStr(attrID), tc.etype, tc.label},
				"value-type":       "blob",
				"cardinality":      "one",
			}}),
		)
		if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{}, nil); err == nil {
			t.Fatalf("%s: add-attr must be rejected", tc.name)
		}
	}
}

func TestAddAttrRejectsUnknownValueType(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	appID, cat, _ := seed(t, db)
	attrID := rand16()
	steps := parseSteps(t,
		mustJSON(t, []any{"add-attr", map[string]any{
			"id":               uuidStr(attrID),
			"forward-identity": []string{uuidStr(attrID), "todos", "weird"},
			"value-type":       "banana",
			"cardinality":      "one",
		}}),
	)
	if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{}, nil); err == nil {
		t.Fatal("unknown value-type must be rejected")
	}
}

func TestAddAttrRejectsReservedOrEmptyReverseIdentity(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	appID, cat, _ := seed(t, db)

	cases := []struct {
		name string
		rev  []string
	}{
		{"reserved reverse etype", []string{uuidStr(rand16()), "$users", "sneaky"}},
		{"empty reverse etype", []string{uuidStr(rand16()), "", "x"}},
		{"empty reverse label", []string{uuidStr(rand16()), "todos", ""}},
	}
	for _, tc := range cases {
		attrID := rand16()
		steps := parseSteps(t,
			mustJSON(t, []any{"add-attr", map[string]any{
				"id":               uuidStr(attrID),
				"forward-identity": []string{uuidStr(attrID), "links", "owner"},
				"value-type":       "ref",
				"cardinality":      "one",
				"reverse-identity": tc.rev,
			}}),
		)
		if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{Admin: true}, nil); err == nil {
			t.Fatalf("%s: must be rejected", tc.name)
		}
	}
}

func TestCheckedDataTypePersistedAndEnforced(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	appID, _, _ := seed(t, db)

	attrID := rand16()
	eid := rand16()
	addAttr := mustJSON(t, []any{"add-attr", map[string]any{
		"id":                uuidStr(attrID),
		"forward-identity":  []string{uuidStr(attrID), "cdtcheck", "score"},
		"value-type":        "blob",
		"cardinality":       "one",
		"checked-data-type": "number",
	}})
	steps := parseSteps(t, addAttr)
	if _, err := transact.Transact(ctx, db, mustCatalog(t, db, appID), appID, steps, transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("add-attr with checked-data-type: %v", err)
	}

	cat2 := mustCatalog(t, db, appID)
	a, ok := cat2.ByID(attrID)
	if !ok {
		t.Fatal("attr missing after commit")
	}
	if a.CheckedDataType == nil || *a.CheckedDataType != "number" {
		t.Fatalf("checked-data-type not persisted: %+v", a.CheckedDataType)
	}

	// Type-conflicting write must fail cleanly.
	bad := parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(attrID), "not-a-number"}),
	)
	if _, err := transact.Transact(ctx, db, cat2, appID, bad, transact.Options{Admin: true}, nil); err == nil {
		t.Fatal("string value into number-typed attr must be rejected")
	}

	good := parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(attrID), float64(42)}),
	)
	if _, err := transact.Transact(ctx, db, cat2, appID, good, transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("number value into number-typed attr must succeed: %v", err)
	}
}

// Audit L1 / adversarial-review gap: tx batches are capped so permission
// probes can't pin a writer connection for O(unbounded) work.
func TestTransactRejectsExcessiveSteps(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	appID, cat, ids := seed(t, db)

	steps := make([]transact.Step, 0, 10001)
	for i := 0; i <= 10000; i++ {
		steps = append(steps, parseSteps(t,
			mustJSON(t, []any{"add-triple", uuidStr(rand16()), uuidToStr(ids.name), "x"}),
		)[0])
	}
	if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{}, nil); err == nil {
		t.Fatal("over-cap tx must be rejected")
	}
}

// Audit M3: value-position lookups require a unique attr — non-unique
// matches made link targets nondeterministic.
func TestValueLookupRequiresUniqueAttr(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	appID, _, ids := seed(t, db)

	var tagAttr platform.Attr
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		var e error
		tagAttr, e = platform.GetOrCreateAttr(ctx, tx, appID, "todos", "tag", "blob", "many", false, true)
		return e
	}); err != nil {
		t.Fatal(err)
	}
	cat2 := mustCatalog(t, db, appID)
	setup := parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(rand16()), uuidStr(tagAttr.ID), "dup"}),
		mustJSON(t, []any{"add-triple", uuidStr(rand16()), uuidStr(tagAttr.ID), "dup"}),
	)
	if _, err := transact.Transact(ctx, db, cat2, appID, setup, transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("setup two dup tags: %v", err)
	}

	lookupRef := []any{uuidStr(tagAttr.ID), "dup"}
	src := rand16()
	steps := parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(src), uuidToStr(ids.link), lookupRef}),
	)
	if _, err := transact.Transact(ctx, db, cat2, appID, steps, transact.Options{Admin: true}, nil); err == nil {
		t.Fatal("value-position lookup on non-unique attr must be rejected")
	}
}
