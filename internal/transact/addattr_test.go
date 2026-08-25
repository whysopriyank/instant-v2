package transact_test

import (
	"context"
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
