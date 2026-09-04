package transact_test

import (
	"context"
	"testing"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/transact"
)

func TestTransactGroupsOpsByFirstAppearance(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seed(t, db)
	eid := rand16()
	// deep-merge(first: true) -> add(last: true) -> deep-merge(second: true)
	// Operation groups run in first-appearance order: both deep merges run
	// before the add, so the add is the final cardinality-one value.
	steps := parseSteps(t,
		mustJSON(t, []any{"deep-merge-triple", uuidStr(eid), uuidStr(ids.name), map[string]any{"first": true}}),
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(ids.name), map[string]any{"last": true}}),
		mustJSON(t, []any{"deep-merge-triple", uuidStr(eid), uuidStr(ids.name), map[string]any{"second": true}}),
	)
	if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{Admin: true}, nil); err != nil {
		t.Fatal(err)
	}
	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{eid}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Triple.E != eid || rows[0].Triple.A != ids.name || canonical(t, rows[0].Triple.V) != `{"last":true}` {
		t.Fatalf("expected first-appearance operation grouping, got %+v", rows)
	}
}

func TestTransactBatchingAndRollback(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seed(t, db)
	eid1 := rand16()
	eid2 := rand16()

	// Same-op add-triples execute together.
	steps := parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(eid1), uuidStr(ids.name), "first"}),
		mustJSON(t, []any{"add-triple", uuidStr(eid2), uuidStr(ids.name), "second"}),
	)
	if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{Admin: true}, nil); err != nil {
		t.Fatal(err)
	}
	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{eid1, eid2}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows from same-operation batch, got %d", len(rows))
	}

	// Rollback verification: batch error rolls back all effects
	failingSteps := parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(eid1), uuidStr(ids.name), "should-rollback"}),
		mustJSON(t, []any{"delete-attr", uuidStr(ids.name)}), // non-admin delete fails
	)
	if _, err := transact.Transact(ctx, db, cat, appID, failingSteps, transact.Options{Admin: false}, nil); err == nil {
		t.Fatal("expected error on unauthorized delete-attr")
	}
	rowsAfter, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{eid1}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rowsAfter) != 1 || rowsAfter[0].Triple.V != "first" {
		t.Fatalf("expected rollback to preserve 'first', got: %+v", rowsAfter)
	}
}

func TestTransactCardinalityOneBatchUsesLastPayloadValue(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seed(t, db)
	eid := rand16()

	steps := parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(ids.name), "first"}),
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(ids.name), "last"}),
	)
	if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{Admin: true}, nil); err != nil {
		t.Fatal(err)
	}

	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{
		EntityIDs: [][16]byte{eid}, AttrIDs: [][16]byte{ids.name},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Triple.V != "last" {
		t.Fatalf("cardinality-one batch winner: got %+v, want one row with last value", rows)
	}
}

// Wire rule-params steps currently do not override Options.RuleParams. This
// characterizes the existing gate before deleting its unused capture map.
func TestTransactRuleParamsUsesOptions(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seed(t, db)
	eid := rand16()
	doc, err := perms.ParseRuleDoc([]byte(`{"todos":{"allow":{"create":"ruleParams.allow"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	steps := parseSteps(t,
		mustJSON(t, []any{"rule-params", uuidStr(eid), map[string]any{"allow": false}}),
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(ids.name), "allowed"}),
	)
	if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{RuleParams: map[string]any{"allow": true}}, doc); err != nil {
		t.Fatal(err)
	}
	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{eid}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Triple.E != eid || rows[0].Triple.A != ids.name || rows[0].Triple.V != "allowed" {
		t.Fatalf("unexpected authorized state: %+v", rows)
	}
}

func TestTransactOrderingRetractAdd(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seed(t, db)
	eid := rand16()
	dummy := rand16()

	// Seed eid with v2 in storage
	initSteps := parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(ids.name), "v2"}),
	)
	if _, err := transact.Transact(ctx, db, cat, appID, initSteps, transact.Options{Admin: true}, nil); err != nil {
		t.Fatal(err)
	}

	// Payload order: add(dummy) -> retract(v2) -> add(v2)
	// Under first-appearance grouping, add-triple runs first, so add(v2) runs
	// before retract(v2), leaving v2 retracted.
	steps := parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(dummy), uuidStr(ids.name), "dummy"}),
		mustJSON(t, []any{"retract-triple", uuidStr(eid), uuidStr(ids.name), "v2"}),
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(ids.name), "v2"}),
	)
	if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{Admin: true}, nil); err != nil {
		t.Fatal(err)
	}

	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{eid}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected v2 retracted by first-appearance grouping, got: %+v", rows)
	}
}

func TestTransactOrderingAddRetractAdd(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seed(t, db)
	eid := rand16()

	// Payload order: add(v1) -> retract(v1) -> add(v1). Both add(v1) steps
	// execute in the first-appearance add group, and retract(v1) executes last,
	// leaving the entity empty.
	steps := parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(ids.name), "v1"}),
		mustJSON(t, []any{"retract-triple", uuidStr(eid), uuidStr(ids.name), "v1"}),
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(ids.name), "v1"}),
	)
	if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{Admin: true}, nil); err != nil {
		t.Fatal(err)
	}

	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{eid}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected v1 retracted by first-appearance grouping, got: %+v", rows)
	}
}
