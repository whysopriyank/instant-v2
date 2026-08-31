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
		t.Fatalf("expected both merge steps before add, got %+v", rows)
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
