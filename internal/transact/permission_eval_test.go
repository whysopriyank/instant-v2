package transact_test

import (
	"context"
	"testing"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/transact"
)

func TestEvaluatePermissionsRollsBackJournalAndTriples(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seed(t, db)
	eid := uuidStr(rand16())
	steps := parseSteps(t, mustJSON(t, []any{"add-triple", eid, uuidToStr(ids.name), "candidate"}))

	allowDoc, err := perms.ParseRuleDoc([]byte(`{"todos":{"allow":{"create":"true"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	var beforeJournal int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE app_id = $1`, appID).Scan(&beforeJournal); err != nil {
		t.Fatal(err)
	}
	evaluation, err := transact.EvaluatePermissions(ctx, db, cat, appID, steps, transact.Options{}, allowDoc)
	if err != nil {
		t.Fatalf("evaluate permissions: %v", err)
	}
	if !evaluation.AllAllowed || len(evaluation.Checks) != 1 || !evaluation.Checks[0].Allowed {
		t.Fatalf("unexpected allow evaluation: %+v", evaluation)
	}
	var entityID [16]byte
	if err := platform.ScanUUID(eid, &entityID); err != nil {
		t.Fatal(err)
	}
	if rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{entityID}}); err != nil {
		t.Fatal(err)
	} else if len(rows) != 0 {
		t.Fatalf("permission evaluation wrote triples: %+v", rows)
	}
	var afterJournal int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE app_id = $1`, appID).Scan(&afterJournal); err != nil {
		t.Fatal(err)
	}
	if afterJournal != beforeJournal {
		t.Fatalf("permission evaluation wrote journal rows: before=%d after=%d", beforeJournal, afterJournal)
	}

	denyDoc, err := perms.ParseRuleDoc([]byte(`{"todos":{"allow":{"create":"false"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	evaluation, err = transact.EvaluatePermissions(ctx, db, cat, appID, steps, transact.Options{}, denyDoc)
	if err != nil {
		t.Fatalf("evaluate denied permissions: %v", err)
	}
	if evaluation.AllAllowed || len(evaluation.Checks) != 1 || evaluation.Checks[0].Allowed {
		t.Fatalf("unexpected deny evaluation: %+v", evaluation)
	}
}

func TestEvaluatePermissionsNilRuleDocAdminParity(t *testing.T) {
	db := testDB(t) // testkit.NewPostgres skips without an owned database
	ctx := context.Background()
	appID, cat, ids := seed(t, db)
	eid := uuidStr(rand16())
	steps := parseSteps(t, mustJSON(t, []any{"add-triple", eid, uuidToStr(ids.name), "candidate"}))

	var beforeJournal int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE app_id = $1`, appID).Scan(&beforeJournal); err != nil {
		t.Fatal(err)
	}
	evaluation, err := transact.EvaluatePermissions(ctx, db, cat, appID, steps, transact.Options{Admin: true}, nil)
	if err != nil {
		t.Fatalf("admin permission evaluation: %v", err)
	}
	if !evaluation.AllAllowed || len(evaluation.Checks) != 0 {
		t.Fatalf("unexpected admin evaluation: %+v", evaluation)
	}

	var entityID [16]byte
	if err := platform.ScanUUID(eid, &entityID); err != nil {
		t.Fatal(err)
	}
	if rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{entityID}}); err != nil {
		t.Fatal(err)
	} else if len(rows) != 0 {
		t.Fatalf("admin permission evaluation wrote triples: %+v", rows)
	}
	var afterJournal int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE app_id = $1`, appID).Scan(&afterJournal); err != nil {
		t.Fatal(err)
	}
	if afterJournal != beforeJournal {
		t.Fatalf("admin permission evaluation wrote journal rows: before=%d after=%d", beforeJournal, afterJournal)
	}

	if _, err := transact.EvaluatePermissions(ctx, db, cat, appID, steps, transact.Options{}, nil); err == nil {
		t.Fatal("non-admin permission evaluation with nil RuleDoc must fail closed")
	}
}

func TestEvaluatePermissionsFollowsRuntimeEntityBeforeAttributeGates(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seed(t, db)
	eid := uuidStr(rand16())
	steps := parseSteps(t,
		mustJSON(t, []any{"update-attr", map[string]any{
			"id": uuidToStr(ids.name), "required?": true,
		}}),
		mustJSON(t, []any{"add-triple", eid, uuidToStr(ids.name), "candidate"}),
		mustJSON(t, []any{"delete-attr", uuidToStr(ids.name)}),
	)
	doc, err := perms.ParseRuleDoc([]byte(`{"todos":{"allow":{"create":"true"}},"attrs":{"allow":{"update":"true"}}}`))
	if err != nil {
		t.Fatal(err)
	}

	var beforeJournal int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE app_id = $1`, appID).Scan(&beforeJournal); err != nil {
		t.Fatal(err)
	}
	beforeCat := mustCatalog(t, db, appID)
	beforeAttr, ok := beforeCat.ByID(ids.name)
	if !ok {
		t.Fatal("seeded name attr missing")
	}

	evaluation, err := transact.EvaluatePermissions(ctx, db, cat, appID, steps, transact.Options{}, doc)
	if err != nil {
		t.Fatalf("evaluate permissions: %v", err)
	}
	if evaluation.AllAllowed {
		t.Fatalf("mixed batch should be denied: %+v", evaluation)
	}
	if len(evaluation.Checks) != 3 {
		t.Fatalf("permission evidence count: got %d, want 3: %+v", len(evaluation.Checks), evaluation.Checks)
	}
	want := []struct {
		stepIndex int
		etype     string
		action    string
		allowed   bool
	}{
		{1, "todos", "create", true},
		{0, "attrs", "update", true},
		{2, "attrs", "delete", false},
	}
	for i, want := range want {
		got := evaluation.Checks[i]
		if got.StepIndex != want.stepIndex || got.Etype != want.etype || got.Action != want.action || got.Allowed != want.allowed {
			t.Fatalf("permission evidence[%d]: got %+v, want step=%d etype=%q action=%q allowed=%t", i, got, want.stepIndex, want.etype, want.action, want.allowed)
		}
	}
	if evaluation.Checks[1].Bindings.Data != nil || evaluation.Checks[1].Bindings.NewData != nil ||
		evaluation.Checks[1].Bindings.Auth != nil || evaluation.Checks[1].Bindings.RuleParams != nil {
		t.Fatalf("attrs.update bindings must be exact empty bindings: %+v", evaluation.Checks[1].Bindings)
	}

	var afterJournal int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE app_id = $1`, appID).Scan(&afterJournal); err != nil {
		t.Fatal(err)
	}
	if afterJournal != beforeJournal {
		t.Fatalf("permission evaluation wrote journal rows: before=%d after=%d", beforeJournal, afterJournal)
	}
	afterCat := mustCatalog(t, db, appID)
	afterAttr, ok := afterCat.ByID(ids.name)
	if !ok || afterAttr.IsRequired != beforeAttr.IsRequired {
		t.Fatalf("permission evaluation persisted attr metadata: before=%+v after=%+v", beforeAttr, afterAttr)
	}
	var entityID [16]byte
	if err := platform.ScanUUID(eid, &entityID); err != nil {
		t.Fatal(err)
	}
	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{entityID}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("permission evaluation persisted triples: %+v", rows)
	}
}
