package adminapi_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/instant-v2/instant-v2/internal/storage"
)

const denyAllRules = `{"$default":{"allow":{"$default":"false"}}}`

// TestAdminPermsChecks exercises both dry-run routes: deny-all rules flip
// allowed=false and nothing is committed.
func TestAdminPermsChecks(t *testing.T) {
	h, db, appID, token, cleanup := env(t)
	defer cleanup()
	ctx := context.Background()

	attrID, _ := seedAttr(t, h, db, appID)
	entity := newUUID()
	steps := [][]any{{"add-triple", uuidStr(entity), uuidStr(attrID), "x"}}

	code, resp := do(t, h, "POST", "/admin/transact_perms_check", authHeaders(token),
		bodyApp(appID, map[string]any{"steps": steps, "rules": json.RawMessage(denyAllRules)}))
	if code != 200 {
		t.Fatalf("transact_perms_check: %d %v", code, resp)
	}
	if ok, _ := resp["all-checks-ok?"].(bool); ok {
		t.Fatalf("expected all-checks-ok?=false under deny-all rules: %v", resp)
	}
	if committed, _ := resp["committed?"].(bool); committed {
		t.Fatal("dry-run must never commit")
	}
	checks, _ := resp["check-results"].([]any)
	if len(checks) != 1 {
		t.Fatalf("expected one check result, got %v", checks)
	}

	// Nothing was written.
	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{entity}})
	if err != nil || len(rows) != 0 {
		t.Fatalf("dry-run wrote data: %v %+v", err, rows)
	}

	code, resp = do(t, h, "POST", "/admin/query_perms_check", authHeaders(token),
		bodyApp(appID, map[string]any{
			"query": map[string]any{"todos": map[string]any{}},
			"rules": json.RawMessage(denyAllRules),
		}))
	if code != 200 {
		t.Fatalf("query_perms_check: %d %v", code, resp)
	}
	checks, _ = resp["check-results"].([]any)
	if len(checks) != 1 {
		t.Fatalf("expected one check result, got %v", checks)
	}
	c := checks[0].(map[string]any)
	if c["etype"] != "todos" || c["action"] != "view" || c["allowed"] != false {
		t.Fatalf("unexpected check result: %v", c)
	}
}
