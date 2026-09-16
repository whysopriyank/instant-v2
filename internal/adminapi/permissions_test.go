package adminapi_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/instant-v2/instant-v2/internal/adminapi"
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
	if resp["rule-source"] != "request-override" || resp["rule-version"] != nil {
		t.Fatalf("unexpected rule metadata: %v", resp)
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
	if _, ok := resp["result"]; ok {
		t.Fatalf("denied query must not return preview data: %v", resp)
	}
	if resp["rule-source"] != "request-override" || resp["rule-version"] != nil {
		t.Fatalf("unexpected query rule metadata: %v", resp)
	}
}

func TestAdminTransactPermsCheckPreservesCoordinatorChecks(t *testing.T) {
	h, _, appID, token, cleanup := env(t)
	defer cleanup()

	attrID, _ := seedAttr(t, h, h.DB, appID)
	eid := newUUID()
	steps := [][]any{
		{"add-triple", uuidStr(eid), uuidStr(attrID), "first"},
		{"add-triple", uuidStr(eid), uuidStr(attrID), "second"},
	}
	rules := `{"todos":{"allow":{"create":"true"}}}`

	code, resp := do(t, h, "POST", "/admin/transact_perms_check", authHeaders(token),
		bodyApp(appID, map[string]any{"steps": steps, "rules": json.RawMessage(rules)}))
	if code != 200 {
		t.Fatalf("transact_perms_check: %d %v", code, resp)
	}
	if resp["all-checks-ok?"] != true || resp["committed?"] != false || resp["tx-id"] != nil {
		t.Fatalf("unexpected dry-run envelope: %v", resp)
	}
	if resp["rule-source"] != "request-override" || resp["rule-version"] != nil {
		t.Fatalf("unexpected rule metadata: %v", resp)
	}
	checks, _ := resp["check-results"].([]any)
	if len(checks) != 2 {
		t.Fatalf("expected one result per coordinator check, got %v", checks)
	}
	for i, raw := range checks {
		check := raw.(map[string]any)
		if check["etype"] != "todos" || check["action"] != "create" || check["allowed"] != true {
			t.Fatalf("unexpected check %d: %v", i, check)
		}
		if _, ok := check["error"]; ok {
			t.Fatalf("unexpected error on allowed check %d: %v", i, check)
		}
	}
}

func setPersistedRules(t *testing.T, h *adminapi.Handler, appID [16]byte, rules string) {
	t.Helper()
	if _, err := h.Pool.Exec(context.Background(), `
		INSERT INTO rules (app_id, code, version) VALUES ($1, $2::jsonb, 0)
		ON CONFLICT (app_id) DO UPDATE SET code=EXCLUDED.code, version=EXCLUDED.version`, appID, rules); err != nil {
		t.Fatal(err)
	}
	h.Catalogs.Invalidate(uuidStr(appID))
}

func seedQueryPreviewData(t *testing.T, h *adminapi.Handler, appID [16]byte, token string) {
	t.Helper()
	code, resp := transactHTTP(t, h, appID, token, nil, []any{
		[]any{"update", "todos", uuidStr(newUUID()), map[string]any{"secret": "protected"}},
	})
	if code != 200 {
		t.Fatalf("seed preview data: %d %v", code, resp)
	}
}

func TestAdminQueryPermsCheckUsesPersistedRulesByDefault(t *testing.T) {
	h, _, appID, token, cleanup := env(t)
	defer cleanup()
	seedQueryPreviewData(t, h, appID, token)
	setPersistedRules(t, h, appID, `{"todos":{"allow":{"view":"true"}}}`)

	code, resp := do(t, h, "POST", "/admin/query_perms_check", authHeaders(token),
		bodyApp(appID, map[string]any{"query": map[string]any{"todos": map[string]any{}}}))
	if code != 200 {
		t.Fatalf("query_perms_check: %d %v", code, resp)
	}
	if resp["rule-source"] != "persisted" || resp["rule-version"] != float64(0) {
		t.Fatalf("unexpected persisted rule metadata: %v", resp)
	}
	if _, ok := resp["result"]; !ok {
		t.Fatalf("allowed persisted query must include preview data: %v", resp)
	}
}

func TestAdminQueryPermsCheckExplicitRulesOverridePersistedSource(t *testing.T) {
	h, _, appID, token, cleanup := env(t)
	defer cleanup()
	seedQueryPreviewData(t, h, appID, token)
	setPersistedRules(t, h, appID, `{"todos":{"allow":{"view":"false"}}}`)

	code, resp := do(t, h, "POST", "/admin/query_perms_check", authHeaders(token),
		bodyApp(appID, map[string]any{
			"query": map[string]any{"todos": map[string]any{}},
			"rules": json.RawMessage(`{"todos":{"allow":{"view":"true"}}}`),
		}))
	if code != 200 {
		t.Fatalf("query_perms_check: %d %v", code, resp)
	}
	if resp["rule-source"] != "request-override" || resp["rule-version"] != nil {
		t.Fatalf("unexpected override rule metadata: %v", resp)
	}
	if _, ok := resp["result"]; !ok {
		t.Fatalf("allowed override query must include preview data: %v", resp)
	}
}

func TestAdminQueryPermsCheckDynamicRulesHaveNoPreview(t *testing.T) {
	h, _, appID, token, cleanup := env(t)
	defer cleanup()
	seedQueryPreviewData(t, h, appID, token)

	code, resp := do(t, h, "POST", "/admin/query_perms_check", authHeaders(token),
		bodyApp(appID, map[string]any{
			"query": map[string]any{"todos": map[string]any{}},
			"rules": json.RawMessage(`{"todos":{"allow":{"view":"auth.id != null"}}}`),
		}))
	if code != 200 {
		t.Fatalf("query_perms_check: %d %v", code, resp)
	}
	checks, _ := resp["check-results"].([]any)
	if len(checks) != 1 || checks[0].(map[string]any)["allowed"] != false {
		t.Fatalf("dynamic view must fail its static check: %v", resp)
	}
	if _, ok := resp["result"]; ok {
		t.Fatalf("dynamic query must not return preview data: %v", resp)
	}
}

func TestAdminQueryPermsCheckNestedDynamicRulesHaveNoPreview(t *testing.T) {
	h, _, appID, token, cleanup := env(t)
	defer cleanup()
	seedQueryPreviewData(t, h, appID, token)

	code, resp := do(t, h, "POST", "/admin/query_perms_check", authHeaders(token),
		bodyApp(appID, map[string]any{
			"query": map[string]any{"todos": map[string]any{"comments": map[string]any{}}},
			"rules": json.RawMessage(`{"todos":{"allow":{"view":"true"}},"comments":{"allow":{"view":"auth.id != null"}}}`),
		}))
	if code != 200 {
		t.Fatalf("query_perms_check: %d %v", code, resp)
	}
	checks, _ := resp["check-results"].([]any)
	if len(checks) != 2 || checks[1].(map[string]any)["allowed"] != false {
		t.Fatalf("nested dynamic view must fail closed: %v", resp)
	}
	if _, ok := resp["result"]; ok {
		t.Fatalf("nested dynamic query must not return preview data: %v", resp)
	}
}

func TestAdminQueryPermsCheckMissingPersistedRulesFailsClosed(t *testing.T) {
	h, _, appID, token, cleanup := env(t)
	defer cleanup()
	seedQueryPreviewData(t, h, appID, token)

	code, resp := do(t, h, "POST", "/admin/query_perms_check", authHeaders(token),
		bodyApp(appID, map[string]any{"query": map[string]any{"todos": map[string]any{}}}))
	if code == 200 {
		t.Fatalf("missing persisted rules must fail closed: %v", resp)
	}
	if _, ok := resp["result"]; ok {
		t.Fatalf("missing persisted rules leaked preview data: %v", resp)
	}
}

func TestAdminQueryPermsCheckNullRulesFailsClosed(t *testing.T) {
	h, _, appID, token, cleanup := env(t)
	defer cleanup()
	seedQueryPreviewData(t, h, appID, token)
	setPersistedRules(t, h, appID, `{"todos":{"allow":{"view":"true"}}}`)

	code, resp := do(t, h, "POST", "/admin/query_perms_check", authHeaders(token),
		bodyApp(appID, map[string]any{
			"query": map[string]any{"todos": map[string]any{}},
			"rules": nil,
		}))
	if code == 200 {
		t.Fatalf("null explicit rules must fail closed: %v", resp)
	}
	if _, ok := resp["result"]; ok {
		t.Fatalf("null explicit rules leaked preview data: %v", resp)
	}
}

func TestAdminQueryPermsCheckPersistedNullRulesFailClosed(t *testing.T) {
	h, _, appID, token, cleanup := env(t)
	defer cleanup()
	seedQueryPreviewData(t, h, appID, token)
	setPersistedRules(t, h, appID, `null`)

	code, resp := do(t, h, "POST", "/admin/query_perms_check", authHeaders(token),
		bodyApp(appID, map[string]any{"query": map[string]any{"todos": map[string]any{}}}))
	if code == 200 {
		t.Fatalf("persisted null rules must fail closed: %v", resp)
	}
	if _, ok := resp["result"]; ok {
		t.Fatalf("persisted null rules leaked preview data: %v", resp)
	}
}

func TestAdminTransactPermsCheckPersistedNullRulesFailClosed(t *testing.T) {
	h, _, appID, token, cleanup := env(t)
	defer cleanup()
	setPersistedRules(t, h, appID, `null`)

	attrID, _ := seedAttr(t, h, h.DB, appID)
	steps := [][]any{{"add-triple", uuidStr(newUUID()), uuidStr(attrID), "blocked"}}
	code, resp := do(t, h, "POST", "/admin/transact_perms_check", authHeaders(token),
		bodyApp(appID, map[string]any{"steps": steps}))
	if code == 200 {
		t.Fatalf("persisted null rules must fail closed: %v", resp)
	}
}
