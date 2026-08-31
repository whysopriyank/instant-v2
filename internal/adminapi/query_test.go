package adminapi_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/instant-v2/instant-v2/internal/triple"
)

// TestAdminQueryAndSchema seeds triples directly via storage, then reads them
// back through /admin/query and /admin/schema.
func TestAdminQueryAndSchema(t *testing.T) {
	h, db, appID, token, cleanup := env(t)
	defer cleanup()
	ctx := context.Background()

	attrID, cat := seedAttr(t, h, db, appID)
	e1, e2 := newUUID(), newUUID()
	_, err := db.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: e1, A: attrID, V: "one"},
		{E: e2, A: attrID, V: "two"},
	}, false)
	if err != nil {
		t.Fatal(err)
	}

	code, resp := do(t, h, "POST", "/admin/query", authHeaders(token),
		bodyApp(appID, map[string]any{"query": map[string]any{"todos": map[string]any{}}}))
	if code != 200 {
		t.Fatalf("query: %d %v", code, resp)
	}
	todosJSON, _ := json.Marshal(resp["todos"]) // bare object-tree, no "data" wrapper
	var todos []map[string]any
	if err := json.Unmarshal(todosJSON, &todos); err != nil {
		t.Fatalf("bare todos shape: %v (%v)", err, resp["todos"])
	}
	if len(todos) != 2 {
		t.Fatalf("expected 2 todos, got %d", len(todos))
	}

	code, resp = do(t, h, "GET", "/admin/schema?app-id="+uuidStr(appID), authHeaders(token), nil)
	if code != 200 {
		t.Fatalf("schema: %d %v", code, resp)
	}
	schema, _ := resp["schema"].(map[string]any)
	attrsJSON, _ := json.Marshal(schema["attrs"])
	var attrs []map[string]any
	if err := json.Unmarshal(attrsJSON, &attrs); err != nil {
		t.Fatalf("schema.attrs shape: %v", err)
	}
	// WireAttrs shape (docs/03 §7): forward-identity is [id, etype, label].
	found := false
	for _, a := range attrs {
		fi, _ := json.Marshal(a["forward-identity"])
		var parts []any
		if err := json.Unmarshal(fi, &parts); err == nil && len(parts) == 3 {
			if parts[1] == "todos" && parts[2] == "name" {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("schema missing todos.name: %v", attrs)
	}
}
