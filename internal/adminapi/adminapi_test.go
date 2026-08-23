package adminapi_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/instant-v2/instant-v2/internal/adminapi"
	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/transact"
	"github.com/instant-v2/instant-v2/internal/triple"
)

func env(t *testing.T) (*adminapi.Handler, *storage.DB, [16]byte, string, func()) {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	sqldb, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public;`); err != nil {
		t.Fatal(err)
	}
	if err := platform.Migrate(ctx, sqldb); err != nil {
		t.Fatal(err)
	}
	st := storage.New(pool)
	cats := platform.NewCatalogCache(pool, pool)
	appID := newUUID()
	token := newUUID()
	err = st.WithTx(ctx, func(tx pgx.Tx) error {
		creator := newUUID()
		if _, e := tx.Exec(ctx, `INSERT INTO instant_users (id,email) VALUES ($1,$2)`, creator, "a@test"); e != nil {
			return e
		}
		if e := platform.CreateApp(ctx, tx, creator, appID, "admin-test"); e != nil {
			return e
		}
		return platform.SetAdminToken(ctx, tx, appID, token)
	})
	if err != nil {
		t.Fatal(err)
	}
	h := &adminapi.Handler{Pool: pool, DB: st, Catalogs: cats}
	cleanup := func() { pool.Close(); _ = sqldb.Close() }
	return h, st, appID, uuidStr(token), cleanup
}

func newUUID() [16]byte {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return b
}

func be32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func be16(b []byte) uint16 { return uint16(b[0])<<8 | uint16(b[1]) }

func uuidStr(u [16]byte) string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		be32(u[0:4]), be16(u[4:6]), be16(u[6:8]), be16(u[8:10]), u[10:16])
}

// do issues one request against h with headers and an optional JSON body.
func do(t *testing.T, h *adminapi.Handler, method, path string, headers map[string]string, body any) (int, map[string]any) {
	t.Helper()
	var rd *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	} else {
		rd = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rd)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

func authHeaders(token string) map[string]string {
	return map[string]string{"X-admin-token": token}
}

func bodyApp(appID [16]byte, extra map[string]any) map[string]any {
	m := map[string]any{"app-id": uuidStr(appID)}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

// seedAttr creates a todos.name blob attr and returns its id plus a fresh cat.
func seedAttr(t *testing.T, h *adminapi.Handler, db *storage.DB, appID [16]byte) ([16]byte, *platform.AttrCatalog) {
	t.Helper()
	ctx := context.Background()
	var attr platform.Attr
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		var e error
		attr, e = platform.GetOrCreateAttr(ctx, tx, appID, "todos", "name", "blob", "one", false, true)
		return e
	}); err != nil {
		t.Fatal(err)
	}
	h.Catalogs.Invalidate(uuidStr(appID))
	cat, err := h.Catalogs.For(ctx, uuidStr(appID))
	if err != nil {
		t.Fatal(err)
	}
	return attr.ID, cat
}

const denyAllRules = `{"$default":{"allow":{"$default":"false"}}}`

// TestAdminBypass proves the admin plane ignores permission rules: the same
// add-triple steps that a non-admin transact.Transact call denies under a
// deny-everything rule doc succeed through POST /admin/transact.
func TestAdminBypass(t *testing.T) {
	h, db, appID, token, cleanup := env(t)
	defer cleanup()
	ctx := context.Background()

	attrID, cat := seedAttr(t, h, db, appID)
	entity := newUUID()
	steps := [][]any{{"add-triple", uuidStr(entity), uuidStr(attrID), "hello"}}

	code, resp := do(t, h, "POST", "/admin/transact", authHeaders(token),
		bodyApp(appID, map[string]any{"steps": steps, "rules": json.RawMessage(denyAllRules)}))
	if code != 200 {
		t.Fatalf("admin transact: %d %v", code, resp)
	}
	txID, ok := resp["tx-id"].(float64)
	if !ok || txID <= 0 {
		t.Fatalf("expected positive tx-id, got %v", resp["tx-id"])
	}

	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{entity}})
	if err != nil || len(rows) != 1 || rows[0].Triple.V != "hello" {
		t.Fatalf("triple not persisted: %v %v %+v", err, len(rows), rows)
	}

	// The same steps under the same rules are denied for a non-admin caller
	// (transact.Options{}), proving admin truly bypassed the CEL gate.
	doc, err := perms.ParseRuleDoc([]byte(denyAllRules))
	if err != nil {
		t.Fatal(err)
	}
	rawSteps, _ := json.Marshal(steps)
	var raws []json.RawMessage
	if err := json.Unmarshal(rawSteps, &raws); err != nil {
		t.Fatal(err)
	}
	parsed, err := transact.ParseSteps(raws)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transact.Transact(ctx, db, cat, appID, parsed, transact.Options{}, doc); err == nil {
		t.Fatal("non-admin transact should be denied by the deny-all rule doc")
	}
}

func TestAdminAuthRejectsBadToken(t *testing.T) {
	h, _, appID, token, cleanup := env(t)
	defer cleanup()
	appStr := uuidStr(appID)
	body := map[string]any{"app-id": appStr, "query": map[string]any{"todos": map[string]any{}}}

	// wrong token
	code, resp := do(t, h, "POST", "/admin/query",
		map[string]string{"X-admin-token": "00000000-0000-4000-8000-000000000000"}, body)
	if code != 401 {
		t.Fatalf("wrong token: %d %v", code, resp)
	}
	// missing token
	code, _ = do(t, h, "POST", "/admin/query", nil, body)
	if code != 401 {
		t.Fatalf("missing token: %d", code)
	}
	// bad bearer header form
	code, _ = do(t, h, "POST", "/admin/query",
		map[string]string{"Authorization": "Bearer not-a-uuid"}, body)
	if code != 401 {
		t.Fatalf("bad bearer: %d", code)
	}
	// unknown app id → 404 even with a syntactically valid token
	code, resp = do(t, h, "POST", "/admin/query", authHeaders(token),
		map[string]any{"app-id": "11111111-2222-4333-8444-555555555555", "query": map[string]any{}})
	if code != 404 {
		t.Fatalf("unknown app: %d %v", code, resp)
	}
	// malformed app-id → 400
	code, _ = do(t, h, "POST", "/admin/query", authHeaders(token),
		map[string]any{"app-id": "zzz"})
	if code != 400 {
		t.Fatalf("malformed app-id: %d", code)
	}
}

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

// TestAdminUserRoutes walks the delegated authn envelopes end-to-end:
// sign_in_guest → users list → magic_code/verify_magic_code → refresh_tokens →
// sign_out → bulk delete.
func TestAdminUserRoutes(t *testing.T) {
	h, _, appID, token, cleanup := env(t)
	defer cleanup()
	appStr := uuidStr(appID)

	// sign_in_guest
	code, guest := do(t, h, "POST", "/admin/sign_in_guest", authHeaders(token), bodyApp(appID, nil))
	if code != 200 {
		t.Fatalf("sign_in_guest: %d %v", code, guest)
	}
	userMap, _ := guest["user"].(map[string]any)
	if userMap["refresh_token"] == "" || userMap["type"] != "guest" {
		t.Fatalf("guest envelope: %v", guest)
	}

	// users list (limit param)
	code, resp := do(t, h, "GET", "/admin/users?limit=10&offset=0&app-id="+appStr, authHeaders(token), nil)
	if code != 200 {
		t.Fatalf("users list: %d %v", code, resp)
	}
	usersJSON, _ := json.Marshal(resp["users"])
	var users []map[string]any
	_ = json.Unmarshal(usersJSON, &users)
	if len(users) != 1 || users[0]["type"] != "guest" {
		t.Fatalf("users list: %v", resp)
	}
	var guestID string
	_ = json.Unmarshal([]byte(users[0]["id"].(string)), &guestID)
	guestID = users[0]["id"].(string)

	// sign_out via the minted refresh token
	code, resp = do(t, h, "POST", "/admin/sign_out", authHeaders(token),
		map[string]any{"app-id": appStr, "refresh_token": userMap["refresh_token"]})
	if code != 200 || resp["ok"] != true {
		t.Fatalf("sign_out: %d %v", code, resp)
	}
	// sign_out with no selector → v1 message
	code, resp = do(t, h, "POST", "/admin/sign_out", authHeaders(token), map[string]any{"app-id": appStr})
	if code != 400 || !strings.Contains(resp["message"].(string), "`id`, `email`, or `refresh_token`") {
		t.Fatalf("sign_out validation: %d %v", code, resp)
	}

	// magic_code returns the generated 6-digit code; verify_magic_code burns it.
	email := "ada@test.example"
	code, resp = do(t, h, "POST", "/admin/magic_code", authHeaders(token),
		map[string]any{"app-id": appStr, "email": email})
	if code != 200 {
		t.Fatalf("magic_code: %d %v", code, resp)
	}
	magicCode, _ := resp["code"].(string)
	if len(magicCode) != 6 {
		t.Fatalf("expected 6-digit code, got %v", resp["code"])
	}
	code, verified := do(t, h, "POST", "/admin/verify_magic_code", authHeaders(token),
		map[string]any{"app-id": appStr, "email": email, "code": magicCode})
	if code != 200 {
		t.Fatalf("verify_magic_code: %d %v", code, verified)
	}
	vUser, _ := verified["user"].(map[string]any)
	if vUser["email"] != email || vUser["refresh_token"] == "" || verified["created"] != true {
		t.Fatalf("verify envelope: %v", verified)
	}

	// refresh_tokens creates then finds by email.
	code, created := do(t, h, "POST", "/admin/refresh_tokens", authHeaders(token),
		map[string]any{"app-id": appStr, "email": "grace@test.example"})
	if code != 200 || created["created"] != true {
		t.Fatalf("refresh_tokens create: %d %v", code, created)
	}
	if cu, _ := created["user"].(map[string]any); cu["refresh_token"] == "" {
		t.Fatalf("refresh_tokens envelope: %v", created)
	}
	code, existing := do(t, h, "POST", "/admin/refresh_tokens", authHeaders(token),
		map[string]any{"app-id": appStr, "email": "grace@test.example"})
	if code != 200 || existing["created"] != false {
		t.Fatalf("refresh_tokens find: %d %v", code, existing)
	}

	// Bulk delete the guest user; list must shrink back to the email users.

	code, del := do(t, h, "DELETE", "/admin/users", authHeaders(token),
		map[string]any{"app-id": appStr, "ids": []string{guestID}})
	if code != 200 {
		t.Fatalf("users delete: %d %v", code, del)
	}
	if n, _ := del["deleted"].(float64); n <= 0 {
		t.Fatalf("expected deleted > 0: %v", del)
	}
	code, resp = do(t, h, "GET", "/admin/users?app-id="+appStr, authHeaders(token), nil)
	if code != 200 {
		t.Fatalf("users relist: %d", code)
	}
	usersJSON, _ = json.Marshal(resp["users"])
	users = nil
	_ = json.Unmarshal(usersJSON, &users)
	for _, u := range users {
		if u["id"] == guestID {
			t.Fatalf("guest still listed after delete: %v", users)
		}
	}
}
