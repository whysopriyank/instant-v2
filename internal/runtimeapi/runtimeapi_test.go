package runtimeapi_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/instant-v2/instant-v2/internal/authn"
	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/runtimeapi"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/triple"
)

// env mirrors internal/authn's fixture: fresh pool, dropped schema, migrated
// catalog, one seeded instant_user + app.
func env(t *testing.T) (*authn.Service, *runtimeapi.Handler, [16]byte, func()) {
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
	err = st.WithTx(ctx, func(tx pgx.Tx) error {
		creator := newUUID()
		if _, e := tx.Exec(ctx, `INSERT INTO instant_users (id,email) VALUES ($1,$2)`, creator, "a@test"); e != nil {
			return e
		}
		return platform.CreateApp(ctx, tx, creator, appID, "runtimeapi-test")
	})
	if err != nil {
		t.Fatal(err)
	}
	svc := &authn.Service{DB: st, Pool: pool, Catalogs: cats}
	h := &runtimeapi.Handler{Pool: pool, DB: st, Catalogs: cats, Auth: svc}
	cleanup := func() { pool.Close(); _ = sqldb.Close() }
	return svc, h, appID, cleanup
}

func newUUID() [16]byte {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return b
}

func uuidStr(u [16]byte) string {
	return platform.UUIDToStr(u)
}

func do(t *testing.T, h httpHandler, method, path string, body any, headers map[string]string) (int, map[string]any) {
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

func post(t *testing.T, h httpHandler, path string, body map[string]any) (int, map[string]any) {
	t.Helper()
	return do(t, h, "POST", path, body, nil)
}

type httpHandler interface {
	ServeHTTP(w http.ResponseWriter, r *http.Request)
}

type captureMailer struct{ code string }

func (m *captureMailer) SendMagicCode(_ context.Context, _, code string) error {
	m.code = code
	return nil
}

// mintUser runs the magic-code flow through the service with a capture
// mailer and returns the fresh refresh token + user id.
func mintUser(t *testing.T, svc *authn.Service, appID [16]byte, email string) (token, userID string) {
	t.Helper()
	mailer := &captureMailer{}
	svc.Mailer = mailer
	if err := svc.SendMagicCode(context.Background(), appID, email); err != nil {
		t.Fatal(err)
	}
	res, err := svc.VerifyMagicCode(context.Background(), appID, email, mailer.code, "", nil, false)
	if err != nil {
		t.Fatalf("verify %s: %v", email, err)
	}
	user := res["user"].(map[string]any)
	return user["refresh_token"].(string), user["id"].(string)
}

// TestFieldSpellings pins the EXACT wire keys each runtime endpoint accepts,
// copied from v1 sources (server/src/instant/runtime/routes.clj and the
// authAPI client surface). Any spelling drift (refresh-token vs refresh_tokens
// vs refresh_token, extra-fields vs extra_fields, code_verifier, …) must fail
// loudly here.
func TestFieldSpellings(t *testing.T) {
	// endpoint → accepted request keys (body unless noted header:/query:).
	spellings := map[string][]string{
		"POST /runtime/auth/send_magic_code":      {"email", "app-id"},
		"POST /runtime/auth/verify_magic_code":    {"email", "code", "app-id", "extra-fields", "refresh-token"},
		"POST /runtime/auth/sign_in_guest":        {"app-id", "extra-fields"},
		"POST /runtime/auth/verify_refresh_token": {"refresh-token", "app-id"},
		"POST /runtime/auth/sign_out":             {"app_id", "refresh_token"},  // snake_case quirk
		"POST /runtime/auth/refresh_tokens":       {"refresh_tokens", "app-id"}, // PLURAL list
		"POST /runtime/signout":                   {"app_id", "refresh_token"},  // snake_case quirk
		"POST /runtime/framework/query":           {"query", "refresh-token", "header:app-id", "query-param:app_id"},
		"GET  /runtime/openid-configuration":      {"path:app_id", "header:app-id", "query-param:app_id"},
	}

	// Wrong-spelling probes that must be rejected as missing fields BEFORE any
	// database access — safe to run without DATABASE_URL.
	rejects := []struct {
		endpoint string
		method   string
		path     string
		body     map[string]any // deliberately wrong spellings of every field
		headers  map[string]string
	}{
		{
			endpoint: "POST /runtime/auth/refresh_tokens",
			method:   "POST", path: "/runtime/auth/refresh_tokens",
			body: map[string]any{"refresh-token": []any{"not-checked"}, "refresh_token": []any{"not-checked"}},
		},
		{
			endpoint: "POST /runtime/signout",
			method:   "POST", path: "/runtime/signout",
			body: map[string]any{"refresh-token": "x", "app-id": "y"},
		},
		{
			endpoint: "POST /runtime/framework/query",
			method:   "POST", path: "/runtime/framework/query",
			headers: map[string]string{"app-id": uuidStr(newUUID())},
			body:    map[string]any{"Query": map[string]any{}, "q": map[string]any{}},
		},
	}

	h := &runtimeapi.Handler{}
	for _, c := range rejects {
		code, resp := do(t, h, c.method, c.path, c.body, c.headers)
		if code != http.StatusBadRequest {
			t.Errorf("%s: wrong-spelling keys %v must be rejected with 400, got %d %v", c.endpoint, c.body, code, resp)
			continue
		}
		if _, ok := resp["message"]; !ok {
			t.Errorf("%s: rejection must carry {\"message\":…}, got %v", c.endpoint, resp)
		}
	}
	for _, c := range rejects {
		accepted := spellings[c.endpoint]
		if accepted == nil {
			t.Fatalf("probe for unlisted endpoint %s", c.endpoint)
		}
		for key := range c.body {
			for _, ok := range accepted {
				if key == ok {
					t.Errorf("probe key %q for %s is an ACCEPTED spelling; probe must use a wrong variant", key, c.endpoint)
				}
			}
		}
	}

	// Every endpoint this package mounts must exist on the mux; the authn
	// spellings above are pinned for documentation and are served by
	// internal/authn.Handler (asserted in that package's tests).
	for _, endpoint := range []string{
		"POST /runtime/auth/refresh_tokens",
		"POST /runtime/signout",
		"POST /runtime/framework/query",
		"GET /runtime/openid-configuration",
	} {
		fields := strings.Fields(endpoint)
		method, path := fields[0], fields[1]
		code, _ := do(t, h, method, path, map[string]any{}, nil)
		if code == http.StatusNotFound {
			t.Errorf("%s: routed to 404; endpoint missing from mux", endpoint)
		}
	}
}

// TestFrameworkQuery seeds todos and asserts the HTTP payload round-trips the
// instaql executor result exactly (v1 framework-query-triples envelope).
func TestFrameworkQuery(t *testing.T) {
	svc, h, appID, cleanup := env(t)
	defer cleanup()
	appStr := uuidStr(appID)

	ctx := context.Background()
	// Load directly (not through the shared cache) so seeding never pins a
	// stale pre-attr snapshot into CatalogCache.
	pre, err := platform.LoadAttrCatalog(ctx, svc.Pool, appID)
	if err != nil {
		t.Fatal(err)
	}
	titleAttr, ok := findOrCreateAttr(t, svc, ctx, appID, pre, "todos", "title")
	if !ok {
		t.Fatal("attr unavailable")
	}
	// Reload so the freshly created attr is visible to inserts and queries.
	cat, err := platform.LoadAttrCatalog(ctx, svc.Pool, appID)
	if err != nil {
		t.Fatal(err)
	}
	t1, t2, t3 := newUUID(), newUUID(), newUUID()
	st := storage.New(svc.Pool)
	if _, err := st.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: t1, A: titleAttr, V: "alpha"},
		{E: t2, A: titleAttr, V: "beta"},
		{E: t3, A: titleAttr, V: "gamma"},
	}, false); err != nil {
		t.Fatal(err)
	}

	raw := map[string]any{"todos": map[string]any{}}
	code, resp := postWithHeaders(t, h, "/runtime/framework/query",
		map[string]any{"query": raw},
		map[string]string{"app-id": appStr})
	if code != 200 {
		t.Fatalf("framework/query: %d %v", code, resp)
	}
	data, ok := resp["data"].(map[string]any)
	if !ok {
		t.Fatalf("response must carry data object, got %v", resp)
	}
	arr, ok := data["todos"].([]any)
	if !ok {
		t.Fatalf("data.todos must be a JSON array, got %v", data)
	}
	titles := map[string]bool{}
	for _, e := range arr {
		m, ok := e.(map[string]any)
		if !ok {
			t.Fatalf("todo entity must be an object, got %v", e)
		}
		titles[m["title"].(string)] = true
	}
	for _, want := range []string{"alpha", "beta", "gamma"} {
		if !titles[want] {
			t.Fatalf("seeded todo %q missing from result %v", want, titles)
		}
	}

	// Round-trip: identical query via instaql.Executor directly must produce
	// byte-identical todos payload.
	q, err := instaql.Coerce(raw)
	if err != nil {
		t.Fatal(err)
	}
	direct, err := (&instaql.Executor{DB: svc.Pool}).Run(ctx, q, cat, appID)
	if err != nil {
		t.Fatal(err)
	}
	directArr := []any{}
	if err := json.Unmarshal(direct.Data["todos"], &directArr); err != nil {
		t.Fatalf("unmarshal direct todos: %v (%s)", err, direct.Data["todos"])
	}
	if !reflect.DeepEqual(directArr, arr) {
		t.Fatalf("round-trip mismatch:\n direct: %s\n http:   %v", direct.Data["todos"], arr)
	}

	// Missing query key → 400 before any DB work.
	code, resp = postWithHeaders(t, h, "/runtime/framework/query",
		map[string]any{"query_x": raw}, map[string]string{"app-id": appStr})
	if code != 400 {
		t.Fatalf("missing query must 400, got %d %v", code, resp)
	}
	// Unknown refresh-token degrades to anonymous (v1 non-bang lookup).
	code, resp = postWithHeaders(t, h, "/runtime/framework/query",
		map[string]any{"query": raw, "refresh-token": uuidStr(newUUID())},
		map[string]string{"app-id": appStr})
	if code != 200 {
		t.Fatalf("bad refresh-token must not fail query, got %d %v", code, resp)
	}
}

// TestRefreshTokensPlural mints two tokens via the magic-code flow and
// resolves both in one POST /runtime/auth/refresh_tokens call.
func TestRefreshTokensPlural(t *testing.T) {
	svc, h, appID, cleanup := env(t)
	defer cleanup()
	appStr := uuidStr(appID)

	tokA, idA := mintUser(t, svc, appID, "a@plural")
	tokB, idB := mintUser(t, svc, appID, "b@plural")

	code, resp := post(t, h, "/runtime/auth/refresh_tokens",
		map[string]any{"refresh_tokens": []string{tokA, tokB}, "app-id": appStr})
	if code != 200 {
		t.Fatalf("refresh_tokens: %d %v", code, resp)
	}
	users, ok := resp["users"].([]any)
	if !ok || len(users) != 2 {
		t.Fatalf("want users array of 2, got %v", resp)
	}
	gotIDs := map[string]string{}
	for _, u := range users {
		m := u.(map[string]any)
		gotIDs[m["id"].(string)] = m["refresh_token"].(string)
	}
	if gotIDs[idA] != tokA || gotIDs[idB] != tokB {
		t.Fatalf("tokens must resolve to their users: %v (want %s→%s, %s→%s)",
			gotIDs, idA, tokA, idB, tokB)
	}

	// One bad token fails the whole batch.
	code, resp = post(t, h, "/runtime/auth/refresh_tokens",
		map[string]any{"refresh_tokens": []string{tokA, uuidStr(newUUID())}, "app-id": appStr})
	if code != 401 {
		t.Fatalf("unknown token in batch must 401, got %d %v", code, resp)
	}
	if _, ok := resp["message"]; !ok {
		t.Fatalf("error must carry message, got %v", resp)
	}
}

// TestSignoutEndpoint proves /runtime/signout kills the token with v1's
// snake_case envelope.
func TestSignoutEndpoint(t *testing.T) {
	svc, h, appID, cleanup := env(t)
	defer cleanup()
	appStr := uuidStr(appID)

	tok, _ := mintUser(t, svc, appID, "signout@t")
	code, resp := post(t, h, "/runtime/signout",
		map[string]any{"app_id": appStr, "refresh_token": tok})
	if code != 200 || len(resp) != 0 {
		t.Fatalf("signout: %d %v", code, resp)
	}
	if _, err := svc.VerifyRefreshToken(context.Background(), appID, tok); err == nil {
		t.Fatal("token must be dead after /runtime/signout")
	}
}

// TestOpenIDConfiguration derives endpoints from the Host header per v1
// openid-configuration-get. No DB needed.
func TestOpenIDConfiguration(t *testing.T) {
	h := &runtimeapi.Handler{}
	appStr := uuidStr(newUUID())

	req := httptest.NewRequest("GET", "/runtime/"+appStr+"/.well-known/openid-configuration", nil)
	req.Host = "api.example.com:8888"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("openid-configuration: %d %s", rec.Code, rec.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	wantAuth := "http://api.example.com:8888/runtime/" + appStr + "/oauth/start"
	wantTok := "http://api.example.com:8888/runtime/" + appStr + "/oauth/token"
	if resp["authorization_endpoint"] != wantAuth {
		t.Fatalf("authorization_endpoint = %v, want %v", resp["authorization_endpoint"], wantAuth)
	}
	if resp["token_endpoint"] != wantTok {
		t.Fatalf("token_endpoint = %v, want %v", resp["token_endpoint"], wantTok)
	}
}

// --- helpers ---

func postWithHeaders(t *testing.T, h httpHandler, path string, body map[string]any, headers map[string]string) (int, map[string]any) {
	t.Helper()
	return do(t, h, "POST", path, body, headers)
}

// findOrCreateAttr registers etype.label (blob one) if absent and returns its id.
func findOrCreateAttr(t *testing.T, svc *authn.Service, ctx context.Context, appID [16]byte, cat *platform.AttrCatalog, etype, label string) ([16]byte, bool) {
	t.Helper()
	if a := cat.FindByEtypeLabel(etype, label); a != nil {
		return a.ID, true
	}
	var id [16]byte
	err := svc.DB.WithTx(ctx, func(tx pgx.Tx) error {
		a, e := platform.GetOrCreateAttr(ctx, tx, appID, etype, label, "blob", "one", false, true)
		if e != nil {
			return e
		}
		id = a.ID
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return id, id != [16]byte{}
}
