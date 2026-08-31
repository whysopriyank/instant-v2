package adminapi_test

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/instant-v2/instant-v2/internal/adminapi"
)

// These preflight failures deliberately need no database: authentication and
// body validation must finish before any app/catalog access.
func TestAdminPreflightContract(t *testing.T) {
	for _, tc := range []struct {
		name, path, token, body, want string
		status                        int
	}{
		{"outside admin", "/runtime/query", "", "", "not found", 404},
		{"admin root", "/admin", "", "", "not found", 404},
		{"missing token before JSON", "/admin/query", "", "{", "missing admin token", 401},
		{"unknown route still authenticates", "/admin/unknown", "", "", "missing admin token", 401},
		{"trailing slash", "/admin/query/", "", "", "missing admin token", 401},
		{"malformed JSON", "/admin/query", "token", "{", "invalid JSON body", 400},
		{"trailing JSON", "/admin/query", "token", "{} {}", "invalid JSON body", 400},
		{"array body", "/admin/query", "token", "[]", "invalid JSON body", 400},
		{"null body", "/admin/query", "token", "null", "missing or invalid app-id", 400},
		{"empty body", "/admin/query", "token", "", "missing or invalid app-id", 400},
		{"invalid app", "/admin/query", "token", `{"app-id":"bad"}`, "missing or invalid app-id", 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
			req.Header.Set("X-admin-token", tc.token)
			rec := httptest.NewRecorder()
			(&adminapi.Handler{}).ServeHTTP(rec, req)
			if rec.Code != tc.status || rec.Body.String() != `{"message":"`+tc.want+`"}`+"\n" {
				t.Fatalf("got %d %q; want %d message %q", rec.Code, rec.Body.String(), tc.status, tc.want)
			}
			if got := rec.Header().Get("Content-Type"); got != "application/json" {
				t.Fatalf("content type = %q", got)
			}
		})
	}
}

type unreadableBody struct{}

func (unreadableBody) Read([]byte) (int, error) { return 0, errors.New("read failed") }
func (unreadableBody) Close() error             { return nil }

var _ io.ReadCloser = unreadableBody{}

func TestAdminUnreadableBody(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/admin/query", nil)
	req.Body = unreadableBody{}
	req.Header.Set("X-admin-token", "token")
	rec := httptest.NewRecorder()
	(&adminapi.Handler{}).ServeHTTP(rec, req)
	if rec.Code != 400 || rec.Body.String() != "{\"message\":\"unreadable request body\"}\n" {
		t.Fatalf("got %d %s", rec.Code, rec.Body.String())
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
