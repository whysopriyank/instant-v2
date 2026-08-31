package runtimeapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/runtimeapi"
)

// Invalid input must be rejected by the JSON boundary, not interpreted as a
// partial request and handed to field validation or service code.
func TestMalformedRequestBody(t *testing.T) {
	for _, path := range []string{"/runtime/auth/refresh_tokens", "/runtime/signout", "/runtime/framework/query"} {
		for _, body := range []string{
			`{"query":{}} {"extra":true}`,
			`{"query":{}} garbage`,
			`{"query":{},`,
			`[]`,
			`null`,
			``,
		} {
			t.Run(path+"/"+body, func(t *testing.T) {
				req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
				rec := httptest.NewRecorder()
				(&runtimeapi.Handler{}).ServeHTTP(rec, req)
				if rec.Code != http.StatusBadRequest {
					t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
				}
				var response struct {
					Message string `json:"message"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				if response.Message != "invalid JSON body" {
					t.Fatalf("decode boundary message = %q, want invalid JSON body", response.Message)
				}
			})
		}
	}
}

func TestMalformedSignoutDoesNotRevokeToken(t *testing.T) {
	svc, h, appID, cleanup := env(t)
	defer cleanup()
	token, _ := mintUser(t, svc, appID, "body-boundary@test")
	body := `{"app_id":"` + platform.UUIDToStr(appID) + `","refresh_token":"` + token + `"} {}`
	req := httptest.NewRequest(http.MethodPost, "/runtime/signout", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("malformed signout status = %d, want 400", rec.Code)
	}
	if _, err := svc.VerifyRefreshToken(context.Background(), appID, token); err != nil {
		t.Fatalf("malformed request revoked the token: %v", err)
	}
}
