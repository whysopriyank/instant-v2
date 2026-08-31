package runtimeapi

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestRequestProjectionContracts(t *testing.T) {
	const app = "00000000-0000-4000-8000-000000000001"
	for _, body := range []string{
		`{"app-id":"` + app + `","refresh-token":"first","extra":{"user-key":true}}`,
		`{"app_id":"` + app + `","refresh_token":"first"}`,
		`{"app-id":"bad","app_id":"` + app + `","refresh_token":"first","refresh-token":"second"}`,
	} {
		req, err := readSignout(httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)))
		if err != nil || req.AppID[15] != 1 || req.Token != "first" {
			t.Fatalf("signout %s: %+v, %v", body, req, err)
		}
	}
	batch, err := readRefreshTokens(httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"app_id":"`+app+`","refresh_tokens":["first","second"]}`)))
	if err != nil || !reflect.DeepEqual(batch.Tokens, []string{"first", "second"}) {
		t.Fatalf("refresh batch = %+v, %v", batch, err)
	}
	query, err := readFrameworkQuery(httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"query":{"todos":{"$":{"where":{"custom":42}}}},"refresh-token":"token","extra":true}`)))
	if err != nil || query.Token != "token" || query.Query["todos"] == nil {
		t.Fatalf("dynamic query = %+v, %v", query, err)
	}
}

func TestResponseEscapingContract(t *testing.T) {
	rec := httptest.NewRecorder()
	writeJSON(rec, http.StatusBadRequest, map[string]any{"message": "<&>"})
	if rec.Code != http.StatusBadRequest || rec.Header().Get("Content-Type") != "application/json" || rec.Body.String() != "{\"message\":\"<&>\"}\n" {
		t.Fatalf("runtime JSON contract changed: %d %v %q", rec.Code, rec.Header(), rec.Body.String())
	}
}
