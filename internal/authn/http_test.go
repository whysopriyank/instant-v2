package authn_test

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/instant-v2/instant-v2/internal/authn"
)

func TestAuthHTTPDirectIDTokenIsUnsupportedBeforeBodyParsing(t *testing.T) {
	h := &authn.Handler{Service: &authn.Service{}}
	valid := `{"app-id":"11111111-1111-4111-8111-111111111111","id_token":"invalid"}`
	for name, body := range map[string]string{
		"trailing object":  valid + `{}`,
		"trailing garbage": valid + `!`,
		"truncated":        `{"app-id":`,
		"array":            `[]`,
		"null":             `null`,
		"empty":            ``,
	} {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/runtime/oauth/id_token", strings.NewReader(body))
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusNotImplemented || !strings.Contains(w.Body.String(), "unsupported") {
				t.Fatalf("direct id_token must remain explicitly unsupported: status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
}

func TestAuthHTTPRejectsBodyFailuresBeforeDispatch(t *testing.T) {
	// No Service is installed: any dispatch past the JSON boundary is a bug.
	h := &authn.Handler{}
	valid := `{"app-id":"11111111-1111-4111-8111-111111111111","email":"test@example.com","code":"123456","refresh_token":"token","refresh-token":"token"}`
	for _, path := range []string{
		"/runtime/auth/send_magic_code", "/runtime/auth/verify_magic_code",
		"/runtime/auth/sign_in_guest", "/runtime/auth/verify_refresh_token",
		"/runtime/auth/sign_out", "/runtime/oauth/token",
	} {
		t.Run(path, func(t *testing.T) {
			for _, failure := range []string{"trailing JSON", "read error"} {
				t.Run(failure, func(t *testing.T) {
					var body io.Reader = strings.NewReader(valid + `{}`)
					if failure == "read error" {
						body = io.MultiReader(strings.NewReader(valid), failedBodyReader{})
					}
					w := httptest.NewRecorder()
					h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, body))
					if w.Code != http.StatusBadRequest || w.Body.String() != "{\"message\":\"invalid JSON body\"}\n" {
						t.Fatalf("body failure response = %d %s", w.Code, w.Body.String())
					}
				})
			}
		})
	}
}

type failedBodyReader struct{}

func (failedBodyReader) Read([]byte) (int, error) {
	return 0, errors.New("injected body read failure")
}

func TestAuthHTTPPreservesValidBodyAliases(t *testing.T) {
	h := &authn.Handler{Service: &authn.Service{}}
	for _, appKey := range []string{"app-id", "app_id"} {
		t.Run(appKey, func(t *testing.T) {
			body := `{"` + appKey + `":"11111111-1111-4111-8111-111111111111","id_token":"invalid","unknown":true}  `
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/runtime/oauth/id_token", strings.NewReader(body)))
			if w.Code != http.StatusNotImplemented {
				t.Fatalf("valid body must retain explicit unsupported status: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestAuthHTTPDirectIDTokenIsExplicitlyUnsupported(t *testing.T) {
	h := &authn.Handler{Service: &authn.Service{}}
	body := `{"app-id":"11111111-1111-4111-8111-111111111111","id_token":"anything"}`
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/runtime/oauth/id_token", strings.NewReader(body)))
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("direct id_token status = %d body=%s, want 501", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "unsupported") {
		t.Fatalf("direct id_token body = %s, want explicit unsupported message", w.Body.String())
	}
}
