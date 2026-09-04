package adminapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPresenceIsExplicitlyUnsupported(t *testing.T) {
	rec := httptest.NewRecorder()
	h := &Handler{}
	h.handlePresence(rec, httptest.NewRequest(http.MethodGet, "/admin/rooms/presence", nil), nil)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusNotImplemented)
	}
	if got, want := rec.Body.String(), "{\"message\":\"admin presence is unsupported\",\"type\":\"unsupported\"}\n"; got != want {
		t.Fatalf("body = %q; want %q", got, want)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("content type = %q; want application/json", got)
	}
}
