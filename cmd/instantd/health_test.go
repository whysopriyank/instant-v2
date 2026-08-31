package main

import (
	"net/http/httptest"
	"testing"
)

func TestHealthWithoutDatabase(t *testing.T) {
	rec := httptest.NewRecorder()
	health(nil, nil).ServeHTTP(rec, httptest.NewRequest("GET", "/health", nil))
	if rec.Code != 200 || rec.Body.String() != `{"ok":true,"db":false}` || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("health response: %d %v %q", rec.Code, rec.Header(), rec.Body.String())
	}
}
