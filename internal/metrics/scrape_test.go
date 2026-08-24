package metrics

import (
	"net/http/httptest"
	"testing"
)

// scrapeRegistry renders the text exposition format for Registry.
func scrapeRegistry(t *testing.T) string {
	t.Helper()
	w := httptest.NewRecorder()
	Handler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if w.Code != 200 {
		t.Fatalf("metrics handler status = %d", w.Code)
	}
	return w.Body.String()
}
