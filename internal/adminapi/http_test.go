package adminapi

import (
	"math"
	"net/http/httptest"
	"testing"
)

func TestWriteJSONContract(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		want  string
	}{
		{"escaping and newline", map[string]any{"text": "<&>"}, "{\"text\":\"\\u003c\\u0026\\u003e\"}\n"},
		{"empty array", map[string]any{"users": []any{}}, "{\"users\":[]}\n"},
		{"null", nil, "null\n"},
		{"encode failure keeps status", math.NaN(), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			writeJSON(rec, 202, tc.value)
			if rec.Code != 202 || rec.Header().Get("Content-Type") != "application/json" || rec.Body.String() != tc.want {
				t.Fatalf("got %d %v %q; want 202 application/json %q", rec.Code, rec.Header(), rec.Body.String(), tc.want)
			}
		})
	}
}
