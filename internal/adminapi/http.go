package adminapi

import (
	"net/http"

	"github.com/instant-v2/instant-v2/internal/httpjson"
)

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func strField(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	// Status is already committed on an encoding/write error; preserve the
	// admin API's existing response behavior rather than writing a second body.
	_ = httpjson.Write(w, status, v, true)
}

// writeErr ports response/error envelopes: {"message": "..."}.
func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"message": msg})
}
