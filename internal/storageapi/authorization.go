package storageapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// requireAdmin authorizes a destructive/admin storage operation via the
// injected token checker. Fail-closed: no checker → 503; bad token → 401.
// The app id may arrive in the header, query, or JSON body (bulk delete);
// body bytes are buffered and restored for downstream handlers. Return the
// authorized app so each operation can bind its target to this credential.
func (h *Handler) requireAdmin(w http.ResponseWriter, r *http.Request) ([16]byte, bool) {
	if h.AdminTokenCheck == nil {
		httpError(w, http.StatusServiceUnavailable, errors.New("storageapi: admin operations not configured"))
		return [16]byte{}, false
	}
	token := bearerOrXToken(r)
	appIDStr := r.Header.Get("app-id")
	if appIDStr == "" {
		appIDStr = firstQuery(r, "app-id", "app_id")
	}
	if appIDStr == "" && r.Body != nil {
		raw, err := io.ReadAll(r.Body)
		if err == nil {
			r.Body = io.NopCloser(bytes.NewReader(raw))
			var body map[string]any
			if json.Unmarshal(raw, &body) == nil {
				for _, k := range []string{"app-id", "app_id"} {
					if s, ok := body[k].(string); ok && s != "" {
						appIDStr = s
						break
					}
				}
			}
		}
	}
	id, err := platform.ScanUUIDErr(appIDStr)
	if err != nil || token == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"message": "invalid admin credentials"})
		return [16]byte{}, false
	}
	ok, err := h.AdminTokenCheck(r.Context(), platform.UUIDToStr(id), token)
	if err != nil || !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"message": "invalid admin credentials"})
		return [16]byte{}, false
	}
	return id, true
}

// bearerOrXToken extracts the caller credential (Authorization: Bearer or
// X-admin-token), mirroring the admin/backup planes.
func bearerOrXToken(r *http.Request) string {
	authz := r.Header.Get("Authorization")
	if len(authz) >= 7 && strings.EqualFold(authz[:7], "bearer ") {
		if t := strings.TrimSpace(authz[7:]); t != "" {
			return t
		}
	}
	return r.Header.Get("X-admin-token")
}

func firstQuery(r *http.Request, keys ...string) string {
	q := r.URL.Query()
	for _, k := range keys {
		if v := q.Get(k); v != "" {
			return v
		}
	}
	return ""
}
