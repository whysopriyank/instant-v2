package adminapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// authenticate ports req->app-id-authed! restricted to app-admin tokens:
//
//   - token: "Authorization: Bearer <t>" or "X-admin-token: <t>" header
//   - app-id: JSON body "app-id" (assignment-frozen), falling back to the
//     "X-app-id" header or "app-id"/"app_id" query params for body-less GETs
//
// Errors: 400 malformed app-id/body, 401 bad token, 404 unknown app.
func (h *Handler) authenticate(w http.ResponseWriter, r *http.Request) (*authedReq, bool) {
	token := bearerOrAdminToken(r)
	if token == "" {
		writeErr(w, http.StatusUnauthorized, "missing admin token")
		return nil, false
	}

	ctx := r.Context()
	var body map[string]any
	if r.Body != nil {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "unreadable request body")
			return nil, false
		}
		r.Body = io.NopCloser(bytes.NewReader(raw)) // downstream handlers reuse it
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &body); err != nil {
				writeErr(w, http.StatusBadRequest, "invalid JSON body")
				return nil, false
			}
		}
	}

	appStr := ""
	for _, v := range []any{body["app-id"], body["app_id"]} {
		if s, ok := v.(string); ok && s != "" {
			appStr = s
			break
		}
	}
	if appStr == "" {
		q := r.URL.Query()
		appStr = firstNonEmpty(r.Header.Get("X-app-id"),
			r.Header.Get("app-id"), q.Get("app-id"), q.Get("app_id"))
	}
	var appID [16]byte
	if appStr == "" || platform.ScanUUID(appStr, &appID) != nil {
		writeErr(w, http.StatusBadRequest, "missing or invalid app-id")
		return nil, false
	}

	cat, err := h.Catalogs.For(ctx, appStr)
	if err != nil {
		if errors.Is(err, platform.ErrNoRows) {
			writeErr(w, http.StatusNotFound, "unknown app")
			return nil, false
		}
		h.logger().Error("adminapi: catalog load", "err", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
		return nil, false
	}
	if platform.ScanUUID(token, new([16]byte)) != nil {
		// Non-uuid tokens can never match app_admin_tokens; 401 without
		// reaching Postgres (uuid cast would raise a 500).
		writeErr(w, http.StatusUnauthorized, "Invalid admin token")
		return nil, false
	}
	// Catalogs.For returns an empty (not erroring) catalog for unknown apps,
	// so probe the apps table for the 404-unknown-app contract.
	var one int
	if err := h.Pool.QueryRow(ctx, `SELECT 1 FROM apps WHERE id=$1::uuid`, appStr).Scan(&one); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeErr(w, http.StatusNotFound, "unknown app")
			return nil, false
		}
		h.logger().Error("adminapi: app lookup", "err", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
		return nil, false
	}
	okTok, err := h.Catalogs.CheckAdminToken(ctx, appStr, token)
	if err != nil {
		h.logger().Error("adminapi: token check", "err", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
		return nil, false
	}
	if !okTok {
		writeErr(w, http.StatusUnauthorized, "Invalid admin token")
		return nil, false
	}
	return &authedReq{appID: appID, appStr: appStr, cat: cat, body: body}, true
}

func bearerOrAdminToken(r *http.Request) string {
	authz := r.Header.Get("Authorization")
	if len(authz) >= 7 && strings.EqualFold(authz[:7], "bearer ") {
		if t := strings.TrimSpace(authz[7:]); t != "" {
			return t
		}
	}
	return r.Header.Get("X-admin-token")
}
