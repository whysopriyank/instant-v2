package runtimeapi

import (
	"errors"
	"net/http"

	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/platform"
)

// frameworkQuery ports framework-query-triples (routes.clj L726-743):
//
//	POST /runtime/framework/query
//	headers: app-id: <uuid>            (or ?app_id=<uuid>, req->app-id-untrusted!)
//	body:    {"query": {…instaql…}, "refresh-token": "<uuid>"?}
//	→ {"data": {etype: […]}} (+ "page-info"/"aggregate" when requested)
//
// The optional refresh-token resolves the caller like v1's
// get-by-refresh-token (non-bang): an unknown token degrades to anonymous —
// it never fails the request. The app's view rules gate execution: closed
// etypes return empty results; dynamic rules are refused until rule-where
// pushdown exists (fail-closed — never serve unfiltered).
func (h *Handler) frameworkQuery(w http.ResponseWriter, r *http.Request) {
	req, err := readFrameworkQuery(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	appStr := r.Header.Get("app-id")
	if appStr == "" {
		appStr = r.URL.Query().Get("app_id")
	}
	appID, err := platform.ScanUUIDErr(appStr)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": "missing or invalid app-id"})
		return
	}
	var authUser map[string]any
	if tok := req.Token; tok != "" {
		// Anonymous-on-failure mirrors v1's non-bang get-by-refresh-token.
		if user, verr := h.Auth.VerifyRefreshToken(r.Context(), appID, tok); verr == nil && user != nil {
			authUser = map[string]any{"id": user.ID}
			if user.Email != "" {
				authUser["email"] = user.Email
			}
			if user.Type != "" {
				authUser["type"] = user.Type
			}
			for k, v := range user.Extra {
				authUser[k] = v
			}
		}
	}
	doc, derr := h.Catalogs.RuleDocFor(r.Context(), platform.UUIDToStr(appID))
	if derr != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"message": "internal error"})
		return
	}
	cat, err := h.Catalogs.For(r.Context(), platform.UUIDToStr(appID))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"message": platform.ClientMessage(err)})
		return
	}
	q, err := instaql.Coerce(req.Query)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	ex := &instaql.Executor{DB: h.Pool, Rules: doc, Auth: authUser}
	res, err := ex.Run(r.Context(), q, cat, appID)
	if err != nil {
		var unsupported *instaql.ErrRuleFilterUnsupported
		if errors.As(err, &unsupported) {
			writeJSON(w, http.StatusForbidden, map[string]any{"message": err.Error()})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"message": platform.ClientMessage(err)})
		return
	}
	writeJSON(w, http.StatusOK, res)
}
