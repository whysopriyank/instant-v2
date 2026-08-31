package adminapi

import (
	"net/http"

	"github.com/instant-v2/instant-v2/internal/instaql"
)

// ---- Query -----------------------------------------------------------------

// handleQuery ports query-post: {"query": {...}} → the InstaQL result
// envelope. Admin callers run the bare executor (v1 permissioned-query with
// :admin? true short-circuits rule wheres).
func (h *Handler) handleQuery(w http.ResponseWriter, r *http.Request, a *authedReq) {
	raw, _ := a.body["query"].(map[string]any)
	if raw == nil {
		writeErr(w, http.StatusBadRequest, "missing or invalid `query` object")
		return
	}
	q, err := instaql.Coerce(raw)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	ex := &instaql.Executor{DB: h.Pool, Admin: true} // admin bypasses perms
	res, err := ex.Run(r.Context(), q, a.cat, a.appID)
	if err != nil {
		h.logger().Error("adminapi: query", "err", err)
		writeErr(w, http.StatusInternalServerError, "query failed")
		return
	}
	// v1 query-post responds (response/ok object-tree): the bare etype map,
	// no envelope key — the Python/TS SDKs index result[<etype>] directly.
	writeJSON(w, http.StatusOK, res.Data)
}
