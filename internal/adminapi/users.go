package adminapi

import (
	"net/http"
	"strconv"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// ---- Users -----------------------------------------------------------------

// handleUsersList lists the app's $users entities. limit/offset come from the
// query params (v1's GET /admin/users fetched a single user; the list shape
// with `{"users": [...]}` is this package's addition per the Phase 5 spec).
func (h *Handler) handleUsersList(w http.ResponseWriter, r *http.Request, a *authedReq) {
	limit, offset := 100, 0
	q := r.URL.Query()
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeErr(w, http.StatusBadRequest, "invalid `limit`")
			return
		}
		limit = n
	}
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeErr(w, http.StatusBadRequest, "invalid `offset`")
			return
		}
		offset = n
	}

	out, err := h.listUsers(r.Context(), a, limit, offset)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"users": out})
}

// handleUsersDelete bulk-deletes users by `ids` (triples + reverse refs),
// mirroring applyDeleteEntity semantics → {"deleted": N}.
func (h *Handler) handleUsersDelete(w http.ResponseWriter, r *http.Request, a *authedReq) {
	arr, ok := a.body["ids"].([]any)
	if !ok || len(arr) == 0 {
		writeErr(w, http.StatusBadRequest, "missing or invalid `ids` array")
		return
	}
	ids := make([][16]byte, 0, len(arr))
	for _, el := range arr {
		s, _ := el.(string)
		var id [16]byte
		if platform.ScanUUID(s, &id) != nil {
			writeErr(w, http.StatusBadRequest, "invalid user id: "+s)
			return
		}
		ids = append(ids, id)
	}

	deleted, err := h.deleteUsers(r.Context(), a, ids)
	if err != nil {
		h.logger().Error("adminapi: users delete", "err", err)
		writeErr(w, http.StatusInternalServerError, "delete failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": deleted})
}
