package adminapi

import (
	"net/http"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// ---- Schema / soft deletions / presence ------------------------------------

// handleSchema ports schema-get. v1 returns {:schema {:attrs [...] :refs ...}};
// the catalog exposes only the flat wire-attr list today, so refs are omitted.
func (h *Handler) handleSchema(w http.ResponseWriter, r *http.Request, a *authedReq) {
	writeJSON(w, http.StatusOK, map[string]any{
		"schema": map[string]any{"attrs": a.cat.WireAttrs()},
	})
}

// handleSoftDeletedAttrs ports soft-deleted-attrs-get: direct SQL over attrs
// WHERE deletion_marked_at IS NOT NULL → {"attrs": [...], "grace-period-days"}.
func (h *Handler) handleSoftDeletedAttrs(w http.ResponseWriter, r *http.Request, a *authedReq) {
	rows, err := h.Pool.Query(r.Context(), `
		SELECT id, etype, label, deletion_marked_at FROM attrs
		WHERE app_id=$1 AND deletion_marked_at IS NOT NULL`, a.appID)
	if err != nil {
		h.logger().Error("adminapi: soft deleted attrs", "err", err)
		writeErr(w, http.StatusInternalServerError, "query failed")
		return
	}
	defer rows.Close()

	attrs := []any{}
	for rows.Next() {
		var id [16]byte
		var etype, label *string
		var markedAt time.Time
		if err := rows.Scan(&id, &etype, &label, &markedAt); err != nil {
			writeErr(w, http.StatusInternalServerError, "scan failed")
			return
		}
		entry := map[string]any{"id": platform.UUIDToStr(id)}
		if etype != nil {
			entry["etype"] = *etype
		}
		if label != nil {
			entry["label"] = *label
		}
		entry["deletion-marked-at"] = markedAt.UTC().Format(time.RFC3339Nano)
		attrs = append(attrs, entry)
	}
	if err := rows.Err(); err != nil {
		writeErr(w, http.StatusInternalServerError, "query failed")
		return
	}
	// sweeper/grace-period-days (hard_deletion_sweeper.clj L17).
	writeJSON(w, http.StatusOK, map[string]any{"attrs": attrs, "grace-period-days": 2})
}

// handlePresence is explicitly unsupported until the coordinator wires a
// read-only presence projection. Returning an error is important: an empty
// successful response would be indistinguishable from a room with no members.
func (h *Handler) handlePresence(w http.ResponseWriter, r *http.Request, a *authedReq) {
	writeJSON(w, http.StatusNotImplemented, map[string]any{
		"type":    "unsupported",
		"message": "admin presence is unsupported",
	})
}
