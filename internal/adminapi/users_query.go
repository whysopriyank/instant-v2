package adminapi

import (
	"context"
	"encoding/json"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// listUsers pages distinct entities before loading their email/type triples.
// The UUID ordering, field projection, and empty-array shape are the admin
// listing contract; extra $users fields are deliberately not exposed here.
func (h *Handler) listUsers(ctx context.Context, a *authedReq, limit, offset int) ([]map[string]any, error) {
	out := []map[string]any{}
	emailAttr := a.cat.FindByEtypeLabel("$users", "email")
	typeAttr := a.cat.FindByEtypeLabel("$users", "type")
	if emailAttr == nil && typeAttr == nil {
		return out, nil
	}
	attrIDs := [][16]byte{}
	for _, at := range []*platform.Attr{emailAttr, typeAttr} {
		if at != nil {
			attrIDs = append(attrIDs, at.ID)
		}
	}
	var emailID, typeID [16]byte
	if emailAttr != nil {
		emailID = emailAttr.ID
	}
	if typeAttr != nil {
		typeID = typeAttr.ID
	}
	// The former in-memory page predicate used offset+limit. Preserve its
	// empty-page result when the sum overflows, without passing a negative
	// limit to PostgreSQL or silently changing accepted query parameters.
	if offset+limit < offset {
		limit = 0
	}
	rows, err := h.Pool.Query(ctx, `
		WITH user_page AS (
			SELECT DISTINCT entity_id FROM triples
			WHERE app_id=$1 AND attr_id = ANY($2::uuid[])
			ORDER BY entity_id LIMIT $3 OFFSET $4
		)
		SELECT t.entity_id, t.attr_id, t.value FROM triples t
		JOIN user_page p ON p.entity_id = t.entity_id
		WHERE t.app_id=$1 AND t.attr_id = ANY($2::uuid[])
		ORDER BY t.entity_id`, a.appID, attrIDs, limit, offset)
	if err != nil {
		h.logger().Error("adminapi: users list", "err", err)
		return nil, err
	}
	defer rows.Close()

	var lastID string
	var user map[string]any
	for rows.Next() {
		var eid string
		var attrID [16]byte
		var value json.RawMessage
		if err := rows.Scan(&eid, &attrID, &value); err != nil {
			h.logger().Error("adminapi: users scan", "err", err)
			return nil, err
		}
		// ORDER BY entity_id keeps each user's rows contiguous, so projection
		// needs neither a full-app lookup map nor a second ordering slice.
		if user == nil || eid != lastID {
			user = map[string]any{"id": eid}
			out = append(out, user)
			lastID = eid
		}
		var v any
		// Preserve the existing float64/null projection of stored JSONB.
		_ = json.Unmarshal(value, &v)
		switch attrID {
		case emailID:
			user["email"] = v
		case typeID:
			user["type"] = v
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
