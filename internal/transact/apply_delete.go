package transact

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func applyDeleteEntity(ctx context.Context, tx pgx.Tx, appID [16]byte, batch []Step) ([][16]byte, error) {
	if len(batch) == 0 {
		return nil, nil
	}
	ids := make([][16]byte, 0, len(batch))
	etypes := make([]string, 0, len(batch))
	for _, st := range batch {
		var eidStr string
		if err := json.Unmarshal(st.Args[0], &eidStr); err != nil {
			return nil, fmt.Errorf("delete-entity: eid: %w", err)
		}
		var u [16]byte
		if err := parseUUID(eidStr, &u); err != nil {
			return nil, fmt.Errorf("delete-entity: eid %s: %w", eidStr, err)
		}
		etype := "$default"
		if len(st.Args) == 2 {
			if err := json.Unmarshal(st.Args[1], &etype); err != nil {
				return nil, fmt.Errorf("delete-entity: etype: %w", err)
			}
		}
		ids = append(ids, u)
		etypes = append(etypes, etype)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	// Both forward object triples and reverse refs are selected by entity id
	// plus namespace. json_uuid_to_uuid follows v1's ref matching and accepts
	// UUID text regardless of letter case. A forged etype must never turn a
	// namespace-scoped delete into an entity-wide delete.
	rows, err := tx.Query(ctx, `
		WITH targets(entity_id, etype) AS (
			SELECT * FROM unnest($2::uuid[], $3::text[])
		),
		forward_rows AS (
			SELECT DISTINCT t.ctid
			  FROM triples t
			  JOIN targets x ON x.entity_id = t.entity_id
			  JOIN attrs a ON a.id = t.attr_id
			 WHERE t.app_id = $1
			   AND a.etype = x.etype
		),
			reverse_rows AS MATERIALIZED (
			SELECT DISTINCT t.ctid, t.entity_id
			  FROM triples t
			 JOIN targets x ON json_uuid_to_uuid(t.value) = x.entity_id
			  JOIN attrs a ON a.id = t.attr_id
			 WHERE t.app_id = $1
			   AND t.vae
			   AND a.reverse_etype = x.etype
		),
		delete_rows AS (
			SELECT ctid FROM forward_rows
			UNION
			SELECT ctid FROM reverse_rows
		),
		deleted AS (
			DELETE FROM triples t
			 USING delete_rows d
			 WHERE t.ctid = d.ctid
			 RETURNING t.ctid, t.entity_id
		)
		SELECT d.entity_id,
		       EXISTS (SELECT 1 FROM reverse_rows r WHERE r.ctid = d.ctid)
		  FROM deleted d`, appID, ids, etypes)
	if err != nil {
		return nil, fmt.Errorf("delete-entity: %w", err)
	}
	defer rows.Close()
	refs := make([][16]byte, 0)
	seen := make(map[[16]byte]struct{})
	for rows.Next() {
		var entityID [16]byte
		var reverse bool
		if err := rows.Scan(&entityID, &reverse); err != nil {
			return nil, fmt.Errorf("delete-entity: %w", err)
		}
		if !reverse {
			continue
		}
		if _, ok := seen[entityID]; ok {
			continue
		}
		seen[entityID] = struct{}{}
		refs = append(refs, entityID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("delete-entity: %w", err)
	}
	return refs, nil
}
