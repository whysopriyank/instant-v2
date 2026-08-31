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
	for _, st := range batch {
		var eidStr string
		_ = json.Unmarshal(st.Args[0], &eidStr)
		var u [16]byte
		if err := parseUUID(eidStr, &u); err != nil {
			// May be lookup form; resolve already handled above — fail gracefully.
			continue
		}
		ids = append(ids, u)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	if _, err := tx.Exec(ctx, `DELETE FROM triples WHERE app_id=$1 AND entity_id = ANY($2::uuid[])`, appID, ids); err != nil {
		return nil, fmt.Errorf("delete-entity: %w", err)
	}
	// Reverse references (value = one of these entity ids). Refs are stored as
	// jsonb strings ('"<uuid>"'), so match via to_jsonb over a text array in
	// ONE parameterized statement. A failure here must abort the transaction:
	// silently keeping dangling forward refs corrupts link semantics.
	texts := make([]string, 0, len(ids))
	for _, id := range ids {
		texts = append(texts, uuidToStr(id))
	}
	rows, err := tx.Query(ctx,
		`DELETE FROM triples WHERE app_id=$1 AND vae AND value = ANY(SELECT to_jsonb(t) FROM unnest($2::text[]) AS t) RETURNING entity_id`,
		appID, texts)
	if err != nil {
		return nil, fmt.Errorf("delete-entity reverse refs: %w", err)
	}
	defer rows.Close()
	refs := make([][16]byte, 0)
	seen := make(map[[16]byte]struct{})
	for rows.Next() {
		var entityID [16]byte
		if err := rows.Scan(&entityID); err != nil {
			return nil, fmt.Errorf("delete-entity reverse refs: %w", err)
		}
		if _, ok := seen[entityID]; ok {
			continue
		}
		seen[entityID] = struct{}{}
		refs = append(refs, entityID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("delete-entity reverse refs: %w", err)
	}
	return refs, nil
}
