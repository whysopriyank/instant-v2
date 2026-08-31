package transact

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// validateAddRequired implements v1's add-attr guard. A required attr may be
// added only while its etype has no live entity. The precheck runs before the
// batch writes; validateRequired below then uses the transaction-local catalog
// so requiredness is enforced for same-batch entities as well.
func validateAddRequired(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, appID [16]byte, attr platform.Attr) error {
	if !attr.IsRequired || attr.Etype == nil {
		return nil
	}
	var exists bool
	err := q.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			  FROM triples t
			  JOIN attrs a ON a.id = t.attr_id
			 WHERE t.app_id = $1
			   AND a.app_id = $1
			   AND a.etype = $2
			   AND a.deletion_marked_at IS NULL
			   AND t.value <> 'null'::jsonb
		)`, appID, *attr.Etype).Scan(&exists)
	if err != nil {
		return fmt.Errorf("check existing entities: %w", err)
	}
	if exists {
		label := "<unknown>"
		if attr.Label != nil {
			label = *attr.Label
		}
		return fmt.Errorf("can't create attribute `%s` as required because `%s` already have entities", label, *attr.Etype)
	}
	return nil
}

func validateRequired(ctx context.Context, q interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}, appID [16]byte, cat *platform.AttrCatalog, touched [][16]byte) error {
	if len(touched) == 0 {
		return nil
	}
	if cat == nil {
		return nil
	}
	etypes := make(map[string]struct{})
	for _, attr := range cat.Attrs() {
		if attr.Etype == nil {
			continue
		}
		if attr.IsRequired || (attr.Label != nil && *attr.Label == "id") {
			etypes[*attr.Etype] = struct{}{}
		}
	}
	if len(etypes) == 0 {
		return nil
	}
	etypeList := make([]string, 0, len(etypes))
	for etype := range etypes {
		etypeList = append(etypeList, etype)
	}
	sort.Strings(etypeList)
	rows, err := q.Query(ctx, `
		WITH touched(entity_id) AS (
			SELECT * FROM unnest($2::uuid[])
		),
		alive AS (
			SELECT DISTINCT t.entity_id, a.etype
			  FROM triples t
			  JOIN attrs a ON a.id = t.attr_id
			  JOIN touched x ON x.entity_id = t.entity_id
			 WHERE t.app_id = $1
			   AND a.app_id = $1
			   AND a.deletion_marked_at IS NULL
			   AND a.etype = ANY($3::text[])
			   AND t.value <> 'null'::jsonb
		),
		required_attrs AS (
			SELECT a.id, a.etype, a.label
			  FROM attrs a
			 WHERE a.app_id = $1
			   AND a.deletion_marked_at IS NULL
			   AND a.etype = ANY($3::text[])
			   AND (a.is_required OR a.label = 'id')
		),
		present AS (
			SELECT DISTINCT t.entity_id, t.attr_id
			  FROM triples t
			  JOIN alive e ON e.entity_id = t.entity_id
			  JOIN required_attrs r ON r.id = t.attr_id AND r.etype = e.etype
			 WHERE t.app_id = $1
			   AND t.value <> 'null'::jsonb
		)
		SELECT e.entity_id, r.etype, r.label
		  FROM alive e
		  JOIN required_attrs r ON r.etype = e.etype
		 WHERE NOT EXISTS (
			SELECT 1 FROM present p
			 WHERE p.entity_id = e.entity_id AND p.attr_id = r.id
		)
		 ORDER BY e.entity_id, r.etype, r.label`, appID, touched, etypeList)
	if err != nil {
		return fmt.Errorf("validate required attrs: %w", err)
	}
	defer rows.Close()
	type missing struct {
		eid          [16]byte
		etype, label string
	}
	var misses []missing
	for rows.Next() {
		var m missing
		if err := rows.Scan(&m.eid, &m.etype, &m.label); err != nil {
			return fmt.Errorf("validate required attrs: %w", err)
		}
		misses = append(misses, m)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("validate required attrs: %w", err)
	}
	if len(misses) == 0 {
		return nil
	}
	parts := make([]string, 0, len(misses))
	for _, m := range misses {
		parts = append(parts, fmt.Sprintf("`%s/%s`: %s", m.etype, m.label, platform.UUIDToStr(m.eid)))
	}
	if len(parts) == 1 {
		//nolint:staticcheck // Preserve the frozen v1 client-visible error text.
		return fmt.Errorf("Missing required attribute %s", parts[0])
	}
	//nolint:staticcheck // Preserve the frozen v1 client-visible error text.
	return fmt.Errorf("Missing required attributes %s", strings.Join(parts, "; "))
}

// validateUpdatedRequired checks all live entities of each attr's etype after
// the complete batch has run. It mirrors v1's validate-update-required! and
// deliberately uses the caller's pgx.Tx so the check is atomic with metadata
// and data mutations.
func validateUpdatedRequired(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, appID [16]byte, updates []platform.Attr) error {
	byID := make(map[[16]byte]platform.Attr, len(updates))
	for _, attr := range updates {
		if attr.IsRequired && attr.Etype != nil {
			byID[attr.ID] = attr
		}
	}
	if len(byID) == 0 {
		return nil
	}
	ordered := make([]platform.Attr, 0, len(byID))
	for _, attr := range byID {
		ordered = append(ordered, attr)
	}
	sort.Slice(ordered, func(i, j int) bool { return platform.UUIDToStr(ordered[i].ID) < platform.UUIDToStr(ordered[j].ID) })
	ids := make([][16]byte, len(ordered))
	etypes := make([]string, len(ordered))
	labels := make([]string, len(ordered))
	for i, attr := range ordered {
		ids[i] = attr.ID
		etypes[i] = *attr.Etype
		if attr.Label != nil {
			labels[i] = *attr.Label
		}
	}
	rows, err := q.Query(ctx, `
		WITH requested(id, etype, label) AS (
			SELECT * FROM unnest($2::uuid[], $3::text[], $4::text[])
		),
		alive AS (
			SELECT DISTINCT t.entity_id, a.etype
			  FROM triples t
			  JOIN attrs a ON a.id = t.attr_id
			 WHERE t.app_id = $1
			   AND a.app_id = $1
			   AND a.deletion_marked_at IS NULL
			   AND a.etype = ANY($3::text[])
			   AND t.value <> 'null'::jsonb
		),
		coverage AS (
			SELECT r.id, r.etype, r.label,
			       count(DISTINCT e.entity_id) AS entity_count,
			       count(DISTINCT CASE WHEN t.value <> 'null'::jsonb THEN t.entity_id END) AS attr_count
			  FROM requested r
			  LEFT JOIN alive e ON e.etype = r.etype
			  LEFT JOIN triples t ON t.app_id = $1
			                    AND t.entity_id = e.entity_id
			                    AND t.attr_id = r.id
			 GROUP BY r.id, r.etype, r.label
		)
		SELECT id, etype, label
		  FROM coverage
		 WHERE entity_count <> attr_count
		 ORDER BY etype, label`, appID, ids, etypes, labels)
	if err != nil {
		return fmt.Errorf("validate updated required attrs: %w", err)
	}
	defer rows.Close()
	var parts []string
	for rows.Next() {
		var id [16]byte
		var etype, label string
		if err := rows.Scan(&id, &etype, &label); err != nil {
			return fmt.Errorf("validate updated required attrs: %w", err)
		}
		parts = append(parts, fmt.Sprintf("Can't update attribute `%s` to required because `%s` already have entities without it", label, etype))
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("validate updated required attrs: %w", err)
	}
	if len(parts) > 0 {
		return fmt.Errorf("%s", strings.Join(parts, "; "))
	}
	return nil
}
