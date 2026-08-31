package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// tripleBatch accumulates decoded triples and flushes them in one statement
// whose flags/md5 are derived by Postgres exactly like storage.InsertTriples'
// enhanced-triples CTE. JSON-null values store as SQL NULL with JSONNullMD5.
type tripleBatch struct {
	entityIDs []string
	attrIDs   []string
	values    []any // string(raw json) | nil
	createdAt []any // time.Time | nil
}

func newTripleBatch() *tripleBatch { return &tripleBatch{} }

func (b *tripleBatch) len() int { return len(b.entityIDs) }

func (b *tripleBatch) addTriple(entityID, attrID string, value json.RawMessage, createdAt time.Time, hasCreated bool) error {
	e, err := platform.ScanUUIDErr(entityID)
	if err != nil {
		return fmt.Errorf("bad entity_id %q: %v", entityID, err)
	}
	a, err := platform.ScanUUIDErr(attrID)
	if err != nil {
		return fmt.Errorf("bad attr_id %q: %v", attrID, err)
	}
	b.entityIDs = append(b.entityIDs, platform.UUIDToStr(e))
	b.attrIDs = append(b.attrIDs, platform.UUIDToStr(a))
	if len(value) == 0 || string(value) == "null" {
		b.values = append(b.values, nil)
	} else {
		b.values = append(b.values, string(value))
	}
	if hasCreated && !createdAt.IsZero() {
		b.createdAt = append(b.createdAt, createdAt)
	} else {
		b.createdAt = append(b.createdAt, nil)
	}
	return nil
}

func (b *tripleBatch) add(body json.RawMessage) error {
	var t struct {
		EntityID  string          `json:"entity_id"`
		AttrID    string          `json:"attr_id"`
		Value     json.RawMessage `json:"value"`
		CreatedAt string          `json:"created_at"`
	}
	if err := strictUnmarshal(body, &t); err != nil {
		return err
	}
	var ts time.Time
	if t.CreatedAt != "" {
		parsed, err := parsePGTime(t.CreatedAt)
		if err != nil {
			return fmt.Errorf("bad created_at %q: %v", t.CreatedAt, err)
		}
		ts = parsed
	}
	return b.addTriple(t.EntityID, t.AttrID, t.Value, ts, t.CreatedAt != "")
}

func (b *tripleBatch) reset() {
	b.entityIDs = b.entityIDs[:0]
	b.attrIDs = b.attrIDs[:0]
	b.values = b.values[:0]
	b.createdAt = b.createdAt[:0]
}

func (b *tripleBatch) flush(ctx context.Context, tx pgx.Tx, appID [16]byte, counter *int64) error {
	if b.len() == 0 {
		return nil
	}
	appStr := platform.UUIDToStr(appID)
	// Reject triples referencing unknown/deleted attrs instead of silently
	// dropping them in the join below.
	var dangling int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM unnest($1::uuid[]) AS u(aid)
		WHERE NOT EXISTS (
			SELECT 1 FROM attrs
			 WHERE id = u.aid AND app_id = $2 AND deletion_marked_at IS NULL)`,
		b.attrIDs, appID).Scan(&dangling); err != nil {
		return err
	}
	if dangling > 0 {
		return fmt.Errorf("%d triple(s) reference unknown attrs (import a matching attr set first)", dangling)
	}
	_, err := tx.Exec(ctx, `
		WITH input AS (
			SELECT * FROM unnest($2::uuid[], $3::uuid[], $4::jsonb[], $5::timestamptz[])
			WITH ORDINALITY AS t(entity_id, attr_id, value, created_at, ord)
		),
		enhanced AS (
			SELECT i.entity_id, i.attr_id,
			       CASE WHEN i.value IS NULL THEN NULL ELSE i.value END AS value,
			       COALESCE(md5(i.value::text), '`+jsonNullMD5+`')       AS value_md5,
			       (a.cardinality = 'one')                              AS ea,
			       (a.value_type  = 'ref')                              AS eav,
			       a.is_unique                                          AS av,
			       a.is_indexed                                         AS ave,
			       (a.value_type  = 'ref')                              AS vae,
			       a.checked_data_type                                  AS checked_data_type,
			       i.created_at                                         AS created_at
			  FROM input i
			  JOIN attrs a ON a.id = i.attr_id
			             AND a.app_id = $1
			             AND a.deletion_marked_at IS NULL
		)
		INSERT INTO triples (app_id, entity_id, attr_id, value, value_md5,
		                     ea, eav, av, ave, vae, checked_data_type, created_at)
		SELECT $1, entity_id, attr_id, value, value_md5,
		       ea, eav, av, ave, vae, checked_data_type, COALESCE(created_at, NOW())
		FROM enhanced
		ON CONFLICT DO NOTHING`,
		appStr, b.entityIDs, b.attrIDs, b.values, b.createdAt)
	if err != nil {
		return err
	}
	// Counts reports logical records consumed from the dump (idempotent
	// re-imports count too, even when every row was a DB no-op).
	*counter += int64(len(b.entityIDs))
	b.reset()
	return nil
}
