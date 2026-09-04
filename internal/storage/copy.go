package storage

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/triple"
)

// copySource adapts precomputed rows to pgx's CopyFromSource.
type copySource struct {
	rows [][]any
	idx  int
}

func (s *copySource) Next() bool { return s.idx < len(s.rows) }

func (s *copySource) Values() ([]any, error) {
	r := s.rows[s.idx]
	s.idx++
	return r, nil
}

func (s *copySource) Err() error { return nil }

// CopyTriples bulk-loads triples through a temp staging table fed by COPY
// (port surface of v1 jdbc/copy.clj). Rows flow: COPY into staging →
// INSERT..SELECT with the same flag/md5 derivation as InsertTriples.
// Conflict semantics match the regular paths: cardinality-one rows overwrite,
// everything else dedups on value_md5. Used by restore tooling and backfills.
func (d *DB) CopyTriples(ctx context.Context, appID [16]byte, cat *platform.AttrCatalog, ts []triple.Triple) (int64, error) {
	if len(ts) == 0 {
		return 0, nil
	}
	for _, t := range ts {
		if _, ok := cat.ByID(t.A); !ok {
			return 0, fmt.Errorf("%w: %x", ErrUnknownAttr, t.A)
		}
	}

	rows := make([][]any, 0, len(ts))
	for i, t := range ts {
		// Preserve nil as the JSON null literal. Converting it to the string
		// "null" would encode a JSON string and change value_md5 semantics.
		enc, err := triple.EncodeValue(t.V)
		if err != nil {
			return 0, err
		}
		rows = append(rows, []any{uuidString(t.E), uuidString(t.A), string(enc), int64(i)})
	}

	var n int64
	err := d.WithTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			CREATE TEMP TABLE staging_triples(
				entity_id uuid, attr_id uuid, value jsonb, input_ord bigint NOT NULL
			) ON COMMIT DROP`); err != nil {
			return err
		}
		if _, err := tx.CopyFrom(ctx,
			pgx.Identifier{"staging_triples"},
			[]string{"entity_id", "attr_id", "value", "input_ord"},
			&copySource{rows: rows}); err != nil {
			return fmt.Errorf("copy: %w", err)
		}
		tag, err := tx.Exec(ctx, `
			WITH enhanced AS (
				SELECT s.entity_id, s.attr_id, s.value, s.input_ord,
				       md5(s.value::text)   AS value_md5,
				       (a.cardinality='one') AS ea,
				       (a.value_type ='ref') AS eav,
				       a.is_unique           AS av,
				       a.is_indexed          AS ave,
				       (a.value_type ='ref') AS vae,
				       a.checked_data_type   AS checked_data_type
				  FROM staging_triples s
				  JOIN attrs a ON a.id = s.attr_id AND a.deletion_marked_at IS NULL
			)
			INSERT INTO triples (`+insertCols+`)
			SELECT app_id, entity_id, attr_id, value, value_md5,
			       ea, eav, av, ave, vae, checked_data_type FROM (
				SELECT $1::uuid AS app_id, * FROM (
					SELECT DISTINCT ON (entity_id, attr_id) *
					  FROM enhanced WHERE ea
					 ORDER BY entity_id, attr_id, input_ord DESC
				) ea_rows
			) upserts
			ON CONFLICT (app_id, entity_id, attr_id) WHERE ea DO UPDATE
			SET value = EXCLUDED.value, value_md5 = EXCLUDED.value_md5`, appID)
		if err != nil {
			return fmt.Errorf("ea insert: %w", wrapUnique(err))
		}
		n = tag.RowsAffected()
		tag2, err := tx.Exec(ctx, `
			WITH enhanced AS (
				SELECT $1::uuid AS app_id, s.entity_id, s.attr_id, s.value,
				       md5(s.value::text)   AS value_md5,
				       (a.cardinality='one') AS ea,
				       (a.value_type ='ref') AS eav,
				       a.is_unique           AS av,
				       a.is_indexed          AS ave,
				       (a.value_type ='ref') AS vae,
				       a.checked_data_type   AS checked_data_type
				  FROM staging_triples s
				  JOIN attrs a ON a.id = s.attr_id AND a.deletion_marked_at IS NULL
			)
			INSERT INTO triples (`+insertCols+`)
			SELECT app_id, entity_id, attr_id, value, value_md5,
			       ea, eav, av, ave, vae, checked_data_type
			  FROM enhanced WHERE NOT ea
			ON CONFLICT (app_id, entity_id, attr_id, value_md5) DO NOTHING`, appID)
		if err != nil {
			return fmt.Errorf("remaining insert: %w", wrapUnique(err))
		}
		n += tag2.RowsAffected()
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// uuidString renders the canonical hyphenated lowercase form COPY expects.
func uuidString(u [16]byte) string {
	const hexdig = "0123456789abcdef"
	out := make([]byte, 0, 36)
	for i, b := range u {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			out = append(out, '-')
		}
		out = append(out, hexdig[b>>4], hexdig[b&0x0f])
	}
	return string(out)
}
