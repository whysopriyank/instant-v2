package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// Export streams appID as a v2 NDJSON dump to w. Memory use is bounded: rows
// stream out of Postgres one at a time via pgx cursors; nothing accumulates.
func Export(ctx context.Context, w io.Writer, pool *pgxpool.Pool, appID [16]byte, opts ExportOptions) (Counts, error) {
	var counts Counts

	var title string
	var creator [16]byte
	err := pool.QueryRow(ctx, `SELECT title, creator_id FROM apps WHERE id=$1`, appID).Scan(&title, &creator)
	if errors.Is(err, pgx.ErrNoRows) {
		return counts, ErrAppNotFound
	}
	if err != nil {
		return counts, fmt.Errorf("backup: app lookup: %w", err)
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return counts, fmt.Errorf("backup: acquire conn: %w", err)
	}
	defer conn.Release()

	hw := hashWriter{w: w, h: sha256.New()}
	header := dumpHeader{
		Kind:      "header",
		Format:    dumpFormat,
		Version:   dumpVer,
		AppID:     platform.UUIDToStr(appID),
		Title:     title,
		CreatorID: platform.UUIDToStr(creator),
	}
	hb, err := json.Marshal(header)
	if err != nil {
		return counts, err
	}
	if err := hw.writeLine(string(hb)); err != nil {
		return counts, err
	}

	rw := &recordWriter{hw: hw, skip: opts.FromOffset}

	const attrSQL = `SELECT json_build_object(
		'kind', 'attr',
		'id', id, 'etype', etype, 'label', label,
		'reverse_etype', reverse_etype, 'reverse_label', reverse_label,
		'value_type', value_type, 'cardinality', cardinality,
		'is_unique', is_unique, 'is_indexed', is_indexed,
		'forward_ident', forward_ident, 'reverse_ident', reverse_ident,
		'checked_data_type', checked_data_type::text,
		'checking_data_type', checking_data_type,
		'is_required', is_required,
		'deletion_marked_at', to_json(deletion_marked_at))::text
	FROM attrs WHERE app_id=$1 ORDER BY id`
	if counts.Attrs, err = streamSection(ctx, conn, rw, attrSQL, appID); err != nil {
		return counts, fmt.Errorf("backup: export attrs: %w", err)
	}

	const tripleSQL = `SELECT json_build_object(
		'kind', 'triple',
		'entity_id', entity_id, 'attr_id', attr_id,
		'value', value,
		'created_at', to_json(created_at))::text
	FROM triples WHERE app_id=$1 ORDER BY entity_id, attr_id, value_md5`
	if counts.Triples, err = streamSection(ctx, conn, rw, tripleSQL, appID); err != nil {
		return counts, fmt.Errorf("backup: export triples: %w", err)
	}

	const ruleSQL = `SELECT json_build_object(
		'kind', 'rule', 'code', code, 'version', version)::text
	FROM rules WHERE app_id=$1`
	if counts.Rules, err = streamSection(ctx, conn, rw, ruleSQL, appID); err != nil {
		return counts, fmt.Errorf("backup: export rules: %w", err)
	}

	const txSQL = `SELECT json_build_object(
		'kind', 'transaction', 'id', id, 'created_at', to_json(created_at))::text
	FROM transactions WHERE app_id=$1 ORDER BY id`
	if counts.Transactions, err = streamSection(ctx, conn, rw, txSQL, appID); err != nil {
		return counts, fmt.Errorf("backup: export transactions: %w", err)
	}

	sum := hw.h.Sum(nil)
	total := counts.Attrs + counts.Triples + counts.Rules + counts.Transactions
	trailer := fmt.Sprintf(`{"kind":"checksum","sha256":"%x","records":%d}`, sum, total)
	if _, err := io.WriteString(w, trailer+"\n"); err != nil {
		return counts, err
	}
	return counts, nil
}

func streamSection(ctx context.Context, conn *pgxpool.Conn, rw *recordWriter, sqlStr string, appID [16]byte) (int64, error) {
	start := rw.count
	rows, err := conn.Query(ctx, sqlStr, appID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var buf bytes.Buffer
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return 0, err
		}
		// Canonicalize to compact JSON: json_build_object pads " : " and a
		// one-line guarantee keeps the NDJSON framing safe.
		buf.Reset()
		if err := json.Compact(&buf, []byte(line)); err != nil {
			return 0, fmt.Errorf("backup: non-JSON export row: %v", err)
		}
		if err := rw.emit(buf.String()); err != nil {
			return 0, err
		}
	}
	return rw.count - start, rows.Err()
}
