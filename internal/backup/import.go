package backup

import (
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

var errBadHeader = errors.New("backup: invalid dump header")

// ErrAppMismatch is returned when a dump's header app_id does not match the
// app the caller authenticated for. Restore must never write across tenant
// boundaries on header data alone.
var ErrAppMismatch = errors.New("backup: dump app_id does not match the authenticated app")

// Import consumes a v2 NDJSON dump from r into the database inside a single
// transaction. routeAppID is the app the caller authenticated against — the
// dump header MUST carry the same id or the import aborts with
// ErrAppMismatch before any row is written. A non-empty target is refused,
// including re-importing an identical dump. The trailing checksum is verified against
// everything consumed before it; any mismatch aborts the transaction.
// Unknown apps are created on the fly (with a synthetic creator user when
// needed), so a fresh schema accepts a dump directly.
func Import(ctx context.Context, pool *pgxpool.Pool, r io.Reader, routeAppID [16]byte) (Counts, error) {
	var counts Counts
	tx, err := pool.Begin(ctx)
	if err != nil {
		return counts, fmt.Errorf("backup: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	sc := newLineScanner(r)
	hash := sha256.New()

	if !sc.Next() {
		return counts, sc.err(errBadHeader)
	}
	line := sc.line()
	hash.Write([]byte(line + "\n"))
	var hdr dumpHeader
	if err := json.Unmarshal([]byte(line), &hdr); err != nil {
		return counts, fmt.Errorf("%w: %v", errBadHeader, err)
	}
	if hdr.Kind != "header" || hdr.Format != dumpFormat || (hdr.Version != 1 && hdr.Version != dumpVer) || hdr.AppID == "" {
		return counts, fmt.Errorf("%w: kind=%q format=%q version=%d", errBadHeader, hdr.Kind, hdr.Format, hdr.Version)
	}
	appID, err := platform.ScanUUIDErr(hdr.AppID)
	if err != nil {
		return counts, fmt.Errorf("%w: bad app_id: %v", errBadHeader, err)
	}
	if appID != routeAppID {
		return counts, ErrAppMismatch
	}
	if err := requireEmptyTarget(ctx, tx, appID); err != nil {
		return counts, err
	}
	creator := appID // deterministic fallback when header lacks creator_id
	if hdr.CreatorID != "" {
		if c, cerr := platform.ScanUUIDErr(hdr.CreatorID); cerr == nil {
			creator = c
		}
	}
	if err := ensureAppRow(ctx, tx, appID, creator, hdr.Title); err != nil {
		return counts, err
	}

	batch := newTripleBatch()
	flushCounts := func() error { return batch.flush(ctx, tx, appID, &counts.Triples) }

	for sc.Next() {
		line := sc.line()
		kind, body, err := decodeKinded(line)
		if err != nil {
			return counts, err
		}
		if kind == "checksum" {
			var ck struct {
				SHA256  string `json:"sha256"`
				Records int64  `json:"records"`
			}
			if err := json.Unmarshal(body, &ck); err != nil {
				return counts, fmt.Errorf("backup: bad checksum line: %v", err)
			}
			logical := counts.Attrs + counts.Triples + int64(batch.len()) +
				counts.Rules + counts.Transactions
			if got := fmt.Sprintf("%x", hash.Sum(nil)); got != ck.SHA256 {
				return counts, fmt.Errorf("backup: checksum mismatch: expected %s, computed %s", ck.SHA256, got)
			}
			if logical != ck.Records {
				return counts, fmt.Errorf("backup: checksum mismatch: expected %d records, saw %d", ck.Records, logical)
			}
			if sc.Next() {
				return counts, fmt.Errorf("backup: unexpected content after checksum line")
			}
			if err := sc.err(nil); err != nil {
				return counts, err
			}
			if err := finishImport(ctx, tx, flushCounts, &counts); err != nil {
				return counts, err
			}
			if err := tx.Commit(ctx); err != nil {
				return counts, restoreCommitError(err)
			}
			return counts, nil
		}

		hash.Write([]byte(line + "\n"))
		switch kind {
		case "attr":
			if err := importAttr(ctx, tx, appID, body, hdr.Version >= 2); err != nil {
				return counts, fmt.Errorf("backup: import attr: %w", err)
			}
			counts.Attrs++
		case "triple":
			if err := batch.add(body); err != nil {
				return counts, fmt.Errorf("backup: import triple: %w", err)
			}
			if batch.len() >= tripleBatchSize {
				if err := flushCounts(); err != nil {
					return counts, err
				}
			}
		case "rule":
			if err := importRule(ctx, tx, appID, body); err != nil {
				return counts, fmt.Errorf("backup: import rule: %w", err)
			}
			counts.Rules++
		case "transaction":
			if err := importTransaction(ctx, tx, appID, body); err != nil {
				return counts, fmt.Errorf("backup: import transaction: %w", err)
			}
			counts.Transactions++
		default:
			return counts, fmt.Errorf("backup: unknown record kind %q", kind)
		}
	}
	if e := sc.err(nil); e != nil {
		return counts, e
	}
	return counts, errors.New("backup: dump ended without checksum line")
}

func finishImport(ctx context.Context, tx pgx.Tx, flush func() error, counts *Counts) error {
	if err := flush(); err != nil {
		return err
	}
	// ALTER SEQUENCE is transactional; setval would survive a failed restore.
	// The admission table lock excludes INSERT/nextval through journal writers.
	var schema, name string
	if err := tx.QueryRow(ctx, `SELECT n.nspname, c.relname FROM pg_class c
		JOIN pg_namespace n ON n.oid=c.relnamespace
		WHERE c.oid=pg_get_serial_sequence('transactions','id')::regclass`).Scan(&schema, &name); err != nil {
		return fmt.Errorf("backup: find transactions sequence: %w", err)
	}
	sequence := pgx.Identifier{schema, name}.Sanitize()
	var next int64
	if err := tx.QueryRow(ctx, `SELECT GREATEST(last_value + CASE WHEN is_called THEN 1 ELSE 0 END,
		(SELECT COALESCE(MAX(id),0)+1 FROM transactions)) FROM `+sequence).Scan(&next); err != nil {
		return fmt.Errorf("backup: read transactions sequence: %w", err)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("ALTER SEQUENCE %s RESTART WITH %d", sequence, next)); err != nil {
		return fmt.Errorf("backup: reset transactions sequence: %w", err)
	}
	return nil
}
