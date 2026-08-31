package transact

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/triple"
)

func applyAddTriples(ctx context.Context, tx pgx.Tx, db *storage.DB, appID [16]byte, cat *platform.AttrCatalog, batch []Step, overwriteT bool) error {
	ts := make([]triple.Triple, 0, len(batch))
	for _, st := range batch {
		ta, err := parseTripleArgs(st, cat)
		if err != nil {
			return err
		}
		var eid [16]byte
		if err := parseUUID(strings.Trim(string(ta.EID), `"`), &eid); err != nil {
			return fmt.Errorf("eid %s: %w", ta.EID, err)
		}
		v, err := parseValueJSON(ta.Value)
		if err != nil {
			return err
		}
		// Enforce the attr's declared value type before insert: the DB CHECK
		// is authoritative but only speaks in opaque constraint errors, and
		// attrs created without a checked-data-type never reach it.
		if a, ok := cat.ByID(ta.AttrID); ok && a.CheckedDataType != nil {
			if err := tripleValueMatches(*a.CheckedDataType, v); err != nil {
				return fmt.Errorf("add-triple: %w", err)
			}
		}
		ts = append(ts, triple.Triple{E: eid, A: ta.AttrID, V: v})
	}
	return db.SetTx(ctx, tx, appID, cat, ts, overwriteT)
}

// applyRetract resolves retract steps into exact (e,a,value) triples and
// deletes them INSIDE the caller's transaction: a failed batch must leave
// previously-retracted values in place, and committed retracts must always
// have their journal row (storage.DeleteTx keeps the single-tx invariant).
func applyRetract(ctx context.Context, tx pgx.Tx, db *storage.DB, appID [16]byte, cat *platform.AttrCatalog, batch []Step) error {
	ts := make([]triple.Triple, 0, len(batch))
	for _, st := range batch {
		ta, err := parseTripleArgs(st, cat)
		if err != nil {
			// A malformed retract must abort the transaction, not silently
			// no-op into a zero-value delete target.
			return fmt.Errorf("retract-triple: %w", err)
		}
		var eid [16]byte
		if err := parseUUID(strings.Trim(string(ta.EID), `"`), &eid); err != nil {
			return fmt.Errorf("retract-triple: eid %s: %w", ta.EID, err)
		}
		v, err := parseValueJSON(ta.Value)
		if err != nil {
			return fmt.Errorf("retract-triple: value: %w", err)
		}
		ts = append(ts, triple.Triple{E: eid, A: ta.AttrID, V: v})
	}
	_, err := db.DeleteTx(ctx, tx, appID, ts)
	return err
}
