package transact

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/triple"
)

// cardinality-one attrs into ONE (e,a)-pair fetch plus ONE batched insert
// (audit backlog: the per-step loop issued 2 queries per step inside the
// write tx).
//
// Cardinality-many attrs retain the old sequential pipeline. A many attr does
// not overwrite its existing row, so collapsing repeated steps to one final
// insert would drop values and would not be equivalent to the old behavior.
// For cardinality-one attrs, deep-merge is a fold where each incoming value is
// applied in step order to the result of the previous step. This preserves
// scalar/map/null transitions exactly while allowing the fetch and write to be
// batched.
func applyDeepMerge(ctx context.Context, tx pgx.Tx, db *storage.DB, appID [16]byte, cat *platform.AttrCatalog, batch []Step, opts Options) error {
	if len(batch) == 0 {
		return nil
	}
	type mergeKey struct{ e, a [16]byte }
	type mergeOp struct {
		triple triple.Triple
		srcs   []any // incoming values in step order
	}
	ops := make([]*mergeOp, 0, len(batch))
	byKey := make(map[mergeKey]*mergeOp, len(batch))
	for _, st := range batch {
		ta, err := parseTripleArgs(st, cat)
		if err != nil {
			return err
		}
		var eid [16]byte
		if err := parseUUID(strings.Trim(string(ta.EID), `"`), &eid); err != nil {
			return err
		}
		incoming, err := parseValueJSON(ta.Value)
		if err != nil {
			return err
		}
		k := mergeKey{eid, ta.AttrID}
		op := byKey[k]
		if op == nil {
			op = &mergeOp{triple: triple.Triple{E: eid, A: ta.AttrID}}
			byKey[k] = op
			ops = append(ops, op)
		}
		op.srcs = append(op.srcs, incoming)
	}

	// The batched write path is equivalent only for cardinality-one attrs.
	// Keep many-cardinality operations in their original step order: each
	// sequential insert can add a distinct value, whereas one final insert
	// would incorrectly discard intermediate values.
	hasMany := false
	oneOps := make([]*mergeOp, 0, len(ops))
	for _, op := range ops {
		a, ok := cat.ByID(op.triple.A)
		if !ok || a.Cardinality != "one" {
			// parseTripleArgs already validates the attr, but preserve the old
			// path if a catalog implementation ever cannot report its shape.
			hasMany = true
			continue
		}
		oneOps = append(oneOps, op)
	}
	if hasMany {
		for _, st := range batch {
			attr, err := parseTripleArgs(st, cat)
			if err != nil {
				return err
			}
			a, ok := cat.ByID(attr.AttrID)
			if !ok || a.Cardinality != "one" {
				if err := applyDeepMergeSequential(ctx, tx, db, appID, cat, []Step{st}, opts); err != nil {
					return err
				}
			}
		}
	}
	if len(oneOps) == 0 {
		return nil
	}

	pairs := make([]storage.EAPair, len(oneOps))
	for i, op := range oneOps {
		pairs[i] = storage.EAPair{E: op.triple.E, A: op.triple.A}
	}
	bases, err := storage.FetchPairs(ctx, tx, appID, pairs)
	if err != nil {
		return err
	}
	baseBy := make(map[mergeKey]any, len(bases))
	for _, row := range bases {
		k := mergeKey{row.Triple.E, row.Triple.A}
		if _, ok := baseBy[k]; !ok {
			// First row per pair = the old rows[0] pick. decodeValue
			// leaves json.Number in place (storage pkg contract); widen
			// so the merged map re-encodes via triple.IsValue.
			baseBy[k] = convertNumbers(row.Triple.V)
		}
	}

	ts := make([]triple.Triple, 0, len(oneOps))
	for _, op := range oneOps {
		merged, hasMerged := baseBy[mergeKey{op.triple.E, op.triple.A}]
		for _, src := range op.srcs { // fold sources in step order
			if !hasMerged {
				merged, hasMerged = src, true
				continue
			}
			merged = deepMergeJSON(merged, src)
		}
		if !hasMerged {
			continue // unreachable: every op has ≥1 src
		}
		op.triple.V = merged
		ts = append(ts, op.triple)
	}
	return db.SetTx(ctx, tx, appID, cat, ts, opts.OverwriteT)
}

// applyDeepMergeSequential is the compatibility path for many-cardinality
// attrs. It intentionally mirrors the pre-batching implementation one step at
// a time, including its first-row selection and insert semantics.
func applyDeepMergeSequential(ctx context.Context, tx pgx.Tx, db *storage.DB, appID [16]byte, cat *platform.AttrCatalog, batch []Step, opts Options) error {
	for _, st := range batch {
		ta, err := parseTripleArgs(st, cat)
		if err != nil {
			return err
		}
		var eid [16]byte
		if err := parseUUID(strings.Trim(string(ta.EID), `"`), &eid); err != nil {
			return err
		}
		incoming, err := parseValueJSON(ta.Value)
		if err != nil {
			return err
		}
		rows, err := storage.FetchTx(ctx, tx, appID, storage.FetchFilter{
			EntityIDs: [][16]byte{eid},
			AttrIDs:   [][16]byte{ta.AttrID},
		})
		if err != nil {
			return err
		}
		var merged any
		if len(rows) == 0 {
			merged = incoming
		} else {
			merged = deepMergeJSON(convertNumbers(rows[0].Triple.V), incoming)
		}
		if err := db.SetTx(ctx, tx, appID, cat, []triple.Triple{{E: eid, A: ta.AttrID, V: merged}}, opts.OverwriteT); err != nil {
			return err
		}
	}
	return nil
}

func deepMergeJSON(dst, src any) any {
	dm, dok := dst.(map[string]any)
	sm, sok := src.(map[string]any)
	if !dok || !sok {
		return src
	}
	out := make(map[string]any, len(dm))
	for k, v := range dm {
		out[k] = v
	}
	for k, v := range sm {
		if ov, ok := out[k]; ok {
			out[k] = deepMergeJSON(ov, v)
		} else {
			out[k] = v
		}
	}
	return out
}
