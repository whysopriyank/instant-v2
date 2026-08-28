package transact

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/triple"
)

// Options control one Transact call (admin bypass, mode defaults, etc.).
type Options struct {
	Admin      bool
	AuthUser   map[string]any // {id: uuid string, email: string, type: string}
	RuleParams map[string]any
	OverwriteT bool
	Request    perms.RequestInfo
}

// Result of one successful transact.
type Result struct {
	TxID int64
	EIDs [][]byte
	// AttrsChanged reports whether the batch mutated attribute metadata
	// (add/update/delete/restore-attr). Callers must drop their cached
	// catalogs when set — the in-tx overlay dies with the transaction.
	AttrsChanged bool
}

// Transact is the Phase 2 entrypoint. It mirrors db/transaction.clj's
// transact-without-tx-conn! pipeline but with permission checks layered by the
// caller-supplied perm gate (Check). Steps:
//
//  1. Group ops by first-appearance order (tx-steps-order).
//  2. Journal the transaction first (transaction-model/create! must be the
//     first write — the invalidator relies on this ordering).
//  3. Per-group execution in group order:
//     rule-params → captured; add-attr → not yet in Phase 2 catalog but
//     written; deep-merge → fetch+merge+insert; retract → delete; the rest.
//  4. Required-field validation for touched entities.
//
// maxTxSteps bounds one transaction's step count. Permission probes cost
// O(steps) point queries inside the write tx, so an unbounded batch pinned
// a pooled writer connection for the full statement timeout (audit L1).
const maxTxSteps = 10000

func Transact(
	ctx context.Context,
	db *storage.DB,
	catalog *platform.AttrCatalog,
	appID [16]byte,
	steps []Step,
	opts Options,
	ruleDoc *perms.RuleDoc,
) (Result, error) {
	if len(steps) > maxTxSteps {
		return Result{}, fmt.Errorf("transact: too many steps (%d > %d)", len(steps), maxTxSteps)
	}
	ordered := orderSteps(steps)
	var res Result
	for _, st := range steps {
		switch st.Op {
		case "add-attr", "update-attr", "delete-attr", "restore-attr":
			res.AttrsChanged = true
		}
	}
	err := db.WithTx(ctx, func(tx pgx.Tx) error {
		txID, err := storage.RecordTransaction(ctx, tx, appID)
		if err != nil {
			return fmt.Errorf("transact: journal: %w", err)
		}
		res.TxID = txID

		// add-attr first: the frozen protocol lets clients mint attr ids and
		// reference them in later steps of the SAME batch, so creation must
		// precede lookup/value resolution. Created attrs register into a
		// per-tx catalog clone — the shared cache updates via invalidation.
		txCat := catalog
		if hasOp(steps, "add-attr") || hasOp(steps, "update-attr") {
			txCat = catalog.Clone()
			// clientAttrID → serverAttrID for attrs the server already
			// stores under another id (replayed or implicit-attr replays,
			// e.g. <etype>/id). Mirrors the TS client's _rewriteMutations.
			attrAlias := map[[16]byte][16]byte{}
			for _, st := range steps {
				if st.Op != "add-attr" {
					continue
				}
				a, err := parseWireAttr(st.Args[0])
				if err != nil {
					return fmt.Errorf("transact: add-attr: %w", err)
				}
				if err := validateAddRequired(ctx, tx, appID, a); err != nil {
					return fmt.Errorf("transact: add-attr: %w", err)
				}
				existing, found, err := platform.FindAttrByIdent(ctx, tx, appID, deref(a.Etype), deref(a.Label))
				if err != nil {
					return fmt.Errorf("transact: add-attr: %w", err)
				}
				if found {
					// Adopt the stored definition; alias the client's id so
					// same-batch triples resolve and are rewritten below.
					attrAlias[a.ID] = existing.ID
					txCat.Add(existing)
					aliased := existing
					aliased.ID = a.ID
					txCat.Add(aliased)
					continue
				}
				if !opts.Admin && ruleDoc != nil {
					allow, err := perms.Check("attrs", "create", ruleDoc, perms.Bindings{
						Auth: opts.AuthUser, RuleParams: opts.RuleParams, Request: opts.Request,
					})
					if err != nil {
						return fmt.Errorf("transact: attrs.create: %w", err)
					}
					if !allow {
						return fmt.Errorf("transact: attrs.create denied")
					}
				}
				if err := platform.CreateAttrWithID(ctx, tx, appID, a); err != nil {
					return fmt.Errorf("transact: add-attr: %w", err)
				}
				txCat.Add(a)
			}
			if len(attrAlias) > 0 {
				rewriteAttrRefs(steps, attrAlias)
			}
		}

		// Resolve any lookup-ref eids/values against committed state so far.
		if err := resolveEntityIDs(ctx, tx, appID, steps, txCat); err != nil {
			return err
		}
		if err := resolveValues(ctx, tx, appID, steps, txCat); err != nil {
			return err
		}

		// Permission gate: every mutating step is checked against the app's
		// RuleDoc with real data/newData/auth bindings BEFORE execution.
		// Fail-closed — compile or eval errors deny the whole batch. Admin
		// callers bypass (v1 :admin? true semantics).
		if ruleDoc != nil && !opts.Admin {
			if err := enforcePerms(ctx, tx, appID, txCat, steps, opts, ruleDoc, storage.FetchTx); err != nil {
				return err
			}
		}

		// Capture rule-params scoped by entity.
		capturedRuleParams := map[string]map[string]any{}
		var requiredUpdates []platform.Attr
		var cascadeTouched [][16]byte

		for _, op := range ordered {
			batch := filterSteps(steps, op)
			switch op {
			case "rule-params":
				for _, st := range batch {
					// [lookup_or_eid, ?etype, params]
					eidStr := rawToString(st.Args[0])
					paramsRaw := st.Args[len(st.Args)-1]
					var params map[string]any
					_ = json.Unmarshal(paramsRaw, &params)
					if eidStr != "" {
						capturedRuleParams[eidStr] = params
					}
				}

			case "add-attr":
				// Handled in the pre-pass above (attrs must exist before
				// same-batch triples resolve); nothing left to do here.

			case "update-attr":
				updates, err := applyRequiredAttrUpdates(ctx, tx, appID, txCat, batch, opts, ruleDoc)
				if err != nil {
					return err
				}
				requiredUpdates = append(requiredUpdates, updates...)

			case "delete-attr":
				if !opts.Admin {
					return fmt.Errorf("transact: attrs.delete denied (admin only in Phase 2)")
				}

			case "add-triple":
				if err := applyAddTriples(ctx, tx, db, appID, txCat, batch, opts.OverwriteT); err != nil {
					return err
				}

			case "retract-triple":
				if err := applyRetract(ctx, tx, db, appID, txCat, batch); err != nil {
					return err
				}

			case "deep-merge-triple":
				if err := applyDeepMerge(ctx, tx, db, appID, txCat, batch, opts); err != nil {
					return err
				}

			case "delete-entity":
				refs, err := applyDeleteEntity(ctx, tx, db, appID, txCat, batch)
				if err != nil {
					return err
				}
				cascadeTouched = append(cascadeTouched, refs...)

			default:
				// forward-compat: unknown ops no-op
				_ = op
			}
		}

		touched, _ := collectTouchedEntities(steps)
		touched = appendDistinctEntities(touched, cascadeTouched...)
		if txCat != nil && len(touched) > 0 {
			// Validate against the transaction-local catalog so required attrs
			// created earlier in this batch are enforced before commit.
			if err := validateRequired(ctx, tx, appID, txCat, touched); err != nil {
				return err
			}
		}
		if err := validateUpdatedRequired(ctx, tx, appID, requiredUpdates); err != nil {
			return err
		}
		return nil
	})
	return res, err
}

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
	return applyInsertInto(ctx, tx, appID, cat, ts, overwriteT)
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

// applyDeepMerge batches the per-step fetch→merge→write pipeline for
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
	return applyInsertInto(ctx, tx, appID, cat, ts, opts.OverwriteT)
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
		if err := applyInsertInto(ctx, tx, appID, cat, []triple.Triple{{E: eid, A: ta.AttrID, V: merged}}, opts.OverwriteT); err != nil {
			return err
		}
	}
	return nil
}

func applyInsertInto(ctx context.Context, tx pgx.Tx, appID [16]byte, cat *platform.AttrCatalog, ts []triple.Triple, overwriteT bool) error {
	d := &storage.DB{Pool: nil}
	// Bypass Pool path; reuse the same tx for atomics.
	if err := d.SetTx(ctx, tx, appID, cat, ts, overwriteT); err != nil {
		return err
	}
	return nil
}

func applyDeleteEntity(ctx context.Context, tx pgx.Tx, db *storage.DB, appID [16]byte, cat *platform.AttrCatalog, batch []Step) ([][16]byte, error) {
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

func collectTouchedEntities(steps []Step) ([][16]byte, error) {
	seen := make(map[[16]byte]struct{})
	var out [][16]byte
	for _, st := range steps {
		switch st.Op {
		case "add-triple", "deep-merge-triple", "retract-triple", "delete-entity":
			if len(st.Args) == 0 {
				continue
			}
			var eidStr string
			_ = json.Unmarshal(st.Args[0], &eidStr)
			var u [16]byte
			if err := parseUUID(eidStr, &u); err == nil {
				if _, ok := seen[u]; !ok {
					seen[u] = struct{}{}
					out = append(out, u)
				}
			}
		}
	}
	return out, nil
}

func appendDistinctEntities(base [][16]byte, extra ...[16]byte) [][16]byte {
	seen := make(map[[16]byte]struct{}, len(base)+len(extra))
	for _, id := range base {
		seen[id] = struct{}{}
	}
	for _, id := range extra {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		base = append(base, id)
	}
	return base
}

// tripleValueMatches mirrors triples_valid_value (migration 001) so clients
// get a clean 4xx-class error instead of an opaque CHECK-constraint failure.
// JSON null is always allowed (v1 semantics); dates accept numbers (epoch ms)
// or strings — the DB CHECK stays authoritative for string date formats.
func tripleValueMatches(cdt string, v any) error {
	if v == nil {
		return nil
	}
	bad := func() error {
		return fmt.Errorf("value does not match attr checked-data-type %q", cdt)
	}
	switch cdt {
	case "string":
		if _, ok := v.(string); !ok {
			return bad()
		}
	case "number":
		switch v.(type) {
		case float64, int64, json.Number:
		default:
			return bad()
		}
	case "boolean":
		if _, ok := v.(bool); !ok {
			return bad()
		}
	case "date":
		switch v.(type) {
		case float64, int64, json.Number, string:
		default:
			return bad()
		}
	}
	return nil
}

func parseValueJSON(raw json.RawMessage) (any, error) {
	if string(raw) == "null" {
		return nil, nil
	}
	var v any
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	// Nested numbers arrive as json.Number, which triple.IsValue rejects;
	// widen them to int64/float64 everywhere so deep-merged objects encode.
	return convertNumbers(v), nil
}

func convertNumbers(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			x[k] = convertNumbers(e)
		}
		return x
	case []any:
		for i, e := range x {
			x[i] = convertNumbers(e)
		}
		return x
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i
		}
		f, _ := x.Float64()
		return f
	default:
		return v
	}
}

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

// applyRequiredAttrUpdates applies the supported update-attr patch and
// returns the resulting rows for the post-write coverage check. Metadata
// updates are kept on the active transaction so a failed coverage check rolls
// back together with the caller's data writes.
func applyRequiredAttrUpdates(ctx context.Context, tx pgx.Tx, appID [16]byte, cat *platform.AttrCatalog, batch []Step, opts Options, ruleDoc *perms.RuleDoc) ([]platform.Attr, error) {
	updates := make([]platform.Attr, 0, len(batch))
	for _, st := range batch {
		if len(st.Args) != 1 {
			return nil, fmt.Errorf("transact: update-attr: want one payload")
		}
		patch, err := parseRequiredAttrUpdate(st.Args[0])
		if err != nil {
			return nil, fmt.Errorf("transact: update-attr: %w", err)
		}
		var id [16]byte
		if err := parseUUID(patch.ID, &id); err != nil {
			return nil, fmt.Errorf("transact: update-attr: %w", err)
		}
		current, found, err := platform.FindAttrByID(ctx, tx, appID, id)
		if err != nil {
			return nil, fmt.Errorf("transact: update-attr: %w", err)
		}
		if !found {
			return nil, fmt.Errorf("transact: update-attr: unknown attr %s", patch.ID)
		}
		// Both identity directions are security-sensitive namespace gates. The
		// stored row, rather than only the wire payload, is authoritative here:
		// update-attr intentionally has no identity fields, so a forged client
		// payload cannot bypass a reserved reverse etype either.
		if current.Etype != nil && perms.ReservedNamespaces[*current.Etype] {
			return nil, fmt.Errorf("transact: update-attr: %q is a reserved namespace", *current.Etype)
		}
		if current.ReverseEtype != nil && perms.ReservedNamespaces[*current.ReverseEtype] {
			return nil, fmt.Errorf("transact: update-attr: reverse namespace %q is reserved", *current.ReverseEtype)
		}
		// update-attr is an attrs-level operation, not an entity mutation, so
		// enforcePerms does not see it. Keep the same fail-closed rule gate and
		// bindings as the add-attr path. Admin bypasses this permission only;
		// namespace gates above still apply.
		if !opts.Admin && ruleDoc != nil {
			allow, err := perms.Check("attrs", "update", ruleDoc, perms.Bindings{
				Auth: opts.AuthUser, RuleParams: opts.RuleParams, Request: opts.Request,
			})
			if err != nil {
				return nil, fmt.Errorf("transact: attrs.update: %w", err)
			}
			if !allow {
				return nil, fmt.Errorf("transact: attrs.update denied")
			}
		}
		current.IsRequired = *patch.Required
		if _, err := tx.Exec(ctx, `
			UPDATE attrs
			   SET is_required = $1
			 WHERE app_id = $2 AND id = $3 AND deletion_marked_at IS NULL`,
			current.IsRequired, appID, id); err != nil {
			return nil, fmt.Errorf("transact: update-attr: %w", err)
		}
		cat.Add(current)
		updates = append(updates, current)
	}
	return updates, nil
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
	for _, attr := range catalogAttrs(cat) {
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
		return fmt.Errorf("Missing required attribute %s", parts[0])
	}
	return fmt.Errorf("Missing required attributes %s", strings.Join(parts, "; "))
}

func catalogAttrs(cat *platform.AttrCatalog) []platform.Attr {
	if cat == nil {
		return nil
	}
	return cat.Attrs()
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

func orderSteps(steps []Step) []string {
	seen := make(map[string]struct{})
	var order []string
	for _, s := range steps {
		if _, ok := seen[s.Op]; !ok {
			seen[s.Op] = struct{}{}
			order = append(order, s.Op)
		}
	}
	return order
}

func filterSteps(steps []Step, op string) []Step {
	var out []Step
	for _, s := range steps {
		if s.Op == op {
			out = append(out, s)
		}
	}
	return out
}

func rawToString(raw json.RawMessage) string {
	var s string
	_ = json.Unmarshal(raw, &s)
	return s
}

func etypeFor(cat *platform.AttrCatalog, attrID [16]byte) string {
	if a, ok := cat.ByID(attrID); ok {
		if a.Etype != nil {
			return *a.Etype
		}
	}
	return "$default"
}

// entityProjection projects one entity's committed triples into a
// label→value map (the `data` binding v1 rules see). Cardinality-many attrs
// accumulate as arrays; unknown attrs are skipped.
type permissionFetcher func(context.Context, pgx.Tx, [16]byte, storage.FetchFilter) ([]storage.Enhanced, error)

func entityProjection(ctx context.Context, tx pgx.Tx, appID [16]byte, eid [16]byte, cat *platform.AttrCatalog) (map[string]any, error) {
	return entityProjectionWithFetcher(ctx, tx, appID, eid, cat, storage.FetchTx)
}

func entityProjectionWithFetcher(ctx context.Context, tx pgx.Tx, appID [16]byte, eid [16]byte, cat *platform.AttrCatalog, fetch permissionFetcher) (map[string]any, error) {
	out := map[string]any{}
	rows, err := fetch(ctx, tx, appID, storage.FetchFilter{
		EntityIDs: [][16]byte{eid},
	})
	if err != nil {
		return nil, fmt.Errorf("fetch entity projection: %w", err)
	}
	for _, r := range rows {
		a, ok := cat.ByID(r.Triple.A)
		if !ok || a.Label == nil {
			continue
		}
		label := *a.Label
		if a.Cardinality == "many" {
			arr, _ := out[label].([]any)
			out[label] = append(arr, r.Triple.V)
		} else {
			out[label] = r.Triple.V
		}
	}
	return out, nil
}

func permBindings(opts Options, data, newData map[string]any) perms.Bindings {
	return perms.Bindings{
		Data:       data,
		NewData:    newData,
		Auth:       opts.AuthUser,
		RuleParams: opts.RuleParams,
		Request:    opts.Request,
	}
}

// enforcePerms checks every mutating step in the batch against doc. Runs
// inside the write transaction after lookup resolution so existence probes
// and data projections see same-batch state. Any check error or denial
// aborts the whole transaction (fail-closed).
func enforcePerms(ctx context.Context, tx pgx.Tx, appID [16]byte,
	cat *platform.AttrCatalog, steps []Step, opts Options, doc *perms.RuleDoc, fetch permissionFetcher,
) error {
	for _, op := range orderSteps(steps) {
		switch op {
		case "add-triple", "deep-merge-triple":
			for _, st := range filterSteps(steps, op) {
				ta, err := parseTripleArgs(st, cat)
				if err != nil {
					return err
				}
				var eid [16]byte
				if err := parseUUID(strings.Trim(string(ta.EID), `"`), &eid); err != nil {
					return fmt.Errorf("perms: eid %s: %w", ta.EID, err)
				}
				action := "create"
				existing, err := fetch(ctx, tx, appID, storage.FetchFilter{
					EntityIDs: [][16]byte{eid}, AttrIDs: [][16]byte{ta.AttrID},
				})
				if err != nil {
					return err
				}
				if len(existing) > 0 {
					action = "update"
				}
				etype := etypeFor(cat, ta.AttrID)
				data, err := entityProjectionWithFetcher(ctx, tx, appID, eid, cat, fetch)
				if err != nil {
					return fmt.Errorf("perms %s projection: %w", etype, err)
				}
				newData := cloneMap(data)
				incoming, err := parseValueJSON(ta.Value)
				if err != nil {
					return err
				}
				if label := attrLabel(cat, ta.AttrID); label != "" {
					newData[label] = incoming
				}
				allow, err := perms.Check(etype, action, doc, permBindings(opts, data, newData))
				if err != nil {
					return fmt.Errorf("perms %s %s: %w", etype, action, err)
				}
				if !allow {
					return fmt.Errorf("transact: permission denied (%s %s)", action, etype)
				}
			}

		case "retract-triple":
			for _, st := range filterSteps(steps, op) {
				ta, err := parseTripleArgs(st, cat)
				if err != nil {
					return err
				}
				var eid [16]byte
				if err := parseUUID(strings.Trim(string(ta.EID), `"`), &eid); err != nil {
					return fmt.Errorf("perms: eid %s: %w", ta.EID, err)
				}
				etype := etypeFor(cat, ta.AttrID)
				data, err := entityProjectionWithFetcher(ctx, tx, appID, eid, cat, fetch)
				if err != nil {
					return fmt.Errorf("perms %s projection: %w", etype, err)
				}
				newData := cloneMap(data)
				if label := attrLabel(cat, ta.AttrID); label != "" {
					delete(newData, label) // post-retract state approximation
				}
				allow, err := perms.Check(etype, "delete", doc, permBindings(opts, data, newData))
				if err != nil {
					return fmt.Errorf("perms %s delete: %w", etype, err)
				}
				if !allow {
					return fmt.Errorf("transact: permission denied (delete %s)", etype)
				}
			}

		case "delete-entity":
			for _, st := range filterSteps(steps, op) {
				var eidStr string
				if err := json.Unmarshal(st.Args[0], &eidStr); err != nil {
					continue // non-uuid eids never reached applyDeleteEntity either
				}
				var eid [16]byte
				if err := parseUUID(eidStr, &eid); err != nil {
					continue
				}
				etype := "$default"
				if len(st.Args) == 2 {
					var e string
					if json.Unmarshal(st.Args[1], &e) == nil && e != "" {
						etype = e
					}
				}
				data, err := entityProjectionWithFetcher(ctx, tx, appID, eid, cat, fetch)
				if err != nil {
					return fmt.Errorf("perms %s projection: %w", etype, err)
				}
				allow, err := perms.Check(etype, "delete", doc, permBindings(opts, data, map[string]any{}))
				if err != nil {
					return fmt.Errorf("perms %s delete: %w", etype, err)
				}
				if !allow {
					return fmt.Errorf("transact: permission denied (delete %s)", etype)
				}
			}
		}
	}
	return nil
}

func cloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func attrLabel(cat *platform.AttrCatalog, id [16]byte) string {
	if a, ok := cat.ByID(id); ok && a.Label != nil {
		return *a.Label
	}
	return ""
}

// TouchedTriple describes one resolved triple-level write for change-routed
// invalidation (docs/09-tier2-architecture.md §T2.5).
type TouchedTriple struct {
	EntityID string // canonical uuid text
	AttrID   string
	Etype    string
}

// ResolveTriples maps triple-class steps onto the entities they touch. The
// second return is false when any step is NOT a plain triple op — delete-entity
// cascades, schema ops, unknown ops — and callers must fall back to
// topic-wide invalidation instead of pretending to know the change set.
func ResolveTriples(steps []Step, cat *platform.AttrCatalog) ([]TouchedTriple, bool) {
	out := make([]TouchedTriple, 0, len(steps))
	for _, st := range steps {
		switch st.Op {
		case "add-triple", "deep-merge-triple", "retract-triple":
			ta, err := parseTripleArgs(st, cat)
			if err != nil {
				return nil, false
			}
			eid := strings.Trim(string(ta.EID), `"`)
			a, ok := cat.ByID(ta.AttrID)
			if !ok {
				return nil, false
			}
			attrID := platform.UUIDToStr(ta.AttrID)
			etype := "$default"
			if a.Etype != nil {
				etype = *a.Etype
			}
			out = append(out, TouchedTriple{EntityID: eid, AttrID: attrID, Etype: etype})
		default:
			return nil, false
		}
	}
	return out, true
}
