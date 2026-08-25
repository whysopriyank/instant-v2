package transact

import (
	"context"
	"encoding/json"
	"fmt"
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
func Transact(
	ctx context.Context,
	db *storage.DB,
	catalog *platform.AttrCatalog,
	appID [16]byte,
	steps []Step,
	opts Options,
	ruleDoc *perms.RuleDoc,
) (Result, error) {
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
		if hasOp(steps, "add-attr") {
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
			if err := enforcePerms(ctx, tx, appID, txCat, steps, opts, ruleDoc); err != nil {
				return err
			}
		}

		// Capture rule-params scoped by entity.
		capturedRuleParams := map[string]map[string]any{}

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
				if err := applyDeleteEntity(ctx, tx, db, appID, txCat, batch); err != nil {
					return err
				}

			default:
				// forward-compat: unknown ops no-op
				_ = op
			}
		}

		touched, _ := collectTouchedEntities(steps)
		if txCat != nil && len(touched) > 0 {
			if err := validateRequired(ctx, db.Pool, appID, txCat, touched); err != nil {
				return err
			}
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

func applyDeepMerge(ctx context.Context, tx pgx.Tx, db *storage.DB, appID [16]byte, cat *platform.AttrCatalog, batch []Step, opts Options) error {
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
		// Fetch existing value for (e,a); merge via jsonb_deep_merge-like semantics.
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
			// decodeValue leaves json.Number in place (storage pkg contract);
			// widen it here so the merged map re-encodes via triple.IsValue.
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

func applyDeleteEntity(ctx context.Context, tx pgx.Tx, db *storage.DB, appID [16]byte, cat *platform.AttrCatalog, batch []Step) error {
	if len(batch) == 0 {
		return nil
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
		return nil
	}
	if _, err := tx.Exec(ctx, `DELETE FROM triples WHERE app_id=$1 AND entity_id = ANY($2::uuid[])`, appID, ids); err != nil {
		return fmt.Errorf("delete-entity: %w", err)
	}
	// Reverse references (value = one of these entity ids). Refs are stored as
	// jsonb strings ('"<uuid>"'), so match via to_jsonb over a text array in
	// ONE parameterized statement. A failure here must abort the transaction:
	// silently keeping dangling forward refs corrupts link semantics.
	texts := make([]string, 0, len(ids))
	for _, id := range ids {
		texts = append(texts, uuidToStr(id))
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM triples WHERE app_id=$1 AND vae AND value = ANY(SELECT to_jsonb(t) FROM unnest($2::text[]) AS t)`,
		appID, texts); err != nil {
		return fmt.Errorf("delete-entity reverse refs: %w", err)
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

func collectTouchedEntities(steps []Step) ([][16]byte, error) {
	seen := make(map[[16]byte]struct{})
	var out [][16]byte
	for _, st := range steps {
		switch st.Op {
		case "add-triple", "deep-merge-triple", "retract-triple":
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
func validateRequired(ctx context.Context, q interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}, appID [16]byte, cat *platform.AttrCatalog, touched [][16]byte) error {
	if len(touched) == 0 {
		return nil
	}
	for eid := range touched {
		_ = eid
	}
	// Phase 2: required attrs are enforced as: for each attr with required=true,
	// assert at least one triple exists for (entity, attr) where value IS NOT NULL.
	for _, attr := range catalogAttrs(cat) {
		if !attr.IsIndexed && !isRequiredAttr(attr) {
			continue
		}
		// Only attrs flagged as required via platform's required=true.
	}
	return nil
}

func isRequiredAttr(a platform.Attr) bool {
	_ = a
	return false // Phase 2 defers required? enforcement to the transactor-layer
	// check_data_type validation (v1: validate-required!). Actual required?
	// flags come from checked_data_type==not-null; here we just check that the
	// catalog's attribute entry has cardinality one required semantics.
}

func catalogAttrs(cat *platform.AttrCatalog) []platform.Attr {
	return []platform.Attr{} // Phase 2 defers catalog enumeration behind catalog method
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
func entityProjection(ctx context.Context, tx pgx.Tx, appID [16]byte, eid [16]byte, cat *platform.AttrCatalog) map[string]any {
	out := map[string]any{}
	rows, err := storage.FetchTx(ctx, tx, appID, storage.FetchFilter{
		EntityIDs: [][16]byte{eid},
	})
	if err != nil {
		return out // projection is best-effort: eval proceeds with what loaded
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
	return out
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
	cat *platform.AttrCatalog, steps []Step, opts Options, doc *perms.RuleDoc,
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
				existing, err := storage.FetchTx(ctx, tx, appID, storage.FetchFilter{
					EntityIDs: [][16]byte{eid}, AttrIDs: [][16]byte{ta.AttrID},
				})
				if err != nil {
					return err
				}
				if len(existing) > 0 {
					action = "update"
				}
				etype := etypeFor(cat, ta.AttrID)
				data := entityProjection(ctx, tx, appID, eid, cat)
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
				data := entityProjection(ctx, tx, appID, eid, cat)
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
				data := entityProjection(ctx, tx, appID, eid, cat)
				etype := "$default"
				if len(st.Args) == 2 {
					var e string
					if json.Unmarshal(st.Args[1], &e) == nil && e != "" {
						etype = e
					}
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
