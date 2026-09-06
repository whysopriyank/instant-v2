package reactive

// Incremental result maintenance behind the Refresh seam
// (docs/reference/09-tier2-architecture.md §T2.5).
//
// Subscriptions whose queries are flat top-level forms (no nested forms, no
// pagination cursors, no aggregates, no order ops, no field projections)
// keep materialized state seeded exclusively by full-refresh results: an
// ordered entity-id list plus cached rendered payloads per form. A known
// ChangeSet then splices updates in O(change):
//
//   - every touched entity gets a targeted membership check (its id run
//     against the form's WHERE) and, if still/presently a member, a batched
//     payload fetch — so update-in-place, create, and delete are the same
//     three-way reconcile;
//   - changes to etypes no root form queries provably cannot affect a flat
//     envelope and short-circuit to the cached bytes.
//
// Full refresh remains the oracle: any uncertainty — unparsed query,
// ineligible shape, source error, envelope surprise, missing baseline —
// bails out (ok=false) and the caller falls back to Refresh, which re-seeds
// materialized state. Emission is identical either way: apply() returns an
// envelope with exactly the same shape the oracle would render, feeding the
// same Frame/group dispatch.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/datalog"
	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/platform"
)

// Incremental is the optional engine hung off Notifier.Inc. A nil pointer
// keeps today's byte-for-byte full-recompute path; nothing else changes.
type Incremental struct {
	// Source resolves current membership and entity payloads against
	// storage for one form probe.
	Source ChangeSource
	// Authorize reports whether a splice over etypes may serve under the
	// subscription's admitted gate. The membership probe mirrors only the
	// query WHERE — never view rules — so splicing under a closed or
	// dynamic-non-admin gate would serve rows the full-query oracle
	// excludes. A false verdict bails to full refresh (fail closed to
	// correct behavior, not to an error). Nil preserves legacy behavior
	// and must only be used where no permission gate exists (hermetic
	// tests); every production wiring sets it.
	Authorize func(sub *Subscription, etypes []string) bool
}

// ChangeSource is the data plane the engine probes per drain. The production
// implementation (InstaqlSource) mirrors instaql's WHERE compiler and
// batched loader; tests substitute an in-memory model.
type ChangeSource interface {
	// Members reports which candidate ids currently satisfy f's WHERE
	// (targeted membership check before insert/remove — docs/09 §T2.5).
	Members(ctx context.Context, appID string, f *instaql.Form, candidates []string) (map[string]bool, error)
	// Entities renders current projected payloads for ids of f's etype,
	// exactly as instaql's projectEntity would; absent key = entity gone
	// (zero surviving triples).
	Entities(ctx context.Context, appID string, f *instaql.Form, ids []string) (map[string]json.RawMessage, error)
}

// matForm is one top-level form's materialized membership: ids ascending —
// instaql's default order (applyOrder sorts by entity id when Order==nil,
// which eligibility requires) — plus each member's last rendered payload.
// Treated as immutable once committed; splices work on clones.
type matForm struct {
	ids  []string
	ents map[string]json.RawMessage
}

func (m *matForm) clone() *matForm {
	c := &matForm{ids: append([]string(nil), m.ids...), ents: make(map[string]json.RawMessage, len(m.ents))}
	for k, v := range m.ents {
		c.ents[k] = v
	}
	return c
}

func (m *matForm) has(id string) bool {
	_, ok := m.ents[id]
	return ok
}

// put inserts or replaces a member, keeping ids ascending.
func (m *matForm) put(id string, payload json.RawMessage) {
	if _, ok := m.ents[id]; !ok {
		i := sort.SearchStrings(m.ids, id)
		m.ids = append(m.ids, "")
		copy(m.ids[i+1:], m.ids[i:])
		m.ids[i] = id
	}
	m.ents[id] = payload
}

func (m *matForm) remove(id string) {
	if _, ok := m.ents[id]; !ok {
		return
	}
	delete(m.ents, id)
	i := sort.SearchStrings(m.ids, id)
	if i < len(m.ids) && m.ids[i] == id {
		m.ids = append(m.ids[:i], m.ids[i+1:]...)
	}
}

// eligState classifies the subscription's query shape. Parsed once at the
// first materialize attempt; the wire query never mutates on a Subscription.
type eligState int

const (
	eligUnknown eligState = iota
	eligYes
	eligNo // permanently ineligible: bail without reparsing forever
)

// matState is Subscription.mat — the optional engine bookkeeping guarded by
// Subscription.mu alongside the delta baseline.
type matState struct {
	ready bool            // baseline captured from a full refresh result
	elig  eligState       // query-shape verdict
	last  json.RawMessage // committed envelope backing this state
	roots map[string]*instaql.Form
	forms map[string]*matForm
}

// classify parses and judges the query's eligibility for incremental
// maintenance (docs/09 §T2.5 scope guard: "top-level forms, no
// cursor/aggregate/order"). Everything else silently takes today's path.
func classify(rawQuery json.RawMessage) (map[string]*instaql.Form, bool) {
	var raw map[string]any
	if err := json.Unmarshal(rawQuery, &raw); err != nil {
		return nil, false
	}
	q, err := instaql.Coerce(raw)
	if err != nil || len(q.Forms) == 0 {
		return nil, false
	}
	roots := make(map[string]*instaql.Form, len(q.Forms))
	for _, f := range q.Forms {
		if len(f.Children) > 0 {
			return nil, false // nested forms join through parents: bail
		}
		if o := f.Options; o != nil {
			if o.Limit != nil || o.First != nil || o.Last != nil ||
				o.Offset != nil || len(o.Before) > 0 || len(o.After) > 0 {
				return nil, false // pagination cursors shift windows: bail
			}
			if o.Aggregate != "" {
				return nil, false // counts are pre-pagination set sizes: bail
			}
			if o.Order != nil {
				return nil, false // non-id order would need re-sorting keys: bail
			}
			if len(o.Fields) > 0 {
				return nil, false // projection is re-derived per fetch: bail
			}
		}
		if _, dup := roots[f.Etype]; dup {
			// Two root forms over one etype share a single data slot (the
			// last form wins in instaql.runForm) — per-form state can't model
			// that: bail.
			return nil, false
		}
		roots[f.Etype] = f
	}
	return roots, true
}

// apply attempts an O(change) splice of sub's materialized state for the
// coalesced change set. ok=false bails to full refresh for ANY reason.
//
// Consistency note: Members and Entities are two reads, not one snapshot —
// identical exposure to instaql's own ids-then-loadEntities pair. A torn read
// only means slightly-new-or-stale rows; the watermark dedupe re-dirties the
// sub for any tx it hasn't processed, so the next drain reconciles.
func (e *Incremental) apply(ctx context.Context, sub *Subscription, changes []Change) (json.RawMessage, bool) {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	st := &sub.mat
	if e == nil || e.Source == nil || !st.ready || st.elig != eligYes || len(changes) == 0 {
		return nil, false
	}
	if e.Authorize != nil {
		// Splice authorization (RT-001): the probe below enforces only
		// WHERE membership. Roots are flat top-level etypes (classify
		// rejects nesting), so a per-root view check covers every etype
		// the splice can touch. Bailing serves the oracle result.
		etypes := make([]string, 0, len(st.roots))
		for et := range st.roots {
			etypes = append(etypes, et)
		}
		sort.Strings(etypes)
		if !e.Authorize(sub, etypes) {
			return nil, false
		}
	}

	// Stage copy-on-write clones for every queried etype actually touched.
	staged := map[string]*matForm{}
	var order []string // deterministic commit order for tests
	for _, c := range changes {
		if _, queried := st.roots[c.Etype]; !queried {
			continue // etype not in any flat root form: cannot affect the envelope
		}
		if _, s := staged[c.Etype]; !s {
			staged[c.Etype] = st.forms[c.Etype].clone()
			order = append(order, c.Etype)
		}
	}
	sort.Strings(order)
	if len(staged) == 0 {
		// Nothing queried was touched: the current envelope is provably exact.
		return st.last, true
	}

	for _, etype := range order {
		f := st.roots[etype]
		nf := staged[etype]
		ids := changedIDs(changes, etype)
		members, err := e.Source.Members(ctx, sub.AppID, f, ids)
		if err != nil {
			return nil, false
		}
		// Fetch payloads only for entities that currently qualify; a member
		// whose triples vanished between the two probes drops out below.
		var fetch []string
		for _, id := range ids {
			if members[id] {
				fetch = append(fetch, id)
			}
		}
		ents, err := e.Source.Entities(ctx, sub.AppID, f, fetch)
		if err != nil {
			return nil, false
		}
		for _, id := range ids {
			inOld, nowMember := nf.has(id), members[id]
			switch {
			case nowMember:
				p, ok := ents[id]
				if !ok {
					// Triples gone entirely: the entity is deleted as far as
					// the store is concerned.
					nf.remove(id)
					continue
				}
				nf.put(id, p) // update-in-place splice, position kept if present
			case inOld:
				nf.remove(id) // delete / flipped out of WHERE
			} // neither: create candidate that failed its membership check
		}
	}

	out, ok := renderEnvelope(st.last, staged)
	if !ok {
		return nil, false
	}
	for etype, nf := range staged {
		st.forms[etype] = nf
	}
	st.last = out
	return out, true
}

// materialize seeds (or re-seeds) materialized state from a full-refresh
// envelope — the only source of truth the engine ever trusts. Any structural
// surprise permanently marks the subscription ineligible.
func (e *Incremental) materialize(sub *Subscription, result json.RawMessage) {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	st := &sub.mat
	if st.elig == eligNo {
		return
	}
	roots, ok := classify(sub.Query)
	if !ok {
		st.elig = eligNo
		st.ready = false
		return
	}
	st.elig = eligYes

	var probe map[string]json.RawMessage
	if err := json.Unmarshal(result, &probe); err != nil || len(probe) != 1 || probe["data"] == nil {
		st.elig = eligNo // page-info/aggregate or foreign envelope shape: bail forever
		st.ready = false
		return
	}
	var data map[string]json.RawMessage
	if err := json.Unmarshal(probe["data"], &data); err != nil {
		st.elig = eligNo
		st.ready = false
		return
	}

	forms := make(map[string]*matForm, len(roots))
	for etype := range roots {
		rawArr, ok := data[etype]
		if !ok {
			st.elig = eligNo // instaql always writes each root form's slot
			st.ready = false
			return
		}
		var arr []json.RawMessage
		if err := json.Unmarshal(rawArr, &arr); err != nil {
			st.elig = eligNo
			st.ready = false
			return
		}
		mf := &matForm{ents: map[string]json.RawMessage{}}
		for i, payload := range arr {
			var ident struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(payload, &ident); err != nil || ident.ID == "" {
				st.elig = eligNo
				st.ready = false
				return
			}
			if i > 0 && ident.ID <= mf.ids[i-1] {
				// Ascending-id order is what instaql renders for eligible
				// shapes; anything else means our splice ordering premise
				// is wrong.
				st.elig = eligNo
				st.ready = false
				return
			}
			mf.ids = append(mf.ids, ident.ID)
			mf.ents[ident.ID] = payload
		}
		forms[etype] = mf
	}
	st.ready = true
	st.roots = roots
	st.forms = forms
	st.last = result
}

// renderEnvelope rebuilds {"data":{...}} with the spliced arrays substituted.
// Byte parity with instaql.Result holds because both marshal maps via
// encoding/json (sorted keys) and entity payloads are verbatim prior-render
// substrings or fresh projectEntity-shaped marshals.
func renderEnvelope(prev json.RawMessage, staged map[string]*matForm) (json.RawMessage, bool) {
	if len(prev) == 0 {
		return nil, false
	}
	var env struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(prev, &env); err != nil || env.Data == nil {
		return nil, false
	}
	data := env.Data
	for etype, nf := range staged {
		arr := make([]json.RawMessage, 0, len(nf.ids))
		for _, id := range nf.ids {
			p, ok := nf.ents[id]
			if !ok {
				return nil, false // internal inconsistency: bail rather than guess
			}
			arr = append(arr, p)
		}
		b, err := json.Marshal(arr)
		if err != nil {
			return nil, false
		}
		data[etype] = b
	}
	out, err := json.Marshal(env)
	if err != nil {
		return nil, false
	}
	return out, true
}

// InstaqlSource is the production ChangeSource. reactive cannot import
// instaql's unexported helpers, so this mirrors two of them — buildConditions
// (internal/instaql/query.go:321) and loadEntities/projectEntity
// (query.go:453/560) — keeping SQL, op coercion, ref/many folding, and label
// projection in lockstep. The fuzz differential in incremental_test.go pins
// byte parity between this splice path and Executor.Run, so drift fails CI.
type InstaqlSource struct {
	DB interface {
		Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	}
	// Catalog resolves the app's live attr catalog (typically
	// CatalogCache.For). An error bails the engine to full refresh.
	Catalog func(ctx context.Context, appID string) (*platform.AttrCatalog, error)
}

// Members wraps the form's EntitySetSQL with an ANY(candidates) restriction:
// same compiled WHERE, probed for exactly the touched ids.
func (s *InstaqlSource) Members(ctx context.Context, appID string, f *instaql.Form, candidates []string) (map[string]bool, error) {
	out := map[string]bool{}
	if len(candidates) == 0 {
		return out, nil
	}
	cat, err := s.Catalog(ctx, appID)
	if err != nil {
		return nil, err
	}
	conds, err := conditionsFor(f, cat)
	if err != nil {
		return nil, err
	}
	setSQL, args := datalog.BuildPlan(appID, f.Etype, conds).EntitySetSQL(cat.ByEtype(f.Etype))
	sqlStr := fmt.Sprintf("SELECT entity_id FROM (%s) m WHERE entity_id = ANY($%d::uuid[])", setSQL, len(args)+1)
	args = append(args, candidates)
	rows, err := s.DB.Query(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out[id] = true
	}
	return out, rows.Err()
}

// Entities batch-fetches all triples for ids in ONE round-trip and folds +
// projects them precisely as instaql does (mirror of query.go loadEntities +
// projectEntity minus the banned Fields option).
func (s *InstaqlSource) Entities(ctx context.Context, appID string, f *instaql.Form, ids []string) (map[string]json.RawMessage, error) {
	out := map[string]json.RawMessage{}
	if len(ids) == 0 {
		return out, nil
	}
	cat, err := s.Catalog(ctx, appID)
	if err != nil {
		return nil, err
	}
	attrs := cat.IDIndex(f.Etype) // prebuilt etype→uuid→attr index (was: full-catalog scan per drain)
	rows, err := s.DB.Query(ctx, `
		SELECT t.entity_id, t.attr_id, t.value, a.cardinality
		  FROM triples t JOIN attrs a ON a.id = t.attr_id
		 WHERE t.app_id = $1::uuid
		   AND t.entity_id = ANY($2::uuid[])`, appID, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type ent struct {
		id     string
		fields map[string]any
	}
	folded := map[string]*ent{}
	get := func(id string) *ent {
		e, ok := folded[id]
		if !ok {
			e = &ent{id: id, fields: map[string]any{"id": id}}
			folded[id] = e
		}
		return e
	}
	for rows.Next() {
		var eid, attrID string
		var raw []byte
		var cardinality string
		if err := rows.Scan(&eid, &attrID, &raw, &cardinality); err != nil {
			return nil, err
		}
		a, ok := attrs[attrID]
		if !ok {
			continue
		}
		var v any
		// UseNumber parity with instaql loadEntities: splice-produced
		// payloads must render numbers identically to executor-produced
		// envelopes, or delta baselines and splices would disagree on
		// every numeric field (audit backlog B5).
		vdec := json.NewDecoder(bytes.NewReader(raw))
		vdec.UseNumber()
		if err := vdec.Decode(&v); err != nil {
			return nil, err
		}
		if str, isStr := v.(string); isStr && a.ValueType == "ref" {
			v = str // refs stay uuid strings (query.go loadEntities parity)
		}
		label := "<nil>"
		if a.Label != nil {
			label = *a.Label
		}
		e := get(eid)
		if cardinality == "many" {
			arr, _ := e.fields[label].([]any)
			e.fields[label] = append(arr, v)
		} else {
			e.fields[label] = v
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Project exactly like projectEntity without Options.Fields: id plus all
	// triple-backed labels. encoding/json sorts keys, matching instaql's
	// marshal of the same map byte for byte.
	for id, e := range folded {
		m := map[string]any{"id": e.id}
		for k, v := range e.fields {
			if k != "id" {
				m[k] = v
			}
		}
		b, err := json.Marshal(m)
		if err != nil {
			return nil, err
		}
		out[id] = b
	}
	return out, nil
}

// conditionsFor mirrors instaql's buildConditions (query.go:321): where
// conds → datalog conditions via the catalog. Kept in lockstep — the
// differential test pins the resulting envelopes against Executor.Run.
func conditionsFor(f *instaql.Form, cat *platform.AttrCatalog) ([]datalog.Condition, error) {
	if f.Options == nil {
		return nil, nil
	}
	var out []datalog.Condition
	for _, w := range f.Options.Where {
		if len(w.Path) != 1 {
			return nil, fmt.Errorf("instaql: dotted where path %q not supported yet", w.Path)
		}
		label := w.Path[0]
		aPtr := cat.FindByEtypeLabel(f.Etype, label)
		if aPtr == nil {
			// Unknown attr: $isNull semantics survive (query.go parity);
			// anything else is a hard error → engine bail-out.
			if m, isMap := w.Value.(map[string]any); isMap {
				if iv, ok := m["$isNull"]; ok {
					if b, _ := iv.(bool); b {
						continue
					}
					out = append(out, datalog.Condition{
						AttrID: "00000000-0000-4000-8000-000000000000",
						Op:     datalog.PredExists,
					})
					continue
				}
			}
			if w.Value == nil {
				continue
			}
			return nil, fmt.Errorf("instaql: no attr %s.%s", f.Etype, label)
		}
		a := *aPtr
		cond := datalog.Condition{AttrID: platform.UUIDToStr(a.ID), Attr: a}
		switch tv := w.Value.(type) {
		case map[string]any:
			op, args, err := coerceOpMapFor(tv)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", f.Etype, label, err)
			}
			cond.Op, cond.Args = op, args
		case nil:
			cond.Op = datalog.PredIsNull
			cond.Args = []any{true}
		default:
			enc, err := json.Marshal(tv)
			if err != nil {
				return nil, err
			}
			cond.Op = datalog.PredEq
			cond.Args = []any{string(enc)}
		}
		cond.IndexHint = datalog.BestIndex(a)
		out = append(out, cond)
	}
	return out, nil
}

// coerceOpMapFor mirrors instaql's coerceOpMap (query.go:384).
func coerceOpMapFor(m map[string]any) (datalog.PredOp, []any, error) {
	for k, v := range m {
		switch k {
		case "$in", "in":
			l, ok := v.([]any)
			if !ok {
				return 0, nil, fmt.Errorf("$in must be a list")
			}
			args := make([]any, 0, len(l))
			for _, item := range l {
				enc, err := json.Marshal(item)
				if err != nil {
					return 0, nil, err
				}
				args = append(args, string(enc))
			}
			return datalog.PredIn, args, nil
		case "$not", "$ne":
			enc, err := json.Marshal(v)
			if err != nil {
				return 0, nil, err
			}
			return datalog.PredNot, []any{string(enc)}, nil
		case "$isNull":
			b, _ := v.(bool)
			return datalog.PredIsNull, []any{b}, nil
		case "$gt", "$gte", "$lt", "$lte":
			enc, err := json.Marshal(v)
			if err != nil {
				return 0, nil, err
			}
			switch k {
			case "$gt":
				return datalog.PredGt, []any{string(enc)}, nil
			case "$gte":
				return datalog.PredGte, []any{string(enc)}, nil
			case "$lt":
				return datalog.PredLt, []any{string(enc)}, nil
			default:
				return datalog.PredLte, []any{string(enc)}, nil
			}
		case "$like", "$ilike":
			// LIKE patterns compare raw text, not JSON; non-strings get
			// JSON-encoded exactly as instaql does.
			sv, isStr := v.(string)
			if !isStr {
				b, err := json.Marshal(v)
				if err != nil {
					return 0, nil, err
				}
				sv = string(b)
			}
			if k == "$like" {
				return datalog.PredLike, []any{sv}, nil
			}
			return datalog.PredILike, []any{sv}, nil
		}
	}
	return 0, nil, fmt.Errorf("empty operator map")
}
