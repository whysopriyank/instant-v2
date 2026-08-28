package instaql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/datalog"
	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
)

// Executor runs coerced queries against Postgres.
//
// Rules/Auth/Admin gate reads: ViewOpen etypes are served normally,
// ViewClosed etypes return empty results, and ViewDynamic etypes are refused
// for non-admin callers with ErrRuleFilterUnsupported — a dynamic rule cannot
// be honored without CEL→SQL rule-where pushdown, and evaluating it without
// data bindings could over-allow data-dependent rules. Admin callers bypass
// (v1 permissioned-query :admin? semantics).
type Executor struct {
	DB interface {
		Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
		QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	}
	Rules *perms.RuleDoc // nil → default-open
	Auth  map[string]any // caller identity (reserved for pushdown)
	Admin bool           // admin callers bypass the view gate
}

// ErrRuleFilterUnsupported is returned when a query touches an etype whose
// view rule is dynamic and the caller is not an admin.
type ErrRuleFilterUnsupported struct{ Etype string }

func (e *ErrRuleFilterUnsupported) Error() string {
	return "instaql: view rule for " + e.Etype +
		" requires server-side filtering (rule-where pushdown); not supported on this endpoint yet"
}

// Result is the wire envelope.
type Result struct {
	Data      map[string]json.RawMessage `json:"data"`
	PageInfo  *PageInfo                  `json:"page-info,omitempty"`
	Aggregate *Aggregate                 `json:"aggregate,omitempty"`
}

// PageInfo mirrors page-info{startCursor,endCursor,hasNextPage,hasPreviousPage}.
type PageInfo struct {
	StartCursor     *string `json:"startCursor,omitempty"`
	EndCursor       *string `json:"endCursor,omitempty"`
	HasNextPage     bool    `json:"hasNextPage"`
	HasPreviousPage bool    `json:"hasPreviousPage"`
}

// Aggregate wraps aggregate{count}.
type Aggregate struct {
	Count int64 `json:"count"`
}

// entity is an in-memory label→value map plus its id.
type entity struct {
	ID     string
	Fields map[string]any
}

// Run executes the query and shapes the envelope. All levels' entities land in
// Data keyed by etype (v1 semantics: data.posts AND data.comments).
func (x *Executor) Run(ctx context.Context, q *Query, cat *platform.AttrCatalog, appID [16]byte) (*Result, error) {
	res := &Result{Data: map[string]json.RawMessage{}}
	appStr := uuidToStr(appID)
	for _, f := range q.Forms {
		if err := x.runForm(ctx, f, cat, appStr, nil, res); err != nil {
			return nil, err
		}
	}
	return res, nil
}

func uuidToStr(u [16]byte) string {
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

func (x *Executor) runForm(ctx context.Context, f *Form, cat *platform.AttrCatalog, appID string, parentRef *refLink, res *Result) error {
	// View gate (every level — nested etypes carry their own rules). Denied
	// etypes surface as empty results; dynamic rules refuse non-admin
	// execution entirely rather than risk over-allowing.
	switch perms.ViewGate(x.Rules, f.Etype) {
	case perms.ViewClosed:
		res.Data[f.Etype] = json.RawMessage(`[]`)
		return nil
	case perms.ViewDynamic:
		if !x.Admin {
			return &ErrRuleFilterUnsupported{Etype: f.Etype}
		}
	}

	etypeAttrs := cat.IDIndex(f.Etype) // prebuilt etype→uuid→attr index (was: per-form full-catalog scan)
	attrIDs := cat.ByEtype(f.Etype)    // prebuilt uuid list backing the same index

	conds, err := buildConditions(f, cat)
	if err != nil {
		return err
	}
	// Aggregate counts the full matching set, pre-pagination.
	var aggCount int64
	if f.Options != nil && f.Options.Aggregate == "count" && (parentRef == nil || parentRef.Mode != "reverse") {
		plan0 := datalog.BuildPlan(appID, f.Etype, conds)
		setSQL, setArgs := plan0.EntitySetSQL(attrIDs)
		if err := x.DB.QueryRow(ctx,
			"SELECT count(*) FROM ("+setSQL+") s", setArgs...).Scan(&aggCount); err != nil {
			return err
		}
	}

	var ids []string
	if parentRef != nil && parentRef.Mode == "reverse" {
		// Children are entities whose triples under parentRef.AttrID carry a
		// value equal to one of the parent ids (vae semantics).
		jsonVals := make([]string, 0, len(parentRef.Values))
		for _, v := range parentRef.Values {
			b, _ := json.Marshal(v) // uuid strings must be JSON-quoted
			jsonVals = append(jsonVals, string(b))
		}
		rows, err := x.DB.Query(ctx, `
			SELECT DISTINCT entity_id FROM triples
			 WHERE app_id = $1::uuid AND attr_id = $2::uuid AND value = ANY($3::jsonb[])`,
			appID, parentRef.AttrID, jsonVals)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
	} else {
		plan := datalog.BuildPlan(appID, f.Etype, conds)
		entitySQL, args := plan.EntitySetSQL(attrIDs)
		sqlStr, args2 := paginateWrap(entitySQL, args, f.Options, cat, appID)
		rows, err := x.DB.Query(ctx, sqlStr, args2...)
		if err != nil {
			return fmt.Errorf("instaql: query %s: %w", f.Etype, err)
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
	}

	// Batched fetch: one round-trip for ALL matched entities. The previous
	// per-entity loop (N+1) made large match-all snapshots issue thousands
	// of tiny queries and starve the pool (found by the v1-vs-v2 soak).
	entities := make([]entity, 0, len(ids))
	if len(ids) > 0 {
		byID, err := x.loadEntities(ctx, appID, ids, etypeAttrs)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if e, ok := byID[id]; ok {
				entities = append(entities, *e)
			}
		}
	}

	// Ordering (in-memory; corpus validates parity with v1's SQL ordering).
	applyOrder(entities, f.Options)

	// Page-info. paginateWrap fetches one sentinel row for positive limits so
	// an exact-size final page is distinguishable from a page with more rows.
	// The sentinel is trimmed before projection and never reaches the wire.
	pageInfo := &PageInfo{}
	slice, hasNextPage, hasPreviousPage := pageSlice(entities, f.Options)
	if needsPageInfo(f.Options) {
		if len(slice) > 0 {
			sc := encodeCursor(slice[0].ID, "", nil)
			ec := encodeCursor(slice[len(slice)-1].ID, "", nil)
			pageInfo.StartCursor = &sc
			pageInfo.EndCursor = &ec
		}
		pageInfo.HasNextPage = hasNextPage
		pageInfo.HasPreviousPage = hasPreviousPage
	}

	// Project into label→value maps.
	out := make([]map[string]any, 0, len(slice))
	for _, e := range slice {
		m := projectEntity(e, f.Options)
		out = append(out, m)
	}

	// Nested children: join via ref attrs of this etype (forward) or reverse
	// links pointing back at this etype.
	for _, child := range f.Children {
		var rl *refLink
		refAttr, hasRefAttr := cat.LabelIndex(f.Etype)[child.Label]
		switch {
		case hasRefAttr && refAttr.ValueType == "ref":
			var vals []any
			for _, e := range slice {
				if vs, ok := e.Fields[child.Label].([]any); ok {
					vals = append(vals, vs...)
				}
			}
			if vals == nil {
				vals = []any{}
			}
			rl = &refLink{Mode: "forward", AttrID: refAttr.UUID(), Values: vals}
		default:
			ra := cat.FindReverseAttr(child.Etype, f.Etype, child.Label)
			if ra == nil {
				return fmt.Errorf("instaql: %s.%s is not a link attribute", f.Etype, child.Label)
			}
			var vals []any
			for _, e := range slice {
				vals = append(vals, e.ID)
			}
			if vals == nil {
				vals = []any{}
			}
			rl = &refLink{Mode: "reverse", AttrID: ra.UUID(), ReverseLabel: derefS(ra.Label), Values: vals}
		}
		if err := x.runForm(ctx, child, cat, appID, rl, res); err != nil {
			return err
		}
		// Attach linked children onto each parent entity.
		if childData, ok := res.Data[child.Etype]; ok && string(childData) != "null" {
			// UseNumber: child bodies must keep the same number fidelity as
			// the level that produced them (they re-marshal into parents).
			var kids []map[string]any
			kdec := json.NewDecoder(bytes.NewReader(childData))
			kdec.UseNumber()
			_ = kdec.Decode(&kids)
			if rl.Mode == "reverse" {
				// Group kids by the parent ids listed under their forward attr.
				byparent := map[string][]map[string]any{}
				for _, k := range kids {
					if vs, ok := k[rl.ReverseLabel].([]any); ok {
						for _, v := range vs {
							byparent[fmt.Sprint(v)] = append(byparent[fmt.Sprint(v)], k)
						}
					}
				}
				for _, p := range out {
					p[child.Label] = byparent[fmt.Sprint(p["id"])]
					if p[child.Label] == nil {
						p[child.Label] = []map[string]any{}
					}
				}
			} else {
				byID := map[string]map[string]any{}
				for _, k := range kids {
					if idv, ok := k["id"].(string); ok {
						byID[idv] = k
					}
				}
				for _, p := range out {
					var linkedList []map[string]any
					if vs, ok := p[child.Label].([]any); ok {
						for _, v := range vs {
							if kid, ok := byID[fmt.Sprint(v)]; ok {
								linkedList = append(linkedList, kid)
							}
						}
					}
					p[child.Label] = linkedList
					if p[child.Label] == nil {
						p[child.Label] = []map[string]any{}
					}
				}
			}
		}
	}

	// Merge this level's entities back into the envelope (re-marshaling so
	// nested links attached above are preserved).
	final, err := json.Marshal(out)
	if err != nil {
		return err
	}
	res.Data[f.Etype] = final

	if f.Options != nil && f.Options.Aggregate == "count" {
		res.Aggregate = &Aggregate{Count: aggCount}
	}
	if f.Level == 0 && needsPageInfo(f.Options) {
		res.PageInfo = pageInfo
	}
	return nil
}

type refLink struct {
	Mode   string // "forward": entity_id IN values; "reverse": value IN values
	AttrID string
	// ReverseLabel is the forward attr's label on the child side ("post" for
	// comments.post); used to group children under parents.
	ReverseLabel string
	Values       []any
}

func needsPageInfo(o *Options) bool {
	return o != nil && (o.Limit != nil || o.First != nil || o.Last != nil ||
		o.Offset != nil || o.Before != nil || o.After != nil || o.Order != nil)
}

func effectiveLimitOffset(o *Options) (limit, offset int) {
	if o == nil {
		return 0, 0
	}
	if o.Limit != nil {
		limit = *o.Limit
	} else if o.First != nil {
		limit = *o.First
	} else if o.Last != nil {
		limit = *o.Last
	}
	if o.Offset != nil {
		offset = *o.Offset
	}
	return
}

// pageSlice removes the SQL sentinel row and derives page direction metadata.
// A forward page (limit/first) has another page only when the sentinel was
// returned. A backward page (last) uses the sentinel to indicate preceding
// rows instead; it does not manufacture a next page from the page size.
func pageSlice(entities []entity, o *Options) (slice []entity, hasNext, hasPrevious bool) {
	slice = entities
	if o == nil {
		return slice, false, false
	}
	_, offset := effectiveLimitOffset(o)
	hasPrevious = offset > 0 || o.Before != nil
	limit, _ := effectiveLimitOffset(o)
	if limit <= 0 {
		return slice, false, hasPrevious
	}
	if o.Last != nil {
		if len(entities) > limit {
			hasPrevious = true
		}
		if len(slice) > limit {
			slice = slice[len(slice)-limit:]
		}
		return slice, false, hasPrevious
	}
	if len(entities) > limit {
		hasNext = true
		slice = slice[:limit]
	}
	return slice, hasNext, hasPrevious
}

// buildConditions converts where-conditions into datalog conditions using the
// catalog for attr resolution and index choice.
func buildConditions(f *Form, cat *platform.AttrCatalog) ([]datalog.Condition, error) {
	if f.Options == nil {
		return nil, nil
	}
	var out []datalog.Condition
	for _, w := range f.Options.Where {
		if len(w.Path) != 1 {
			// Dotted paths across refs are Phase 3+ (needs join); reject loudly
			// rather than silently mis-filtering.
			return nil, fmt.Errorf("instaql: dotted where path %q not supported yet", strings.Join(w.Path, "."))
		}
		label := w.Path[0]
		a, found := cat.LabelIndex(f.Etype)[label]
		if !found {
			// Unknown attr: $isNull semantics still work — every entity of
			// this etype lacks it (v1 allows querying absent attrs with
			// $isNull). true → no constraint; false → impossible set.
			if m, isMap := w.Value.(map[string]any); isMap {
				if iv, ok := m["$isNull"]; ok {
					if b, _ := iv.(bool); b {
						continue // all entities qualify
					}
					out = append(out, datalog.Condition{
						AttrID: "00000000-0000-4000-8000-000000000000",
						Op:     datalog.PredExists, // never matches
					})
					continue
				}
			}
			if w.Value == nil {
				continue
			}
			return nil, fmt.Errorf("instaql: no attr %s.%s", f.Etype, label)
		}
		cond := datalog.Condition{AttrID: a.UUID(), Attr: a}
		switch tv := w.Value.(type) {
		case map[string]any:
			op, args, err := coerceOpMap(tv)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", f.Etype, label, err)
			}
			cond.Op = op
			cond.Args = args
		case nil:
			cond.Op = datalog.PredIsNull
			cond.Args = []any{true}
		default:
			enc, err := encodeValueJSON(w.Value)
			if err != nil {
				return nil, err
			}
			cond.Op = datalog.PredEq
			cond.Args = []any{enc}
		}
		cond.IndexHint = datalog.BestIndex(a)
		out = append(out, cond)
	}
	return out, nil
}

func coerceOpMap(m map[string]any) (datalog.PredOp, []any, error) {
	for k, v := range m {
		if !validOps[k] {
			return 0, nil, fmt.Errorf("unknown operator %q", k)
		}
		switch k {
		case "$in", "in":
			l, ok := v.([]any)
			if !ok {
				return 0, nil, fmt.Errorf("$in must be a list")
			}
			args := make([]any, 0, len(l))
			for _, item := range l {
				enc, err := encodeValueJSON(item)
				if err != nil {
					return 0, nil, err
				}
				args = append(args, enc)
			}
			return datalog.PredIn, args, nil
		case "$not", "$ne":
			enc, err := encodeValueJSON(v)
			if err != nil {
				return 0, nil, err
			}
			return datalog.PredNot, []any{enc}, nil
		case "$isNull":
			b, _ := v.(bool)
			return datalog.PredIsNull, []any{b}, nil
		case "$gt", "$gte", "$lt", "$lte", "$like", "$ilike":
			var b []byte
			if sv, isStr := v.(string); isStr && (k == "$like" || k == "$ilike") {
				// Audit L2: patterns are parameterized but wildcard-rich;
				// cap their length so "%"-style scans stay cheap to express.
				if len(sv) > 512 {
					return 0, nil, fmt.Errorf("%s pattern too long", k)
				}
				b = []byte(sv) // LIKE patterns compare raw text, not JSON
			} else {
				enc, err := json.Marshal(v)
				if err != nil {
					return 0, nil, err
				}
				b = enc
			}
			switch k {
			case "$gt":
				return datalog.PredGt, []any{string(b)}, nil
			case "$gte":
				return datalog.PredGte, []any{string(b)}, nil
			case "$lt":
				return datalog.PredLt, []any{string(b)}, nil
			case "$lte":
				return datalog.PredLte, []any{string(b)}, nil
			case "$like":
				return datalog.PredLike, []any{string(b)}, nil
			default:
				return datalog.PredILike, []any{string(b)}, nil
			}
		}
	}
	return 0, nil, fmt.Errorf("empty operator map")
}

func encodeValueJSON(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// loadEntities fetches all triples for a batch of entities in ONE query and
// folds each entity's triples into fields. Returns a map keyed by entity id.
func (x *Executor) loadEntities(ctx context.Context, appID string, ids []string, attrs map[string]platform.Attr) (map[string]*entity, error) {
	out := make(map[string]*entity, len(ids))
	rows, err := x.DB.Query(ctx, `
		SELECT t.entity_id, t.attr_id, t.value, a.cardinality
		  FROM triples t JOIN attrs a ON a.id = t.attr_id
		 WHERE t.app_id = $1::uuid
		   AND t.entity_id = ANY($2::uuid[])`, appID, ids)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	get := func(id string) *entity {
		e, ok := out[id]
		if !ok {
			e = &entity{ID: id, Fields: map[string]any{"id": id}}
			out[id] = e
		}
		return e
	}
	for rows.Next() {
		var eid, attrID string
		var raw []byte
		var cardinality string
		if err := rows.Scan(&eid, &attrID, &raw, &cardinality); err != nil {
			return out, err
		}
		a, ok := attrs[attrID]
		if !ok {
			continue
		}
		// UseNumber keeps numeric literals verbatim through the pipeline:
		// float64 re-rendering reformatted stored numbers (1.0 → 1) and
		// silently corrupted integers beyond 2^53 (audit backlog B5).
		// json.Marshal renders json.Number literals byte-exactly.
		var v any
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(&v); err != nil {
			return out, err
		}
		if s, ok := v.(string); ok && a.ValueType == "ref" {
			v = s // refs stay uuid strings
		}
		label := "<nil>"
		if a.Label != nil {
			label = *a.Label
		}
		e := get(eid)
		if cardinality == "many" {
			arr, _ := e.Fields[label].([]any)
			e.Fields[label] = append(arr, v)
		} else {
			e.Fields[label] = v
		}
	}
	if err := rows.Err(); err != nil {
		return out, err
	}
	return out, nil
}

func applyOrder(entities []entity, o *Options) {
	if o == nil || o.Order == nil {
		sort.SliceStable(entities, func(i, j int) bool { return entities[i].ID < entities[j].ID })
		return
	}
	k := o.Order.K
	dir := 1
	if o.Order.Desc {
		dir = -1
	}
	sort.SliceStable(entities, func(i, j int) bool {
		a, b := comparableField(entities[i], k), comparableField(entities[j], k)
		switch av := a.(type) {
		case string:
			bv, _ := b.(string)
			return dir*strings.Compare(av, bv) < 0
		case float64:
			bv, _ := b.(float64)
			return float64(dir)*(av-bv) < 0
		case json.Number:
			// UseNumber pipeline: order fields may arrive as numeric
			// literals; compare numerically.
			bn, _ := b.(json.Number)
			af, aerr := av.Float64()
			bf, berr := bn.Float64()
			if aerr != nil || berr != nil {
				return false
			}
			return float64(dir)*(af-bf) < 0
		default:
			return false
		}
	})
}

func comparableField(e entity, k string) any {
	if k == "serverCreatedAt" {
		return "" // ordering by created_at handled SQL-side later; stable no-op here
	}
	return e.Fields[k]
}

func projectEntity(e entity, o *Options) map[string]any {
	m := map[string]any{"id": e.ID}
	for k, v := range e.Fields {
		if k == "id" {
			continue
		}
		m[k] = v
	}
	if o != nil && len(o.Fields) > 0 {
		fm := map[string]bool{"id": true}
		for _, f := range o.Fields {
			fm[f] = true
		}
		for k := range m {
			if !fm[k] {
				delete(m, k)
			}
		}
	}
	return m
}

// paginateWrap adds ORDER BY/LIMIT/OFFSET to the entity-id query when possible;
// complex cursors fall back to in-memory slicing after fetch.
func paginateWrap(entitySQL string, args []any, o *Options, cat *platform.AttrCatalog, appID string) (string, []any) {
	limit, offset := effectiveLimitOffset(o)
	if limit <= 0 && offset <= 0 {
		return "SELECT entity_id FROM (" + entitySQL + ") s", args
	}
	wrapped := "SELECT entity_id FROM (" + entitySQL + ") s"
	if limit > 0 || offset > 0 {
		// Deterministic paging: without a total order LIMIT takes an
		// arbitrary subset of the matched set and OFFSET pages can overlap
		// or skip rows as the planner's emit order shifts between runs.
		wrapped += " ORDER BY s.entity_id"
	}
	if limit > 0 {
		// Keep one row beyond the requested page as a sentinel. runForm trims
		// it after ordering and uses its presence for hasNextPage.
		wrapped += fmt.Sprintf(" LIMIT %d", limit+1)
	}
	if offset > 0 {
		wrapped += fmt.Sprintf(" OFFSET %d", offset)
	}
	return wrapped, args
}

// Attrs exposes the catalog entries (added to platform.AttrCatalog).
func init() {}

func derefS(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
