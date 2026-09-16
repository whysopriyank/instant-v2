// Package instaql parses and evaluates InstaQL queries against the triple
// store. Port of the observable semantics of db/instaql.clj:
//
//	{ posts: { $: { where: {...}, limit, offset, order }, comments: {} } }
//
// produces
//
//	{ data: { posts: [...], comments: [...] },
//	  "page-info": {...}, aggregate: { count } }
//
// Entities are label→value maps; cardinality-one attrs surface as scalars,
// cardinality-many as arrays. Nested keys join through ref attrs.
package instaql

import (
	"encoding/json"
	"fmt"
	"sort"
)

// Options is the coerced "$" map (db/instaql.clj option-map).
type Options struct {
	Where           []WhereCond
	Order           *Order
	fallbackIDOrder bool
	Limit           *int
	First           *int
	Last            *int
	Offset          *int
	Before          []any // cursor tuple [eid, attrId, value, (inclusive?)]
	After           []any
	BeforeInclusive bool
	AfterInclusive  bool
	Aggregate       string // "" | "count"
	Fields          []string
}

// Order is {k, direction}; k == "serverCreatedAt" orders by triple created_at.
type Order struct {
	K    string
	Desc bool
}

// WhereCond is one condition: dotted path + value-or-operator-map.
type WhereCond struct {
	Path  []string // ["owner","name"] — first element is the label on this etype
	Value any      // scalar | []any ($in) | map with $ops
}

// Form is one level of the query tree.
type Form struct {
	Etype    string
	Label    string // attr label that led here ("" at root)
	Level    int
	Options  *Options
	Children []*Form
}

// Query is a parsed InstaQL query: root forms keyed by etype.
type Query struct {
	Forms []*Form
}

// Coerce validates and converts the raw wire map into a Query.
// Unknown option keys are rejected; unknown operators are rejected.
func Coerce(raw map[string]any) (*Query, error) {
	q := &Query{}
	for etype, v := range raw {
		f, err := coerceForm(etype, v, 0)
		if err != nil {
			return nil, fmt.Errorf("instaql: %s: %w", etype, err)
		}
		q.Forms = append(q.Forms, f)
	}
	return q, nil
}

func coerceForm(etype string, v any, level int) (*Form, error) {
	f := &Form{Etype: etype, Level: level}
	m, ok := v.(map[string]any)
	if !ok || len(m) == 0 {
		return f, nil
	}
	for k, sub := range m {
		switch k {
		case "$":
			opts, err := coerceOptions(sub)
			if err != nil {
				return nil, err
			}
			f.Options = opts
		default:
			if level >= maxFormDepth {
				return nil, fmt.Errorf("query nesting exceeds depth %d", maxFormDepth)
			}
			child, err := coerceForm(k, sub, level+1)
			if err != nil {
				return nil, err
			}
			child.Label = k
			f.Children = append(f.Children, child)
		}
	}
	return f, nil
}

// maxFormDepth bounds InstaQL tree recursion; each level costs at least two
// sequential queries at execution time (audit L1).
const maxFormDepth = 10

var validOps = map[string]bool{
	"$in": true, "in": true, "$not": true, "$ne": true, "$isNull": true,
	"$gt": true, "$gte": true, "$lt": true, "$lte": true,
	"$like": true, "$ilike": true, "$entityIdStartsWith": true,
}

var validOptionKeys = map[string]bool{
	"where": true, "order": true, "orderBy": true, "limit": true,
	"first": true, "last": true, "offset": true, "before": true,
	"after": true, "beforeInclusive": true, "afterInclusive": true,
	"aggregate": true, "fields": true,
}

func coerceOptions(v any) (*Options, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("$ must be an object")
	}
	o := &Options{}
	for k, val := range m {
		if !validOptionKeys[k] {
			return nil, fmt.Errorf("unknown $ option %q", k)
		}
		switch k {
		case "where":
			wm, ok := val.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("where must be an object")
			}
			var conds []WhereCond
			for path, wv := range wm {
				if m, isMap := wv.(map[string]any); isMap {
					ops := make([]string, 0, len(m))
					for op := range m {
						if !validOps[op] {
							return nil, fmt.Errorf("where %q: unknown operator %q", path, op)
						}
						ops = append(ops, op)
					}
					if len(ops) == 0 {
						return nil, fmt.Errorf("where %q: empty operator map", path)
					}
					// An operator map is a conjunction. Keep the original map
					// untouched and expand it into deterministic singleton maps so
					// every consumer of Options.Where receives every predicate.
					sort.Strings(ops)
					for _, op := range ops {
						conds = append(conds, WhereCond{Path: splitPath(path), Value: map[string]any{op: m[op]}})
					}
					continue
				}
				conds = append(conds, WhereCond{Path: splitPath(path), Value: wv})
			}
			o.Where = conds
		case "order", "orderBy":
			om, ok := val.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("%s must be {k, direction}", k)
			}
			kk, _ := om["k"].(string)
			dir, _ := om["direction"].(string)
			if kk == "" {
				return nil, fmt.Errorf("order.k required")
			}
			o.Order = &Order{K: kk, Desc: dir == "desc"}
		case "limit":
			n, err := posInt(val)
			if err != nil {
				return nil, fmt.Errorf("limit: %w", err)
			}
			o.Limit = &n
		case "first":
			n, err := posInt(val)
			if err != nil {
				return nil, fmt.Errorf("first: %w", err)
			}
			o.First = &n
		case "last":
			n, err := posInt(val)
			if err != nil {
				return nil, fmt.Errorf("last: %w", err)
			}
			o.Last = &n
		case "offset":
			n, ok := val.(float64)
			if !ok || n < 0 {
				return nil, fmt.Errorf("offset must be >= 0")
			}
			i := int(n)
			o.Offset = &i
		case "before":
			c, err := decodeCursor(val)
			if err != nil {
				return nil, fmt.Errorf("before: %w", err)
			}
			o.Before = c
		case "beforeInclusive":
			b, ok := val.(bool)
			if !ok {
				return nil, fmt.Errorf("beforeInclusive must be boolean")
			}
			o.BeforeInclusive = b
		case "after":
			c, err := decodeCursor(val)
			if err != nil {
				return nil, fmt.Errorf("after: %w", err)
			}
			o.After = c
		case "afterInclusive":
			b, ok := val.(bool)
			if !ok {
				return nil, fmt.Errorf("afterInclusive must be boolean")
			}
			o.AfterInclusive = b
		case "aggregate":
			s, _ := val.(string)
			if s != "count" {
				return nil, fmt.Errorf("aggregate: only \"count\" is supported")
			}
			o.Aggregate = s
		case "fields":
			l, ok := val.([]any)
			if !ok {
				return nil, fmt.Errorf("fields must be a list of strings")
			}
			var fs []string
			for _, x := range l {
				fs = append(fs, fmt.Sprint(x))
			}
			o.Fields = fs
		}
	}
	return o, nil
}

func posInt(v any) (int, error) {
	n, ok := v.(float64)
	if !ok || n < 1 || n != float64(int(n)) {
		return 0, fmt.Errorf("must be positive integer")
	}
	return int(n), nil
}

func splitPath(s string) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == '.' {
			out = append(out, cur)
			cur = ""
		} else {
			cur += string(r)
		}
	}
	out = append(out, cur)
	return out
}

// Cursor encoding: opaque JSON [eid, attrID, value]; page-info exposes it.
func encodeCursor(eid, attrID string, value any) string {
	b, _ := json.Marshal([]any{eid, attrID, value})
	return string(b)
}

// encodeServerCreatedAtCursor preserves the frozen v1 cursor tuple for the
// built-in order: [entity-id, id-attr-id, entity-id, epoch-millis]. The
// fourth component is required to continue after the cursor entity has been
// deleted; the first three retain the ordinary cursor identity shape.
func encodeServerCreatedAtCursor(eid, idAttrID string, createdAtMillis int64) string {
	b, _ := json.Marshal([]any{eid, idAttrID, eid, createdAtMillis})
	return string(b)
}

func decodeCursor(v any) ([]any, error) {
	switch c := v.(type) {
	case []any:
		if len(c) < 3 {
			return nil, fmt.Errorf("cursor needs [eid, attrId, value]")
		}
		return c, nil
	case string:
		var out []any
		if err := json.Unmarshal([]byte(c), &out); err != nil {
			return nil, err
		}
		if len(out) < 3 {
			return nil, fmt.Errorf("cursor needs [eid, attrId, value]")
		}
		return out, nil
	default:
		return nil, fmt.Errorf("cursor must be array or opaque string")
	}
}
