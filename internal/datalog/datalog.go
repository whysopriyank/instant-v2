// Package datalog is the pattern-IR + SQL planner layer. InstaQL compiles to
// pattern sets here; each set targets one etype level and generates SQL that
// hits the flag-column partial indexes (ea/eav/av/ave/vae) chosen by BestIndex.
//
// Design note (deviation from v1, tracked for the golden corpus): v1 compiles
// symbol-joined pattern graphs into one mega-CTE (db/datalog.clj query /
// query-nested). V2 compiles the SAME observable semantics — entity sets per
// etype satisfying conjunctive where-conditions, nested levels joined through
// ref attrs — into per-level INTERSECT queries. The Phase 4 corpus differential
// validates equivalence; divergences get fixed here, not by re-porting the join
// machinery.
package datalog

import (
	"fmt"
	"strings"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// IndexKind names the five partial indexes.
type IndexKind string

const (
	IdxEA  IndexKind = "ea"
	IdxEAV IndexKind = "eav"
	IdxAV  IndexKind = "av"
	IdxAVE IndexKind = "ave"
	IdxVAE IndexKind = "vae"
)

// Topic identifies what invalidates a plan: an attr (values reserved for the
// reactive layer's fine-grained matching).
type Topic struct {
	AttrID string
	Vals   []string // empty → whole attr
}

// PredOp is the value comparison for one condition.
type PredOp int

const (
	PredExists PredOp = iota // attr present (any value)
	PredEq                   // value = ?
	PredIn                   // value = ANY(?)
	PredNot                  // value IS DISTINCT FROM ?
	PredIsNull               // value-json-null check: true → 'null', false → != 'null'
	PredGt                   // via jsonb comparison helpers on typed extraction
	PredGte
	PredLt
	PredLte
	PredLike
	PredILike
)

// Condition constrains one attr of the level's etype.
type Condition struct {
	AttrID    string
	Attr      platform.Attr
	Op        PredOp
	Args      []any     // JSON-encoded values (strings) for the op
	IndexHint IndexKind // chosen at compile time, informational + planner input
}

// Plan is the compiled form of one etype level's where-clause.
type Plan struct {
	AppID      string
	Etype      string
	Conditions []Condition
}

// Topics derives invalidation topics from the plan's attrs.
func (p *Plan) Topics() []Topic {
	seen := map[string]bool{}
	var out []Topic
	for _, c := range p.Conditions {
		if !seen[c.AttrID] {
			seen[c.AttrID] = true
			out = append(out, Topic{AttrID: c.AttrID})
		}
	}
	return out
}

// BestIndex chooses which partial index a condition can use:
//
//	ea  — always usable   eav — ref equality   av — unique lookup
//	ave — indexed scan    vae — reverse-ref membership
func BestIndex(a platform.Attr) IndexKind {
	f := platform.FlagsFor(a)
	switch {
	case f.EAV && f.AV:
		return IdxAV
	case f.AV: // unique: av_index is the most selective value probe
		return IdxAV
	case f.AVE && a.Cardinality == "one":
		return IdxAVE
	case f.EAV:
		return IdxEAV
	case f.AVE:
		return IdxAVE
	default:
		return IdxEA
	}
}

// BuildPlan compiles conditions for one etype level.
func BuildPlan(appID, etype string, conds []Condition) *Plan {
	return &Plan{AppID: appID, Etype: etype, Conditions: conds}
}

// EntitySetSQL renders the query producing matching entity ids. Conjunction =
// INTERSECT of one index-hitting subquery per condition; zero conditions
// degenerates to "all entities having any triple of this etype".
func (p *Plan) EntitySetSQL(etypeAttrIDs []string) (string, []any) {
	var args []any
	n := 1
	bind := func(v any) string {
		args = append(args, v)
		s := fmt.Sprintf("$%d", n)
		n++
		return s
	}
	appPh := bind(p.AppID)

	if len(p.Conditions) == 0 {
		attrsPh := bind(etypeAttrIDs)
		return fmt.Sprintf(
			"SELECT t.entity_id FROM triples t WHERE t.app_id = %s AND t.attr_id = ANY(%s::uuid[]) GROUP BY t.entity_id",
			appPh, attrsPh), args
	}

	var parts []string
	for _, c := range p.Conditions {
		attrPh := bind(c.AttrID) + "::uuid"
		var b strings.Builder
		b.WriteString("SELECT entity_id FROM triples WHERE app_id = ")
		b.WriteString(appPh)
		b.WriteString(" AND attr_id = ")
		b.WriteString(attrPh)
		if c.Op != PredExists {
			b.WriteString(" AND ")
			b.WriteString(c.predPlaceholders(&n, &args))
		}
		parts = append(parts, "("+b.String()+")")
	}
	return strings.Join(parts, " INTERSECT "), args
}

// predPlaceholders renders the predicate consuming c.Args in order.
func (c *Condition) predPlaceholders(n *int, args *[]any) string {
	bind := func(v any) string {
		*args = append(*args, v)
		s := fmt.Sprintf("$%d", *n)
		*n++
		return s
	}
	switch c.Op {
	case PredEq:
		return "value = " + bind(c.Args[0]) + "::jsonb"
	case PredIn:
		var ps []string
		for _, a := range c.Args {
			ps = append(ps, bind(a)+"::jsonb")
		}
		return "value IN (" + strings.Join(ps, ", ") + ")"
	case PredNot:
		return "value IS DISTINCT FROM " + bind(c.Args[0]) + "::jsonb"
	case PredIsNull:
		if boolArg(c.Args) {
			return "value = 'null'::jsonb"
		}
		return "value != 'null'::jsonb"
	case PredGt, PredGte, PredLt, PredLte:
		op := map[PredOp]string{PredGt: ">", PredGte: ">=", PredLt: "<", PredLte: "<="}[c.Op]
		ph := bind(c.Args[0])
		// Same-type comparison only: numbers compare numerically, strings
		// and booleans lexically, mismatched types match nothing (both arms
		// false / NULL) instead of falling back to text ordering where
		// '10' < '9'.
		pj := ph + "::jsonb"
		num := "(jsonb_typeof(value) = 'number' AND jsonb_typeof(" + pj + ") = 'number'" +
			" AND (value->>0)::numeric " + op + " (" + pj + "->>0)::numeric)"
		txt := "(jsonb_typeof(value) IS NOT NULL AND jsonb_typeof(value) <> 'number'" +
			" AND jsonb_typeof(value) = jsonb_typeof(" + pj + ")" +
			" AND value->>0 " + op + " " + pj + "->>0)"
		return "(" + num + " OR " + txt + ")"
	case PredLike:
		return "(value->>0) LIKE " + bind(c.Args[0])
	case PredILike:
		return "(value->>0) ILIKE " + bind(c.Args[0])
	}
	return ""
}

func boolArg(args []any) bool {
	if len(args) == 0 {
		return false
	}
	b, ok := args[0].(bool)
	return ok && b
}
