package datalog

import (
	"testing"

	"github.com/instant-v2/instant-v2/internal/platform"
)

func attr(vt, card string, uniq, idx bool) platform.Attr {
	return platform.Attr{ValueType: vt, Cardinality: card, IsUnique: uniq, IsIndexed: idx}
}

func TestBestIndex(t *testing.T) {
	for _, tc := range []struct {
		name string
		a    platform.Attr
		want IndexKind
	}{
		{"unique ref (lookup handle)", attr("ref", "one", true, true), IdxAV},
		{"indexed one blob", attr("blob", "one", false, true), IdxAVE},
		{"plain ref link", attr("ref", "many", false, false), IdxEAV},
		{"unique blob", attr("blob", "one", true, true), IdxAV},
		{"indexed many blob", attr("blob", "many", false, true), IdxAVE},
		{"plain object value", attr("blob", "one", false, false), IdxEA},
	} {
		if got := BestIndex(tc.a); got != tc.want {
			t.Fatalf("%s: got %s want %s", tc.name, got, tc.want)
		}
	}
}

func TestEntitySetSQLZeroConditions(t *testing.T) {
	p := BuildPlan("app-1", "posts", nil)
	sqlStr, args := p.EntitySetSQL([]string{"a1", "a2"})
	if len(args) != 2 {
		t.Fatalf("args %v want 2 (app + attrs)", args)
	}
	if args[0] != "app-1" {
		t.Fatalf("app arg: %v", args[0])
	}
	if !contains(sqlStr, "::uuid[]") || !contains(sqlStr, "GROUP BY t.entity_id") {
		t.Fatalf("zero-cond SQL missing pieces: %s", sqlStr)
	}
}

func TestEntitySetSQLConjunction(t *testing.T) {
	conds := []Condition{
		{AttrID: "attr-a", Op: PredEq, Args: []any{`"red"`}, IndexHint: IdxAVE},
		{AttrID: "attr-b", Op: PredIn, Args: []any{`1`, `2`, `3`}},
	}
	p := BuildPlan("app-1", "posts", conds)
	sqlStr, args := p.EntitySetSQL([]string{"attr-a"})
	// Two subqueries joined by INTERSECT.
	count := 0
	for _, part := range splitAll(sqlStr, "INTERSECT") {
		_ = part
		count++
	}
	if count != 2 {
		t.Fatalf("want 2 intersected parts, got %d: %s", count, sqlStr)
	}
	// Placeholders: $1 app, $2 attrA, $3 value, $4 attrB, $5..$7 in-list
	want := []string{"$1", "$2", "$3", "$4", "$5", "$6", "$7"}
	for _, w := range want {
		if !contains(sqlStr, w) {
			t.Fatalf("missing placeholder %s in: %s", w, sqlStr)
		}
	}
	if len(args) != 7 {
		t.Fatalf("args len %d want 7: %v", len(args), args)
	}
	if args[0] != "app-1" || args[2] != `"red"` || args[4] != `1` || args[6] != `3` {
		t.Fatalf("arg values: %v", args)
	}
}

func TestTopicsDedup(t *testing.T) {
	conds := []Condition{
		{AttrID: "a"}, {AttrID: "b"}, {AttrID: "a"},
	}
	topics := BuildPlan("app", "t", conds).Topics()
	if len(topics) != 2 {
		t.Fatalf("topics %v want 2 unique", topics)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func splitAll(s, sep string) []string {
	var out []string
	for {
		i := indexOf(s, sep)
		if i < 0 {
			out = append(out, s)
			return out
		}
		out = append(out, s[:i])
		s = s[i+len(sep):]
	}
}
