package instaql

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// TestPaginateWrapOrdersDeterministically pins SQL-side paging semantics:
// LIMIT/OFFSET without a total order take an arbitrary subset of matches and
// pages can overlap or skip rows as the planner's emit order shifts. The
// wrap must impose ORDER BY entity_id whenever it slices; unpaged queries
// must stay untouched.
func TestPaginateWrapOrdersDeterministically(t *testing.T) {
	inner := "SELECT id FROM triples WHERE app_id = $1"

	lim, off := 2, 4
	wrapped, _ := paginateWrap(inner, nil, &Options{Limit: &lim}, nil, "")
	if !strings.Contains(wrapped, "ORDER BY s.entity_id") {
		t.Fatalf("LIMIT without ORDER BY — pages are nondeterministic:\n%s", wrapped)
	}

	wrapped, _ = paginateWrap(inner, nil, &Options{Offset: &off}, nil, "")
	if !strings.Contains(wrapped, "ORDER BY s.entity_id") {
		t.Fatalf("OFFSET without ORDER BY — pages are nondeterministic:\n%s", wrapped)
	}

	wrapped, _ = paginateWrap(inner, nil, nil, nil, "")
	if strings.Contains(wrapped, "ORDER BY") {
		t.Fatalf("unpaged query should not be ordered:\n%s", wrapped)
	}
}

func TestPageEntitiesCombinedBoundaries(t *testing.T) {
	rows := []entity{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "d"}, {ID: "e"}}
	cur := func(id string) []any { return []any{id, "", nil} }
	one := 1
	for _, tc := range []struct {
		name       string
		opts       Options
		want       []string
		next, prev bool
	}{
		{"intersection", Options{After: cur("b"), Before: cur("e")}, []string{"c", "d"}, false, true},
		{"inclusive", Options{After: cur("b"), Before: cur("d"), AfterInclusive: true, BeforeInclusive: true}, []string{"b", "c", "d"}, false, true},
		{"crossed", Options{After: cur("e"), Before: cur("a")}, []string{}, false, true},
		{"before-last", Options{Before: cur("e"), Last: &one}, []string{"d"}, true, true},
		{"offset-first", Options{After: cur("a"), Before: cur("e"), Offset: &one, First: &one}, []string{"c"}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, next, prev, err := pageEntities(rows, &tc.opts, "posts", false)
			if err != nil {
				t.Fatal(err)
			}
			ids := make([]string, 0, len(got))
			for _, row := range got {
				ids = append(ids, row.ID)
			}
			if !reflect.DeepEqual(ids, tc.want) || next != tc.next || prev != tc.prev {
				t.Fatalf("got %v next=%v prev=%v, want %v next=%v prev=%v", ids, next, prev, tc.want, tc.next, tc.prev)
			}
		})
	}
}

func TestOrderValueDomain(t *testing.T) {
	for _, values := range [][]any{{json.Number("1"), "two"}, {true}, {json.Number("1e400")}} {
		rows := make([]entity, len(values))
		for i, value := range values {
			rows[i] = entity{Fields: map[string]any{"value": value}}
		}
		if err := applyOrder(rows, &Options{Order: &Order{K: "value"}}); err == nil {
			t.Fatalf("unsupported order values accepted: %v", values)
		}
	}
	rows := []entity{{ID: "b", Fields: map[string]any{"value": json.Number("10")}}, {ID: "a", Fields: map[string]any{"value": json.Number("2")}}, {ID: "c"}}
	if err := applyOrder(rows, &Options{Order: &Order{K: "value"}}); err != nil {
		t.Fatal(err)
	}
	if rows[0].ID != "c" || rows[1].ID != "a" || rows[2].ID != "b" {
		t.Fatalf("numeric/null order = %v", rows)
	}
}

func TestPaginateWrapFetchesSentinel(t *testing.T) {
	limit := 2
	wrapped, _ := paginateWrap("SELECT entity_id FROM triples", nil, &Options{Limit: &limit}, nil, "")
	if !strings.Contains(wrapped, "LIMIT 3") {
		t.Fatalf("paged query must fetch one sentinel row:\n%s", wrapped)
	}
}

func TestPageSliceBoundaries(t *testing.T) {
	entities := func(n int) []entity {
		out := make([]entity, n)
		for i := range out {
			out[i] = entity{ID: string(rune('a' + i))}
		}
		return out
	}
	intPtr := func(n int) *int { return &n }

	tests := []struct {
		name               string
		rows               int
		options            *Options
		wantRows           int
		wantNext, wantPrev bool
	}{
		{name: "zero", rows: 0, options: &Options{Limit: intPtr(2)}, wantRows: 0},
		{name: "under-limit", rows: 1, options: &Options{Limit: intPtr(2)}, wantRows: 1},
		{name: "exact-limit", rows: 2, options: &Options{Limit: intPtr(2)}, wantRows: 2},
		{name: "over-limit", rows: 3, options: &Options{Limit: intPtr(2)}, wantRows: 2, wantNext: true},
		{name: "first-over-limit", rows: 3, options: &Options{First: intPtr(2)}, wantRows: 2, wantNext: true},
		{name: "last-exact", rows: 2, options: &Options{Last: intPtr(2)}, wantRows: 2},
		{name: "last-over-limit", rows: 3, options: &Options{Last: intPtr(2)}, wantRows: 2, wantPrev: true},
		{name: "before", rows: 2, options: &Options{Limit: intPtr(2), Before: []any{"e", "a", nil}}, wantRows: 2, wantPrev: true},
		{name: "after-over-limit", rows: 3, options: &Options{Limit: intPtr(2), After: []any{"e", "a", nil}}, wantRows: 2, wantNext: true},
		{name: "offset-under-limit", rows: 1, options: &Options{Limit: intPtr(2), Offset: intPtr(2)}, wantRows: 1, wantPrev: true},
		{name: "offset-over-limit", rows: 4, options: &Options{Limit: intPtr(2), Offset: intPtr(1)}, wantRows: 2, wantNext: true, wantPrev: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, next, prev := pageSlice(entities(tt.rows), tt.options)
			if len(got) != tt.wantRows || next != tt.wantNext || prev != tt.wantPrev {
				t.Fatalf("pageSlice rows=%d next=%v prev=%v, want rows=%d next=%v prev=%v", len(got), next, prev, tt.wantRows, tt.wantNext, tt.wantPrev)
			}
		})
	}
}

func TestPageEntitiesCursorInclusivity(t *testing.T) {
	entities := []entity{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	cur := []any{"b", "", nil}
	count := func(o *Options) []entity {
		got, _, _, err := pageEntities(entities, o, "posts", false)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	if got := count(&Options{After: cur}); len(got) != 1 || got[0].ID != "c" {
		t.Fatalf("exclusive after = %v, want [c]", got)
	}
	if got := count(&Options{After: cur, AfterInclusive: true}); len(got) != 2 || got[0].ID != "b" {
		t.Fatalf("inclusive after = %v, want [b c]", got)
	}
	if got := count(&Options{Before: cur}); len(got) != 1 || got[0].ID != "a" {
		t.Fatalf("exclusive before = %v, want [a]", got)
	}
	if got := count(&Options{Before: cur, BeforeInclusive: true}); len(got) != 2 || got[1].ID != "b" {
		t.Fatalf("inclusive before = %v, want [a b]", got)
	}
	_, _, _, err := pageEntities(entities, &Options{Order: &Order{K: "title"}, After: []any{"missing", "", nil}}, "posts", false)
	var missing *ErrCursorNotFound
	if !errors.As(err, &missing) {
		t.Fatalf("missing explicit-order cursor error = %v, want ErrCursorNotFound", err)
	}
}
