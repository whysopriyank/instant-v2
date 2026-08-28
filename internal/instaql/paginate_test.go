package instaql

import (
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
