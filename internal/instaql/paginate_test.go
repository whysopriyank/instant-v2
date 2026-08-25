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
