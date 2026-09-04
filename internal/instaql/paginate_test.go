package instaql

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
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
	idOrder := func(o Options) Options {
		o.Order = &Order{K: "id"}
		return o
	}
	one := 1
	for _, tc := range []struct {
		name       string
		opts       Options
		want       []string
		next, prev bool
	}{
		{"intersection", idOrder(Options{After: cur("b"), Before: cur("e")}), []string{"c", "d"}, false, true},
		{"inclusive", idOrder(Options{After: cur("b"), Before: cur("d"), AfterInclusive: true, BeforeInclusive: true}), []string{"b", "c", "d"}, false, true},
		{"crossed", idOrder(Options{After: cur("e"), Before: cur("a")}), []string{}, false, true},
		{"before-last", idOrder(Options{Before: cur("e"), Last: &one}), []string{"d"}, true, true},
		{"offset-first", idOrder(Options{After: cur("a"), Before: cur("e"), Offset: &one, First: &one}), []string{"c"}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, next, prev, err := pageEntities(rows, &tc.opts, "posts", false, "")
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

func TestServerCreatedAtOrderUsesIDTripleTimeAndDirectionalTieBreak(t *testing.T) {
	timestamp := func(ms int64) time.Time { return time.UnixMilli(ms) }
	rows := []entity{
		{ID: "00000000-0000-0000-0000-000000000003", ServerCreatedAt: timestamp(1000), HasServerCreatedAt: true},
		{ID: "00000000-0000-0000-0000-000000000001", ServerCreatedAt: timestamp(2000), HasServerCreatedAt: true},
		{ID: "00000000-0000-0000-0000-000000000002", ServerCreatedAt: timestamp(3000), HasServerCreatedAt: true},
		{ID: "00000000-0000-0000-0000-000000000004", ServerCreatedAt: timestamp(3000), HasServerCreatedAt: true},
	}
	if err := applyOrder(rows, &Options{Order: &Order{K: "serverCreatedAt"}}); err != nil {
		t.Fatal(err)
	}
	if got, want := entityIDs(rows), []string{
		"00000000-0000-0000-0000-000000000003",
		"00000000-0000-0000-0000-000000000001",
		"00000000-0000-0000-0000-000000000002",
		"00000000-0000-0000-0000-000000000004",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("serverCreatedAt asc = %v, want %v", got, want)
	}
	if err := applyOrder(rows, &Options{Order: &Order{K: "serverCreatedAt", Desc: true}}); err != nil {
		t.Fatal(err)
	}
	if got, want := entityIDs(rows), []string{
		"00000000-0000-0000-0000-000000000004",
		"00000000-0000-0000-0000-000000000002",
		"00000000-0000-0000-0000-000000000001",
		"00000000-0000-0000-0000-000000000003",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("serverCreatedAt desc = %v, want %v", got, want)
	}
}

func TestServerCreatedAtRemovedCursorUsesTimestamp(t *testing.T) {
	rows := []entity{
		{ID: "00000000-0000-0000-0000-000000000001", ServerCreatedAt: time.UnixMilli(1000), HasServerCreatedAt: true},
		{ID: "00000000-0000-0000-0000-000000000003", ServerCreatedAt: time.UnixMilli(3000), HasServerCreatedAt: true},
		{ID: "00000000-0000-0000-0000-000000000004", ServerCreatedAt: time.UnixMilli(3000), HasServerCreatedAt: true},
	}
	cursor := []any{"00000000-0000-0000-0000-000000000002", "id", "00000000-0000-0000-0000-000000000002", float64(2000)}
	limit := 10
	got, _, _, err := pageEntities(rows, &Options{
		Order: &Order{K: "serverCreatedAt"}, After: cursor, Limit: &limit,
	}, "posts", false, "id")
	if err != nil {
		t.Fatal(err)
	}
	if ids := entityIDs(got); !reflect.DeepEqual(ids, []string{
		"00000000-0000-0000-0000-000000000003",
		"00000000-0000-0000-0000-000000000004",
	}) {
		t.Fatalf("after removed serverCreatedAt cursor = %v", ids)
	}
}

func TestPaginatedNoOrderDefaultsToServerCreatedAt(t *testing.T) {
	rows := []entity{
		{ID: "c", ServerCreatedAt: time.UnixMilli(1000), HasServerCreatedAt: true},
		{ID: "a", ServerCreatedAt: time.UnixMilli(2000), HasServerCreatedAt: true},
		{ID: "b", ServerCreatedAt: time.UnixMilli(3000), HasServerCreatedAt: true},
	}
	limit := 2
	opts := &Options{Limit: &limit}
	if canUseIDFastPath(opts, nil) {
		t.Fatal("paginated no-order form must bypass the ID fast path")
	}
	if err := applyOrder(rows, opts); err != nil {
		t.Fatal(err)
	}
	got, hasNext, _, err := pageEntities(rows, opts, "posts", false, "id")
	if err != nil {
		t.Fatal(err)
	}
	if ids := entityIDs(got); !reflect.DeepEqual(ids, []string{"c", "a"}) || !hasNext {
		t.Fatalf("default paginated order = %v, hasNext=%v; want [c a], true", ids, hasNext)
	}
}

func TestServerCreatedAtCursorRequiresExactTuple(t *testing.T) {
	rows := []entity{{ID: "a", ServerCreatedAt: time.UnixMilli(1000), HasServerCreatedAt: true}}
	limit := 1
	bad := [][]any{
		{"a", "id", "a"},                         // missing timestamp
		{"a", "id", "a", float64(1000), "extra"}, // extra tuple member
		{"a", "other-id", "a", float64(1000)},    // cursor from another order
		{"a", "id", "other", float64(1000)},      // malformed id triple value
		{"a", "id", "a", "1000"},                 // timestamp must be integral JSON number
	}
	for i, cursor := range bad {
		opts := &Options{Order: &Order{K: "serverCreatedAt"}, After: cursor, Limit: &limit}
		if _, _, _, err := pageEntities(rows, opts, "posts", false, "id"); err == nil {
			t.Errorf("malformed serverCreatedAt cursor %d was accepted", i)
		}
	}
}

func TestServerCreatedAtCursorUsesEncodedBoundaryAfterOverwrite(t *testing.T) {
	rows := []entity{
		{ID: "a", ServerCreatedAt: time.UnixMilli(1000), HasServerCreatedAt: true},
		{ID: "b", ServerCreatedAt: time.UnixMilli(2000), HasServerCreatedAt: true},
		{ID: "c", ServerCreatedAt: time.UnixMilli(3000), HasServerCreatedAt: true},
	}
	cursor := []any{"b", "id", "b", float64(2000)}
	// Simulate OverwriteT moving the cursor entity after c. The issued cursor
	// still denotes b@2000, so c and then b remain after its encoded boundary.
	rows[1].ServerCreatedAt = time.UnixMilli(4000)
	limit := 10
	opts := &Options{Order: &Order{K: "serverCreatedAt"}, After: cursor, Limit: &limit}
	if err := applyOrder(rows, opts); err != nil {
		t.Fatal(err)
	}
	got, _, _, err := pageEntities(rows, opts, "posts", false, "id")
	if err != nil {
		t.Fatal(err)
	}
	if ids := entityIDs(got); !reflect.DeepEqual(ids, []string{"c", "b"}) {
		t.Fatalf("overwrite-stable cursor page = %v, want [c b]", ids)
	}
}

func entityIDs(rows []entity) []string {
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.ID)
	}
	return ids
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
		if o.Order == nil {
			o.Order = &Order{K: "id"}
		}
		got, _, _, err := pageEntities(entities, o, "posts", false, "")
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
	_, _, _, err := pageEntities(entities, &Options{Order: &Order{K: "title"}, After: []any{"missing", "", nil}}, "posts", false, "")
	var missing *ErrCursorNotFound
	if !errors.As(err, &missing) {
		t.Fatalf("missing explicit-order cursor error = %v, want ErrCursorNotFound", err)
	}
}
