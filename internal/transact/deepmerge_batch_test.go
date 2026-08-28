package transact_test

// Deep-merge batching semantics (audit backlog): applyDeepMerge now folds a
// batch's sources in step order over ONE pair-scoped fetch, replacing the
// per-step fetch/write chain. These tests pin the observable equivalences:
// sequential fold across same-(e,a) steps, cross-attr batching, and the
// null-source replace behavior. DATABASE_URL-gated like the rest of the
// live transactor suite.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/transact"
)

func canonical(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestDeepMergeBatchedFoldSemantics(t *testing.T) {
	db := testDB(t)
	appID, cat, ids := seed(t, db)

	e := rand16()
	steps := parseSteps(t,
		mustJSON(t, []any{"deep-merge-triple", uuidStr(e), uuidToStr(ids.name), map[string]any{"nested": map[string]any{"x": 1}, "keep": "a"}}),
		mustJSON(t, []any{"deep-merge-triple", uuidStr(e), uuidToStr(ids.name), map[string]any{"nested": map[string]any{"y": 2}, "more": true}}),
	)
	if _, err := transact.Transact(context.Background(), db, cat, appID, steps, transact.Options{}, &perms.RuleDoc{Etypes: map[string]*perms.EtypeRule{}}); err != nil {
		t.Fatalf("transact: %v", err)
	}
	rows, err := db.FetchTriples(context.Background(), appID, storage.FetchFilter{
		EntityIDs: [][16]byte{e},
		AttrIDs:   [][16]byte{ids.name},
	})
	if err != nil || len(rows) != 1 {
		t.Fatalf("fetch: %v rows=%d", err, len(rows))
	}
	got := canonical(t, rows[0].Triple.V)
	want := canonical(t, map[string]any{
		"nested": map[string]any{"x": 1, "y": 2}, // both steps folded, in order
		"keep":   "a",
		"more":   true,
	})
	if got != want {
		t.Fatalf("folded merge:\n got=%s\nwant=%s", got, want)
	}
}

func TestDeepMergeBatchedAcrossAttrs(t *testing.T) {
	db := testDB(t)
	appID, cat, ids := seed(t, db)

	e := rand16()
	e2 := rand16()
	// A ref attr's deep-merged value must stay a uuid string (CHECK binds
	// (value->>0)::uuid); deep-merge with a non-map src replaces verbatim.
	const linkTarget = "9f296e7e-1a41-4f21-9b8e-000000000001"
	steps := parseSteps(t,
		mustJSON(t, []any{"deep-merge-triple", uuidStr(e), uuidToStr(ids.name), map[string]any{"a": 1}}),
		mustJSON(t, []any{"deep-merge-triple", uuidStr(e2), uuidToStr(ids.name), map[string]any{"b": 2}}),
		mustJSON(t, []any{"deep-merge-triple", uuidStr(e), uuidToStr(ids.link), linkTarget}),
	)
	if _, err := transact.Transact(context.Background(), db, cat, appID, steps, transact.Options{}, &perms.RuleDoc{Etypes: map[string]*perms.EtypeRule{}}); err != nil {
		t.Fatalf("transact: %v", err)
	}
	rows, err := db.FetchTriples(context.Background(), appID, storage.FetchFilter{
		EntityIDs: [][16]byte{e, e2},
	})
	if err != nil || len(rows) != 3 {
		t.Fatalf("fetch: %v rows=%d", err, len(rows))
	}
	seen := map[string]string{}
	for _, r := range rows {
		seen[uuidToStr(r.Triple.E)+":"+uuidToStr(r.Triple.A)] = canonical(t, r.Triple.V)
	}
	nameE := uuidToStr(ids.name)
	if seen[uuidToStr(e)+":"+nameE] != canonical(t, map[string]any{"a": 1}) {
		t.Fatalf("name attr for e: %s", seen[uuidToStr(e)+":"+nameE])
	}
	if seen[uuidToStr(e2)+":"+nameE] != canonical(t, map[string]any{"b": 2}) {
		t.Fatalf("name attr for e2: %s", seen[uuidToStr(e2)+":"+nameE])
	}
	linkE := uuidToStr(ids.link)
	if seen[uuidToStr(e)+":"+linkE] != canonical(t, linkTarget) {
		t.Fatalf("link attr: %s", seen[uuidToStr(e)+":"+linkE])
	}
}

func TestDeepMergeNullSourceReplaces(t *testing.T) {
	db := testDB(t)
	appID, cat, ids := seed(t, db)

	e := rand16()
	base := mustJSON(t, []any{"deep-merge-triple", uuidStr(e), uuidToStr(ids.name), map[string]any{"x": 1}})
	if _, err := transact.Transact(context.Background(), db, cat, appID, parseSteps(t, base), transact.Options{}, &perms.RuleDoc{Etypes: map[string]*perms.EtypeRule{}}); err != nil {
		t.Fatalf("base transact: %v", err)
	}
	// A null source must REPLACE the value (deepMergeJSON with a non-map
	// src returns src) — the batched fold must reproduce that, not keep
	// the base.
	steps := parseSteps(t,
		mustJSON(t, []any{"deep-merge-triple", uuidStr(e), uuidToStr(ids.name), nil}),
	)
	if _, err := transact.Transact(context.Background(), db, cat, appID, steps, transact.Options{}, &perms.RuleDoc{Etypes: map[string]*perms.EtypeRule{}}); err != nil {
		t.Fatalf("null transact: %v", err)
	}
	rows, err := db.FetchTriples(context.Background(), appID, storage.FetchFilter{
		EntityIDs: [][16]byte{e},
		AttrIDs:   [][16]byte{ids.name},
	})
	if err != nil || len(rows) != 1 {
		t.Fatalf("fetch: %v rows=%d", err, len(rows))
	}
	if rows[0].Triple.V != nil {
		t.Fatalf("null source must replace, got %s", canonical(t, rows[0].Triple.V))
	}
}

func TestDeepMergeSequentialScalarMapTransition(t *testing.T) {
	db := testDB(t)
	appID, cat, ids := seed(t, db)
	e := rand16()
	ctx := context.Background()
	all := func(values ...any) []transact.Step {
		raw := make([]json.RawMessage, 0, len(values))
		for _, value := range values {
			raw = append(raw, mustJSON(t, []any{"deep-merge-triple", uuidStr(e), uuidToStr(ids.name), value}))
		}
		return parseSteps(t, raw...)
	}
	if _, err := transact.Transact(ctx, db, cat, appID, all(map[string]any{"a": 1}), transact.Options{}, &perms.RuleDoc{Etypes: map[string]*perms.EtypeRule{}}); err != nil {
		t.Fatalf("base transact: %v", err)
	}
	// This is the regression from the audit: sequentially applying scalar 2
	// then map {b:3} must not retain the old map key a.
	if _, err := transact.Transact(ctx, db, cat, appID, all(2, map[string]any{"b": 3}), transact.Options{}, &perms.RuleDoc{Etypes: map[string]*perms.EtypeRule{}}); err != nil {
		t.Fatalf("transition transact: %v", err)
	}
	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{e}, AttrIDs: [][16]byte{ids.name}})
	if err != nil || len(rows) != 1 {
		t.Fatalf("fetch: %v rows=%d", err, len(rows))
	}
	if got, want := canonical(t, rows[0].Triple.V), canonical(t, map[string]any{"b": int64(3)}); got != want {
		t.Fatalf("map/scalar transition: got=%s want=%s", got, want)
	}
	if _, err := transact.Transact(ctx, db, cat, appID, all(nil, map[string]any{"c": 4}), transact.Options{}, &perms.RuleDoc{Etypes: map[string]*perms.EtypeRule{}}); err != nil {
		t.Fatalf("null transition transact: %v", err)
	}
	rows, err = db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{e}, AttrIDs: [][16]byte{ids.name}})
	if err != nil || len(rows) != 1 {
		t.Fatalf("fetch after null transition: %v rows=%d", err, len(rows))
	}
	if got, want := canonical(t, rows[0].Triple.V), canonical(t, map[string]any{"c": int64(4)}); got != want {
		t.Fatalf("null/map transition: got=%s want=%s", got, want)
	}
}

func TestDeepMergeManyRepeatedPairPreservesSequentialInserts(t *testing.T) {
	db := testDB(t)
	appID, cat, ids := seed(t, db)
	e := rand16()
	ctx := context.Background()
	steps := parseSteps(t,
		mustJSON(t, []any{"deep-merge-triple", uuidStr(e), uuidToStr(ids.link), "9f296e7e-1a41-4f21-9b8e-000000000001"}),
		mustJSON(t, []any{"deep-merge-triple", uuidStr(e), uuidToStr(ids.link), "9f296e7e-1a41-4f21-9b8e-000000000002"}),
	)
	if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{}, &perms.RuleDoc{Etypes: map[string]*perms.EtypeRule{}}); err != nil {
		t.Fatalf("transact: %v", err)
	}
	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{e}, AttrIDs: [][16]byte{ids.link}})
	if err != nil || len(rows) != 2 {
		t.Fatalf("fetch: %v rows=%d", err, len(rows))
	}
}
