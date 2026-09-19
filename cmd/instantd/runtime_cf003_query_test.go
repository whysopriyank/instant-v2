package main

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// TestCF003AssembledQueryConjunctionAndOrder closes the CF-003 HTTP
// conjunction remainder (manifest row query-conjunction-gap): multi-predicate
// where conjunctions and explicit ordering stay stable over the mounted
// production POST /runtime/framework/query path against an owned PostgreSQL
// fixture. Every leg asserts the exact whole result so a losing or orphan
// entity cannot hide behind a subset check.
func TestCF003AssembledQueryConjunctionAndOrder(t *testing.T) {
	mux, appID, adminToken := cf003PostgresMux(t)
	app := platform.UUIDToStr(appID)
	entityB := "00000000-0000-4000-8000-000000000006"
	entityC := "00000000-0000-4000-8000-000000000007"

	transact := func(steps ...any) {
		t.Helper()
		status, body, _ := cf003Serve(mux, http.MethodPost, "/admin/transact", cf003JSON(t, map[string]any{
			"app-id": app, "steps": steps,
		}), map[string]string{"X-admin-token": adminToken})
		if status != http.StatusOK {
			t.Fatalf("query conjunction seed = %d %q", status, body)
		}
	}
	query := func(queryMap map[string]any) (any, map[string]any) {
		t.Helper()
		status, body, _ := cf003Serve(mux, http.MethodPost, "/runtime/framework/query",
			cf003JSON(t, map[string]any{"query": queryMap}), map[string]string{"app-id": app})
		if status != http.StatusOK {
			t.Fatalf("query conjunction request %+v = %d %q", queryMap, status, body)
		}
		var envelope struct {
			Data     map[string]json.RawMessage `json:"data"`
			PageInfo map[string]any             `json:"page-info"`
		}
		if err := json.Unmarshal([]byte(body), &envelope); err != nil {
			t.Fatalf("query conjunction body = %q: %v", body, err)
		}
		var todos []any
		if err := json.Unmarshal(envelope.Data["todos"], &todos); err != nil {
			t.Fatalf("query conjunction todos = %q: %v", body, err)
		}
		if todos == nil {
			todos = []any{}
		}
		return todos, envelope.PageInfo
	}
	transact(
		[]any{"update", "todos", cf003EntityID, map[string]any{"title": "cf003-alpha", "priority": 1, "team": "red"}},
		[]any{"update", "todos", entityB, map[string]any{"title": "cf003-beta", "priority": 2, "team": "red"}},
		[]any{"update", "todos", entityC, map[string]any{"title": "cf003-gamma", "priority": 3, "team": "blue"}},
	)
	wantA := map[string]any{"id": cf003EntityID, "title": "cf003-alpha", "priority": float64(1), "team": "red"}
	wantB := map[string]any{"id": entityB, "title": "cf003-beta", "priority": float64(2), "team": "red"}
	wantC := map[string]any{"id": entityC, "title": "cf003-gamma", "priority": float64(3), "team": "blue"}

	// Unfiltered baseline is exactly the three seeded entities in ID order.
	todos, _ := query(map[string]any{"todos": map[string]any{}})
	if !reflect.DeepEqual(todos, []any{wantA, wantB, wantC}) {
		t.Fatalf("conjunction baseline = %#v; want exactly three seeded todos", todos)
	}

	// Two bounds on one numeric field conjoin: only the middle row matches.
	todos, _ = query(map[string]any{"todos": map[string]any{"$": map[string]any{
		"where": map[string]any{"priority": map[string]any{"$gt": 1, "$lt": 3}},
	}}})
	if !reflect.DeepEqual(todos, []any{wantB}) {
		t.Fatalf("single-field conjunction = %#v; want exactly [%#v]", todos, wantB)
	}

	// Contradictory bounds match nothing, not everything.
	todos, _ = query(map[string]any{"todos": map[string]any{"$": map[string]any{
		"where": map[string]any{"priority": map[string]any{"$gt": 3, "$lt": 1}},
	}}})
	if !reflect.DeepEqual(todos, []any{}) {
		t.Fatalf("contradictory conjunction = %#v; want exactly []", todos)
	}

	// Predicates across two fields conjoin: only the red row with priority>=2.
	todos, _ = query(map[string]any{"todos": map[string]any{"$": map[string]any{
		"where": map[string]any{"team": "red", "priority": map[string]any{"$gte": 2}},
	}}})
	if !reflect.DeepEqual(todos, []any{wantB}) {
		t.Fatalf("multi-field conjunction = %#v; want exactly [%#v]", todos, wantB)
	}

	// Explicit numeric ordering is stable in both directions over HTTP.
	todos, _ = query(map[string]any{"todos": map[string]any{"$": map[string]any{
		"order": map[string]any{"k": "priority", "direction": "asc"},
	}}})
	if !reflect.DeepEqual(todos, []any{wantA, wantB, wantC}) {
		t.Fatalf("ascending order = %#v; want exactly [%#v %#v %#v]", todos, wantA, wantB, wantC)
	}
	todos, _ = query(map[string]any{"todos": map[string]any{"$": map[string]any{
		"order": map[string]any{"k": "priority", "direction": "desc"},
	}}})
	if !reflect.DeepEqual(todos, []any{wantC, wantB, wantA}) {
		t.Fatalf("descending order = %#v; want exactly [%#v %#v %#v]", todos, wantC, wantB, wantA)
	}

	// Ordered pages partition the match set: first page plus its cursor-held
	// continuation are disjoint and jointly cover all three entities.
	first, info := query(map[string]any{"todos": map[string]any{"$": map[string]any{
		"order": map[string]any{"k": "priority", "direction": "asc"}, "limit": 2,
	}}})
	if !reflect.DeepEqual(first, []any{wantA, wantB}) {
		t.Fatalf("ordered first page = %#v; want exactly [%#v %#v]", first, wantA, wantB)
	}
	endCursor, _ := info["endCursor"].(string)
	if info["hasNextPage"] != true || endCursor == "" {
		t.Fatalf("ordered first page info = %#v; want hasNextPage with endCursor", info)
	}
	second, secondInfo := query(map[string]any{"todos": map[string]any{"$": map[string]any{
		"order": map[string]any{"k": "priority", "direction": "asc"}, "limit": 2, "after": endCursor,
	}}})
	if !reflect.DeepEqual(second, []any{wantC}) {
		t.Fatalf("ordered second page = %#v; want exactly [%#v]", second, wantC)
	}
	if secondInfo["hasNextPage"] != false {
		t.Fatalf("ordered second page info = %#v; want exhausted page", secondInfo)
	}
}
