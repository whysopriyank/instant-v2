package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// TestCF003AssembledSSELookupLifecycle closes the CF-003
// transactions-lookup-lifecycle remainder (manifest row
// transactions-lookup-lifecycle, surface ws.transact.lookup, transport SSE)
// over the production-mounted routes against an owned PostgreSQL fixture. An
// SSE subscriber watches the todos nodelist while lookup-eid mutations ride
// the mounted POST /admin/transact path: one update through
// lookup__id__"<uuid>" retargets the seeded entity, and a second lookup
// update mints a fresh entity. Each mutation must surface on the SSE stream
// as a refresh-ok frame at the exact triggering transaction watermark and
// converge the mounted HTTP query path to the exact whole result. Every leg
// asserts lengths before elements with exact float watermark compares so a
// partial write or a diverged stream cannot hide behind a subset check. This
// is mounted-route boundary evidence only; no corpus NDJSON leg is added or
// claimed.
func TestCF003AssembledSSELookupLifecycle(t *testing.T) {
	mux, appID, adminToken := cf003PostgresMux(t)
	app := platform.UUIDToStr(appID)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	transact := func(steps ...any) int64 {
		t.Helper()
		status, body, _ := cf003Serve(mux, http.MethodPost, "/admin/transact", cf003JSON(t, map[string]any{
			"app-id": app, "steps": steps,
		}), map[string]string{"X-admin-token": adminToken})
		if status != http.StatusOK {
			t.Fatalf("lookup lifecycle transact = %d %q", status, body)
		}
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal([]byte(body), &envelope); err != nil || len(envelope) != 1 {
			t.Fatalf("lookup lifecycle envelope = %q: %v", body, err)
		}
		var txID int64
		if raw, ok := envelope["tx-id"]; !ok || json.Unmarshal(raw, &txID) != nil || txID <= 0 {
			t.Fatalf("lookup lifecycle tx-id = %q", body)
		}
		return txID
	}
	queryTodos := func() []any {
		t.Helper()
		status, body, _ := cf003Serve(mux, http.MethodPost, "/runtime/framework/query",
			`{"query":{"todos":{}}}`, map[string]string{"app-id": app})
		if status != http.StatusOK {
			t.Fatalf("lookup lifecycle query = %d %q", status, body)
		}
		var envelope struct {
			Data struct {
				Todos []any `json:"todos"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(body), &envelope); err != nil {
			t.Fatalf("lookup lifecycle query body = %q: %v", body, err)
		}
		if envelope.Data.Todos == nil {
			envelope.Data.Todos = []any{}
		}
		return envelope.Data.Todos
	}

	transact([]any{"update", "todos", cf003EntityID, map[string]any{"title": "lookup-one"}})
	idAttr, titleAttr := cf003TransactionAttrs(t, mux, app, adminToken)

	sse := cf003OpenSSE(t, ctx, server, app)
	sse.post(t, ctx, map[string]any{"op": "init", "app-id": app})
	initIDAttr, initTitleAttr := cf003AssertSSEInit(t, cf003ReadSSE(t, sse.scanner), app)
	if initIDAttr != idAttr || initTitleAttr != titleAttr {
		t.Fatalf("lookup SSE attr drift: id=%q/%q title=%q/%q", initIDAttr, idAttr, initTitleAttr, titleAttr)
	}
	sse.post(t, ctx, map[string]any{
		"op": "add-query", "q": map[string]any{"todos": map[string]any{}},
		"client-event-id": "cf003-lookup-query",
	})
	cf003AssertSSEAddQuery(t, cf003ReadSSE(t, sse.scanner), "cf003-lookup-query")
	cf003AssertSSETodos(t, cf003ReadSSE(t, sse.scanner), "lookup-one", true)

	// A lookup-eid update retargets the seeded entity without naming its
	// storage id directly; the stream must carry the exact new title at the
	// exact triggering watermark and the HTTP path must converge exactly.
	lookupSeed := `lookup__id__"` + cf003EntityID + `"`
	firstTx := transact([]any{"update", "todos", lookupSeed, map[string]any{"title": "lookup-two"}})
	cf003AssertSSERefreshTodo(t, cf003ReadSSE(t, sse.scanner), idAttr, titleAttr, "lookup-two", firstTx)
	todos := queryTodos()
	if len(todos) != 1 {
		t.Fatalf("lookup update todos = %#v; want exactly one row", todos)
	}
	cf003ExactTodo(t, cf003TodoByID(t, todos, cf003EntityID), map[string]any{
		"id": cf003EntityID, "title": "lookup-two",
	})

	// A lookup-eid update against an absent id mints a fresh entity; both
	// rows must appear on the stream at the exact new watermark and in the
	// exact HTTP whole result in id order.
	absentLookup := `lookup__id__"00000000-0000-4000-8000-000000000046"`
	secondTx := transact([]any{"update", "todos", absentLookup, map[string]any{"title": "lookup-fresh"}})
	if secondTx <= firstTx {
		t.Fatalf("lookup create tx %d did not advance past update tx %d", secondTx, firstTx)
	}
	refresh := cf003ReadSSE(t, sse.scanner)
	txID, ok := refresh["processed-tx-id"].(float64)
	if !ok || int64(txID) != secondTx {
		t.Fatalf("lookup create watermark = %#v; want exactly %d", refresh["processed-tx-id"], secondTx)
	}
	comps, ok := refresh["computations"].([]any)
	if !ok || len(comps) != 1 {
		t.Fatalf("lookup create computations = %#v; want exactly one", refresh["computations"])
	}
	comp, _ := comps[0].(map[string]any)
	results, ok := comp["instaql-result"].([]any)
	if !ok || len(results) != 1 {
		t.Fatalf("lookup create result = %#v; want exactly one node", comp["instaql-result"])
	}
	node, _ := results[0].(map[string]any)
	rows, ok := node["data"].(map[string]any)
	if !ok {
		t.Fatalf("lookup create node = %#v; want datalog data", node)
	}
	datalog, _ := rows["datalog-result"].(map[string]any)
	joinRows, ok := datalog["join-rows"].([]any)
	if !ok || len(joinRows) == 0 {
		t.Fatalf("lookup create join-rows = %#v; want at least one row", datalog["join-rows"])
	}
	// Join-row grouping varies by plan (one row per triple or several
	// triples per row), so flatten every triple and pin the exact total:
	// two entities times id plus title.
	titles := map[string]string{}
	eids := map[string]bool{}
	triples := 0
	for _, rawRow := range joinRows {
		row, _ := rawRow.([]any)
		if len(row) == 0 {
			t.Fatalf("lookup create row = %#v; want at least one triple", rawRow)
		}
		for _, raw := range row {
			triple, _ := raw.([]any)
			if len(triple) != 3 {
				t.Fatalf("lookup create triple = %#v; want exactly three parts", raw)
			}
			eid, _ := triple[0].(string)
			attr, _ := triple[1].(string)
			if eid == "" || attr == "" {
				t.Fatalf("lookup create triple = %#v; want string entity and attr", raw)
			}
			triples++
			eids[eid] = true
			if attr == titleAttr {
				title, _ := triple[2].(string)
				titles[eid] = title
			}
		}
	}
	if triples != 4 {
		t.Fatalf("lookup create triples = %d; want exactly four", triples)
	}
	// The absent-id lookup mints a fresh storage id rather than adopting the
	// requested string, so discover it exactly: two entities, the seeded one
	// retitled and one fresh row carrying the lookup title.
	if len(eids) != 2 || !eids[cf003EntityID] {
		t.Fatalf("lookup create entities = %#v; want seeded plus one fresh", eids)
	}
	var freshID string
	for eid := range eids {
		if eid != cf003EntityID {
			freshID = eid
		}
	}
	if _, err := platform.ScanUUIDErr(freshID); err != nil {
		t.Fatalf("lookup fresh id = %q: %v", freshID, err)
	}
	if !reflect.DeepEqual(titles, map[string]string{cf003EntityID: "lookup-two", freshID: "lookup-fresh"}) {
		t.Fatalf("lookup create titles = %#v", titles)
	}
	todos = queryTodos()
	if len(todos) != 2 {
		t.Fatalf("lookup create todos = %#v; want exactly two rows", todos)
	}
	cf003ExactTodo(t, cf003TodoByID(t, todos, cf003EntityID), map[string]any{
		"id": cf003EntityID, "title": "lookup-two",
	})
	cf003ExactTodo(t, cf003TodoByID(t, todos, freshID), map[string]any{
		"id": freshID, "title": "lookup-fresh",
	})

	sse.closeAndAwaitUnauthorized(t, ctx)
}
