package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// TestCF003AssembledDeltaCrossTransportConvergence closes the cross-transport
// remainder of the CF-003 refresh-delta-boundary row (manifest
// refresh-delta-boundary, surface ws.reactive.delta) over the
// production-mounted routes against an owned PostgreSQL fixture. One WS
// delta-refresh member (0.23.0) and one SSE full member share the same
// nodelist query group; a single delta-eligible title update fans out as a
// structural refresh-ok-delta patch on WS and a full refresh-ok envelope on
// SSE at the exact same transaction watermark, and the patch applied to the
// WS baseline converges to the exact SSE whole result. Every leg asserts the
// exact whole result (lengths before elements) with exact float watermark
// compares. This is mounted-route boundary evidence only; no corpus NDJSON
// leg is added or claimed.
func TestCF003AssembledDeltaCrossTransportConvergence(t *testing.T) {
	mux, appID, adminToken := cf003PostgresMux(t)
	app := platform.UUIDToStr(appID)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	eids := []string{
		cf003EntityID,
		"00000000-0000-4000-8000-000000000043",
		"00000000-0000-4000-8000-000000000044",
		"00000000-0000-4000-8000-000000000045",
	}
	steps := make([]any, len(eids))
	wantInitial := make(map[string]string, len(eids))
	wantInitialTree := make([]any, len(eids))
	for i, eid := range eids {
		title := "delta-" + string(rune('a'+i))
		steps[i] = []any{"update", "todos", eid, map[string]any{"title": title}}
		wantInitial[eid] = title
		wantInitialTree[i] = map[string]any{"id": eid, "title": title}
	}
	status, body, _ := cf003Serve(mux, http.MethodPost, "/admin/transact", cf003JSON(t, map[string]any{
		"app-id": app, "steps": steps,
	}), map[string]string{"X-admin-token": adminToken})
	if status != http.StatusOK {
		t.Fatalf("cross-transport delta seed = %d %q", status, body)
	}

	ws := cf003DialWS(t, ctx, server)
	ws.send(t, ctx, map[string]any{
		"op": "init", "app-id": app,
		"versions": map[string]string{"@instantdb/core": "0.23.0"},
	})
	wsInit := ws.nextOp(t, "init-ok")
	ws.sessionID, _ = wsInit["session-id"].(string)
	wsIDAttr, wsTitleAttr := cf003AssertSSEInit(t, wsInit, app)
	ws.send(t, ctx, map[string]any{
		"op": "add-query", "q": map[string]any{"todos": map[string]any{}},
		"client-event-id": "xport-ws-delta",
	})
	wsAck := ws.nextOp(t, "add-query-ok")
	wsTx, _, _ := cf003AssertWSQueryAck(t, wsAck, "xport-ws-delta")
	wsInitial := cf003NodeTitles(t, wsAck["result"], wsIDAttr, wsTitleAttr)
	if len(wsInitial) != len(wantInitial) {
		t.Fatalf("WS baseline titles = %#v; want exactly %#v", wsInitial, wantInitial)
	}
	if !reflect.DeepEqual(wsInitial, wantInitial) {
		t.Fatalf("WS baseline titles = %#v; want exactly %#v", wsInitial, wantInitial)
	}

	sse := cf003OpenSSE(t, ctx, server, app)
	sse.post(t, ctx, map[string]any{"op": "init", "app-id": app})
	sseIDAttr, sseTitleAttr := cf003AssertSSEInit(t, cf003ReadSSE(t, sse.scanner), app)
	if sseIDAttr != wsIDAttr || sseTitleAttr != wsTitleAttr {
		t.Fatalf("attr identity drift: ws=%q/%q sse=%q/%q", wsIDAttr, wsTitleAttr, sseIDAttr, sseTitleAttr)
	}
	sse.post(t, ctx, map[string]any{
		"op": "add-query", "q": map[string]any{"todos": map[string]any{}},
		"client-event-id": "xport-sse-full",
	})
	cf003AssertSSEAddQuery(t, cf003ReadSSE(t, sse.scanner), "xport-sse-full")
	sseSnapshot := cf003ReadSSE(t, sse.scanner)
	cf003AssertSSEOrderedSnapshot(t, sseSnapshot, wantInitialTree)
	sseBaselineTitles := cf003SSETreeTitles(t, sseSnapshot)
	if len(sseBaselineTitles) != len(wsInitial) {
		t.Fatalf("baseline cross-transport titles = sse %#v ws %#v", sseBaselineTitles, wsInitial)
	}
	if !reflect.DeepEqual(sseBaselineTitles, wsInitial) {
		t.Fatalf("baseline cross-transport titles = sse %#v ws %#v", sseBaselineTitles, wsInitial)
	}

	txID := cf003TransactTitle(t, mux, app, adminToken, "delta-mutated")
	if float64(txID) <= wsTx {
		t.Fatalf("cross-transport trigger tx %d did not advance WS baseline %v", txID, wsTx)
	}
	snapTx, ok := sseSnapshot["processed-tx-id"].(float64)
	if !ok || snapTx != wsTx {
		t.Fatalf("baseline watermarks diverged: ws %v sse %#v", wsTx, sseSnapshot["processed-tx-id"])
	}

	wsFrame := ws.nextFrame(t)
	patchEntity := cf003AssertWSDeltaRefresh(t, wsFrame, txID, cf003EntityID)
	sseFrame := cf003ReadSSE(t, sse.scanner)
	wantFinal := map[string]string{
		eids[0]: "delta-mutated", eids[1]: "delta-b", eids[2]: "delta-c", eids[3]: "delta-d",
	}
	cf003AssertSSEOrderedRefresh(t, sseFrame, sseIDAttr, sseTitleAttr, txID, wantFinal)

	wsConverged := make(map[string]string, len(wsInitial))
	for eid, title := range wsInitial {
		wsConverged[eid] = title
	}
	patchedTitle, _ := patchEntity["title"].(string)
	wsConverged[cf003EntityID] = patchedTitle
	if len(wsConverged) != len(wantFinal) {
		t.Fatalf("cross-transport converged titles = %#v; want exactly %#v", wsConverged, wantFinal)
	}
	if !reflect.DeepEqual(wsConverged, wantFinal) {
		t.Fatalf("cross-transport converged titles = %#v; want exactly %#v", wsConverged, wantFinal)
	}
	computations, _ := sseFrame["computations"].([]any)
	if len(computations) != 1 {
		t.Fatalf("cross-transport SSE computations = %#v", sseFrame["computations"])
	}
	entry, _ := computations[0].(map[string]any)
	sseTitles := cf003SSEOrderedRefreshTitles(t, entry["instaql-result"], sseIDAttr, sseTitleAttr)
	if len(sseTitles) != len(wsConverged) {
		t.Fatalf("cross-transport WS/SSE titles = ws %#v sse %#v", wsConverged, sseTitles)
	}
	if !reflect.DeepEqual(sseTitles, wsConverged) {
		t.Fatalf("cross-transport WS/SSE titles = ws %#v sse %#v", wsConverged, sseTitles)
	}

	ws.assertQuiet(t, 100*time.Millisecond)
	sse.closeAndAwaitUnauthorized(t, ctx)
}

func cf003SSETreeTitles(t *testing.T, frame map[string]any) map[string]string {
	t.Helper()
	computations, _ := frame["computations"].([]any)
	if len(computations) != 1 {
		t.Fatalf("SSE tree computations = %#v", frame["computations"])
	}
	entry, _ := computations[0].(map[string]any)
	if len(entry) != 2 || !reflect.DeepEqual(entry["instaql-query"], map[string]any{"todos": map[string]any{}}) {
		t.Fatalf("SSE tree computation = %#v", entry)
	}
	tree, _ := entry["instaql-result"].(map[string]any)
	if len(tree) != 1 {
		t.Fatalf("SSE tree result = %#v", entry["instaql-result"])
	}
	rows, _ := tree["todos"].([]any)
	if len(rows) != 4 {
		t.Fatalf("SSE tree todos = %#v; want exactly four rows", tree["todos"])
	}
	titles := make(map[string]string, len(rows))
	for _, raw := range rows {
		todo, _ := raw.(map[string]any)
		if len(todo) != 2 {
			t.Fatalf("SSE tree todo = %#v; want exactly id/title", raw)
		}
		eid, _ := todo["id"].(string)
		title, _ := todo["title"].(string)
		if eid == "" || title == "" {
			t.Fatalf("SSE tree todo = %#v", todo)
		}
		if _, dup := titles[eid]; dup {
			t.Fatalf("SSE tree duplicate todo for %q", eid)
		}
		titles[eid] = title
	}
	return titles
}
