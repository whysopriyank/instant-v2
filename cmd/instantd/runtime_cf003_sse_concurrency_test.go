package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// TestCF003AssembledSSEQueryConcurrencyOrderedSnapshot closes the CF-003
// SSE/query concurrency remainder (manifest rows query-concurrency-gap and
// refresh-convergence-concurrency) over the production-mounted GET/POST SSE
// path against an owned PostgreSQL fixture. Two SSE subscribers attach the
// same query through a concurrent subscription barrier and both observe one
// identical ordered whole-result snapshot; a single committed admin change
// then converges both to identical refresh frames at the exact transaction
// watermark with the exact final titles. Every leg asserts the exact whole
// result (lengths before elements) so a partial, reordered, or diverged
// delivery cannot hide behind a subset check.
func TestCF003AssembledSSEQueryConcurrencyOrderedSnapshot(t *testing.T) {
	mux, appID, adminToken := cf003PostgresMux(t)
	app := platform.UUIDToStr(appID)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	entityB := "00000000-0000-4000-8000-000000000006"
	entityC := "00000000-0000-4000-8000-000000000007"
	status, body, _ := cf003Serve(mux, http.MethodPost, "/admin/transact", cf003JSON(t, map[string]any{
		"app-id": app,
		"steps": []any{
			[]any{"update", "todos", cf003EntityID, map[string]any{"title": "conc-alpha"}},
			[]any{"update", "todos", entityB, map[string]any{"title": "conc-beta"}},
			[]any{"update", "todos", entityC, map[string]any{"title": "conc-gamma"}},
		},
	}), map[string]string{"X-admin-token": adminToken})
	if status != http.StatusOK {
		t.Fatalf("concurrency seed = %d %q", status, body)
	}

	a := cf003OpenSSE(t, ctx, server, app)
	b := cf003OpenSSE(t, ctx, server, app)
	if a.sessionID == b.sessionID || a.token == b.token {
		t.Fatalf("SSE clients reused credentials: sessions=%q/%q tokens=%q/%q", a.sessionID, b.sessionID, a.token, b.token)
	}
	clients := []*cf003SSEClient{a, b}
	attrs := make([][2]string, len(clients))
	for i, client := range clients {
		client.post(t, ctx, map[string]any{"op": "init", "app-id": app})
		attrs[i][0], attrs[i][1] = cf003AssertSSEInit(t, cf003ReadSSE(t, client.scanner), app)
	}
	if attrs[0] != attrs[1] {
		t.Fatalf("SSE attr identity drift: %q vs %q", attrs[0], attrs[1])
	}
	idAttr, titleAttr := attrs[0][0], attrs[0][1]

	// Concurrent subscription: both add-query POSTs race through a start
	// barrier. Raw HTTP is used here (instead of client.post) because the
	// test helpers fail via t inside the spawned goroutines.
	eventIDs := []string{"conc-query-a", "conc-query-b"}
	type postResult struct {
		status      int
		contentType string
		body        string
		err         error
	}
	results := make([]postResult, len(clients))
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, client := range clients {
		wg.Add(1)
		go func(i int, client *cf003SSEClient) {
			defer wg.Done()
			<-start
			payload, err := json.Marshal(map[string]any{
				"machine_id": "cf003-sse-conc", "app_id": app,
				"session_id": client.sessionID, "sse_token": client.token,
				"messages": []any{map[string]any{
					"op": "add-query", "q": map[string]any{"todos": map[string]any{}},
					"client-event-id": eventIDs[i],
				}},
			})
			if err != nil {
				results[i] = postResult{err: err}
				return
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/runtime/sse", strings.NewReader(string(payload)))
			if err != nil {
				results[i] = postResult{err: err}
				return
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := server.Client().Do(req)
			if err != nil {
				results[i] = postResult{err: err}
				return
			}
			defer func() { _ = resp.Body.Close() }()
			raw, err := io.ReadAll(resp.Body)
			if err != nil {
				results[i] = postResult{err: err}
				return
			}
			results[i] = postResult{status: resp.StatusCode, contentType: resp.Header.Get("Content-Type"), body: string(raw)}
		}(i, client)
	}
	close(start)
	wg.Wait()
	for i, result := range results {
		if result.err != nil {
			t.Fatalf("concurrent add-query %d: %v", i, result.err)
		}
		if result.status != http.StatusOK || result.contentType != "application/json" || result.body != "{}\n" {
			t.Fatalf("concurrent add-query %d = %d %q %q; want 200 {}\\n", i, result.status, result.contentType, result.body)
		}
	}

	// Both subscribers observe one identical ordered whole-result snapshot.
	wantInitial := []any{
		map[string]any{"id": cf003EntityID, "title": "conc-alpha"},
		map[string]any{"id": entityB, "title": "conc-beta"},
		map[string]any{"id": entityC, "title": "conc-gamma"},
	}
	snapshots := make([]map[string]any, len(clients))
	for i, client := range clients {
		cf003AssertSSEAddQuery(t, cf003ReadSSE(t, client.scanner), eventIDs[i])
		snapshots[i] = cf003ReadSSE(t, client.scanner)
		cf003AssertSSEOrderedSnapshot(t, snapshots[i], wantInitial)
	}
	if !reflect.DeepEqual(snapshots[0], snapshots[1]) {
		t.Fatalf("concurrent snapshots diverged: %#v vs %#v", snapshots[0], snapshots[1])
	}

	// One committed change converges both subscribers at its exact watermark.
	status, body, _ = cf003Serve(mux, http.MethodPost, "/admin/transact", cf003JSON(t, map[string]any{
		"app-id": app,
		"steps":  []any{[]any{"update", "todos", entityB, map[string]any{"title": "conc-beta-2"}}},
	}), map[string]string{"X-admin-token": adminToken})
	if status != http.StatusOK {
		t.Fatalf("concurrency trigger = %d %q", status, body)
	}
	var txEnvelope map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &txEnvelope); err != nil || len(txEnvelope) != 1 {
		t.Fatalf("concurrency trigger envelope = %q", body)
	}
	var txID int64
	if raw, ok := txEnvelope["tx-id"]; !ok || json.Unmarshal(raw, &txID) != nil || txID <= 0 {
		t.Fatalf("concurrency trigger tx-id = %q", body)
	}
	wantFinal := map[string]string{
		cf003EntityID: "conc-alpha", entityB: "conc-beta-2", entityC: "conc-gamma",
	}
	refreshes := make([]map[string]any, len(clients))
	for i, client := range clients {
		refreshes[i] = cf003ReadSSE(t, client.scanner)
		cf003AssertSSEOrderedRefresh(t, refreshes[i], idAttr, titleAttr, txID, wantFinal)
	}
	if !reflect.DeepEqual(refreshes[0], refreshes[1]) {
		t.Fatalf("concurrent refreshes diverged at tx %d: %#v vs %#v", txID, refreshes[0], refreshes[1])
	}

	a.closeAndAwaitUnauthorized(t, ctx)
	b.closeAndAwaitUnauthorized(t, ctx)
}

func cf003AssertSSEOrderedSnapshot(t *testing.T, frame map[string]any, want []any) {
	t.Helper()
	if len(want) == 0 {
		t.Fatalf("ordered snapshot oracle must be non-empty")
	}
	txID, ok := frame["processed-tx-id"].(float64)
	if !ok || txID != 0 {
		t.Fatalf("SSE concurrent snapshot tx = %#v; want 0", frame["processed-tx-id"])
	}
	wantFrame := map[string]any{
		"op": "refresh-ok", "processed-tx-id": txID,
		"computations": []any{map[string]any{
			"instaql-query":  map[string]any{"todos": map[string]any{}},
			"instaql-result": map[string]any{"todos": want},
		}},
	}
	if !reflect.DeepEqual(frame, wantFrame) {
		t.Fatalf("SSE concurrent snapshot = %#v; want %#v", frame, wantFrame)
	}
}

func cf003AssertSSEOrderedRefresh(t *testing.T, frame map[string]any, idAttr, titleAttr string, txID int64, want map[string]string) {
	t.Helper()
	if len(want) == 0 {
		t.Fatalf("ordered refresh oracle must be non-empty")
	}
	gotTx, ok := frame["processed-tx-id"].(float64)
	if !ok || gotTx != float64(txID) {
		t.Fatalf("SSE concurrent refresh tx = %#v; want %d", frame["processed-tx-id"], txID)
	}
	if len(frame) != 3 || frame["op"] != "refresh-ok" {
		t.Fatalf("SSE concurrent refresh envelope = %#v; want tx %d", frame, txID)
	}
	computations, _ := frame["computations"].([]any)
	if len(computations) != 1 {
		t.Fatalf("SSE concurrent computations = %#v", frame["computations"])
	}
	entry, _ := computations[0].(map[string]any)
	if len(entry) != 2 || !reflect.DeepEqual(entry["instaql-query"], map[string]any{"todos": map[string]any{}}) {
		t.Fatalf("SSE concurrent computation = %#v", entry)
	}
	if _, hasDelta := entry["delta"]; hasDelta {
		t.Fatalf("SSE concurrent refresh carried delta: %#v", entry)
	}
	titles := cf003SSEOrderedRefreshTitles(t, entry["instaql-result"], idAttr, titleAttr)
	if len(titles) != len(want) {
		t.Fatalf("SSE concurrent titles = %#v; want exactly %#v", titles, want)
	}
	if !reflect.DeepEqual(titles, want) {
		t.Fatalf("SSE concurrent titles = %#v; want exactly %#v", titles, want)
	}
}

func cf003SSEOrderedRefreshTitles(t *testing.T, raw any, idAttr, titleAttr string) map[string]string {
	t.Helper()
	nodes, _ := raw.([]any)
	if len(nodes) == 0 {
		t.Fatalf("SSE concurrent result nodes = %#v", raw)
	}
	titles := map[string]string{}
	idSeen := map[string]bool{}
	for _, rawNode := range nodes {
		node, _ := rawNode.(map[string]any)
		if len(node) != 2 {
			t.Fatalf("SSE concurrent node = %#v", rawNode)
		}
		if children, _ := node["child-nodes"].([]any); len(children) != 0 {
			t.Fatalf("SSE concurrent child nodes = %#v", node["child-nodes"])
		}
		data, _ := node["data"].(map[string]any)
		datalog, _ := data["datalog-result"].(map[string]any)
		rows, _ := datalog["join-rows"].([]any)
		if len(rows) == 0 {
			t.Fatalf("SSE concurrent join rows = %#v", datalog["join-rows"])
		}
		for _, rawRow := range rows {
			triples, _ := rawRow.([]any)
			if len(triples) == 0 {
				t.Fatalf("SSE concurrent row = %#v", rawRow)
			}
			for _, rawTriple := range triples {
				triple, _ := rawTriple.([]any)
				if len(triple) != 3 {
					t.Fatalf("SSE concurrent triple = %#v", rawTriple)
				}
				eid, _ := triple[0].(string)
				attr, _ := triple[1].(string)
				value, _ := triple[2].(string)
				if eid == "" || value == "" {
					t.Fatalf("SSE concurrent triple = %#v", triple)
				}
				switch attr {
				case idAttr:
					if eid != value {
						t.Fatalf("SSE concurrent id triple = %#v", triple)
					}
					if idSeen[eid] {
						t.Fatalf("SSE concurrent duplicate id triple for %q", eid)
					}
					idSeen[eid] = true
				case titleAttr:
					if _, dup := titles[eid]; dup {
						t.Fatalf("SSE concurrent duplicate title triple for %q", eid)
					}
					titles[eid] = value
				default:
					t.Fatalf("SSE concurrent triple has unexpected attr: %#v", triple)
				}
			}
		}
	}
	if len(idSeen) != len(titles) {
		t.Fatalf("SSE concurrent id/title coverage = ids %d titles %d", len(idSeen), len(titles))
	}
	for eid, title := range titles {
		if !idSeen[eid] || title == "" {
			t.Fatalf("SSE concurrent entity %q incomplete: id=%v title=%q", eid, idSeen[eid], title)
		}
	}
	return titles
}
