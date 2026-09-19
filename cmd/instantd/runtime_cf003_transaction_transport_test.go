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

// TestCF003AssembledTransactionTransportMatrix closes the CF-003
// transaction WS/SSE-transport remainder over the production-mounted routes:
// the low-level "transact" op through the mounted WS session path and the
// mounted SSE POST path. Each transport proves one positive low-level write
// converging to the exact whole HTTP query result plus one exact validation
// denial that mutates nothing. The WS path additionally proves the
// same-batch cardinality-one boundary (final value wins exactly once).
// Every leg asserts the exact whole result so a partial write cannot hide
// behind a subset check.
func TestCF003AssembledTransactionTransportMatrix(t *testing.T) {
	mux, appID, adminToken := cf003PostgresMux(t)
	app := platform.UUIDToStr(appID)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	// Open the WS session before seeding: init-ok carries the live catalog
	// attrs, which must be empty at connect time (cf003OpenWS pins that).
	ws := cf003OpenWS(t, ctx, server, app)
	sseGet := cf003OpenSSE(t, ctx, server, app)

	status, body, _ := cf003Serve(mux, http.MethodPost, "/admin/transact", cf003JSON(t, map[string]any{
		"app-id": app, "steps": []any{[]any{"update", "todos", cf003EntityID, map[string]any{"title": "transport-baseline"}}},
	}), map[string]string{"X-admin-token": adminToken})
	if status != http.StatusOK {
		t.Fatalf("transport matrix seed = %d %q", status, body)
	}
	_, titleAttr := cf003TransactionAttrs(t, mux, app, adminToken)

	queryTodos := func() []any {
		t.Helper()
		status, respBody, _ := cf003Serve(mux, http.MethodPost, "/runtime/framework/query",
			`{"query":{"todos":{}}}`, map[string]string{"app-id": app})
		if status != http.StatusOK {
			t.Fatalf("transport matrix query = %d %q", status, respBody)
		}
		var envelope struct {
			Data struct {
				Todos []any `json:"todos"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(respBody), &envelope); err != nil {
			t.Fatalf("transport matrix query body = %q: %v", respBody, err)
		}
		if envelope.Data.Todos == nil {
			envelope.Data.Todos = []any{}
		}
		return envelope.Data.Todos
	}
	exactTitle := func(want string) {
		t.Helper()
		todos := queryTodos()
		if len(todos) != 1 {
			t.Fatalf("transport matrix todos = %#v; want exactly one row", todos)
		}
		cf003ExactTodo(t, cf003TodoByID(t, todos, cf003EntityID), map[string]any{
			"id": cf003EntityID, "title": want,
		})
	}
	exactTransactOK := func(frame map[string]any, eventID string) float64 {
		t.Helper()
		txID, ok := frame["tx-id"].(float64)
		if !ok || txID <= 0 {
			t.Fatalf("transport transact-ok tx id = %#v", frame["tx-id"])
		}
		want := map[string]any{"op": "transact-ok", "tx-id": txID, "client-event-id": eventID}
		if !reflect.DeepEqual(frame, want) {
			t.Fatalf("transport transact-ok = %#v; want %#v", frame, want)
		}
		return txID
	}
	exactDenial := func(frame map[string]any) {
		t.Helper()
		want := map[string]any{
			"op": "error", "status": float64(400), "type": "tx-step-validation",
			"message": "transact: add-triple: want 3 or 4 args, got 1",
		}
		if !reflect.DeepEqual(frame, want) {
			t.Fatalf("transport denial = %#v; want %#v", frame, want)
		}
	}

	exactTitle("transport-baseline")

	// WS positive low-level write converges exactly.
	ws.send(t, ctx, map[string]any{
		"op": "transact", "client-event-id": "ws-t1",
		"tx-steps": []any{[]any{"add-triple", cf003EntityID, titleAttr, "ws-one"}},
	})
	wsTx := exactTransactOK(ws.nextOp(t, "transact-ok"), "ws-t1")
	exactTitle("ws-one")

	// WS validation failure mutates nothing.
	ws.send(t, ctx, map[string]any{
		"op": "transact", "client-event-id": "ws-bad",
		"tx-steps": []any{[]any{"add-triple", cf003EntityID}},
	})
	exactDenial(ws.nextOp(t, "error"))
	exactTitle("ws-one")

	// WS same-batch cardinality-one boundary: the final value wins exactly
	// once through the mounted WS session path.
	ws.send(t, ctx, map[string]any{
		"op": "transact", "client-event-id": "ws-card",
		"tx-steps": []any{
			[]any{"add-triple", cf003EntityID, titleAttr, "ws-card-first"},
			[]any{"add-triple", cf003EntityID, titleAttr, "ws-card-last"},
		},
	})
	cardTx := exactTransactOK(ws.nextOp(t, "transact-ok"), "ws-card")
	if cardTx <= wsTx {
		t.Fatalf("WS cardinality tx %v did not advance past WS tx %v", cardTx, wsTx)
	}
	wsTx = cardTx
	exactTitle("ws-card-last")

	// SSE init after seeding observes the exact live catalog identity.
	sseGet.post(t, ctx, map[string]any{"op": "init", "app-id": app})
	sseIDAttr, sseTitleAttr := cf003AssertSSEInit(t, cf003ReadSSE(t, sseGet.scanner), app)
	if sseTitleAttr != titleAttr || sseIDAttr == "" {
		t.Fatalf("SSE attr identity drift: id=%q title=%q; want title=%q", sseIDAttr, sseTitleAttr, titleAttr)
	}

	// SSE positive low-level write converges exactly past the WS watermark.
	sseGet.post(t, ctx, map[string]any{
		"op": "transact", "client-event-id": "sse-t1",
		"tx-steps": []any{[]any{"add-triple", cf003EntityID, titleAttr, "sse-one"}},
	})
	sseTx := exactTransactOK(cf003ReadSSE(t, sseGet.scanner), "sse-t1")
	if sseTx <= wsTx {
		t.Fatalf("SSE tx %v did not advance past WS tx %v", sseTx, wsTx)
	}
	exactTitle("sse-one")

	// SSE validation failure mutates nothing.
	sseGet.post(t, ctx, map[string]any{
		"op": "transact", "client-event-id": "sse-bad",
		"tx-steps": []any{[]any{"add-triple", cf003EntityID}},
	})
	exactDenial(cf003ReadSSE(t, sseGet.scanner))
	exactTitle("sse-one")

	sseGet.closeAndAwaitUnauthorized(t, ctx)
}
