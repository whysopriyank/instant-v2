package main

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// TestCF003AssembledRuntimeTransactMatrix closes the CF-003 runtime-transact
// remainder over the production-mounted POST /runtime/transact path against an
// owned PostgreSQL fixture. The runtime plane speaks low-level tx-steps (not
// the admin high-level steps spelling) under default-open rules: one positive
// low-level write converges to the exact whole query result, malformed and
// admin-only batches fail with stable envelopes without mutating state, bad
// app/json inputs fail with stable envelopes, and a high-level admin op rides
// the forward-compat no-op without writing. Every leg asserts the exact whole
// result so a partial write cannot hide behind a subset check.
func TestCF003AssembledRuntimeTransactMatrix(t *testing.T) {
	mux, appID, adminToken := cf003PostgresMux(t)
	app := platform.UUIDToStr(appID)

	status, body, _ := cf003Serve(mux, http.MethodPost, "/admin/transact", cf003JSON(t, map[string]any{
		"app-id": app, "steps": []any{[]any{"update", "todos", cf003EntityID, map[string]any{"title": "runtime-baseline"}}},
	}), map[string]string{"X-admin-token": adminToken})
	if status != http.StatusOK {
		t.Fatalf("runtime matrix seed = %d %q", status, body)
	}
	idAttr, titleAttr := cf003TransactionAttrs(t, mux, app, adminToken)

	runtimeTransact := func(payload string) (int, string, string) {
		t.Helper()
		status, respBody, headers := cf003Serve(mux, http.MethodPost, "/runtime/transact", payload, nil)
		return status, respBody, headers.Get("Content-Type")
	}
	txIDOf := func(body string) int64 {
		t.Helper()
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal([]byte(body), &envelope); err != nil || len(envelope) != 1 {
			t.Fatalf("runtime transact envelope = %q: %v", body, err)
		}
		var txID int64
		raw, ok := envelope["tx-id"]
		if !ok || json.Unmarshal(raw, &txID) != nil || txID <= 0 {
			t.Fatalf("runtime transact tx-id = %q", body)
		}
		return txID
	}
	queryTodos := func() []any {
		t.Helper()
		status, respBody, _ := cf003Serve(mux, http.MethodPost, "/runtime/framework/query",
			`{"query":{"todos":{}}}`, map[string]string{"app-id": app})
		if status != http.StatusOK {
			t.Fatalf("runtime matrix query = %d %q", status, respBody)
		}
		var envelope struct {
			Data struct {
				Todos []any `json:"todos"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(respBody), &envelope); err != nil {
			t.Fatalf("runtime matrix query body = %q: %v", respBody, err)
		}
		return envelope.Data.Todos
	}
	exactTitle := func(want string) {
		t.Helper()
		cf003ExactTodo(t, cf003TodoByID(t, queryTodos(), cf003EntityID), map[string]any{
			"id": cf003EntityID, "title": want,
		})
	}

	exactTitle("runtime-baseline")

	// Positive low-level write converges exactly.
	status, body = func() (int, string) {
		s, b, ct := runtimeTransact(cf003JSON(t, map[string]any{
			"app-id": app, "tx-steps": []any{[]any{"add-triple", cf003EntityID, titleAttr, "runtime-one"}},
		}))
		if ct != "application/json" {
			t.Fatalf("runtime positive content-type = %q; want application/json", ct)
		}
		return s, b
	}()
	if status != http.StatusOK {
		t.Fatalf("runtime positive = %d %q", status, body)
	}
	firstTx := txIDOf(body)
	exactTitle("runtime-one")

	// Bad arity fails without mutating.
	status, body, ct := runtimeTransact(cf003JSON(t, map[string]any{
		"app-id": app, "tx-steps": []any{[]any{"add-triple", cf003EntityID}},
	}))
	if status != http.StatusBadRequest || ct != "application/json" ||
		body != "{\"error\":\"transact: add-triple: want 3 or 4 args, got 1\"}\n" {
		t.Fatalf("runtime arity = %d %q %q; want 400 exact denial", status, ct, body)
	}
	exactTitle("runtime-one")

	// Unknown attr fails without mutating.
	status, body, ct = runtimeTransact(cf003JSON(t, map[string]any{
		"app-id": app, "tx-steps": []any{[]any{"add-triple", cf003EntityID, "00000000-0000-4000-8000-000000000099", "x"}},
	}))
	if status != http.StatusForbidden || ct != "application/json" ||
		body != "{\"error\":\"unknown attr 00000000-0000-4000-8000-000000000099\"}\n" {
		t.Fatalf("runtime unknown attr = %d %q %q; want 403 exact denial", status, ct, body)
	}
	exactTitle("runtime-one")

	// Admin-only attr metadata fails without mutating.
	status, body, ct = runtimeTransact(cf003JSON(t, map[string]any{
		"app-id": app, "tx-steps": []any{[]any{"delete-attr", map[string]any{"id": idAttr}}},
	}))
	if status != http.StatusForbidden || ct != "application/json" ||
		body != "{\"error\":\"transact: attrs.delete denied (admin only in Phase 2)\"}\n" {
		t.Fatalf("runtime delete-attr = %d %q %q; want 403 exact denial", status, ct, body)
	}
	exactTitle("runtime-one")

	// Malformed app id never reaches the transactor.
	status, body, ct = runtimeTransact(cf003JSON(t, map[string]any{
		"app-id": "not-a-uuid", "tx-steps": []any{},
	}))
	if status != http.StatusNotFound || body != "{\"error\":\"unknown app\"}\n" {
		t.Fatalf("runtime bad app = %d %q; want 404 exact denial", status, body)
	}
	if ct != "text/plain; charset=utf-8" {
		t.Fatalf("runtime bad app content-type = %q; want text/plain", ct)
	}
	exactTitle("runtime-one")

	// Malformed JSON fails before any work.
	status, body, ct = runtimeTransact(`{`)
	if status != http.StatusBadRequest || body != "{\"error\":\"bad json\"}\n" {
		t.Fatalf("runtime bad json = %d %q; want 400 exact denial", status, body)
	}
	if ct != "text/plain; charset=utf-8" {
		t.Fatalf("runtime bad json content-type = %q; want text/plain", ct)
	}
	exactTitle("runtime-one")

	// High-level admin spelling rides the forward-compat no-op: it commits a
	// transaction id but writes nothing, so the exact state is unchanged.
	status, body, ct = runtimeTransact(cf003JSON(t, map[string]any{
		"app-id": app, "tx-steps": []any{[]any{"update", "todos", cf003EntityID, map[string]any{"title": "should-not-land"}}},
	}))
	if status != http.StatusOK || ct != "application/json" {
		t.Fatalf("runtime high-level no-op = %d %q %q; want 200", status, ct, body)
	}
	noopTx := txIDOf(body)
	if noopTx <= firstTx {
		t.Fatalf("runtime high-level tx %d did not advance positive tx %d", noopTx, firstTx)
	}
	exactTitle("runtime-one")
}
