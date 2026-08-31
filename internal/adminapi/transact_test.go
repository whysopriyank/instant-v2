package adminapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/reactive"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/transact"
)

func TestAdminCommitNotificationContract(t *testing.T) {
	h, db, appID, token, cleanup := env(t)
	defer cleanup()
	attrID, _ := seedAttr(t, h, db, appID)
	attrStr := uuidStr(attrID)
	entity := newUUID()
	entityStr := uuidStr(entity)
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })
	requestTrace := trace.TraceID{15: 1}
	propagatedTrace := trace.TraceID{15: 2}
	requestCtx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: requestTrace, SpanID: trace.SpanID{7: 1},
	}))

	for _, tc := range []struct {
		name                  string
		changes, fallback     bool
		steps                 [][]any
		wantChanges, wantAttr bool
		wantFallback          bool
		status                int
	}{
		{"prefer changes", true, true, [][]any{{"add-triple", entityStr, attrStr, "persisted"}}, true, true, false, 200},
		{"only changes", true, false, [][]any{{"add-triple", entityStr, attrStr, "persisted"}}, true, true, false, 200},
		{"only fallback", false, true, [][]any{{"add-triple", entityStr, attrStr, "persisted"}}, false, true, true, 200},
		{"no callbacks", false, false, [][]any{{"add-triple", entityStr, attrStr, "persisted"}}, false, true, false, 200},
		{"empty fallback", true, true, [][]any{}, false, false, true, 200},
		{"empty changes only", true, false, [][]any{}, false, false, false, 200},
		{"mixed fallback", true, true, [][]any{{"add-triple", entityStr, attrStr, "persisted"}, {"delete-entity", uuidStr(newUUID())}}, false, true, true, 200},
		{"unresolved changes only", true, false, [][]any{{"delete-entity", uuidStr(newUUID())}}, false, false, false, 200},
		{"unknown operation fallback", true, true, [][]any{{"invalid-operation"}}, false, false, true, 200},
		{"invalid triple never notifies", true, true, [][]any{{"add-triple", entityStr}}, false, false, false, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changeCalls, fallbackCalls := 0, 0
			var notifiedTxID int64
			checkCommit := func(ctx context.Context, gotApp [16]byte, txID int64, attrsChanged bool, wantTrace trace.TraceID) {
				t.Helper()
				if gotApp != appID || txID <= 0 || attrsChanged {
					t.Fatalf("callback app/tx/attrs: %x %d %v", gotApp, txID, attrsChanged)
				}
				if got := trace.SpanContextFromContext(ctx).TraceID(); got != wantTrace {
					t.Fatalf("callback trace = %s; want %s", got, wantTrace)
				}
				notifiedTxID = txID
				if tc.wantAttr {
					rows, err := db.FetchTriples(context.Background(), appID, storage.FetchFilter{EntityIDs: [][16]byte{entity}})
					if err != nil || len(rows) != 1 || rows[0].Triple.V != "persisted" {
						t.Fatalf("callback must follow commit: %+v %v", rows, err)
					}
				}
			}
			h.OnCommitChanges = nil
			h.OnCommit = nil
			if tc.changes {
				h.OnCommitChanges = func(ctx context.Context, gotApp [16]byte, changes []reactive.Change, txID int64, attrsChanged bool) {
					changeCalls++
					checkCommit(ctx, gotApp, txID, attrsChanged, propagatedTrace)
					want := []reactive.Change{{Etype: "todos", EntityID: entityStr, AttrIDs: []string{attrStr}}}
					if !reflect.DeepEqual(changes, want) {
						t.Fatalf("changes = %#v; want %#v", changes, want)
					}
				}
			}
			if tc.fallback {
				h.OnCommit = func(ctx context.Context, gotApp [16]byte, attrs []string, txID int64, attrsChanged bool) {
					fallbackCalls++
					checkCommit(ctx, gotApp, txID, attrsChanged, requestTrace)
					var want []string
					if tc.wantAttr {
						want = []string{attrStr}
					}
					if !reflect.DeepEqual(attrs, want) {
						t.Fatalf("attrs = %#v; want %#v", attrs, want)
					}
				}
			}
			body, err := json.Marshal(bodyApp(appID, map[string]any{"steps": tc.steps}))
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, "/admin/transact", bytes.NewReader(body)).WithContext(requestCtx)
			req.Header.Set("X-admin-token", token)
			req.Header.Set("traceparent", "00-"+propagatedTrace.String()+"-0000000000000002-01")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.status {
				t.Fatalf("response: %d %s", rec.Code, rec.Body.String())
			}
			wantChanges, wantFallback := 0, 0
			if tc.wantChanges {
				wantChanges = 1
			}
			if tc.wantFallback {
				wantFallback = 1
			}
			if changeCalls != wantChanges || fallbackCalls != wantFallback {
				t.Fatalf("callback counts: changes=%d fallback=%d; want %d/%d", changeCalls, fallbackCalls, wantChanges, wantFallback)
			}
			if notifiedTxID != 0 {
				var resp struct {
					TxID int64 `json:"tx-id"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || resp.TxID != notifiedTxID {
					t.Fatalf("response must name notified tx %d: %s (%v)", notifiedTxID, rec.Body.String(), err)
				}
			}
		})
	}
}

// TestAdminBypass proves the admin plane ignores permission rules: the same
// add-triple steps that a non-admin transact.Transact call denies under a
// deny-everything rule doc succeed through POST /admin/transact.
func TestAdminBypass(t *testing.T) {
	h, db, appID, token, cleanup := env(t)
	defer cleanup()
	ctx := context.Background()

	attrID, cat := seedAttr(t, h, db, appID)
	entity := newUUID()
	steps := [][]any{{"add-triple", uuidStr(entity), uuidStr(attrID), "hello"}}

	code, resp := do(t, h, "POST", "/admin/transact", authHeaders(token),
		bodyApp(appID, map[string]any{"steps": steps, "rules": json.RawMessage(denyAllRules)}))
	if code != 200 {
		t.Fatalf("admin transact: %d %v", code, resp)
	}
	txID, ok := resp["tx-id"].(float64)
	if !ok || txID <= 0 {
		t.Fatalf("expected positive tx-id, got %v", resp["tx-id"])
	}

	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{entity}})
	if err != nil || len(rows) != 1 || rows[0].Triple.V != "hello" {
		t.Fatalf("triple not persisted: %v %v %+v", err, len(rows), rows)
	}

	// The same steps under the same rules are denied for a non-admin caller
	// (transact.Options{}), proving admin truly bypassed the CEL gate.
	doc, err := perms.ParseRuleDoc([]byte(denyAllRules))
	if err != nil {
		t.Fatal(err)
	}
	rawSteps, _ := json.Marshal(steps)
	var raws []json.RawMessage
	if err := json.Unmarshal(rawSteps, &raws); err != nil {
		t.Fatal(err)
	}
	parsed, err := transact.ParseSteps(raws)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transact.Transact(ctx, db, cat, appID, parsed, transact.Options{}, doc); err == nil {
		t.Fatal("non-admin transact should be denied by the deny-all rule doc")
	}
}
