package sync_test

// Phase-6 hardening tests for workstream 6A: delta-refresh negotiation
// (≥0.23.0), per-app subscription caps, and v1-conformant refresh result
// shaping (docs/03-protocol.md §5 + frozen client extractTriples parity).

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	syncpkg "github.com/instant-v2/instant-v2/internal/sync"
)

// extractTriplesMirror is a Go mirror of the frozen TS client's
// model/instaqlResult.js extractTriples: it walks the node list and flattens
// data['datalog-result']['join-rows'] into [eid, attrId, value] triples.
// It fails on anything the frozen SDK could not consume.
func extractTriplesMirror(t *testing.T, raw json.RawMessage) [][3]any {
	t.Helper()
	var nodes []struct {
		Data struct {
			K             string `json:"k"`
			DatalogResult struct {
				JoinRows [][][]any `json:"join-rows"`
			} `json:"datalog-result"`
		} `json:"data"`
		ChildNodes []json.RawMessage `json:"child-nodes"`
	}
	if err := json.Unmarshal(raw, &nodes); err != nil {
		t.Fatalf("instaql-result is not a node list (frozen SDK would break): %v", err)
	}
	if len(nodes) == 0 {
		t.Fatal("empty node list")
	}
	var out [][3]any
	for _, n := range nodes {
		for _, row := range n.Data.DatalogResult.JoinRows {
			for _, triple := range row {
				if len(triple) < 3 {
					t.Fatalf("triple shorter than [e,a,v]: %v", triple)
				}
				out = append(out, [3]any{triple[0], triple[1], triple[2]})
			}
		}
	}
	return out
}

func dialInit(t *testing.T, ctx context.Context, env *wsEnv, version string) (*websocket.Conn, chan map[string]any) {
	t.Helper()
	conn, frames := env.dial(t, ctx)
	sendFrame(t, conn, ctx, map[string]any{
		"op":       "init",
		"app-id":   env.AppID,
		"versions": map[string]string{"@instantdb/core": version},
	})
	expectOp(t, frames, "init-ok")
	return conn, frames
}

func addQueryTodos(t *testing.T, ctx context.Context, conn *websocket.Conn, frames chan map[string]any) {
	t.Helper()
	sendFrame(t, conn, ctx, map[string]any{
		"op": "add-query", "q": map[string]any{"todos": map[string]any{}},
		"client-event-id": "q1",
	})
	expectOp(t, frames, "add-query-ok")
}

func transactTodo(t *testing.T, ctx context.Context, conn *websocket.Conn,
	frames chan map[string]any, op string, eid string, attrID string, value any) {
	t.Helper()
	steps := []any{[]any{op, eid, attrID, value}}
	sendFrame(t, conn, ctx, map[string]any{
		"op": "transact", "tx-steps": steps, "client-event-id": "t-" + eid,
	})
	expectOp(t, frames, "transact-ok")
}

func computationsOf(t *testing.T, f map[string]any) map[string]any {
	t.Helper()
	comps, ok := f["computations"].([]any)
	if !ok || len(comps) != 1 {
		t.Fatalf("expected exactly one computation entry, got %v", f["computations"])
	}
	entry, _ := comps[0].(map[string]any)
	return entry
}

// TestDeltaRefreshNegotiation drives two live WebSocket sessions against one
// app. The pre-0.23.0 session must receive refresh-ok full envelopes with
// exactly today's frozen-SDK surface; the 0.23.0 session must receive a
// structural patch frame on single-entity mutation.
func TestDeltaRefreshNegotiation(t *testing.T) {
	env := newWSEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	titleAttr := uuidStr(env.IDs.title)

	// Old session seeds four todos BEFORE subscribing so both sessions'
	// baselines hold real data and a later single-row mutation stays under
	// the >50% fallback threshold.
	oldConn, oldFrames := dialInit(t, ctx, env, "0.22.9")
	eids := make([]string, 4)
	for i := range eids {
		eids[i] = fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
		transactTodo(t, ctx, oldConn, oldFrames, "add-triple", eids[i], titleAttr, fmt.Sprintf("title-%d", i))
	}

	addQueryTodos(t, ctx, oldConn, oldFrames)
	initOld := expectOp(t, oldFrames, "refresh-ok") // initial snapshot (full)

	newConn, newFrames := dialInit(t, ctx, env, "0.23.0")
	addQueryTodos(t, ctx, newConn, newFrames)
	initNew := expectOp(t, newFrames, "refresh-ok") // initial snapshot (full)

	// Both initial snapshots are full envelopes with the exact legacy shape.
	for name, snap := range map[string]map[string]any{
		"old": initOld,
		"new": initNew,
	} {
		// Snapshots echo client-event-id; async refreshes don't.
		wantKeys := map[string]bool{
			"op": true, "computations": true,
			"processed-tx-id": true, "client-event-id": true,
		}
		if len(snap) != len(wantKeys) {
			t.Fatalf("%s snapshot keys drifted: %v", name, snap)
		}
		for k := range wantKeys {
			if _, has := snap[k]; !has {
				t.Fatalf("%s snapshot missing %s", name, k)
			}
		}
		entry := computationsOf(t, snap)
		if _, hasDelta := entry["delta"]; hasDelta {
			t.Fatalf("%s snapshot must not carry a delta", name)
		}
		result, _ := json.Marshal(entry["instaql-result"])
		extractTriplesMirror(t, result) // frozen SDK can consume it
	}

	// Single-entity mutation: rewrite todo #2's title.
	transactTodo(t, ctx, oldConn, oldFrames, "add-triple", eids[2], titleAttr, "mutated!")

	// OLD session: byte-compatible refresh-ok full envelope.
	full := expectOp(t, oldFrames, "refresh-ok")
	if op, _ := full["op"].(string); op != "refresh-ok" {
		t.Fatalf("old session op = %q", op)
	}
	wantKeys := map[string]bool{"op": true, "computations": true, "processed-tx-id": true}
	if len(full) != len(wantKeys) {
		t.Fatalf("old-session refresh-ok keys drifted: %v", full)
	}
	entry := computationsOf(t, full)
	if _, hasDelta := entry["delta"]; hasDelta {
		t.Fatal("old session must never receive a delta")
	}
	result, _ := json.Marshal(entry["instaql-result"])
	triples := extractTriplesMirror(t, result)
	gotTitles := map[string]string{}
	for _, tr := range triples {
		if a, _ := tr[1].(string); a == titleAttr {
			eid, _ := tr[0].(string)
			v, _ := tr[2].(string)
			gotTitles[eid] = v
		}
	}
	wantTitles := map[string]string{
		eids[0]: "title-0", eids[1]: "title-1",
		eids[2]: "mutated!", eids[3]: "title-3",
	}
	for eid, want := range wantTitles {
		if gotTitles[eid] != want {
			t.Fatalf("extractTriples lost seeded triple %s: got %q want %q", eid, gotTitles[eid], want)
		}
	}

	// NEW session: structural patch on the same mutation.
	delta := expectOp(t, newFrames, "refresh-ok-delta")
	dentry := computationsOf(t, delta)
	if _, hasFull := dentry["instaql-result"]; hasFull {
		t.Fatal("delta frame must not carry the full envelope")
	}
	rawDelta, err := json.Marshal(dentry["delta"])
	if err != nil {
		t.Fatal(err)
	}
	var patch struct {
		Ops []struct {
			Op     string          `json:"op"`
			Etype  string          `json:"etype"`
			ID     string          `json:"id"`
			Entity json.RawMessage `json:"entity"`
		} `json:"ops"`
	}
	if err := json.Unmarshal(rawDelta, &patch); err != nil {
		t.Fatalf("bad patch payload: %v (%s)", err, rawDelta)
	}
	if len(patch.Ops) != 1 ||
		patch.Ops[0].Op != "update" ||
		patch.Ops[0].Etype != "todos" ||
		patch.Ops[0].ID != eids[2] {
		t.Fatalf("unexpected patch ops: %s", rawDelta)
	}
	var ent map[string]any
	if err := json.Unmarshal(patch.Ops[0].Entity, &ent); err != nil {
		t.Fatal(err)
	}
	// fakeStack's seeded attr label is "text".
	if ent["text"] != "mutated!" || ent["id"] != eids[2] {
		t.Fatalf("patch entity wrong: %v", ent)
	}
}

func TestSSEInitQueryTreeShape(t *testing.T) {
	env := newWSEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET",
		env.Server.URL+"/runtime/sse?app_id="+env.AppID, nil)
	resp, err := env.Server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	scanner := bufio.NewScanner(resp.Body)
	readEvent := func() map[string]any {
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				var f map[string]any
				_ = json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &f)
				return f
			}
		}
		t.Fatal("stream ended")
		return nil
	}

	hello := readEvent()
	sessID, _ := hello["session-id"].(string)
	token, _ := hello["sse-token"].(string)
	sendMsgs := func(msgs ...map[string]any) {
		b, _ := json.Marshal(map[string]any{
			"machine_id": "m-delta", "app_id": env.AppID,
			"session_id": sessID, "sse_token": token,
			"messages": msgs,
		})
		r2, err := http.NewRequestWithContext(ctx, "POST",
			env.Server.URL+"/runtime/sse", strings.NewReader(string(b)))
		if err != nil {
			t.Fatal(err)
		}
		r2.Header.Set("Content-Type", "application/json")
		resp2, err := env.Server.Client().Do(r2)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp2.Body.Close()
		if resp2.StatusCode != 200 {
			t.Fatalf("post status %d", resp2.StatusCode)
		}
	}

	sendMsgs(map[string]any{"op": "init", "app-id": env.AppID})
	if f := readEvent(); f["op"] != "init-ok" {
		t.Fatalf("expected init-ok, got %v", f)
	}
	sendMsgs(map[string]any{
		"op": "add-query", "q": map[string]any{"todos": map[string]any{}},
	})
	if f := readEvent(); f["op"] != "add-query-ok" {
		t.Fatalf("expected add-query-ok, got %v", f)
	}
	snap := readEvent() // refresh-ok carrying the :tree-shaped result
	entry := computationsOf(t, snap)
	tree, hasTree := entry["instaql-result"].(map[string]any)
	if !hasTree {
		t.Fatalf("SSE init-query must return an object tree, got %T", entry["instaql-result"])
	}
	if _, wrapped := tree["data"]; wrapped {
		t.Fatalf(`SSE init-query tree must NOT carry a "data" wrapper: %v`, tree)
	}
	if _, hasTodos := tree["todos"]; !hasTodos {
		t.Fatalf("tree missing todos level: %v", tree)
	}
}

// TestSubscriptionCap enforces reactive.Store.MaxSubsPerApp at the protocol
// layer: breaching add-query gets a typed error frame and the connection is
// torn down.
func TestSubscriptionCap(t *testing.T) {
	env := newWSEnv(t)
	store := env.Mgr.Deps.Store
	store.MaxSubsPerApp = 1 // 0 would be unlimited

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	connA, framesA := dialInit(t, ctx, env, "0.23.0")
	addQueryTodos(t, ctx, connA, framesA)
	expectOp(t, framesA, "refresh-ok")

	// Second session breaches the per-app cap.
	connB, framesB := dialInit(t, ctx, env, "0.23.0")
	sendFrame(t, connB, ctx, map[string]any{
		"op": "add-query", "q": map[string]any{"todos": map[string]any{}},
	}) // server replies with the cap error, never add-query-ok
	errFrame := expectOp(t, framesB, "error")
	status, _ := errFrame["status"].(float64)
	typ, _ := errFrame["type"].(string)
	if int(status) != 429 || typ != "subscription-limit" {
		t.Fatalf("expected 429 subscription-limit, got %v", errFrame)
	}
	// ...and the transport closes the connection right after the frame.
	select {
	case _, open := <-framesB:
		if open {
			t.Fatal("expected stream end after subscription-limit")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("connection was not closed after subscription-limit")
	}

	// Tearing down session A's connection releases its subscription,
	// freeing per-app capacity for a fresh session.
	_ = connA.Close(websocket.StatusNormalClosure, "")
	time.Sleep(300 * time.Millisecond) // let the server reap the conn
	connC, framesC := dialInit(t, ctx, env, "0.23.0")
	addQueryTodos(t, ctx, connC, framesC)
	expectOp(t, framesC, "refresh-ok")
}

var _ = syncpkg.ErrCloseSession
