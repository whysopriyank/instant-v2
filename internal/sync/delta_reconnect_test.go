package sync_test

// RT-002 task E: delta-enabled reconnect evidence.
//
// Two live WebSocket sessions advertising @instantdb/core 0.23.0 share one
// query group. A single-entity mutation is certified as a structural patch
// (refresh-ok-delta) on BOTH members, proving the delta path is eligible.
// One member then drops (client close); while it is gone a NEW
// delta-eligible single-entity mutation commits (the missed tx) and the
// surviving sibling receives it as refresh-ok-delta. A fresh delta session
// rejoins while the sibling holds the group: it must converge via FULL
// replay (add-query-ok with the complete result — full replay is SUFFICIENT
// for delta clients per DEC-001), carrying the exact post-missed state and
// the missed tx's watermark. A later transact from the rejoiner must then
// reach both members (continued valid delivery).
//
// This is NOT steady-state delta convergence: the rejoiner's only path to
// the missed generation is the reconnect full replay, and the test asserts
// the termination (drop) plus missed-tx ordering explicitly.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// awaitRefreshEither returns the next refresh-ok or refresh-ok-delta frame
// within the window, or fails the test. Both envelopes are valid continued
// delivery for a delta-capable member.
func awaitRefreshEither(t *testing.T, frames chan map[string]any, window time.Duration) map[string]any {
	t.Helper()
	deadline := time.After(window)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for refresh-ok / refresh-ok-delta")
			return nil
		case f, ok := <-frames:
			if !ok {
				t.Fatal("connection closed waiting for refresh-ok / refresh-ok-delta")
				return nil
			}
			if got, _ := f["op"].(string); got == "refresh-ok" || got == "refresh-ok-delta" {
				return f
			}
		}
	}
}

// requirePatchUpdate asserts frame is a single-op structural patch updating
// one entity's title to wantText.
func requirePatchUpdate(t *testing.T, frame map[string]any, etype, id, wantText string) {
	t.Helper()
	if op, _ := frame["op"].(string); op != "refresh-ok-delta" {
		t.Fatalf("expected refresh-ok-delta, got op %q: %v", op, frame["op"])
	}
	dentry := computationsOf(t, frame)
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
		patch.Ops[0].Etype != etype ||
		patch.Ops[0].ID != id {
		t.Fatalf("unexpected patch ops: %s", rawDelta)
	}
	var ent map[string]any
	if err := json.Unmarshal(patch.Ops[0].Entity, &ent); err != nil {
		t.Fatal(err)
	}
	// fakeStack's seeded attr label is "text".
	if ent["text"] != wantText || ent["id"] != id {
		t.Fatalf("patch entity wrong: %v", ent)
	}
}

// watermarkOf extracts the processed-tx-id watermark, failing on absence.
func watermarkOf(t *testing.T, frame map[string]any) float64 {
	t.Helper()
	tx, _ := frame["processed-tx-id"].(float64)
	if tx == 0 {
		t.Fatalf("frame carries no watermark: %v", frame)
	}
	return tx
}

// titlesByEID flattens an add-query-ok result through the frozen-SDK mirror
// into eid → title text.
func titlesByEID(t *testing.T, result any, titleAttr string) map[string]string {
	t.Helper()
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	triples := extractTriplesMirror(t, raw) // frozen SDK can consume it
	out := map[string]string{}
	for _, tr := range triples {
		if a, _ := tr[1].(string); a == titleAttr {
			eid, _ := tr[0].(string)
			v, _ := tr[2].(string)
			out[eid] = v
		}
	}
	return out
}

func TestDeltaReconnectFullReplayCoversMissedTx(t *testing.T) {
	env := newWSEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	titleAttr := uuidStr(env.IDs.title)

	// Baseline: one delta session seeds four todos BEFORE subscribing so a
	// later single-row mutation stays under the >50% fallback threshold;
	// then two live 0.23.0 members share the group.
	connA, framesA := dialInit(t, ctx, env, "0.23.0")
	eids := make([]string, 4)
	for i := range eids {
		eids[i] = fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
		transactTodo(t, ctx, connA, framesA, "add-triple", eids[i], titleAttr, fmt.Sprintf("title-%d", i))
	}
	ackA := addQueryTodos(t, ctx, connA, framesA)

	connB, framesB := dialInit(t, ctx, env, "0.23.0")
	ackB := addQueryTodos(t, ctx, connB, framesB)

	if got, want := canonicalJSON(t, ackA["result"]), canonicalJSON(t, ackB["result"]); got != want {
		t.Fatalf("baseline members diverged:\n got %s\nwant %s", got, want)
	}
	baseTxA, _ := ackA["processed-tx-id"].(float64)
	baseTxB, _ := ackB["processed-tx-id"].(float64)
	if baseTxA != baseTxB {
		t.Fatalf("baseline watermarks diverged: %v vs %v", baseTxA, baseTxB)
	}

	// Delta-eligible mutation, certified on both live members as a
	// structural patch: this proves the delta path is eligible for the
	// single-entity shape the missed tx will reuse.
	transactTodo(t, ctx, connA, framesA, "add-triple", eids[2], titleAttr, "mutated!")
	deltaA := expectOp(t, framesA, "refresh-ok-delta")
	deltaB := expectOp(t, framesB, "refresh-ok-delta")
	requirePatchUpdate(t, deltaA, "todos", eids[2], "mutated!")
	requirePatchUpdate(t, deltaB, "todos", eids[2], "mutated!")
	gen1A, gen1B := watermarkOf(t, deltaA), watermarkOf(t, deltaB)
	if gen1A != gen1B {
		t.Fatalf("certified generation watermarks diverged: %v vs %v", gen1A, gen1B)
	}
	if gen1A <= baseTxA {
		t.Fatalf("certified watermark %v did not advance past baseline %v", gen1A, baseTxA)
	}

	// Terminate member B. Server-side detach races the close handshake;
	// nothing below depends on its timing: B's socket is dead, so B can
	// never observe the next generation except via reconnect replay. The
	// sibling keeps the group (and its snapshot) alive either way.
	_ = connB.Close(websocket.StatusGoingAway, "delta-reconnect drop")

	// Missed tx: a NEW delta-eligible single-entity mutation commits while
	// B is gone. The surviving delta sibling must receive it as
	// refresh-ok-delta — proving the missed generation was delta-eligible
	// and therefore distinguishing this from a full-only world.
	transactTodo(t, ctx, connA, framesA, "add-triple", eids[0], titleAttr, "missed-tx!")
	missed := expectOp(t, framesA, "refresh-ok-delta")
	requirePatchUpdate(t, missed, "todos", eids[0], "missed-tx!")
	missedTx := watermarkOf(t, missed)
	if missedTx <= gen1A {
		t.Fatalf("missed watermark %v did not advance past certified %v", missedTx, gen1A)
	}

	// Reconnect through a fresh delta session while the sibling holds the
	// group. Full replay is SUFFICIENT for delta clients: the rejoiner gets
	// add-query-ok with the full result, not a patch.
	connC, framesC := dialInit(t, ctx, env, "0.23.0")
	ackC := addQueryTodos(t, ctx, connC, framesC)
	if _, hasDelta := ackC["delta"]; hasDelta {
		t.Fatalf("rejoin add-query-ok must be full replay, got delta: %v", ackC)
	}
	if _, ok := ackC["result"]; !ok {
		t.Fatalf("rejoin add-query-ok missing full result: %v", ackC)
	}
	gotTitles := titlesByEID(t, ackC["result"], titleAttr)
	wantTitles := map[string]string{
		eids[0]: "missed-tx!",
		eids[1]: "title-1",
		eids[2]: "mutated!",
		eids[3]: "title-3",
	}
	for eid, want := range wantTitles {
		if gotTitles[eid] != want {
			t.Fatalf("rejoin state lost triple %s: got %q want %q (full=%v)", eid, gotTitles[eid], want, gotTitles)
		}
	}
	if gotTx := watermarkOf(t, ackC); gotTx != missedTx {
		t.Fatalf("rejoiner watermark %v; want missed %v (skipped or extra generation)", gotTx, missedTx)
	}

	// Continued valid delivery: a transact from the rejoiner reaches both
	// members (either envelope is valid for delta-capable receivers).
	newEID := uuidStr(rand16())
	transactTodo(t, ctx, connC, framesC, "add-triple", newEID, titleAttr, "after-rejoin")
	afterC := awaitRefreshEither(t, framesC, 5*time.Second)
	if body, _ := json.Marshal(afterC); !strings.Contains(string(body), "after-rejoin") {
		t.Fatalf("rejoined refresh missing its own row: %s", body)
	}
	afterA := awaitRefreshEither(t, framesA, 5*time.Second)
	if body, _ := json.Marshal(afterA); !strings.Contains(string(body), "after-rejoin") {
		t.Fatalf("sibling refresh missing rejoiner row: %s", body)
	}
	if txC, txA := watermarkOf(t, afterC), watermarkOf(t, afterA); txC != txA {
		t.Fatalf("post-rejoin watermarks diverged: %v vs %v", txC, txA)
	} else if txC <= missedTx {
		t.Fatalf("post-rejoin watermark %v did not advance past missed %v", txC, missedTx)
	}
}
