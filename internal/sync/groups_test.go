package sync_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"
)

// TestGroupDedupeSharedSubscription proves query-group dedupe end-to-end:
// two sessions holding the identical query share ONE store subscription and
// receive byte-identical full envelopes per write (docs/08-tier1-hotpath.md
// §T1.1).
func TestGroupDedupeSharedSubscription(t *testing.T) {
	env := newWSEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	connA, framesA := dialInit(t, ctx, env, "0.22.0")
	addQueryTodos(t, ctx, connA, framesA)
	connB, framesB := dialInit(t, ctx, env, "0.22.0")
	addQueryTodos(t, ctx, connB, framesB)

	if n := env.Mgr.Deps.Store.Len(); n != 1 {
		t.Fatalf("expected 1 shared subscription for identical queries, got %d", n)
	}

	transactTodo(t, ctx, connA, framesA, "add-triple",
		"eeeeeeee-0000-4000-8000-000000000001", uuidStr(env.IDs.title), "dedupe")

	frA := expectOp(t, framesA, "refresh-ok")
	frB := expectOp(t, framesB, "refresh-ok")

	ca, _ := json.Marshal(frA["computations"])
	cb, _ := json.Marshal(frB["computations"])
	if !bytes.Equal(ca, cb) {
		t.Fatalf("group members must receive byte-identical envelopes:\nA=%s\nB=%s", ca, cb)
	}
}
