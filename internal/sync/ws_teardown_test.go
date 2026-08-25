package sync_test

import (
	"context"
	"testing"
	"time"
)

// TestWSTeardownOnCapBreachClose pins teardown-on-every-exit-path: a
// subscription-cap breach makes Handle return ErrCloseSession, ws.ServeHTTP
// returns after pushing the 429 — and the deferred hook MUST detach the
// session's already-attached subscription. Regression target: teardown used
// to live only in the read-error branch, so this path leaked the membership
// (and its per-app cap slot) for the life of the process.
func TestWSTeardownOnCapBreachClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	env := newWSEnv(t)
	store := env.Mgr.Deps.Store

	conn, frames := env.dial(t, ctx)
	initWS(t, conn, frames, ctx, env.AppID)

	sendFrame(t, conn, ctx, map[string]any{
		"op": "add-query", "q": map[string]any{"todos": map[string]any{}},
		"client-event-id": "q1",
	})
	expectOp(t, frames, "add-query-ok")
	if got := store.Len(); got != 1 {
		t.Fatalf("after first add-query store.Len() = %d, want 1", got)
	}

	// Breach the per-app cap: the second add-query gets a 429 and closes
	// the session through the early-return path (not a read error).
	store.MaxSubsPerApp = 1
	sendFrame(t, conn, ctx, map[string]any{
		"op": "add-query", "q": map[string]any{"items": map[string]any{}},
		"client-event-id": "q2",
	})
	// ErrFrames ride the wire as {op:"error", type:<name>, status, message}.
	limitSeen := false
	deadline := time.Now().Add(5 * time.Second)
	for !limitSeen {
		if time.Now().After(deadline) {
			t.Fatal("never received subscription-limit error frame")
		}
		select {
		case f, ok := <-frames:
			if !ok {
				t.Fatal("stream closed before subscription-limit arrived")
			}
			limitSeen = f["op"] == "error" && f["type"] == "subscription-limit"
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}

	// The deferred teardown must free the first subscription promptly.
	pollDeadline := time.Now().Add(5 * time.Second)
	for store.Len() > 0 {
		if time.Now().After(pollDeadline) {
			t.Fatalf("session closed but %d subscription(s) leaked: teardown missed an exit path", store.Len())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestWSTeardownOnDeadConn pins the dead-conn leak path end to end: a client
// that vanishes WITHOUT a close handshake (CloseNow = RST-class death, the
// laptop-sleep/NAT case) must surface as a server-side read error and run
// the same deferred teardown — subscriptions detached, no ghost sessions.
func TestWSTeardownOnDeadConn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	env := newWSEnv(t)
	store := env.Mgr.Deps.Store

	conn, frames := env.dial(t, ctx)
	initWS(t, conn, frames, ctx, env.AppID)

	sendFrame(t, conn, ctx, map[string]any{
		"op": "add-query", "q": map[string]any{"todos": map[string]any{}},
		"client-event-id": "q1",
	})
	expectOp(t, frames, "add-query-ok")
	if got := store.Len(); got != 1 {
		t.Fatalf("after add-query store.Len() = %d, want 1", got)
	}

	// Kill the TCP conn without any websocket close handshake.
	if err := conn.CloseNow(); err != nil {
		t.Fatalf("CloseNow: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for store.Len() > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("dead conn left %d subscription(s) attached: ghost session", store.Len())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
