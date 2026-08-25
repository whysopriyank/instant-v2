package sync

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestRunKeepaliveDeadOnPingError: a failing ping must invoke onDead exactly
// once and stop the loop — that cancellation is what funnels half-open peers
// into the normal read-error teardown.
func TestRunKeepaliveDeadOnPingError(t *testing.T) {
	var calls, dead atomic.Int32
	h := &WSHandler{PingInterval: 5 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		h.runKeepalive(ctx, func(context.Context) error {
			calls.Add(1)
			return errors.New("broken pipe")
		}, func() { dead.Add(1) })
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("keepalive did not exit after ping failure")
	}
	if dead.Load() != 1 {
		t.Fatalf("onDead called %d times, want 1", dead.Load())
	}
	if calls.Load() != 1 {
		t.Fatalf("write attempted %d times, want 1 (loop should stop)", calls.Load())
	}
}

// TestRunKeepaliveHangTimesOut: a ping that blocks past the interval budget
// (peer ACK path wedged, zero-window peer) counts as dead via the per-ping
// timeout — not just an explicit write error.
func TestRunKeepaliveHangTimesOut(t *testing.T) {
	var dead atomic.Int32
	h := &WSHandler{PingInterval: 10 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	start := time.Now()
	go func() {
		h.runKeepalive(ctx, func(wctx context.Context) error {
			<-wctx.Done() // hang until the per-ping timeout fires
			return wctx.Err()
		}, func() { dead.Add(1) })
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("keepalive did not exit on hung ping")
	}
	if dead.Load() != 1 {
		t.Fatalf("onDead called %d times, want 1", dead.Load())
	}
	if elapsed := time.Since(start); elapsed < 5*time.Millisecond {
		t.Fatalf("onDead fired at %v — hung ping was not given its timeout budget", elapsed)
	}
}

// TestRunKeepaliveHealthyNoDead: successful pings keep the loop running and
// onDead silent; ctx cancellation is the only clean exit.
func TestRunKeepaliveHealthyNoDead(t *testing.T) {
	var dead atomic.Int32
	var pings atomic.Int32
	h := &WSHandler{PingInterval: 5 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())

	go h.runKeepalive(ctx, func(context.Context) error {
		pings.Add(1)
		return nil
	}, func() { dead.Add(1) })

	time.Sleep(40 * time.Millisecond)
	cancel()
	time.Sleep(10 * time.Millisecond)
	if got := pings.Load(); got < 2 {
		t.Fatalf("expected repeated pings on healthy conn, got %d", got)
	}
	if dead.Load() != 0 {
		t.Fatal("onDead fired on a healthy connection")
	}
}
