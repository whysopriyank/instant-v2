package sync

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

// TestRunKeepaliveDeadOnPingError: a failing ping must invoke onDead exactly
// once and stop the loop — that cancellation is what funnels half-open peers
// into the normal read-error teardown.
func TestRunKeepaliveDeadOnPingError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls, dead int
		h := &WSHandler{PingInterval: 5 * time.Millisecond}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan struct{})
		go func() {
			h.runKeepalive(ctx, func(context.Context) error {
				calls++
				return errors.New("broken pipe")
			}, func() { dead++ })
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("keepalive did not exit after ping failure")
		}
		if dead != 1 {
			t.Fatalf("onDead called %d times, want 1", dead)
		}
		if calls != 1 {
			t.Fatalf("write attempted %d times, want 1 (loop should stop)", calls)
		}
	})
}

// TestRunKeepaliveHangTimesOut: a ping that blocks past the interval budget
// (peer ACK path wedged, zero-window peer) counts as dead via the per-ping
// timeout — not just an explicit write error.
func TestRunKeepaliveHangTimesOut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var dead int
		h := &WSHandler{PingInterval: 10 * time.Millisecond}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan struct{})
		start := time.Now()
		go func() {
			h.runKeepalive(ctx, func(wctx context.Context) error {
				<-wctx.Done() // hang until the per-ping timeout fires
				if !errors.Is(wctx.Err(), context.DeadlineExceeded) {
					t.Error("ping did not reach its deadline")
				}
				return wctx.Err()
			}, func() { dead++ })
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("keepalive did not exit on hung ping")
		}
		if dead != 1 {
			t.Fatalf("onDead called %d times, want 1", dead)
		}
		if elapsed := time.Since(start); elapsed != 2*h.PingInterval {
			t.Fatalf("hung ping took %v; want one tick plus one timeout (%v)", elapsed, 2*h.PingInterval)
		}
	})
}

// TestRunKeepaliveHealthyNoDead: successful pings keep the loop running and
// onDead silent; ctx cancellation is the only clean exit.
func TestRunKeepaliveHealthyNoDead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var dead int
		pings := make(chan struct{}, 2)
		done := make(chan struct{})
		h := &WSHandler{PingInterval: 5 * time.Millisecond}
		ctx, cancel := context.WithCancel(context.Background())

		defer cancel()
		go func() {
			h.runKeepalive(ctx, func(context.Context) error {
				pings <- struct{}{}
				return nil
			}, func() { dead++ })
			close(done)
		}()

		for range 2 {
			select {
			case <-pings:
			case <-time.After(time.Second):
				t.Fatal("healthy connection did not receive repeated pings")
			}
		}
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("keepalive did not stop after cancellation")
		}
		if dead != 0 {
			t.Fatal("onDead fired on a healthy connection")
		}
	})
}
