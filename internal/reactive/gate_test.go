package reactive

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// newGatedStack builds one subscription on a notifier without a Run loop;
// drains happen synchronously via drainPass so queue depths are exact.
func newGatedStack(t *testing.T) (*Notifier, *Subscription) {
	t.Helper()
	s := NewStore()
	sub := &Subscription{
		ID: "g1", AppID: "app1",
		Query:  json.RawMessage(`{}`),
		Topics: map[string]bool{"t": true},
		Emit:   func(Frame) {},
	}
	if _, err := s.Add(sub); err != nil {
		t.Fatalf("add: %v", err)
	}
	n := &Notifier{Store: s}
	n.Refresh = func(context.Context, *Subscription) (json.RawMessage, error) {
		return json.RawMessage(`{}`), nil
	}
	return n, sub
}

// TestGateFloodDenyDrainAllow covers docs/09-tier2-architecture.md §T2.1
// acceptance: flood pending until depth > max → deny; drain below the
// low-water mark → allow again. Hysteresis holds inside the middle band.
func TestGateFloodDenyDrainAllow(t *testing.T) {
	n, _ := newGatedStack(t)
	ctx := context.Background()

	// Flood 10 pending notifications past maxDepth 4.
	for i := 1; i <= 10; i++ {
		n.Notify(ctx, "app1", []string{"t"}, int64(i))
	}
	if got := n.QueueDepth(); got != 10 {
		t.Fatalf("depth after flood = %d, want 10", got)
	}

	gate := n.Gate(4)
	err := gate("app1")
	var shed *ShedError
	if !errors.As(err, &shed) {
		t.Fatalf("gate at depth 10: want ShedError, got %v", err)
	}
	if shed.RetryAfter != 250*time.Millisecond {
		t.Fatalf("RetryAfter = %s, want 250ms", shed.RetryAfter)
	}
	if !strings.Contains(shed.Error(), "shed") {
		t.Fatalf("Error() must mention shed: %q", shed.Error())
	}

	// Middle band (low=2 < depth=3 < max=4): latched denial must hold —
	// no flapping either way. The gauge is driven directly to place the
	// queue inside the band without a full drain.
	n.gauge.Store(3)
	if err := gate("app1"); !errors.As(err, &shed) {
		t.Fatalf("gate in hysteresis band: want ShedError, got %v", err)
	}

	// Drain below the low-water mark: the gate reopens.
	n.gauge.Store(1)
	if err := gate("app1"); err != nil {
		t.Fatalf("gate below low-water: want nil, got %v", err)
	}
	// Real Notify path floods the gauge (10 notifications coalesce into one
	// pending entry); denial holds until the queue truly drops below the
	// low-water mark.
	n.gauge.Store(0)
	for i := 11; i <= 20; i++ {
		n.Notify(ctx, "app1", []string{"t"}, int64(i))
	}
	if err := gate("app1"); !errors.As(err, &shed) {
		t.Fatalf("gate after reflood: want ShedError, got %v", err)
	}
	n.drainPass(ctx, slog.Default()) // coalesced: one entry drains
	if err := n.Gate(4)("app1"); !errors.As(err, &shed) {
		t.Fatalf("fresh gate at residual depth must consult live gauge: %v", err)
	}
	n.gauge.Store(0) // queue fully drained
	if err := gate("app1"); err != nil {
		t.Fatalf("gate after full drain: want nil, got %v", err)
	}
}

// TestGateDisabledForNonPositiveMax pins INSTANT_V2_MAX_QUEUE_DEPTH=0
// semantics: always allow, allocation-free.
func TestGateDisabledForNonPositiveMax(t *testing.T) {
	n, _ := newGatedStack(t)
	for _, max := range []int64{0, -1} {
		gate := n.Gate(max)
		n.gauge.Store(1 << 20)
		if err := gate("app1"); err != nil {
			t.Fatalf("Gate(%d) denied: %v", max, err)
		}
	}
	n.gauge.Store(0)
}
