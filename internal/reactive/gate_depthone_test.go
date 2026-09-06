package reactive

// RT-003 (queue depth one): recovery and shed determinism. Hermetic: the
// gauge is driven directly (precedent: TestGateFloodDenyDrainAllow), no
// clocks, no sleeps, no database, no sockets.

import (
	"context"
	"errors"
	"log/slog"
	"testing"
)

// Desired: a depth-one gate latches at depth 1 and reopens at depth 0 —
// throughput resumes instead of wedging shut. FAILS while low-water is
// maxDepth/2 == 0 (PROVEN_RED); passes after the low-water special case.
func TestGateDepthOneReopensAfterDrain(t *testing.T) {
	n, _ := newGatedStack(t)
	gate := n.Gate(1)

	n.gauge.Store(1)
	var shed *ShedError
	if err := gate("app1"); !errors.As(err, &shed) {
		t.Fatalf("depth-one gate at depth 1: want ShedError, got %v", err)
	}

	n.gauge.Store(0)
	if err := gate("app1"); err != nil {
		t.Fatalf("RT-003 RED: depth-one gate at depth 0 after drain: want nil, got %v", err)
	}
}

// Desired: a real fill→shed→drain→resume cycle on a depth-one gate.
// Fill via Notify, observe the explicit ShedError, drain synchronously,
// confirm the gate reopens and the next notification is admitted —
// bounded, deterministic, no sleeps.
func TestGateDepthOneFillShedDrainResume(t *testing.T) {
	n, _ := newGatedStack(t)
	ctx := context.Background()
	gate := n.Gate(1)

	n.Notify(ctx, "app1", []string{"t"}, 1)
	if got := n.QueueDepth(); got != 1 {
		t.Fatalf("depth after fill = %d, want 1", got)
	}
	var shed *ShedError
	if err := gate("app1"); !errors.As(err, &shed) {
		t.Fatalf("filled depth-one gate: want ShedError, got %v", err)
	}

	n.drainPass(ctx, slog.Default())
	if got := n.QueueDepth(); got != 0 {
		t.Fatalf("depth after drain = %d, want 0", got)
	}
	if err := gate("app1"); err != nil {
		t.Fatalf("drained depth-one gate: want nil, got %v", err)
	}

	n.Notify(ctx, "app1", []string{"t"}, 2)
	if got := n.QueueDepth(); got != 1 {
		t.Fatalf("depth after resume = %d, want 1", got)
	}
	n.drainPass(ctx, slog.Default())
	if got := n.QueueDepth(); got != 0 {
		t.Fatalf("depth after second drain = %d, want 0", got)
	}
}

// Desired: identical allocation and event sequences yield identical
// shed/allow outcomes. The gate holds no randomness; this test pins that
// across the repair.
func TestGateShedSequenceDeterministic(t *testing.T) {
	run := func() []bool {
		n, _ := newGatedStack(t)
		gate := n.Gate(2)
		var out []bool
		for _, depth := range []int64{0, 1, 2, 2, 1, 1, 0, 3, 0} {
			n.gauge.Store(depth)
			var shed *ShedError
			out = append(out, errors.As(gate("app1"), &shed))
		}
		return out
	}
	a, b := run(), run()
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("shed sequence diverged at step %d: %v vs %v", i, a, b)
		}
	}
}
