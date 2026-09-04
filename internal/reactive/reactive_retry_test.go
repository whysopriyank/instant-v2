package reactive

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
)

// TestRefreshFailureDoesNotHotLoop pins the first half of P-004's retry
// contract: a failed refresh remains recoverable, but is not immediately
// re-enqueued into the same scheduler pass.
func TestRefreshFailureDoesNotHotLoop(t *testing.T) {
	ctx := context.Background()
	store := NewStore()
	clock := &fakeRetryClock{}

	var calls atomic.Int32
	n := &Notifier{
		Store:       store,
		clock:       clock,
		retryJitter: zeroRetryJitter,
		Refresh: func(ctx context.Context, sub *Subscription) (json.RawMessage, error) {
			if calls.Add(1) == 1 {
				return nil, errors.New("pg: pool exhausted")
			}
			return json.RawMessage(`{"v":2}`), nil
		},
	}

	sub := &Subscription{
		ID:     "s1",
		AppID:  "a",
		Query:  json.RawMessage(`{}`),
		Topics: map[string]bool{"t1": true},
		Emit:   func(Frame) {},
	}
	if _, err := store.Add(sub); err != nil {
		t.Fatalf("store.Add: %v", err)
	}
	t.Cleanup(func() { store.Remove(sub.ID) })

	n.Notify(ctx, "a", []string{"t1"}, 5)
	if !n.drainPass(ctx, slog.Default()) {
		t.Fatal("expected dirty work on first drain")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("refresh calls = %d, want 1", got)
	}
	if sub.TxID.Load() >= 5 {
		t.Fatalf("failed refresh advanced watermark to %d", sub.TxID.Load())
	}

	// A second immediate pass must not retry. The retry is timer-owned.
	if n.drainPass(ctx, slog.Default()) {
		t.Fatal("failed refresh hot-looped in the next scheduler pass")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("refresh calls after immediate second pass = %d, want 1", got)
	}
}
