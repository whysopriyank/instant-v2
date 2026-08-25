package reactive

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
)

// TestRefreshFailureReenqueues pins the re-arm discipline in refreshOne:
// a transient Refresh failure must NOT drop the invalidation. The batch has
// already left `pending` when the failure surfaces, so without re-enqueue the
// subscriber stays stale until an unrelated commit happens to re-dirty it.
func TestRefreshFailureReenqueues(t *testing.T) {
	ctx := context.Background()
	store := NewStore()

	var calls atomic.Int32
	n := &Notifier{
		Store: store,
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

	// The failure must leave the sub re-armed for a retry.
	n.mu.Lock()
	_, armed := n.pending["s1"]
	n.mu.Unlock()
	if !armed {
		t.Fatal("failed refresh was dropped: subscriber left stale until an unrelated commit")
	}

	// Next drain retries; success advances the watermark.
	if !n.drainPass(ctx, slog.Default()) {
		t.Fatal("re-enqueued entry lost on second drain")
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("retry did not re-run Refresh (calls=%d)", got)
	}
	if got := sub.TxID.Load(); got != 5 {
		t.Fatalf("after successful retry TxID = %d, want 5", got)
	}

	// And a third drain is idle: no hot spin left behind.
	if n.drainPass(ctx, slog.Default()) {
		t.Fatal("pending not drained clean after successful retry")
	}
}
