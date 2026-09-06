package reactive

// Generation-epoch coordination (RT-001) at the notifier layer. White-box:
// attempts are driven directly so a concurrent rule change can be staged
// deterministically. No database, no sockets.

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

// TestAttemptStampsGenerationEpoch proves the stamp-early contract: the
// emitted frame carries the epoch from attempt start, even when a swap
// lands mid-computation. Dropping mismatches is the transport's job
// (sync Emit coordination); the notifier's job is an honest stamp.
// It also pins the preserved fake-Emit-compatible commit behavior.
func TestAttemptStampsGenerationEpoch(t *testing.T) {
	ctx := context.Background()
	store := NewStore()
	sub := &Subscription{ID: "g", AppID: "app", Query: json.RawMessage(`{}`)}
	if _, err := store.Add(sub); err != nil {
		t.Fatalf("store add: %v", err)
	}
	var mu sync.Mutex
	var captured []Frame
	n := &Notifier{
		Store: store,
		Refresh: func(context.Context, *Subscription) (json.RawMessage, error) {
			sub.Gen.Add(1) // concurrent re-gate landing mid-computation
			return json.RawMessage(`{"ok":true}`), nil
		},
	}
	sub.Emit = func(f Frame) error {
		mu.Lock()
		captured = append(captured, f)
		mu.Unlock()
		return nil
	}
	sub.Gen.Store(41)
	n.refreshOneAttempt(ctx, slog.Default(), sub.ID, 1, nil, 0)

	mu.Lock()
	defer mu.Unlock()
	if len(captured) != 1 {
		t.Fatalf("emitted %d frames; want 1", len(captured))
	}
	if captured[0].Gen != 41 {
		t.Fatalf("frame stamped epoch %d; want attempt-start epoch 41", captured[0].Gen)
	}
	if string(sub.Snapshot()) != `{"ok":true}` || sub.TxID.Load() != 1 {
		t.Fatal("attempt must still commit on accepted emit (fake-Emit-compatible behavior)")
	}
}

// TestIncrementalSuccessPublishesStamped covers the splice path the
// epoch design must not break: a change the incremental engine absorbs
// is published through the same stamped Emit, with snapshot and
// watermark committed. The Refresh-call counter proves the change did
// not bail to full refresh — without that, this test would exercise
// the wrong path while claiming Inc coverage.
func TestIncrementalSuccessPublishesStamped(t *testing.T) {
	ctx := context.Background()
	h := newDiffHarness(t, true)
	h.seed(ctx)

	sub := h.subs["match-all"]
	var mu sync.Mutex
	var got []Frame
	sub.Emit = func(f Frame) error {
		mu.Lock()
		got = append(got, f)
		mu.Unlock()
		return nil
	}
	before := h.calls[sub.ID].Load()

	c := h.world.create(h.world.etypes()[0], map[string]any{diffTitle: "t-splice"})
	h.n.NotifyChanges(ctx, diffApp, []Change{c}, 2)
	for h.n.drainPass(ctx, slog.Default()) {
	}
	if h.calls[sub.ID].Load() != before {
		t.Fatal("spliceable change took the full-refresh path; Inc coverage unproven")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) == 0 {
		t.Fatal("spliced generation never published")
	}
	last := got[len(got)-1]
	if last.Gen != 0 {
		t.Fatalf("spliced frame stamped epoch %d; want stable epoch 0 (no swaps in this flow)", last.Gen)
	}
	if !strings.Contains(string(last.ResultJSON), "t-splice") {
		t.Fatalf("spliced frame missing the change: %s", last.ResultJSON)
	}
	if sub.TxID.Load() != 2 {
		t.Fatalf("watermark = %d; want 2 (spliced generation committed)", sub.TxID.Load())
	}
}
