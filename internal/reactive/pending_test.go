package reactive

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestPendingUnknownCannotBecomeKnown(t *testing.T) {
	for _, cause := range []string{"unknown-notification", "change-cap"} {
		t.Run(cause, func(t *testing.T) {
			ctx := context.Background()
			h := newDiffHarness(t, true)
			h.seed(ctx)
			var first []Change
			count := 1
			if cause == "change-cap" {
				count = maxTrackedChanges + 1
			}
			for i := 0; i < count; i++ {
				first = append(first, h.world.create("posts", map[string]any{diffTitle: fmt.Sprintf("first-%d", i)}))
			}
			if cause == "change-cap" {
				h.n.NotifyChanges(ctx, diffApp, first, 2)
			} else {
				h.n.Notify(ctx, diffApp, []string{diffTitle}, 2)
			}
			last := h.world.create("posts", map[string]any{diffTitle: "last"})
			h.n.NotifyChanges(ctx, diffApp, []Change{last}, 3)
			want := renderOracle(h.world, h.subs["match-all"].Query)
			if !h.n.drainPass(ctx, slog.Default()) {
				t.Fatal("expected coalesced work")
			}
			if got := h.frames["sub-match-all"]; string(got) != string(want) {
				t.Error("drain lost earlier mutations after a known notification replaced unknown change knowledge")
			}
			if got := h.calls["sub-match-all"].Load(); got != 2 {
				t.Errorf("full refresh calls = %d, want initial seed plus mandatory full refresh", got)
			}
			if got := h.subs["match-all"].TxID.Load(); got != 3 {
				t.Errorf("coalesced watermark = %d, want 3", got)
			}
		})
	}
}

func TestDrainOwnsChangeEpoch(t *testing.T) {
	// One worker gives a real barrier: while the first refresh is blocked,
	// the other subscription is in the detached batch but has not started.
	previousWorkers := Workers
	Workers = 1
	t.Cleanup(func() { Workers = previousWorkers })
	store := NewStore()
	frames := make(chan Frame, 3)
	for _, id := range []string{"a", "b"} {
		if _, err := store.Add(&Subscription{
			ID: id, AppID: "app", Topics: map[string]bool{id: true},
			Emit: func(frame Frame) { frames <- frame },
		}); err != nil {
			t.Fatal(err)
		}
	}
	started := make(chan string, 1)
	release := make(chan struct{})
	var unblock sync.Once
	first := true // accessed only by the single drain worker
	n := &Notifier{Store: store, Refresh: func(_ context.Context, sub *Subscription) (json.RawMessage, error) {
		if first {
			first = false
			started <- sub.ID
			<-release
		}
		return json.RawMessage(`{"data":{}}`), nil
	}}
	ctx := context.Background()
	n.NotifyChanges(ctx, "app", []Change{{Etype: "posts", EntityID: "old", AttrIDs: []string{"a", "b"}}}, 1)
	drained := make(chan struct{})
	go func() {
		n.drainPass(ctx, slog.Default())
		close(drained)
	}()
	t.Cleanup(func() {
		unblock.Do(func() { close(release) })
		select {
		case <-drained:
		case <-time.After(3 * time.Second):
			t.Error("blocked drain did not stop")
		}
	})
	var waiting string
	select {
	case id := <-started:
		waiting = "a"
		if id == "a" {
			waiting = "b"
		}
	case <-time.After(3 * time.Second):
		t.Fatal("first refresh did not start")
	}
	// This belongs only to the next epoch, not the waiting old refresh.
	next := []Change{{Etype: "posts", EntityID: "new", AttrIDs: []string{waiting}}}
	n.NotifyChanges(ctx, "app", next, 2)
	unblock.Do(func() { close(release) })
	select {
	case <-drained:
	case <-time.After(3 * time.Second):
		t.Fatal("first drain did not complete")
	}
	n.mu.Lock()
	gotChanges, gotTx := n.pendingCh[waiting], n.pending[waiting]
	n.mu.Unlock()
	if !reflect.DeepEqual(gotChanges, next) || gotTx != 2 {
		t.Fatalf("old drain consumed next epoch: changes=%+v tx=%d, want %+v tx=2", gotChanges, gotTx, next)
	}
	if !n.drainPass(ctx, slog.Default()) {
		t.Fatal("next epoch disappeared")
	}
	nextFrame := func() Frame {
		t.Helper()
		select {
		case frame := <-frames:
			return frame
		case <-time.After(3 * time.Second):
			t.Fatal("drain did not emit the expected frame")
			return Frame{}
		}
	}
	for range 2 {
		if frame := nextFrame(); frame.ProcessedTxID != 1 {
			t.Fatalf("old batch emitted wrong watermark: %+v", frame)
		}
	}
	if frame := nextFrame(); frame.SubID != waiting || frame.ProcessedTxID != 2 {
		t.Fatalf("next epoch frame = %+v", frame)
	}
	if n.QueueDepth() != 0 {
		t.Fatal("next epoch did not drain cleanly")
	}
}
