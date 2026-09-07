package reactive

// H-01e: incremental splice overlapping revoke must bail, never serve
// stale-spliced state. Barrier: Members probe parks mid-splice; revoke clears
// baselines (clear-then-bump); resume must observe Gen mismatch and return
// false so the caller falls back to full refresh under the new gate.

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/instaql"
)

type barrierSource struct {
	entered chan struct{}
	release chan struct{}
	once    *sync.Once
	members map[string]bool
	ents    map[string]json.RawMessage
}

func (b *barrierSource) Members(ctx context.Context, appID string, f *instaql.Form, candidates []string) (map[string]bool, error) {
	b.once.Do(func() { close(b.entered) })
	select {
	case <-b.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	out := map[string]bool{}
	for _, id := range candidates {
		out[id] = b.members[id]
	}
	return out, nil
}

func (b *barrierSource) Entities(ctx context.Context, appID string, f *instaql.Form, ids []string) (map[string]json.RawMessage, error) {
	out := map[string]json.RawMessage{}
	for _, id := range ids {
		if p, ok := b.ents[id]; ok {
			out[id] = p
		}
	}
	return out, nil
}

func TestH01eSpliceOverlappingRevokeBails(t *testing.T) {
	sub := &Subscription{
		ID:    "splice-revoke",
		AppID: "app",
		Query: json.RawMessage(`{"todos":{}}`),
	}
	allowResult := json.RawMessage(`{"data":{"todos":[{"id":"e1"}]}}`)
	var once sync.Once
	fake := &barrierSource{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		once:    &once,
		members: map[string]bool{"e1": true, "e2": true},
		ents: map[string]json.RawMessage{
			"e1": json.RawMessage(`{"id":"e1"}`),
			"e2": json.RawMessage(`{"id":"e2"}`),
		},
	}
	inc := &Incremental{
		Source:    fake,
		Authorize: func(*Subscription, []string) bool { return true },
	}
	// Arm mat under allow (materialize is the only trusted seeder).
	inc.materialize(sub, allowResult)
	if !sub.mat.ready {
		t.Fatal("materialize did not arm; test staged nothing")
	}
	gen0 := sub.Gen.Load()
	changes := []Change{{Etype: "todos", EntityID: "e2", AttrIDs: []string{"a1"}}}
	done := make(chan struct {
		out json.RawMessage
		ok  bool
	}, 1)
	go func() {
		out, ok := inc.apply(context.Background(), sub, changes)
		done <- struct {
			out json.RawMessage
			ok  bool
		}{out, ok}
	}()
	select {
	case <-fake.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("splice Members probe never parked")
	}
	// Revoke mid-splice: production clear-then-bump order.
	sub.ClearServedState()
	sub.Gen.Add(1)
	if sub.Gen.Load() == gen0 {
		t.Fatal("Gen did not bump; test staged nothing")
	}
	close(fake.release)
	select {
	case r := <-done:
		if r.ok {
			t.Fatalf("splice served across revoke: %s", r.out)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("splice did not return after release")
	}
}
