package reactive

import (
	"context"
	"encoding/json"
	"testing"
)

func TestAddRemoveAndTopicIndex(t *testing.T) {
	s := NewStore()
	sub := &Subscription{
		AppID:  "app1",
		Query:  json.RawMessage(`{"posts":{}}`),
		Topics: map[string]bool{"attr-a": true, "attr-b": true},
	}
	id, err := s.Add(sub)
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	if s.Len() != 1 {
		t.Fatalf("len %d", s.Len())
	}
	got := s.SubsForTopics([]string{"attr-a"})
	if len(got) != 1 || got[0].ID != id {
		t.Fatalf("SubsForTopics: %v", got)
	}
	// Unrelated topic misses.
	if got := s.SubsForTopics([]string{"nope"}); len(got) != 0 {
		t.Fatalf("unexpected subs: %v", got)
	}
	if !s.Remove(id) {
		t.Fatal("remove failed")
	}
	if s.Len() != 0 {
		t.Fatalf("len after remove %d", s.Len())
	}
	if got := s.SubsForTopics([]string{"attr-a"}); len(got) != 0 {
		t.Fatalf("subs after remove: %v", got)
	}
}

func TestNotifierCoalesces(t *testing.T) {
	s := NewStore()
	frames := 0
	sub := &Subscription{
		ID:     "s1",
		AppID:  "app1",
		Query:  json.RawMessage(`{"posts":{}}`),
		Topics: map[string]bool{"attr-a": true},
		Emit: func(f Frame) {
			frames++
			if f.ProcessedTxID != 15 {
				t.Errorf("coalesced frame tx = %d, want 15", f.ProcessedTxID)
			}
		},
	}
	sub.TxID.Store(5)
	if _, err := s.Add(sub); err != nil {
		t.Fatalf("add: %v", err)
	}

	n := &Notifier{Store: s}
	n.Refresh = func(ctx context.Context, sub *Subscription) (json.RawMessage, error) {
		return json.RawMessage(`{"posts":[]}`), nil
	}
	ctx := context.Background()

	// Queue the complete burst before draining, so coalescing doesn't depend
	// on whether a worker happened to run between two notifications.
	for i := range 10 {
		n.Notify(ctx, "app1", []string{"attr-a"}, int64(6+i))
	}
	if !n.drainPass(ctx, nil) {
		t.Fatal("expected queued work")
	}
	if frames != 1 || n.drainPass(ctx, nil) {
		t.Fatalf("burst must produce exactly one refresh, got %d", frames)
	}
}

func TestNotifierQueueDepthCountsUniquePendingSubscriptions(t *testing.T) {
	s := NewStore()
	sub := &Subscription{
		ID: "s1", AppID: "app1", Query: json.RawMessage(`{}`),
		Topics: map[string]bool{"a": true, "b": true}, Emit: func(Frame) {},
	}
	if _, err := s.Add(sub); err != nil {
		t.Fatalf("add: %v", err)
	}
	n := &Notifier{Store: s, Refresh: func(context.Context, *Subscription) (json.RawMessage, error) {
		return json.RawMessage(`{}`), nil
	}}
	ctx := context.Background()

	// The repeated topic and repeated notification both target the same
	// subscription. They must occupy one pending queue slot while the latest
	// transaction watermark still wins.
	n.Notify(ctx, "app1", []string{"a", "a", "b", "a"}, 6)
	n.Notify(ctx, "app1", []string{"b", "a"}, 7)
	n.Notify(ctx, "app1", []string{"a"}, 4) // an older coalesced notification
	if got := n.QueueDepth(); got != 1 {
		t.Fatalf("queue depth for one coalesced subscription = %d, want 1", got)
	}
	n.mu.Lock()
	gotTx := n.pending[sub.ID]
	n.mu.Unlock()
	if gotTx != 7 {
		t.Fatalf("coalesced watermark = %d, want 7", gotTx)
	}

	if !n.drainPass(ctx, nil) {
		t.Fatal("expected pending work")
	}
	if got := n.QueueDepth(); got != 0 {
		t.Fatalf("queue depth after drain = %d, want 0", got)
	}
}

func TestDedupeTopicIDsPreservesFirstSeenOrder(t *testing.T) {
	got := dedupeTopicIDs([]string{"b", "a", "b", "c", "a"})
	want := []string{"b", "a", "c"}
	if len(got) != len(want) {
		t.Fatalf("deduped topics = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("deduped topics = %v, want %v", got, want)
		}
	}
	noDup := []string{"a", "b"}
	if got := dedupeTopicIDs(noDup); &got[0] != &noDup[0] {
		t.Fatal("duplicate-free topics should not be copied")
	}
}

func TestNotifierIgnoresStaleTxIDs(t *testing.T) {
	s := NewStore()
	called := false
	sub := &Subscription{
		ID: "s1", AppID: "app1",
		Query:  json.RawMessage(`{}`),
		Topics: map[string]bool{"a": true},
		Emit:   func(Frame) { called = true },
	}
	sub.TxID.Store(10)
	if _, err := s.Add(sub); err != nil {
		t.Fatalf("add: %v", err)
	}
	n := &Notifier{Store: s}
	n.Refresh = func(ctx context.Context, sub *Subscription) (json.RawMessage, error) {
		return json.RawMessage(`{}`), nil
	}
	ctx := context.Background()
	n.Notify(ctx, "app1", []string{"a"}, 3) // stale
	if n.drainPass(ctx, nil) || called {
		t.Fatal("stale tx should not refresh")
	}
}

func TestAppIsolation(t *testing.T) {
	s := NewStore()
	if _, err := s.Add(&Subscription{ID: "a-app1", AppID: "app1", Topics: map[string]bool{"x": true}}); err != nil {
		t.Fatalf("add: %v", err)
	}
	if _, err := s.Add(&Subscription{ID: "b-app2", AppID: "app2", Topics: map[string]bool{"x": true}}); err != nil {
		t.Fatalf("add: %v", err)
	}
	got := s.SubsForTopics([]string{"x"})
	if len(got) != 2 {
		t.Fatalf("topic index lost a sub: %v", got)
	}
}
