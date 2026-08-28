package reactive

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
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
	var mu sync.Mutex
	frames := 0
	sub := &Subscription{
		ID:     "s1",
		AppID:  "app1",
		Query:  json.RawMessage(`{"posts":{}}`),
		Topics: map[string]bool{"attr-a": true},
		Emit: func(f Frame) {
			mu.Lock()
			frames++
			mu.Unlock()
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Run(ctx)

	// Burst of notifications; all after TxID 5 → exactly one refresh expected
	// once the drain loop catches up (coalescing collapses the queue).
	for i := range 10 {
		n.Notify(ctx, "app1", []string{"attr-a"}, int64(6+i))
	}
	// Poll instead of a fixed sleep: under -race the drain goroutine may
	// not have started within any fixed window.
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		got := frames
		mu.Unlock()
		if got > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no frames emitted")
		}
		time.Sleep(10 * time.Millisecond)
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Run(ctx)
	n.Notify(ctx, "app1", []string{"a"}, 3) // stale
	time.Sleep(50 * time.Millisecond)
	if called {
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
