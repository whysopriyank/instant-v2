package sync

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
)

// A stale admission load must not roll back a gate installed while it waited.
func TestAdmissionDelayedLoadPreservesNewerGate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	allow := &perms.RuleDoc{Raw: json.RawMessage(`{"allow":"all"}`)}
	deny := &perms.RuleDoc{Raw: json.RawMessage(`{"deny":"all"}`)}
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	m := NewManager(Deps{Store: reactive.NewStore(), Rules: func(ctx context.Context, _ string) (*perms.RuleDoc, error) {
		if calls.Add(1) == 1 {
			close(entered)
			select {
			case <-release:
				return allow, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return deny, nil
	}})
	query := json.RawMessage(`{"todos":{}}`)
	s1 := &Session{AppID: "app", Subs: map[string]bool{}}
	g, err := m.attachGroup(ctx, s1, query, nil, &platform.AttrCatalog{}, wireTree, allow)
	if err != nil {
		t.Fatal(err)
	}
	s2 := &Session{AppID: "app", Subs: map[string]bool{}}
	done := make(chan error, 1)
	go func() { _, err := m.attachGroup(ctx, s2, query, nil, g.cat, wireTree, deny); done <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("admission did not enter loader")
	}
	if _, _, err := m.RefreshGate(ctx, g.sub); err != nil {
		t.Fatal(err)
	}
	epoch := g.sub.Gen.Load()
	want := json.RawMessage(`{"data":{"todos":[]}}`)
	if !m.PublishGeneration(g.sub, epoch, want, 7) {
		t.Fatal("deny publication refused")
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("admission did not finish")
	}
	gate, _ := g.sub.AttachCtx.(*QueryGate)
	if gate == nil || GateHash(gate.Rules) != GateHash(deny) {
		t.Fatal("delayed allow load replaced the newer deny gate")
	}
	if g.sub.Gen.Load() != epoch || string(g.sub.Snapshot()) != string(want) {
		t.Fatal("stale admission disturbed the newer served generation")
	}
	assertAdmission(t, m, s2, g)
}

func assertAdmission(t *testing.T, m *Manager, sess *Session, g *queryGroup) {
	t.Helper()
	m.groupsMu.Lock()
	defer m.groupsMu.Unlock()
	if m.groups[g.key] != g {
		t.Fatal("admitted group is absent from registry")
	}
	if sub, ok := m.Deps.Store.Get(g.key); !ok || sub != g.sub {
		t.Fatal("admitted subscription is absent from store")
	}
	g.mu.Lock()
	_, present := g.members[sess]
	g.mu.Unlock()
	sess.mu.Lock()
	owned := sess.Subs[g.key]
	sess.mu.Unlock()
	if !present || !owned {
		t.Fatal("admission did not publish both membership records")
	}
}

func TestAdmissionRecreatesGroupRemovedDuringLoad(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	allow := &perms.RuleDoc{Raw: json.RawMessage(`{"allow":"all"}`)}
	deny := &perms.RuleDoc{Raw: json.RawMessage(`{"deny":"all"}`)}
	entered, release := make(chan struct{}), make(chan struct{})
	m := NewManager(Deps{Store: reactive.NewStore(), Rules: func(ctx context.Context, _ string) (*perms.RuleDoc, error) {
		close(entered)
		select {
		case <-release:
			return deny, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}})
	query := json.RawMessage(`{"todos":{}}`)
	s1 := &Session{AppID: "app", Subs: map[string]bool{}}
	old, err := m.attachGroup(ctx, s1, query, nil, &platform.AttrCatalog{}, wireTree, allow)
	if err != nil {
		t.Fatal(err)
	}
	s2 := &Session{AppID: "app", Subs: map[string]bool{}}
	type result struct {
		g   *queryGroup
		err error
	}
	done := make(chan result, 1)
	go func() { g, err := m.attachGroup(ctx, s2, query, nil, old.cat, wireTree, deny); done <- result{g, err} }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("admission did not enter loader")
	}
	m.DetachAll(s1)
	close(release)
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatal(got.err)
		}
		if got.g == old {
			t.Fatal("admission reused deleted group")
		}
		assertAdmission(t, m, s2, got.g)
		if m.appMembers["app"] != 1 {
			t.Fatal("incorrect admission count")
		}
	case <-ctx.Done():
		t.Fatal("admission did not finish")
	}
}

func TestAdmissionRejectsClosingSessionAfterBlockedLoad(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	allow := &perms.RuleDoc{Raw: json.RawMessage(`{"allow":"all"}`)}
	deny := &perms.RuleDoc{Raw: json.RawMessage(`{"deny":"all"}`)}
	entered, release := make(chan struct{}), make(chan struct{})
	m := NewManager(Deps{Store: reactive.NewStore(), Rules: func(ctx context.Context, _ string) (*perms.RuleDoc, error) {
		close(entered)
		select {
		case <-release:
			return deny, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}})
	query := json.RawMessage(`{"todos":{}}`)
	s1 := &Session{AppID: "app", Subs: map[string]bool{}}
	old, err := m.attachGroup(ctx, s1, query, nil, &platform.AttrCatalog{}, wireTree, allow)
	if err != nil {
		t.Fatal(err)
	}

	s2 := &Session{AppID: "app", Subs: map[string]bool{}}
	done := make(chan error, 1)
	go func() {
		_, err := m.attachGroup(ctx, s2, query, nil, old.cat, wireTree, deny)
		done <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("admission did not enter loader")
	}

	// Remove the observed group, then terminate the still-unadmitted session
	// while its admission is blocked in rule I/O.
	m.DetachAll(s1)
	m.DetachAll(s2)
	close(release)

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("blocked admission unexpectedly admitted the closing session")
		}
	case <-ctx.Done():
		t.Fatal("admission did not finish")
	}

	m.groupsMu.Lock()
	defer m.groupsMu.Unlock()
	if len(m.groups) != 0 || m.Deps.Store.Len() != 0 || m.appMembers["app"] != 0 {
		t.Fatalf("closing admission published state: groups=%d store=%d members=%d", len(m.groups), m.Deps.Store.Len(), m.appMembers["app"])
	}
	s2.mu.Lock()
	defer s2.mu.Unlock()
	if len(s2.Subs) != 0 {
		t.Fatalf("closing session retained subscriptions: %v", s2.Subs)
	}
}

func TestStoreAddFailureRollsBackAdmissionState(t *testing.T) {
	store := reactive.NewStore()
	store.MaxSubsPerApp = 1
	if _, err := store.Add(&reactive.Subscription{ID: "occupied", AppID: "app"}); err != nil {
		t.Fatal(err)
	}
	m := NewManager(Deps{Store: store})
	sess := &Session{ID: "joining", AppID: "app", Subs: map[string]bool{}}
	_, err := m.attachGroup(context.Background(), sess, json.RawMessage(`{"todos":{}}`), nil,
		&platform.AttrCatalog{}, wireTree, &perms.RuleDoc{})
	if err == nil {
		t.Fatal("expected store capacity failure")
	}

	m.groupsMu.Lock()
	defer m.groupsMu.Unlock()
	if len(m.groups) != 0 || m.appMembers["app"] != 0 {
		t.Fatalf("failed admission left manager state: groups=%d members=%d", len(m.groups), m.appMembers["app"])
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if len(sess.Subs) != 0 {
		t.Fatalf("failed admission left session state: %v", sess.Subs)
	}
}
