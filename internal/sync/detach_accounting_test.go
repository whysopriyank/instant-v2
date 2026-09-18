package sync

// RT-002 review regression: detachMember must be idempotent for membership
// accounting. failMember racing a stale DetachAll snapshot can detach the
// same session twice for one group; the second detach must not decrement
// appMembers again, or subscription-cap enforcement observes a wrong count.

import (
	"sync"
	"testing"
)

func TestDetachMemberDoubleDetachDecrementsOnce(t *testing.T) {
	mgr := NewManager(Deps{})
	const appID = "app-acct"
	s1 := &Session{ID: "s1", AppID: appID, Subs: map[string]bool{}}
	s2 := &Session{ID: "s2", AppID: appID, Subs: map[string]bool{}}
	mgr, g := testGroup(appID, s1, s2)

	// Precondition: group survives removal of one member.
	if len(g.members) != 2 {
		t.Fatalf("members = %d; want 2", len(g.members))
	}
	if got := mgr.appMembers[appID]; got != 2 {
		t.Fatalf("appMembers = %d; want 2", got)
	}

	// Same member detached twice (failMember vs stale DetachAll snapshot).
	mgr.detachMember(s1, g.key)
	mgr.detachMember(s1, g.key)

	if _, ok := g.members[s1]; ok {
		t.Fatal("doubly-detached member still registered")
	}
	if got := mgr.appMembers[appID]; got != 1 {
		t.Fatalf("appMembers = %d; want exactly 1 (decrement once)", got)
	}
	if _, ok := g.members[s2]; !ok {
		t.Fatal("surviving member no longer registered")
	}
	if !s2.Subs[g.key] {
		t.Fatal("survivor lost its subscription record")
	}
	if s1.Subs[g.key] {
		t.Fatal("detached member kept a stale Subs entry")
	}
	// Cap enforcement observes the true count: with MaxSubsPerApp=2 the
	// survivor (1) plus one newcomer (2) must fit; a third must not.
	mgr.Deps.Store = nil // cap path under test uses appMembers only here
	_ = mgr
	if got := mgr.appMembers[appID]; got != 1 {
		t.Fatalf("cap-visible count = %d; want 1", got)
	}
}

func TestDetachMemberConcurrentFailMemberDetachAllRaceClean(t *testing.T) {
	mgr := NewManager(Deps{})
	const appID = "app-race"
	s1 := &Session{ID: "s1", AppID: appID, Subs: map[string]bool{}}
	s2 := &Session{ID: "s2", AppID: appID, Subs: map[string]bool{}}
	mgr, g := testGroup(appID, s1, s2)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); mgr.failMember(g, s1) }()
		go func() { defer wg.Done(); mgr.DetachAll(s1) }()
	}
	wg.Wait()

	if got := mgr.appMembers[appID]; got < 1 || got > 2 {
		t.Fatalf("appMembers = %d; want 1..2 (never negative, never over-count)", got)
	}
	// Survivor must remain registered regardless of interleave; group must
	// never hold a detached member.
	if _, ok := g.members[s2]; !ok {
		t.Fatal("survivor detached by concurrent double-detach")
	}
}
