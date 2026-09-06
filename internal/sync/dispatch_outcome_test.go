package sync

// White-box hermetic tests for RT-002 fan-out outcomes. No database, no
// sockets: sessions carry fake SendRaw/Close writers and groups are built
// directly. DB-backed red/green for the full path remains owed to a runnable
// environment (see rt001_rebind_test.go).

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/instant-v2/instant-v2/internal/reactive"
)

func testGroup(appID string, members ...*Session) (*Manager, *queryGroup) {
	mgr := NewManager(Deps{})
	sub := &reactive.Subscription{ID: "grp-test", AppID: appID, Query: json.RawMessage(`{"todos":{}}`)}
	g := &queryGroup{
		key:     "grp-test",
		class:   wireTree,
		appID:   appID,
		sub:     sub,
		members: map[*Session]member{},
	}
	mgr.groups["grp-test"] = g
	mgr.appMembers[appID] = len(members)
	for _, s := range members {
		if s.Subs == nil {
			s.Subs = map[string]bool{}
		}
		g.members[s] = member{sess: s}
		s.Subs["grp-test"] = true
	}
	sub.Emit = func(fr reactive.Frame) error { return mgr.dispatchGroup(g, fr) }
	return mgr, g
}

func treeFrame() reactive.Frame {
	return reactive.Frame{
		SubID:         "grp-test",
		QueryJSON:     json.RawMessage(`{"todos":{}}`),
		ResultJSON:    json.RawMessage(`{"data":{"todos":[{"id":"e1"}]}}`),
		ProcessedTxID: 7,
	}
}

// Render failure must serve nothing and report the error so the generation
// is retried, never published (RT-002b).
func TestDispatchRenderFailureServesNothing(t *testing.T) {
	var sent [][]byte
	sess := &Session{SendRaw: func(b []byte) error { sent = append(sent, b); return nil }}
	mgr, g := testGroup("app1", sess)
	bad := treeFrame()
	bad.ResultJSON = json.RawMessage(`not json{{{`)
	if err := mgr.dispatchGroup(g, bad); err == nil {
		t.Fatal("render failure must return an error")
	}
	if len(sent) != 0 {
		t.Fatalf("render failure served %d frames; want none", len(sent))
	}
}

// A member send failure detaches and closes exactly that member; siblings
// are still served and the generation succeeds (RT-002c).
func TestDispatchSendFailureDetachesOnlyFailedMember(t *testing.T) {
	var goodSent [][]byte
	closed := 0
	bad := &Session{
		Subs:    map[string]bool{},
		SendRaw: func(b []byte) error { return errors.New("conn reset") },
		Close:   func() { closed++ },
	}
	good := &Session{
		Subs:    map[string]bool{},
		SendRaw: func(b []byte) error { goodSent = append(goodSent, b); return nil },
	}
	mgr, g := testGroup("app1", bad, good)
	if err := mgr.dispatchGroup(g, treeFrame()); err != nil {
		t.Fatalf("sibling-served generation must succeed: %v", err)
	}
	if bad.Subs["grp-test"] {
		t.Fatal("failed member still attached")
	}
	if closed != 1 {
		t.Fatalf("failed member Close called %d times; want 1", closed)
	}
	if len(goodSent) != 1 || len(goodSent[0]) == 0 {
		t.Fatalf("sibling got %d frames; want 1 non-empty", len(goodSent))
	}
	if !good.Subs["grp-test"] {
		t.Fatal("healthy sibling detached")
	}
}

// No empty frame may reach the wire on any path (RT-002e).
func TestDispatchNeverEmitsEmptyFrames(t *testing.T) {
	var sent [][]byte
	sess := &Session{SendRaw: func(b []byte) error { sent = append(sent, b); return nil }}
	mgr, g := testGroup("app1", sess)
	if err := mgr.dispatchGroup(g, treeFrame()); err != nil {
		t.Fatalf("valid generation must succeed: %v", err)
	}
	for _, b := range sent {
		if len(b) == 0 {
			t.Fatal("empty frame emitted")
		}
	}
}
