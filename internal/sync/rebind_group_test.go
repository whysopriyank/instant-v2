package sync

// RT-001d preparation: shared subscription groups cannot mix members
// authorized under incompatible rule versions.
//
// Desired invariant: a joiner admitted under different persisted rules than
// the group forces a reload and rebind — the group gate moves to the newest
// doc and the possibly stale snapshot is dropped so the joiner's initial
// answer refreshes under the new gate. A joiner matching the group gate
// shares it without disturbing anything. White-box: attachGroup is
// unexported; hermetic (fake Rules loader,Seeded snapshot, no database, no
// sockets).

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
)

func TestVersionMismatchedJoinRebindsGroup(t *testing.T) {
	ctx := context.Background()
	allowDoc := &perms.RuleDoc{Raw: json.RawMessage(`{"allow":"all"}`)}
	denyDoc := &perms.RuleDoc{Raw: json.RawMessage(`{"deny":"all"}`)}

	var current atomic.Pointer[perms.RuleDoc]
	current.Store(allowDoc)
	mgr := NewManager(Deps{
		Store: reactive.NewStore(),
		Rules: func(context.Context, string) (*perms.RuleDoc, error) {
			return current.Load(), nil
		},
	})
	rawQ := json.RawMessage(`{"todos":{}}`)

	// Member A attaches under allow; a served allow generation seeds the
	// group snapshot, as snapshotOrRefresh would.
	sessA := &Session{ID: "a", AppID: "app", Subs: map[string]bool{}}
	g, err := mgr.attachGroup(ctx, sessA, rawQ, map[string]bool{}, &platform.AttrCatalog{}, wireNodelist, allowDoc)
	if err != nil {
		t.Fatalf("attach A: %v", err)
	}
	g.sub.SetSnapshot(json.RawMessage(`{"allow-content":true}`))

	// Persisted rules move to deny while the group still runs allow (no
	// invalidation has re-gated it yet). Member B joins carrying a deny
	// doc: the mismatch must reload, rebind, and drop the stale snapshot.
	current.Store(denyDoc)
	sessB := &Session{ID: "b", AppID: "app", Subs: map[string]bool{}}
	g2, err := mgr.attachGroup(ctx, sessB, rawQ, map[string]bool{}, &platform.AttrCatalog{}, wireNodelist, denyDoc)
	if err != nil {
		t.Fatalf("attach B: %v", err)
	}
	if g2 != g {
		t.Fatal("mismatched join must reuse the group, not fork it")
	}
	gate, ok := g.sub.AttachCtx.(*QueryGate)
	if !ok || gate == nil || GateHash(gate.Rules) != GateHash(denyDoc) {
		t.Fatal("RT-001d RED: group gate did not move to the newest doc")
	}
	if snap := g.sub.Snapshot(); snap != nil {
		t.Fatalf("RT-001d RED: stale snapshot survived the rebind: %s", snap)
	}
	if _, ok := g.members[sessA]; !ok {
		t.Fatal("rebind must not evict the existing member")
	}
	if _, ok := g.members[sessB]; !ok {
		t.Fatal("joiner was not admitted")
	}

	// Member C joins under the now-current deny doc: no churn — gate and
	// (re-seeded) snapshot are shared untouched.
	g.sub.SetSnapshot(json.RawMessage(`{"deny-content":true}`))
	sessC := &Session{ID: "c", AppID: "app", Subs: map[string]bool{}}
	if _, err := mgr.attachGroup(ctx, sessC, rawQ, map[string]bool{}, &platform.AttrCatalog{}, wireNodelist, denyDoc); err != nil {
		t.Fatalf("attach C: %v", err)
	}
	gate, _ = g.sub.AttachCtx.(*QueryGate)
	if GateHash(gate.Rules) != GateHash(denyDoc) {
		t.Fatal("matching join disturbed the group gate")
	}
	if snap := g.sub.Snapshot(); string(snap) != `{"deny-content":true}` {
		t.Fatalf("matching join disturbed the group snapshot: %s", snap)
	}
	if len(g.members) != 3 {
		t.Fatalf("members = %d, want 3", len(g.members))
	}
}

// TestEmitRejectsSupersededGeneration is the pause-after-final-check
// test: publication pauses inside Emit (past every generation-time
// authorization check), a real re-gate installs deny, publication
// resumes — and must emit nothing. No stale frame, no snapshot, no
// watermark advance. No sleeps: channel ordering is total.
func TestEmitRejectsSupersededGeneration(t *testing.T) {
	ctx := context.Background()
	allowDoc := &perms.RuleDoc{Raw: json.RawMessage(`{"allow":"all"}`)}
	denyDoc := &perms.RuleDoc{Raw: json.RawMessage(`{"deny":"all"}`)}

	var current atomic.Pointer[perms.RuleDoc]
	current.Store(allowDoc)
	mgr := NewManager(Deps{
		Store: reactive.NewStore(),
		Rules: func(context.Context, string) (*perms.RuleDoc, error) {
			return current.Load(), nil
		},
	})
	rawQ := json.RawMessage(`{"todos":{}}`)
	var sent [][]byte
	sess := &Session{ID: "s", AppID: "app", Subs: map[string]bool{},
		SendRaw: func(b []byte) error {
			sent = append(sent, b)
			return nil
		}}
	g, err := mgr.attachGroup(ctx, sess, rawQ, map[string]bool{}, &platform.AttrCatalog{}, wireTree, allowDoc)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}

	orig := g.sub.Emit
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	g.sub.Emit = func(fr reactive.Frame) error {
		once.Do(func() { close(entered) })
		<-release
		return orig(fr)
	}
	gen := treeFrame()
	gen.Gen = g.sub.Gen.Load() // stamp exactly as the notifier does
	done := make(chan error, 1)
	go func() { done <- g.sub.Emit(gen) }()

	<-entered
	current.Store(denyDoc)
	if _, _, err := mgr.RefreshGate(ctx, g.sub); err != nil {
		t.Fatalf("re-gate: %v", err)
	}
	close(release)

	if err := <-done; !errors.Is(err, errGenerationSuperseded) {
		t.Fatalf("superseded Emit returned %v; want superseded drop", err)
	}
	if len(sent) != 0 {
		t.Fatalf("superseded generation fanned out %d frames", len(sent))
	}
	if snap := g.sub.Snapshot(); snap != nil {
		t.Fatalf("superseded generation seeded a snapshot: %s", snap)
	}
	if tx := g.sub.TxID.Load(); tx != 0 {
		t.Fatalf("superseded generation advanced the watermark to %d", tx)
	}
	gate, _ := g.sub.AttachCtx.(*QueryGate)
	if GateHash(gate.Rules) != GateHash(denyDoc) {
		t.Fatal("re-gate did not install deny; the test staged nothing")
	}
}

// TestFlightRejectsSupersededRefresh proves the initial-answer flight is
// coordinated with re-gating: the flight's refresh pauses mid-run, a real
// re-gate installs deny, the flight resumes — and must fail without
// seeding a snapshot or answering the waiter. No sleeps.
func TestFlightRejectsSupersededRefresh(t *testing.T) {
	ctx := context.Background()
	allowDoc := &perms.RuleDoc{Raw: json.RawMessage(`{"allow":"all"}`)}
	denyDoc := &perms.RuleDoc{Raw: json.RawMessage(`{"deny":"all"}`)}

	var current atomic.Pointer[perms.RuleDoc]
	current.Store(allowDoc)
	mgr := NewManager(Deps{
		Store: reactive.NewStore(),
		Rules: func(context.Context, string) (*perms.RuleDoc, error) {
			return current.Load(), nil
		},
	})
	sub := &reactive.Subscription{
		ID:        "flight",
		AppID:     "app",
		AttachCtx: NewQueryGate(allowDoc, false),
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	refresh := func(context.Context, *reactive.Subscription) (json.RawMessage, error) {
		once.Do(func() { close(entered) })
		<-release
		return json.RawMessage(`{"allow-content":true}`), nil
	}
	type outcome struct {
		res json.RawMessage
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		res, err := mgr.snapshotOrRefresh(ctx, refresh, sub)
		done <- outcome{res, err}
	}()

	<-entered
	current.Store(denyDoc)
	if _, _, err := mgr.RefreshGate(ctx, sub); err != nil {
		t.Fatalf("re-gate: %v", err)
	}
	close(release)

	out := <-done
	if !errors.Is(out.err, errGenerationSuperseded) {
		t.Fatalf("superseded flight returned (%s, %v); want superseded drop", out.res, out.err)
	}
	if snap := sub.Snapshot(); snap != nil {
		t.Fatalf("superseded flight seeded a snapshot: %s", snap)
	}
}

// TestRefreshGateSwapDropsSnapshot closes the gate-new/snapshot-old join
// window (RT-001d): installing a new gate clears the previous gate's
// snapshot, so a joiner admitted under the new doc can never read state
// rendered under the old one — it recomputes instead.
func TestRefreshGateSwapDropsSnapshot(t *testing.T) {
	ctx := context.Background()
	allowDoc := &perms.RuleDoc{Raw: json.RawMessage(`{"allow":"all"}`)}
	denyDoc := &perms.RuleDoc{Raw: json.RawMessage(`{"deny":"all"}`)}

	var current atomic.Pointer[perms.RuleDoc]
	current.Store(denyDoc)
	mgr := NewManager(Deps{
		Rules: func(context.Context, string) (*perms.RuleDoc, error) {
			return current.Load(), nil
		},
	})
	sub := &reactive.Subscription{
		ID:        "swap-clear",
		AppID:     "app",
		AttachCtx: NewQueryGate(allowDoc, false),
	}
	sub.SetSnapshot(json.RawMessage(`{"allow-content":true}`))

	gate, regated, err := mgr.RefreshGate(ctx, sub)
	if err != nil {
		t.Fatalf("RefreshGate: %v", err)
	}
	if !regated || gate == nil || GateHash(gate.Rules) != GateHash(denyDoc) {
		t.Fatal("expected a re-gate to the deny doc")
	}
	if snap := sub.Snapshot(); snap != nil {
		t.Fatalf("swapped gate kept the old snapshot: %s", snap)
	}
}

// TestSpliceAuthorizeMatrix pins the splice-authorization truth table
// (RT-001): the rule-unaware splice probe may serve only what the
// full-query oracle would render. Closed or dynamic-non-admin roots fail
// closed to full refresh; open and dynamic-admin roots may splice.
func TestSpliceAuthorizeMatrix(t *testing.T) {
	mustDoc := func(raw string) *perms.RuleDoc {
		t.Helper()
		doc, err := perms.ParseRuleDoc([]byte(raw))
		if err != nil {
			t.Fatalf("parse rule doc: %v", err)
		}
		return doc
	}
	open := mustDoc(`{"todos":{"allow":{"view":"true"}}}`)
	closed := mustDoc(`{"todos":{"allow":{"view":"false"}}}`)
	dyn := mustDoc(`{"todos":{"allow":{"view":"auth.uid != null"}}}`)
	mixed := mustDoc(`{"todos":{"allow":{"view":"true"}},"notes":{"allow":{"view":"false"}}}`)

	gated := func(doc *perms.RuleDoc, admin bool) *reactive.Subscription {
		sub := &reactive.Subscription{ID: "m", AppID: "app"}
		gate := NewQueryGate(doc, admin)
		sub.AttachCtx = gate
		sub.SetAuthGate(gate)
		return sub
	}

	cases := []struct {
		name string
		sub  *reactive.Subscription
		ets  []string
		want bool
	}{
		{"nil sub", nil, []string{"todos"}, false},
		{"no gate recorded", &reactive.Subscription{}, []string{"todos"}, false},
		{"open non-admin", gated(open, false), []string{"todos"}, true},
		{"open admin", gated(open, true), []string{"todos"}, true},
		{"absent doc splices", gated(nil, false), []string{"todos"}, true},
		{"closed non-admin", gated(closed, false), []string{"todos"}, false},
		{"closed admin", gated(closed, true), []string{"todos"}, false},
		{"dynamic non-admin", gated(dyn, false), []string{"todos"}, false},
		{"dynamic admin", gated(dyn, true), []string{"todos"}, true},
		{"mixed open plus closed", gated(mixed, false), []string{"todos", "notes"}, false},
		{"unlisted etype is open", gated(closed, false), []string{"other"}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := SpliceAuthorize(c.sub, c.ets); got != c.want {
				t.Fatalf("SpliceAuthorize = %v; want %v", got, c.want)
			}
		})
	}
}
