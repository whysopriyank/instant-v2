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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func TestVersionStaleJoinRebindsGroupBeforeAdmission(t *testing.T) {
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

	first := &Session{ID: "first", AppID: "app", Subs: map[string]bool{}}
	g, err := mgr.attachGroup(ctx, first, rawQ, map[string]bool{}, &platform.AttrCatalog{}, wireNodelist, allowDoc)
	if err != nil {
		t.Fatalf("attach first: %v", err)
	}
	g.sub.SetSnapshot(json.RawMessage(`{"allow-content":true}`))

	// The caller's admission read is stale: persisted rules changed after it
	// returned, but before the existing group admits the new member.
	current.Store(denyDoc)
	staleRead := allowDoc
	second := &Session{ID: "second", AppID: "app", Subs: map[string]bool{}}
	if _, err := mgr.attachGroup(ctx, second, rawQ, map[string]bool{}, &platform.AttrCatalog{}, wireNodelist, staleRead); err != nil {
		t.Fatalf("attach second: %v", err)
	}

	gate, _ := g.sub.AttachCtx.(*QueryGate)
	if gate == nil || GateHash(gate.Rules) != GateHash(denyDoc) {
		t.Fatal("RT-001d RED: stale admission read left the group under the old allow gate")
	}
	if snap := g.sub.Snapshot(); snap != nil {
		t.Fatalf("RT-001d RED: stale snapshot survived the admission rebind: %s", snap)
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

// TestSwapMidFanOutStopsSpreadAndRefusesCommit stages the exact P1
// interleaving: the admission check passes, then a real re-gate lands
// inside the first member's send — after the check, mid-fan-out, before
// publication. The stale spread must stop at the next member, Emit must
// fail, and the coordinated commit must refuse the stale generation so
// no allow-era snapshot or watermark is restored over the deny that
// cleared it. Either member may send first (map order), so both carry
// the trigger; exactly one frame total may go out — the irreducible
// remainder, never certified.
func TestSwapMidFanOutStopsSpreadAndRefusesCommit(t *testing.T) {
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

	var mu sync.Mutex
	var frames1, frames2 [][]byte
	var swapped atomic.Bool
	var g *queryGroup
	mkSend := func(dst *[][]byte) func([]byte) error {
		return func(b []byte) error {
			// First send anywhere trips a REAL re-gate (deny install:
			// epoch bump + snapshot clear), then succeeds — the bytes
			// are already admitted and cannot be recalled.
			if swapped.CompareAndSwap(false, true) {
				current.Store(denyDoc)
				if _, _, err := mgr.RefreshGate(ctx, g.sub); err != nil {
					t.Errorf("mid-fan-out re-gate: %v", err)
				}
			}
			mu.Lock()
			*dst = append(*dst, b)
			mu.Unlock()
			return nil
		}
	}
	s1 := &Session{ID: "s1", AppID: "app", Subs: map[string]bool{}, SendRaw: mkSend(&frames1)}
	s2 := &Session{ID: "s2", AppID: "app", Subs: map[string]bool{}, SendRaw: mkSend(&frames2)}
	var err error
	g, err = mgr.attachGroup(ctx, s1, rawQ, map[string]bool{}, &platform.AttrCatalog{}, wireTree, allowDoc)
	if err != nil {
		t.Fatalf("attach s1: %v", err)
	}
	if _, err = mgr.attachGroup(ctx, s2, rawQ, map[string]bool{}, &platform.AttrCatalog{}, wireTree, allowDoc); err != nil {
		t.Fatalf("attach s2: %v", err)
	}

	gen := treeFrame()
	gen.Gen = g.sub.Gen.Load() // stamp exactly as the notifier does
	if g.sub.Emit == nil {
		t.Fatal("attach did not install Emit")
	}
	if err := g.sub.Emit(gen); !errors.Is(err, errGenerationSuperseded) {
		t.Fatalf("mid-fan-out swap: Emit returned %v; want superseded drop", err)
	}
	mu.Lock()
	total := len(frames1) + len(frames2)
	mu.Unlock()
	if total != 1 {
		t.Fatalf("stale spread reached %d members; want exactly the irreducible one", total)
	}
	// Honest remainder, pinned rather than wished away: the one admitted
	// send went out (its wire bytes postdate the swap by construction of
	// the trigger, watermark included) and its member stays attached.
	mu.Lock()
	var stale []byte
	if len(frames1) == 1 {
		stale = frames1[0]
	} else {
		stale = frames2[0]
	}
	mu.Unlock()
	if !bytes.Contains(stale, []byte(`"e1"`)) {
		t.Fatalf("irreducible frame is not the stale generation: %s", stale)
	}
	sessMu := func(s *Session) bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.Subs[g.key]
	}
	if !sessMu(s1) || !sessMu(s2) {
		t.Fatal("partial generation detached a member; healing requires attachment")
	}

	// The commit must refuse the stale generation: no allow-era snapshot
	// restored, no watermark advanced.
	if mgr.PublishGeneration(g.sub, gen.Gen, gen.ResultJSON, gen.ProcessedTxID) {
		t.Fatal("stale generation committed over the deny-cleared snapshot")
	}
	if snap := g.sub.Snapshot(); snap != nil {
		t.Fatalf("deny-cleared snapshot re-poisoned: %s", snap)
	}
	if tx := g.sub.TxID.Load(); tx != 0 {
		t.Fatalf("watermark advanced to %d for a refused generation", tx)
	}

	// Healing leg: the armed retry re-serves every attached member under
	// the new gate at the same txID, superseding the stale envelope.
	heal := treeFrame()
	heal.Gen = g.sub.Gen.Load()
	heal.ResultJSON = json.RawMessage(`{"data":{"todos":[]}}`)
	if err := g.sub.Emit(heal); err != nil {
		t.Fatalf("healing generation refused: %v", err)
	}
	mu.Lock()
	n1, n2 := len(frames1), len(frames2)
	last1, last2 := frames1[n1-1], frames2[n2-1]
	mu.Unlock()
	if n1 != 2 || n2 != 1 {
		// One member sent first (stale + heal = 2 frames); the other was
		// stopped pre-send (heal only = 1). Either order is legal.
		if n1 != 1 || n2 != 2 {
			t.Fatalf("healing fan-out = (%d, %d); want (2,1) or (1,2)", n1, n2)
		}
		last1, last2 = last2, last1
	}
	if !bytes.Contains(last1, []byte(`"todos":[]`)) || !bytes.Contains(last2, []byte(`"todos":[]`)) {
		t.Fatalf("members did not converge on deny-state:\n%s\n%s", last1, last2)
	}
	if !mgr.PublishGeneration(g.sub, heal.Gen, heal.ResultJSON, heal.ProcessedTxID) {
		t.Fatal("healing generation refused; commit path is broken, not strict")
	}

	// Positive control: the current epoch still commits.
	cur := g.sub.Gen.Load()
	if !mgr.PublishGeneration(g.sub, cur, json.RawMessage(`{"deny":"state"}`), 8) {
		t.Fatal("current generation refused; commit path is broken, not strict")
	}
	if snap := g.sub.Snapshot(); string(snap) != `{"deny":"state"}` {
		t.Fatalf("current commit did not publish: %s", snap)
	}
	if tx := g.sub.TxID.Load(); tx != 8 {
		t.Fatalf("watermark = %d; want 8", tx)
	}
	gate, _ := g.sub.AttachCtx.(*QueryGate)
	if GateHash(gate.Rules) != GateHash(denyDoc) {
		t.Fatal("re-gate did not install deny; the test staged nothing")
	}
}

// TestMidFanOutSwapHealsThroughNotifierRetry executes the automatic
// recovery the unit test above only stages: a real notifier drain fans
// a stale generation, a real re-gate lands mid-fan-out, Emit refuses,
// and the notifier's own retry (no manual re-emit) re-serves every
// attached member under the new gate. A follow-up invalidation then
// proves continued liveness. Member order is map-random, so assertions
// are order-agnostic; all synchronization is frame-driven (the ~100ms
// retry timer is production behavior awaited under a bound, never slept
// on).
func TestMidFanOutSwapHealsThroughNotifierRetry(t *testing.T) {
	ctx := context.Background()
	allowDoc := &perms.RuleDoc{Raw: json.RawMessage(`{"allow":"all"}`)}
	denyDoc := &perms.RuleDoc{Raw: json.RawMessage(`{"deny":"all"}`)}
	allowResult := json.RawMessage(`{"data":{"todos":[{"id":"e1"}]}}`)
	denyResult := json.RawMessage(`{"data":{"todos":[]}}`)

	var current atomic.Pointer[perms.RuleDoc]
	current.Store(allowDoc)
	store := reactive.NewStore()
	mgr := NewManager(Deps{
		Store: store,
		Rules: func(context.Context, string) (*perms.RuleDoc, error) {
			return current.Load(), nil
		},
	})
	rawQ := json.RawMessage(`{"todos":{}}`)
	topics := map[string]bool{"attr1": true}

	// Refresh models rules changing mid-flight: the first computation
	// runs under allow, every later one under deny.
	var refreshCalls atomic.Int64
	refresh := func(context.Context, *reactive.Subscription) (json.RawMessage, error) {
		if refreshCalls.Add(1) == 1 {
			return allowResult, nil
		}
		return denyResult, nil
	}
	notifier := &reactive.Notifier{
		Store:   store,
		Refresh: refresh,
		Revalidate: func(ctx context.Context, sub *reactive.Subscription) (bool, error) {
			_, changed, err := mgr.RefreshGate(ctx, sub)
			return changed, err
		},
		Publish: mgr.PublishGeneration,
	}
	nctx, stop := context.WithCancel(context.Background())
	defer stop()
	go notifier.Run(nctx)

	var mu sync.Mutex
	frames := map[string][][]byte{}
	arrived := make(chan struct{}, 8)
	var swapped atomic.Bool
	var g *queryGroup
	mkSend := func(id string) func([]byte) error {
		return func(b []byte) error {
			// First send anywhere installs a REAL deny re-gate, then
			// succeeds: the admitted bytes go out post-swap.
			if swapped.CompareAndSwap(false, true) {
				current.Store(denyDoc)
				if _, _, err := mgr.RefreshGate(ctx, g.sub); err != nil {
					t.Errorf("mid-fan-out re-gate: %v", err)
				}
			}
			mu.Lock()
			frames[id] = append(frames[id], b)
			mu.Unlock()
			arrived <- struct{}{}
			return nil
		}
	}
	mkSess := func(id string) *Session {
		return &Session{ID: id, AppID: "app", Subs: map[string]bool{}, SendRaw: mkSend(id)}
	}
	s1, s2 := mkSess("s1"), mkSess("s2")
	var err error
	g, err = mgr.attachGroup(ctx, s1, rawQ, topics, &platform.AttrCatalog{}, wireTree, allowDoc)
	if err != nil {
		t.Fatalf("attach s1: %v", err)
	}
	if _, err = mgr.attachGroup(ctx, s2, rawQ, topics, &platform.AttrCatalog{}, wireTree, allowDoc); err != nil {
		t.Fatalf("attach s2: %v", err)
	}

	// Attempt 1 fans stale; the retry re-serves deny; a follow-up
	// invalidation proves liveness. Exactly 1+2+2 frames, ever: the
	// retry timer fires exactly once and attempt 3 schedules nothing.
	notifier.Notify(ctx, "app", []string{"attr1"}, 5)
	waitFrames := func(n int, what string) {
		t.Helper()
		deadline := time.After(10 * time.Second)
		for i := 0; i < n; i++ {
			select {
			case <-arrived:
			case <-deadline:
				t.Fatalf("timed out waiting for %s frame %d/%d", what, i+1, n)
			}
		}
	}
	waitFrames(3, "healing")
	// Attempt 3 runs strictly after attempt 2's publish (drain passes
	// serialize per subscription), so observing its frames proves the
	// commit landed without any sleep or poll.
	notifier.Notify(ctx, "app", []string{"attr1"}, 6)
	waitFrames(2, "liveness")

	mu.Lock()
	defer mu.Unlock()
	f1, f2 := frames["s1"], frames["s2"]
	// Order-agnostic: one member saw stale then two deny envelopes; the
	// other saw deny twice. No member saw stale twice; none missed deny.
	var three, two [][]byte
	switch {
	case len(f1) == 3 && len(f2) == 2:
		three, two = f1, f2
	case len(f1) == 2 && len(f2) == 3:
		three, two = f2, f1
	default:
		t.Fatalf("fan-out = (%d, %d); want (3,2) or (2,3)", len(f1), len(f2))
	}
	if !bytes.Contains(three[0], []byte(`"e1"`)) {
		t.Fatalf("first envelope is not the stale generation: %s", three[0])
	}
	for i, f := range three[1:] {
		if !bytes.Contains(f, []byte(`"todos":[]`)) {
			t.Fatalf("healing envelope %d missed deny-state: %s", i, f)
		}
	}
	for i, f := range two {
		if !bytes.Contains(f, []byte(`"todos":[]`)) {
			t.Fatalf("converged envelope %d missed deny-state: %s", i, f)
		}
	}
	if snap := g.sub.Snapshot(); string(snap) != string(denyResult) {
		t.Fatalf("snapshot = %s; want committed deny-state", snap)
	}
	if tx := g.sub.TxID.Load(); tx != 6 {
		t.Fatalf("watermark = %d; want 6", tx)
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
