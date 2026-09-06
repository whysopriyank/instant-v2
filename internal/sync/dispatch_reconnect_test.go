package sync

// White-box hermetic tests for RT-002a (watermark semantics) and RT-002d
// (reconnect convergence). No database, no sockets: sessions carry fake
// SendRaw/Close writers and groups are built directly, reusing the
// dispatch_outcome_test.go harness.

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
)

// TestWatermarkMatchesDeliveredGeneration pins RT-002a at the fan-out
// seam: every frame of one generation carries the processed-tx-id of the
// generation the transport accepted — on both wire classes (WS nodelist
// and SSE tree) and for delta and full members alike. The precise claim
// is "accepted by the transport with failure-triggered reconnect", not
// "written to the client": WS SendRaw is a synchronous socket write, but
// SSE SendRaw only enqueues (overflow surfaces as an error and detaches).
// What this test rules out is a watermark that silently means "computed"
// or "queued" while certifying a different generation.
func TestWatermarkMatchesDeliveredGeneration(t *testing.T) {
	var treeFull, treeDelta [][]byte
	full := &Session{Subs: map[string]bool{}, SendRaw: func(b []byte) error {
		treeFull = append(treeFull, b)
		return nil
	}}
	delta := &Session{Subs: map[string]bool{}, SendRaw: func(b []byte) error {
		treeDelta = append(treeDelta, b)
		return nil
	}}
	mgrT, gT := testGroup("app1", full, delta)
	gT.members[delta] = member{sess: delta, delta: true}

	gen := treeFrame()
	gen.PatchJSON = json.RawMessage(`{"todos":[{"id":"e1"}]}`)
	if err := mgrT.dispatchGroup(gT, gen); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	// Nodelist (WS) leg: empty payload still carries the generation's
	// watermark through the render path.
	var nodeSent [][]byte
	nodeSess := &Session{Subs: map[string]bool{}, SendRaw: func(b []byte) error {
		nodeSent = append(nodeSent, b)
		return nil
	}}
	mgrN := NewManager(Deps{})
	subN := &reactive.Subscription{ID: "grp-node", AppID: "app1", Query: json.RawMessage(`{"todos":{}}`)}
	gN := &queryGroup{key: "grp-node", class: wireNodelist, appID: "app1", sub: subN,
		cat: &platform.AttrCatalog{}, members: map[*Session]member{}}
	mgrN.groups["grp-node"] = gN
	gN.members[nodeSess] = member{sess: nodeSess}
	nodeSess.Subs["grp-node"] = true
	nodeGen := reactive.Frame{SubID: "grp-node", QueryJSON: subN.Query,
		ResultJSON: json.RawMessage(`{"data":{}}`), ProcessedTxID: 7}
	if err := mgrN.dispatchGroup(gN, nodeGen); err != nil {
		t.Fatalf("nodelist dispatch: %v", err)
	}

	for name, frames := range map[string][][]byte{
		"tree/full": treeFull, "tree/delta": treeDelta, "nodelist": nodeSent,
	} {
		if len(frames) != 1 {
			t.Fatalf("%s: got %d frames; want exactly the delivered generation", name, len(frames))
		}
		if !strings.Contains(string(frames[0]), `"processed-tx-id":7`) {
			t.Fatalf("%s: frame lacks the delivered watermark: %s", name, frames[0])
		}
	}
}

// TestSSESnapshotErrorEndsStream pins the swallowed-error fix: a failed
// initial refresh (here a rule-loader-style failure) is returned, so the
// caller pushes 500 and ends the stream instead of leaving the subscriber
// connected with no state — and nothing is served.
func TestSSESnapshotErrorEndsStream(t *testing.T) {
	store := reactive.NewStore()
	rawQ := json.RawMessage(`{"todos":{}}`)
	key := groupKey("app", wireNodelist, rawQ, false)
	sub := &reactive.Subscription{ID: key, AppID: "app", Query: rawQ}
	if _, err := store.Add(sub); err != nil {
		t.Fatalf("store add: %v", err)
	}
	mgr := NewManager(Deps{Store: store})
	var sent []Frame
	sess := &Session{AppID: "app", Send: func(f Frame) error {
		sent = append(sent, f)
		return nil
	}}
	h := &SSEHandler{Manager: mgr, Store: store,
		Refresh: func(context.Context, *reactive.Subscription) (json.RawMessage, error) {
			return nil, errors.New("injected refresh failure")
		}}
	if err := h.snapshot(context.Background(), sess, Frame{"q": rawQ}); err == nil {
		t.Fatal("snapshot swallowed the refresh error; caller cannot end the stream")
	}
	if len(sent) != 0 {
		t.Fatalf("served %d frames despite refresh failure", len(sent))
	}
}

// TestReconnectConvergesAfterSendFailure pins RT-002d through the real
// seams: members attach via attachGroup, the generation is published
// through sub.Emit (the exact call the notifier makes), the failed member
// is detached by dispatch, and the rejoined member's initial answer comes
// from snapshotOrRefresh. A healthy control member certifies the
// generation's watermark; the rejoiner's answer must equal the served
// result exactly — the failed transaction is not skipped.
func TestReconnectConvergesAfterSendFailure(t *testing.T) {
	ctx := context.Background()
	doc := &perms.RuleDoc{Raw: json.RawMessage(`{"allow":"all"}`)}
	mgr := NewManager(Deps{
		Store: reactive.NewStore(),
		Rules: fixedRules(doc),
	})
	rawQ := json.RawMessage(`{"todos":{}}`)
	mkSess := func(id string, send func([]byte) error) *Session {
		return &Session{ID: id, AppID: "app", Subs: map[string]bool{},
			SendRaw: send, Close: func() {}}
	}
	var goodSent [][]byte
	sessF := mkSess("f", func([]byte) error { return errors.New("conn reset") })
	sessG := mkSess("g", func(b []byte) error {
		goodSent = append(goodSent, b)
		return nil
	})
	attach := func(s *Session) *queryGroup {
		t.Helper()
		g, err := mgr.attachGroup(ctx, s, rawQ, map[string]bool{}, &platform.AttrCatalog{}, wireTree, doc)
		if err != nil {
			t.Fatalf("attach %s: %v", s.ID, err)
		}
		return g
	}
	gF := attach(sessF)
	if gG := attach(sessG); gG != gF {
		t.Fatal("members of one query must share one group")
	}

	// Publish generation 7 (contains e1) exactly as the notifier does.
	gen := treeFrame()
	if err := gF.sub.Emit(gen); err != nil {
		t.Fatalf("generation must succeed via the healthy sibling: %v", err)
	}
	if _, ok := sessF.Subs[gF.key]; ok {
		t.Fatal("failed member still attached")
	}
	// Control: the healthy sibling certified watermark 7.
	if len(goodSent) != 1 || !strings.Contains(string(goodSent[0]), `"processed-tx-id":7`) {
		t.Fatal("healthy sibling did not certify generation 7")
	}
	// Notifier post-emit bookkeeping (reactive refreshOneAttempt: snapshot
	// then watermark, jointly after acceptable fan-out). Replicated here
	// because this test drives publication directly instead of running a
	// notifier drain.
	gF.sub.SetSnapshot(gen.ResultJSON)
	gF.sub.TxID.Store(gen.ProcessedTxID)

	// Reconnect through the real attach path; the doc is unchanged, so
	// the rejoiner shares the group without disturbing it.
	sessR := mkSess("r", func([]byte) error { return errors.New("initial answer must not fan out") })
	if gR := attach(sessR); gR != gF {
		t.Fatal("rejoiner did not land on the live group")
	}
	// Its initial answer must equal the served generation exactly, with
	// no recompute that could certify a different state. WS initial
	// answers carry no watermark (processed-tx-id appears only on
	// dispatch frames), so exact-state equality is the WS convergence
	// assertion; the snapshot/watermark pair below is the watermark
	// seam the SSE rejoin path actually reads (SnapshotPair).
	recomputed := false
	answer, err := mgr.snapshotOrRefresh(ctx, func(context.Context, *reactive.Subscription) (json.RawMessage, error) {
		recomputed = true
		return nil, errors.New("must not recompute")
	}, gF.sub)
	if err != nil {
		t.Fatalf("reconnect initial answer: %v", err)
	}
	if recomputed {
		t.Fatal("reconnect recomputed instead of converging on the snapshot")
	}
	if string(answer) != string(gen.ResultJSON) {
		t.Fatalf("reconnected member diverged from the served generation:\n got %s\nwant %s", answer, gen.ResultJSON)
	}
	snap, tx := gF.sub.SnapshotPair()
	if string(snap) != string(gen.ResultJSON) || tx != gen.ProcessedTxID {
		t.Fatalf("rejoin pair = (%s, %d); want served generation (result, 7)", snap, tx)
	}
}

func fixedRules(doc *perms.RuleDoc) func(context.Context, string) (*perms.RuleDoc, error) {
	return func(context.Context, string) (*perms.RuleDoc, error) { return doc, nil }
}
