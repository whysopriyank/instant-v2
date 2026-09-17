package sync_test

// RT-002 task C: production-chain delivery-failure evidence.
//
// These tests drive the full production chain — PostgreSQL commit via
// Manager.Handle transact → OnCommit → reactive Notifier drain → refresh →
// group dispatch (Manager.dispatchGroup through sub.Emit) → coordinated
// PublishGeneration — with deterministic transport failure injected at the
// existing Session.SendRaw boundary. No production seam, no sockets, no
// scheduler sleeps: all waits are bounded (5-10s) deadline polls.
//
// Policy under test (DEC-001: explicit disconnect plus full replay):
//   - Case 1 (all delivery fails, generation still current): the generation
//     is WITHHELD — no snapshot, no watermark — members stay attached, and
//     the notifier's bounded retry re-arms the SAME txID until the transport
//     heals, committing exactly once. There is no certified gap to repair by
//     reconnect, so in-place healing is safe.
//   - Case 2 (one succeeds, one fails): the failed member is detached+closed
//     (it has a real gap), the healthy sibling certifies, and the failed
//     member's fresh session rejoins onto the live group converging on the
//     exact certified state + watermark, missed transaction included.
//
// Wire-shape note: WS (nodelist) refresh frames carry the RENDERED node list,
// not the raw instaql result stored as the snapshot. Exact-equality
// assertions therefore compare sub.SnapshotPair() against the certified
// refresh bytes (recorded at the Refresh seam) canonically, and compare the
// delivered frames by watermark + marker containment.

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
	"github.com/instant-v2/instant-v2/internal/storage"
	syncpkg "github.com/instant-v2/instant-v2/internal/sync"
)

// chainTapCall is one SendRaw invocation: the wire bytes offered to the
// transport and whether the transport accepted them.
type chainTapCall struct {
	txID float64
	ok   bool
}

// chainTap is a deterministic SendRaw replacement. While fail is set every
// call records the offered frame and returns an error (the transport accepts
// nothing); after FlipToSucceed calls succeed. All methods are safe for the
// notifier's drain workers and dispatch goroutines.
type chainTap struct {
	fail atomic.Bool

	mu    sync.Mutex
	calls []chainTapCall
}

func newFailingTap() *chainTap {
	t := &chainTap{}
	t.fail.Store(true)
	return t
}

func newHealthyTap() *chainTap { return &chainTap{} }

// send is installed as Session.SendRaw.
func (t *chainTap) send(b []byte) error {
	tx := parseChainTxID(b)
	ok := !t.fail.Load()
	t.mu.Lock()
	t.calls = append(t.calls, chainTapCall{txID: tx, ok: ok})
	t.mu.Unlock()
	if !ok {
		return errChainTransport
	}
	return nil
}

func (t *chainTap) snapshot() []chainTapCall {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]chainTapCall(nil), t.calls...)
}

func (t *chainTap) countTx(tx float64, ok bool) int {
	n := 0
	for _, c := range t.snapshot() {
		if c.txID == tx && c.ok == ok {
			n++
		}
	}
	return n
}

func (t *chainTap) total() int { return len(t.snapshot()) }

// chainTransportErr is the deterministic injected delivery failure.
type chainTransportErr struct{}

func (chainTransportErr) Error() string { return "chain test: injected SendRaw failure" }

var errChainTransport = chainTransportErr{}

func parseChainTxID(b []byte) float64 {
	var f map[string]any
	if err := json.Unmarshal(b, &f); err != nil {
		return -1
	}
	tx, _ := f["processed-tx-id"].(float64)
	return tx
}

// chainPublishCall records one coordinated-commit attempt.
type chainPublishCall struct {
	txID int64
	gen  uint64
	ok   bool
}

// chainStack wires the production chain over an isolated testkit database:
// Manager.Handle transact commits to PostgreSQL, OnCommit notifies the real
// Notifier, drains run the real refresh, dispatch fans out through the real
// group Emit, and Publish delegates to the real Manager.PublishGeneration
// while recording every attempt. Refresh results are recorded in drain
// order so tests can name the exact certified bytes per generation.
type chainStack struct {
	mgr   *syncpkg.Manager
	store *reactive.Store
	appID string

	titleID string

	mu           sync.Mutex
	refreshCalls int
	refreshSubs  []*reactive.Subscription
	results      []json.RawMessage
	publishes    []chainPublishCall
}

func newChainStack(t *testing.T) *chainStack {
	t.Helper()
	pool := newPostgres(t)
	ctx := context.Background()

	st := storage.New(pool)
	cats := platform.NewCatalogCache(pool, pool)
	appID := rand16()
	seedApp(t, st, appID)

	var title platform.Attr
	if err := st.WithTx(ctx, func(tx pgx.Tx) error {
		var err error
		title, err = platform.GetOrCreateAttr(ctx, tx, appID, "todos", "text", "blob", "one", false, true)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := cats.For(ctx, uuidStr(appID)); err != nil {
		t.Fatal(err)
	}

	cs := &chainStack{appID: uuidStr(appID), titleID: platform.UUIDToStr(title.ID)}
	store := reactive.NewStore()
	cs.store = store
	ex := &instaql.Executor{DB: pool}
	var mgr *syncpkg.Manager
	notifier := &reactive.Notifier{
		Store: store,
		Refresh: func(ctx context.Context, sub *reactive.Subscription) (json.RawMessage, error) {
			res, err := runQuery(ex, cats, sub)
			if err == nil {
				cs.mu.Lock()
				cs.refreshCalls++
				cs.refreshSubs = append(cs.refreshSubs, sub)
				cs.results = append(cs.results, append(json.RawMessage(nil), res...))
				cs.mu.Unlock()
			}
			return res, err
		},
		// Production-faithful coordinated commit, counted: every served
		// generation must pass through here exactly as cmd/instantd wires
		// it, so the count pins "commits exactly once / never".
		Publish: func(sub *reactive.Subscription, gen uint64, res json.RawMessage, txID int64) bool {
			ok := mgr.PublishGeneration(sub, gen, res, txID)
			cs.mu.Lock()
			cs.publishes = append(cs.publishes, chainPublishCall{txID: txID, gen: gen, ok: ok})
			cs.mu.Unlock()
			return ok
		},
	}
	runNotifier(t, notifier)

	mgr = syncpkg.NewManager(syncpkg.Deps{
		Rooms:    syncpkg.NewRoomHub(),
		DB:       st,
		Catalogs: cats,
		Store:    store,
		OnCommit: func(ctx context.Context, appID string, attrIDs []string, txID int64, attrsChanged bool) {
			notifier.Notify(ctx, appID, attrIDs, txID)
		},
	})
	cs.mgr = mgr
	return cs
}

// chainSub returns the single live subscription for the stack's attr topic.
// It must be called only at points where exactly one subscription exists;
// the length assertion itself pins no-leak / no-churn invariants.
func (cs *chainStack) chainSub(t *testing.T) *reactive.Subscription {
	t.Helper()
	subs := cs.mgr.Deps.Store.SubsForTopics([]string{cs.titleID})
	if len(subs) != 1 {
		t.Fatalf("live subscriptions for topic = %d; want exactly 1 (shared group)", len(subs))
	}
	return subs[0]
}

func (cs *chainStack) publishCount(txID int64) int {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	n := 0
	for _, p := range cs.publishes {
		if p.txID == txID {
			n++
		}
	}
	return n
}

func (cs *chainStack) refreshSnapshot() (calls int, subs []*reactive.Subscription, results []json.RawMessage) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.refreshCalls, append([]*reactive.Subscription(nil), cs.refreshSubs...),
		append([]json.RawMessage(nil), cs.results...)
}

// chainAttach attaches sessID to {"todos":{}} through the production
// add-query path and returns the session with tap installed.
func (cs *chainStack) chainAttach(t *testing.T, sessID string, tap *chainTap, closed *atomic.Int64) *syncpkg.Session {
	t.Helper()
	sess := &syncpkg.Session{
		ID:    sessID,
		AppID: cs.appID,
		Subs:  map[string]bool{},
		SendRaw: func(b []byte) error {
			return tap.send(append([]byte(nil), b...))
		},
	}
	if closed != nil {
		sess.Close = func() { closed.Add(1) }
	}
	rawQ := json.RawMessage(`{"todos":{}}`)
	replies, err := cs.mgr.Handle(context.Background(), sess, syncpkg.Frame{
		"op":              json.RawMessage(`"add-query"`),
		"q":               rawQ,
		"client-event-id": json.RawMessage(`"q1"`),
	})
	if err != nil {
		t.Fatalf("attach %s: %v", sessID, err)
	}
	if len(replies) != 1 {
		t.Fatalf("attach %s replies = %d; want 1", sessID, len(replies))
	}
	if op, _ := replies[0].GetOp(); op != "add-query-ok" {
		t.Fatalf("attach %s op = %q; want add-query-ok", sessID, op)
	}
	return sess
}

// chainTransact commits one todo row through the production transact path
// and returns the committed PostgreSQL transaction ID from transact-ok.
func (cs *chainStack) chainTransact(t *testing.T, marker, eventID string) int64 {
	t.Helper()
	steps, err := json.Marshal([]any{
		[]any{"add-triple", uuidStr(rand16()), cs.titleID, marker},
	})
	if err != nil {
		t.Fatal(err)
	}
	sess := &syncpkg.Session{ID: "tx-" + eventID, AppID: cs.appID, Subs: map[string]bool{}}
	replies, err := cs.mgr.Handle(context.Background(), sess, syncpkg.Frame{
		"op":              json.RawMessage(`"transact"`),
		"tx-steps":        steps,
		"client-event-id": json.RawMessage(`"` + eventID + `"`),
	})
	if err != nil {
		t.Fatalf("transact %s: %v", eventID, err)
	}
	if len(replies) != 1 {
		t.Fatalf("transact %s replies = %d; want 1", eventID, len(replies))
	}
	if op, _ := replies[0].GetOp(); op != "transact-ok" {
		t.Fatalf("transact %s op = %q; want transact-ok (%v)", eventID, op, replies[0])
	}
	var txID int64
	if err := json.Unmarshal(replies[0]["tx-id"], &txID); err != nil || txID == 0 {
		t.Fatalf("transact %s carries no tx-id: %v err=%v", eventID, replies[0], err)
	}
	return txID
}

// awaitChain polls cond until true or the bound lapses. No scheduler sleeps:
// 10ms polls under an explicit deadline, matching the existing sync-test
// convention for production retry timers.
func awaitChain(t *testing.T, bound time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(bound)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("timed out after %s waiting for %s", bound, what)
	}
}

// TestNotifierChainAllDeliveryFailsWithholdsAndHeals pins RT-002c Case 1
// over the production chain: a lone member whose transport rejects every
// frame must see no commit, an autonomous same-tx retry, and exactly-once
// corrective delivery once the transport heals.
//
//  1. Single member attached; its SendRaw always errors.
//  2. One transact commits tx1 to PostgreSQL (transact-ok names it).
//  3. Await >=2 dispatch attempts within 10s; every attempt carries tx1.
//     Each attempt recomputed (refreshCalls == attempts) through the real
//     refresh — attempts are full chain traversals, not bare re-sends.
//  4. With attempts observed and the transport still failing: snapshot is
//     nil, watermark is not tx1, Publish was never called for tx1, the
//     member is still attached (exactly its group), and Close never fired
//     (no disconnect for an uncertified generation).
//  5. Flip the transport to succeed; await the same-tx corrective delivery.
//  6. Exactly one successful delivery for tx1, snapshot+watermark == tx1,
//     exactly one Publish for tx1: committed exactly once, no duplicate.
func TestNotifierChainAllDeliveryFailsWithholdsAndHeals(t *testing.T) {
	cs := newChainStack(t)

	tap := newFailingTap()
	var closed atomic.Int64
	sess := cs.chainAttach(t, "c1-a", tap, &closed)

	const marker = "chain-c1-v1"
	tx1 := cs.chainTransact(t, marker, "t1")
	wantTx := float64(tx1)

	// Bounded retry: the first attempt fails fast on Notify drain; the
	// production retry timer (100ms base + stable jitter) re-arms the same
	// txID autonomously. Await the retry, not just the initial attempt.
	awaitChain(t, 10*time.Second, "same-tx retry dispatch attempts", func() bool {
		return tap.total() >= 2
	})
	calls := tap.snapshot()
	if len(calls) < 2 {
		t.Fatalf("dispatch attempts = %d; want >=2 (initial + same-tx retry)", len(calls))
	}
	for i, c := range calls {
		if c.txID != wantTx {
			t.Fatalf("attempt %d carried txID %v; want original transaction %v", i, c.txID, wantTx)
		}
		if c.ok {
			t.Fatalf("attempt %d unexpectedly succeeded while the transport fails", i)
		}
	}
	// Every attempt recomputed through the real refresh: full chain
	// traversals (commit → notify → drain → refresh → dispatch), proving
	// the retry path rather than a transport-level re-send.
	cs.mu.Lock()
	rc := cs.refreshCalls
	cs.mu.Unlock()
	if rc != len(calls) {
		t.Fatalf("refresh recomputes = %d for %d dispatch attempts; want 1:1 chain traversals", rc, len(calls))
	}

	// No commit while nothing was served: Publish never ran for tx1, the
	// snapshot is absent, and the watermark never advanced to tx1.
	if n := cs.publishCount(tx1); n != 0 {
		t.Fatalf("Publish ran %d time(s) for an unserved generation; want none", n)
	}
	sub := cs.chainSub(t)
	if snap := sub.Snapshot(); snap != nil {
		t.Fatalf("snapshot committed for an unserved generation: %s", snap)
	}
	if got := sub.TxID.Load(); got == tx1 {
		t.Fatalf("watermark advanced to %d with zero successful deliveries", got)
	}
	// In-place healing requires attachment: the member was neither
	// detached (still exactly its group) nor disconnected.
	if len(sess.Subs) != 1 {
		t.Fatalf("withheld member holds %d subscriptions; want exactly its group (kept for retry)", len(sess.Subs))
	}
	if n := closed.Load(); n != 0 {
		t.Fatalf("Close fired %d time(s) for an uncertified generation; want none (no disconnect, retry instead)", n)
	}

	// Release the transport failure: the next bounded retry is the
	// corrective delivery of the same transaction.
	tap.fail.Store(false)
	awaitChain(t, 10*time.Second, "same-tx corrective delivery", func() bool {
		return tap.countTx(wantTx, true) >= 1
	})
	// The commit lands synchronously after the successful fan-out on the
	// same drain worker; barrier on it, then assert singularity: exactly
	// one successful delivery and exactly one commit for tx1.
	awaitChain(t, 10*time.Second, "corrective commit of tx1", func() bool {
		return cs.publishCount(tx1) == 1 && sub.TxID.Load() == tx1
	})
	if n := tap.countTx(wantTx, true); n != 1 {
		t.Fatalf("successful deliveries for tx %v = %d; want exactly 1 (no duplicate corrective)", tx1, n)
	}
	if n := cs.publishCount(tx1); n != 1 {
		t.Fatalf("commits for tx %v = %d; want exactly 1 (no duplicate commit)", tx1, n)
	}
	snap, wm := sub.SnapshotPair()
	if wm != tx1 {
		t.Fatalf("watermark = %d; want corrective transaction %d", wm, tx1)
	}
	if !strings.Contains(string(snap), marker) {
		t.Fatalf("committed snapshot misses the corrective transaction row %q: %s", marker, snap)
	}
	if n := closed.Load(); n != 0 {
		t.Fatalf("Close fired %d time(s); the healed member never needed a disconnect", n)
	}
	if n := cs.mgr.Deps.Store.Len(); n != 1 {
		t.Fatalf("store subscriptions = %d; want 1 (no leak)", n)
	}
}

// TestNotifierChainPartialDeliveryCertifiesAndReconnects pins RT-002c/d
// Case 2 over the production chain: with two members sharing one group,
// the failing member is detached+closed while the healthy sibling certifies;
// the failed member's fresh session rejoins onto the live group converging
// on the exact certified state + watermark (missed transaction included),
// and subsequent transactions stay live on both sessions.
//
//  1. Two members, one failing SendRaw (+Close counting), one healthy.
//  2. One transact commits tx1. Healthy receives exactly one non-empty
//     refresh-ok with processed-tx-id == tx1; snapshot+watermark commit.
//  3. Failed detached (Subs entry gone, Close exactly once) with zero
//     successful bytes for tx1; group survives via the sibling.
//  4. Fresh session re-attaches the same query; SnapshotPair equals the
//     certified refresh bytes (canonical) + tx1, missed row included.
//  5. Second transact: both live sessions receive the new txID; watermark
//     advances; no subscription leak.
func TestNotifierChainPartialDeliveryCertifiesAndReconnects(t *testing.T) {
	cs := newChainStack(t)

	failTap := newFailingTap()
	var failClosed atomic.Int64
	sessF := cs.chainAttach(t, "c2-f", failTap, &failClosed)
	healthyTap := newHealthyTap()
	sessH := cs.chainAttach(t, "c2-h", healthyTap, nil)
	_ = sessH

	const marker1 = "chain-c2-v1"
	tx1 := cs.chainTransact(t, marker1, "t1")
	wantTx1 := float64(tx1)

	// Healthy sibling certifies: exactly one non-empty frame for tx1.
	awaitChain(t, 5*time.Second, "healthy sibling delivery of tx1", func() bool {
		return healthyTap.countTx(wantTx1, true) >= 1
	})
	// The commit follows the successful fan-out on the same drain worker.
	sub := cs.chainSub(t)
	awaitChain(t, 5*time.Second, "certification of tx1", func() bool {
		return sub.TxID.Load() == tx1
	})
	if n := healthyTap.countTx(wantTx1, true); n != 1 {
		t.Fatalf("healthy deliveries for tx %v = %d; want exactly 1", tx1, n)
	}
	for _, c := range healthyTap.snapshot() {
		if c.txID != wantTx1 || !c.ok {
			t.Fatalf("healthy tap saw unexpected call %+v; want only the tx1 success", c)
		}
	}
	snap1, wm1 := sub.SnapshotPair()
	if wm1 != tx1 {
		t.Fatalf("watermark = %d; want certified transaction %d", wm1, tx1)
	}
	if snap1 == nil || !strings.Contains(string(snap1), marker1) {
		t.Fatalf("certified snapshot misses tx1 row %q: %s", marker1, snap1)
	}

	// Failed member: detached (Subs entry removed) + closed exactly once,
	// and never served (zero successful bytes for tx1, though the same
	// generation was offered to it). Close follows detach in failMember,
	// and the healthy delivery above proves the attempt's dispatch ran, so
	// observing both barriers makes the Subs read race-free.
	awaitChain(t, 5*time.Second, "failed member detach+close", func() bool {
		return failClosed.Load() == 1
	})
	if len(sessF.Subs) != 0 {
		t.Fatalf("failed member still holds %d subscriptions; want detached", len(sessF.Subs))
	}
	if n := failClosed.Load(); n != 1 {
		t.Fatalf("failed member Close calls = %d; want exactly 1", n)
	}
	fcalls := failTap.snapshot()
	if len(fcalls) == 0 {
		t.Fatal("failed writer saw no dispatch attempt; failure premise unproven")
	}
	for i, c := range fcalls {
		if c.txID != wantTx1 {
			t.Fatalf("failed attempt %d carried txID %v; want the shared generation %v", i, c.txID, wantTx1)
		}
		if c.ok {
			t.Fatalf("failed writer recorded a success for tx %v; want zero successful bytes", tx1)
		}
	}
	// The shared group (and its certified snapshot) survives via the
	// healthy sibling.
	if n := cs.mgr.Deps.Store.Len(); n != 1 {
		t.Fatalf("store subscriptions = %d; want 1 (group survives via sibling)", n)
	}

	// Reconnect the failed member via a FRESH session on the same query.
	// It must converge on the exact certified state + watermark, missed
	// transaction included — never skipping tx1.
	reTap := newHealthyTap()
	cs.chainAttach(t, "c2-r", reTap, nil)
	sub2 := cs.chainSub(t)
	if sub2 != sub {
		t.Fatal("rejoiner did not land on the live shared group (subscription churned)")
	}
	rsnap, rwm := sub2.SnapshotPair()
	if rwm != tx1 {
		t.Fatalf("rejoiner watermark = %d; want certified %d (skipped or extra generation)", rwm, tx1)
	}
	_, _, results := cs.refreshSnapshot()
	if len(results) == 0 {
		t.Fatal("refresh result never recorded; certified-bytes comparison is vacuous")
	}
	if got, want := canonicalJSON(t, rsnap), canonicalJSON(t, results[0]); got != want {
		t.Fatalf("rejoiner diverged from the certified generation:\n got %s\nwant %s", got, want)
	}
	if !strings.Contains(string(rsnap), marker1) {
		t.Fatalf("rejoiner state misses the failed transaction row %q: %s", marker1, rsnap)
	}

	// Both live sessions stay live: a second transaction reaches each
	// exactly once with the new watermark, and the served pair advances.
	const marker2 = "chain-c2-v2"
	tx2 := cs.chainTransact(t, marker2, "t2")
	wantTx2 := float64(tx2)
	awaitChain(t, 5*time.Second, "healthy delivery of tx2", func() bool {
		return healthyTap.countTx(wantTx2, true) >= 1
	})
	awaitChain(t, 5*time.Second, "rejoined delivery of tx2", func() bool {
		return reTap.countTx(wantTx2, true) >= 1
	})
	if n := healthyTap.countTx(wantTx2, true); n != 1 {
		t.Fatalf("healthy deliveries for tx %v = %d; want exactly 1", tx2, n)
	}
	if n := reTap.countTx(wantTx2, true); n != 1 {
		t.Fatalf("rejoined deliveries for tx %v = %d; want exactly 1", tx2, n)
	}
	// The detached writer is never routed to again.
	if n := len(failTap.snapshot()); n != len(fcalls) {
		t.Fatalf("detached writer saw %d calls after detach (was %d); want no further routing", n, len(fcalls))
	}
	awaitChain(t, 5*time.Second, "watermark advance to tx2", func() bool {
		return sub2.TxID.Load() == tx2
	})
	snap2, wm2 := sub2.SnapshotPair()
	if wm2 != tx2 {
		t.Fatalf("watermark = %d; want %d", wm2, tx2)
	}
	if body := string(snap2); !strings.Contains(body, marker1) || !strings.Contains(body, marker2) {
		t.Fatalf("advanced snapshot misses rows %q/%q: %s", marker1, marker2, body)
	}
	if n := cs.mgr.Deps.Store.Len(); n != 1 {
		t.Fatalf("store subscriptions = %d; want 1 (no leak)", n)
	}
}
