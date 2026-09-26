// Package reactive owns live query subscriptions: registration, topic-indexed
// invalidation, and coalesced refresh fan-out. Port surface of v1's
// reactive/store + reactive/invalidator with the DataScript session state
// replaced by plain structures (docs/reference/02-architecture.md §5.3).
//
// Invalidation sources: the direct post-commit notifier (single-instance
// default) and waltail records (multi-instance). Both funnel into Invalidate.
package reactive

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/instant-v2/instant-v2/internal/metrics"
	"github.com/instant-v2/instant-v2/internal/tracing"
)

// Frame is one outbound refresh payload for a subscription.
//
// Delta-refresh sessions receive PatchJSON (a structural patch) instead of
// the full ResultJSON envelope when the change set is expressible as such;
// ResultJSON always carries the authoritative full result either way.
type Frame struct {
	SubID         string          `json:"-"`
	QueryJSON     json.RawMessage `json:"instaql-query"`
	ResultJSON    json.RawMessage `json:"instaql-result"`
	PatchJSON     json.RawMessage `json:"-"`
	ProcessedTxID int64           `json:"processed-tx-id"`
	// Gen is the rule-generation epoch stamped by the notifier at
	// attempt start (RT-001). Transports carrying a permission gate
	// refuse frames whose epoch no longer matches the subscription,
	// so a generation overtaken by a concurrent re-gate between
	// computation and fan-out is dropped instead of published.
	Gen uint64 `json:"-"`
}

// Subscription is one registered live query.
type Subscription struct {
	// AttachCtx carries transport-scoped metadata alongside the subscription.
	// The sync layer stores its permission-gate snapshot here (rule doc +
	// caller class) so refresh executors can reproduce exactly the visibility
	// the group was admitted under. Opaque to this package.
	AttachCtx any

	ID     string
	AppID  string
	Query  json.RawMessage
	Topics map[string]bool // attr-id set from the compiled plan
	// TxID is the last SERVED transaction watermark: stored only for
	// generations with an acceptable fan-out outcome, jointly after the
	// snapshot (RT-002a). Atomic: Notify reads it on caller goroutines
	// while drain workers write it.
	TxID atomic.Int64
	// Gen is the permission-gate epoch (RT-001): bumped by the sync
	// layer on every gate swap, stamped by the notifier onto each
	// emitted frame. Monotonic: values are never reused, so epoch
	// comparison has no ABA hazard. Opaque to this package beyond the
	// stamp; sync owns the bumps.
	Gen atomic.Uint64
	// deliveryMu orders bounded transport writes/publication against gate
	// changes. Writers take a read lease; a pending gate change blocks new
	// leases without serializing independent readers of this subscription.
	deliveryMu sync.RWMutex
	// Delta marks groups with at least one delta-refresh member; eligible
	// refreshes ship patches. Atomic: upgraded from session goroutines at
	// member attach while drain workers read it.
	Delta atomic.Bool
	// Emit fans one computed generation out to the transport and reports
	// whether it was acceptably served: nil means rendered, with every
	// member served or terminally detached; non-nil means nothing was
	// served and the generation must be retried, never published
	// (RT-002b). Production is sync group dispatch; tests may fake it.
	Emit      func(Frame) error
	Cancelled bool

	mu sync.Mutex
	// cancelCh is closed by Store.Remove. Retry timers select it so an
	// unsubscribed subscription cannot wake a refresh after its removal.
	cancelCh        chan struct{}
	cancelled       atomic.Bool
	cancelMu        sync.Mutex
	cancelHook      func()
	cancelHookState *retryState
	// mat is the optional incremental-engine state (docs/reference/09-tier2-architecture.md
	// §T2.5): materialized member lists per top-level form, seeded only by full
	// refresh results. Guarded by mu alongside last. Zero value = disabled.
	mat  matState
	last json.RawMessage // last full snapshot; diff baseline for delta-refresh
	// authGate is the permission gate admitted for the subscription,
	// recorded by the sync layer alongside every AttachCtx install so the
	// incremental engine can authorize splices without touching AttachCtx
	// itself (which is owned by the sync layer's lock). Guarded by
	// authMu; nil until the first install.
	authMu   sync.Mutex
	authGate any
}

// LockDelivery excludes generation deliveries during a gate change. Acquire
// before any registry lock; never hold a registry lock while waiting here.
func (s *Subscription) LockDelivery() { s.deliveryMu.Lock() }

func (s *Subscription) UnlockDelivery() { s.deliveryMu.Unlock() }

// WithCurrentGeneration runs fn only for this live subscription's current
// epoch, holding a delivery lease through fn. Network callbacks must enforce
// a finite write deadline. A gate change waits for already admitted writes,
// but cannot be overtaken by new leases once it is waiting.
func (s *Subscription) WithCurrentGeneration(gen uint64, fn func() error) (bool, error) {
	s.deliveryMu.RLock()
	defer s.deliveryMu.RUnlock()
	if s.cancelled.Load() || s.Gen.Load() != gen {
		return false, nil
	}
	return true, fn()
}

// SetAuthGate records the gate admitted for the subscription. Called by
// the sync layer on every AttachCtx install (attach, re-gate, rebind);
// nil clears it. A generation that cannot load the current gate never
// reaches the splice path, so a stale recording is never consumed.
func (s *Subscription) SetAuthGate(g any) {
	s.authMu.Lock()
	defer s.authMu.Unlock()
	s.authGate = g
}

// AuthGate returns the last recorded admitted gate (nil if none).
func (s *Subscription) AuthGate() any {
	s.authMu.Lock()
	defer s.authMu.Unlock()
	return s.authGate
}

// Snapshot returns the last full result emitted for this subscription
// (nil before the first snapshot). Diff baseline for delta-refresh.
func (s *Subscription) Snapshot() json.RawMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

// SnapshotPair returns the served snapshot and its watermark as one pair
// (RT-002a/RT-002d): reconnecting clients establish continuity from exactly
// this state. Read order is load-bearing — TxID first, then snapshot.
// Publication order is snapshot first, TxID second, and a watermark is only
// ever stored for an acceptably served generation. Together these imply the
// returned pair can only skew conservative: the watermark never describes
// state newer than the snapshot (at worst it understates coverage, which
// converges by re-delivery, never by silent staleness).
func (s *Subscription) SnapshotPair() (json.RawMessage, int64) {
	tx := s.TxID.Load()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last, tx
}

// SetSnapshot records the full result as the diff baseline after it has been
// delivered. Called by the notifier after each refresh and by the sync layer
// after seeding an initial snapshot.
func (s *Subscription) SetSnapshot(result json.RawMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last = result
}

// ClearIncremental drops incremental-engine state (RT-001): a gate swap must
// never leave a baseline spliced under the old gate for a later generation
// to extend. Called under the swap lock (groupsMu → sub.mu order); materialize
// re-seeds from the next successful full refresh under the new gate.
func (s *Subscription) ClearIncremental() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mat.ready = false
	s.mat.last = nil
	s.mat.forms = nil
	s.mat.roots = nil
}

// ClearServedState drops the served snapshot, delta baseline, and incremental
// baseline atomically (RT-001d/H-01): a swap must never leave any baseline
// from the old gate for a later generation to reuse or extend. Caller must
// hold the swap lock (groupsMu); this takes sub.mu once so snapshot+mat clear
// together. The generation bump follows (clear-then-bump) so readers either see
// old Gen+old baselines (consistent, dropped at Emit/Publish) or cleared
// baselines (bail to full recompute) — never new Gen+stale baselines.
func (s *Subscription) ClearServedState() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last = nil
	s.mat.ready = false
	s.mat.last = nil
	s.mat.forms = nil
	s.mat.roots = nil
}

// Store is a per-app subscription registry with an inverted topic index.
type Store struct {
	// MaxSubsPerApp caps concurrent subscriptions per app id; 0 = unlimited.
	// Set before the first Add. Breaches fail Add with a *SubLimitError so
	// the transport can reject the query and drop the connection.
	MaxSubsPerApp int

	mu      sync.RWMutex
	byID    map[string]*Subscription
	topics  map[string]map[string]struct{} // topicAttrID -> subIDs
	appSubs map[string]int                 // appID -> active sub count
	nextID  int64
}

func NewStore() *Store {
	return &Store{
		byID:    map[string]*Subscription{},
		topics:  map[string]map[string]struct{}{},
		appSubs: map[string]int{},
	}
}

// SubLimitError is returned by Store.Add when MaxSubsPerApp is exceeded.
type SubLimitError struct {
	AppID string
	Max   int
}

func (e *SubLimitError) Error() string {
	return fmt.Sprintf("reactive: per-app subscription cap (%d) exceeded for app %s", e.Max, e.AppID)
}

// Add registers a subscription; topics is its attr-id set. Returns a
// *SubLimitError when the store's MaxSubsPerApp cap for this app is hit —
// the subscription is then NOT registered.
func (s *Store) Add(sub *Subscription) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.MaxSubsPerApp > 0 && s.appSubs[sub.AppID] >= s.MaxSubsPerApp {
		return "", &SubLimitError{AppID: sub.AppID, Max: s.MaxSubsPerApp}
	}
	if sub.cancelCh == nil || sub.cancelled.Load() {
		sub.cancelCh = make(chan struct{})
		sub.cancelled.Store(false)
		sub.Cancelled = false
		sub.cancelMu.Lock()
		sub.cancelHook = nil
		sub.cancelHookState = nil
		sub.cancelMu.Unlock()
	}
	s.nextID++
	if sub.ID == "" {
		sub.ID = fmt.Sprintf("sub-%d", s.nextID)
	}
	s.byID[sub.ID] = sub
	for t := range sub.Topics {
		m, ok := s.topics[t]
		if !ok {
			m = map[string]struct{}{}
			s.topics[t] = m
		}
		m[sub.ID] = struct{}{}
	}
	s.appSubs[sub.AppID]++
	return sub.ID, nil
}

// Remove tears down a subscription.
func (s *Store) Remove(id string) bool {
	s.mu.Lock()
	sub, ok := s.byID[id]
	if !ok {
		s.mu.Unlock()
		return false
	}
	delete(s.byID, id)
	for t := range sub.Topics {
		if m, ok := s.topics[t]; ok {
			delete(m, id)
			if len(m) == 0 {
				delete(s.topics, t)
			}
		}
	}
	if s.appSubs[sub.AppID] > 0 {
		s.appSubs[sub.AppID]--
	}
	sub.Cancelled = true
	sub.cancelled.Store(true)
	if sub.cancelCh != nil {
		close(sub.cancelCh)
	}
	sub.cancelMu.Lock()
	cancelHook := sub.cancelHook
	sub.cancelHook = nil
	sub.cancelHookState = nil
	sub.cancelMu.Unlock()
	s.mu.Unlock()
	// Invoke outside Store's lock: the retry callback takes the notifier lock
	// and must never make removal contend with registry snapshots.
	if cancelHook != nil {
		cancelHook()
	}
	return true
}

// Get returns the live subscription pointer, not a copy.
func (s *Store) Get(id string) (*Subscription, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sub, ok := s.byID[id]
	return sub, ok
}

// Len reports active subscriptions (test hook).
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.byID)
}

// Notifier fans invalidations out to subscribers of the store.
type Notifier struct {
	Store *Store

	// Refresh re-runs one subscription's query and returns the result JSON.
	// Wired to instaql by cmd/instantd; kept as a function so tests can fake it.
	Refresh func(ctx context.Context, sub *Subscription) (json.RawMessage, error)
	Logger  *slog.Logger

	// Inc, when non-nil, enables incremental result maintenance behind the
	// Refresh seam (docs/reference/09-tier2-architecture.md §T2.5). nil — the default —
	// keeps today's byte-for-byte full-recompute path.
	Inc *Incremental

	// Revalidate, when non-nil, runs before incremental maintenance for one
	// generation and reports whether the subscription's admission context
	// changed (RT-001: persisted rule change). A true result forces full
	// recompute so spliced state admitted under stale rules can never emit;
	// an error drops the generation fail-closed. nil — the default —
	// preserves the exact pre-RT-001 splice behavior.
	Revalidate func(ctx context.Context, sub *Subscription) (changed bool, err error)

	// Publish commits one computed generation as the subscription's served
	// state (snapshot + watermark) and reports whether the generation was
	// still current. When non-nil, it MUST coordinate with whatever
	// invalidates generations (RT-001: the sync layer compares the
	// stamped epoch against the live epoch under the swap lock, so a
	// re-gate landing between fan-out and publication refuses the stale
	// commit instead of restoring superseded state). A false result drops
	// the generation and re-arms the retry, exactly like an emit failure.
	// nil — the default — publishes unconditionally and must only be
	// used where generations cannot be invalidated (hermetic tests);
	// every production wiring sets it.
	Publish func(sub *Subscription, gen uint64, result json.RawMessage, txID int64) bool

	pending map[string]int64 // subID → latest tx-id awaiting refresh
	// pendingCh belongs to the same queued epoch as pending. Both maps are
	// detached together by drainPass. Absent entry means unknown changes and
	// full recompute. Guarded by mu; changes.go owns accumulation.
	pendingCh map[string][]Change
	// pendingAttempt records how many failed attempts preceded a due retry.
	// Zero means the first refresh attempt; a retry timer transfers its
	// failure count here when it makes work due.
	pendingAttempt map[string]int
	// retries owns at most one timer per subscription. A timer callback moves
	// its latest state back to pending; it never creates a second timer.
	retries map[string]*retryState

	mu    sync.Mutex
	wake  chan struct{}
	once  sync.Once
	clock retryClock
	// retryJitter is package-private so tests can inject zero jitter. Production
	// uses stableRetryJitter, derived only from the non-secret subscription ID.
	retryJitter func(string, time.Duration) time.Duration

	gauge atomic.Int64 // pending refreshes; lock-free backpressure signal
}

// retryTimer and retryClock are the smallest scheduler seam needed to test
// retry deadlines without wall-clock sleeps. The production implementation
// delegates to time.AfterFunc; tests provide a manually advanced clock.
type retryTimer interface {
	Stop() bool
}

type retryClock interface {
	AfterFunc(time.Duration, func()) retryTimer
}

type systemRetryClock struct{}

func (systemRetryClock) AfterFunc(d time.Duration, f func()) retryTimer {
	return time.AfterFunc(d, f)
}

type retryState struct {
	txID     int64
	changes  []Change
	failures int
	sub      *Subscription
	timer    retryTimer
	ctx      context.Context
	done     chan struct{}
}

var retryDelays = [...]time.Duration{
	100 * time.Millisecond,
	200 * time.Millisecond,
	400 * time.Millisecond,
	800 * time.Millisecond,
	1600 * time.Millisecond,
	3200 * time.Millisecond,
	5 * time.Second,
}

func retryDelay(failures int) time.Duration {
	if failures < 1 {
		failures = 1
	}
	i := failures - 1
	if i >= len(retryDelays) {
		i = len(retryDelays) - 1
	}
	return retryDelays[i]
}

// stableRetryJitter intentionally uses only the subscription identity, not
// query data or credentials. The resulting factor is stable for that
// subscription and lies in [0.8, 1.2], with the final policy capped at 5s.
func stableRetryJitter(id string, base time.Duration) time.Duration {
	sum := sha256.Sum256([]byte(id))
	v := binary.BigEndian.Uint64(sum[:8])
	factor := 0.8 + 0.4*(float64(v)/float64(^uint64(0)))
	d := time.Duration(float64(base) * factor)
	if d > retryDelays[len(retryDelays)-1] {
		return retryDelays[len(retryDelays)-1]
	}
	if d < time.Nanosecond {
		return time.Nanosecond
	}
	return d
}

func (s *Subscription) isCancelled() bool { return s.cancelled.Load() }

// QueueDepth reports how many subscriptions currently await a refresh. The
// WAL invalidator can poll this as a backpressure gauge (e.g. log or shed
// checkpoint frequency when it climbs); it never blocks.
func (n *Notifier) QueueDepth() int { return int(n.gauge.Load()) }

// shedRetryAfter is the fixed denial hint (docs/reference/09-tier2-architecture.md
// §T2.1: "Denial is *ShedError{RetryAfter} (250 ms fixed)").
const shedRetryAfter = 250 * time.Millisecond

// ShedError is the backpressure denial produced by a Notifier gate when the
// refresh queue for an app is over capacity. The sync layer maps it onto the
// same 429-shaped error frame its rate-limit gate emits; HTTP surfaces it as
// 429 + Retry-After.
type ShedError struct {
	RetryAfter time.Duration
}

func (e *ShedError) Error() string {
	return fmt.Sprintf("reactive: transact shed under queue backpressure; retry after %s", e.RetryAfter)
}

// Gate returns the transact-path admission check wired into
// sync.Deps.TransactGate (docs/09 §T2.1). It reads Notifier.QueueDepth's
// underlying gauge with atomic loads/stores only — no locks on the transact
// path — and latches hysteresis-style: deny once depth reaches maxDepth,
// stay denying until depth falls below maxDepth/2, so a queue hovering in
// the middle band flaps neither way.
//
// maxDepth <= 0 yields an always-allow gate (INSTANT_V2_MAX_QUEUE_DEPTH=0,
// today's behavior). RT-003: the low-water mark is at least 1, so a
// depth-one gate latches at depth 1 and reopens at depth 0 instead of
// wedging shut (maxDepth/2 == 0 can never be undercut). Behavior for
// maxDepth >= 2 is unchanged.
func (n *Notifier) Gate(maxDepth int64) func(appID string) error {
	if maxDepth <= 0 {
		return func(string) error { return nil }
	}
	low := maxDepth / 2
	if low < 1 {
		low = 1
	}
	var shedding atomic.Bool
	return func(string) error {
		depth := n.gauge.Load()
		switch {
		case !shedding.Load() && depth >= maxDepth:
			shedding.Store(true)
		case shedding.Load() && depth < low:
			shedding.Store(false)
		}
		if !shedding.Load() {
			return nil
		}
		metrics.NotifierSheds.Inc()
		return &ShedError{RetryAfter: shedRetryAfter}
	}
}

// Notify enqueues invalidation for the given attrs after txID committed.
// Coalescing: multiple notifications collapse into one refresh per sub.
// Legacy path: no change records are attached, so drains take the full
// recompute exactly as before (docs/09 §T2.5).
func (n *Notifier) Notify(ctx context.Context, appID string, attrIDs []string, txID int64) {
	_ = ctx
	n.enqueue(appID, n.Store.SubsForTopics(dedupeTopicIDs(attrIDs)), txID, nil)
}

// Workers is the refresh concurrency. Queries hit Postgres independently, so
// draining in parallel is safe; per-subscription ordering stays deterministic
// because a sub re-dirtied mid-refresh is simply re-enqueued with its newer
// tx-id and drained again.
var Workers = 16

// Run processes pending refreshes until ctx ends. Each drain pass snapshots
// the pending set and re-queries it through a bounded worker pool — one pass
// costs O(dirty/workers) instead of serializing every subscription.
func (n *Notifier) Run(ctx context.Context) {
	log := n.Logger
	if log == nil {
		log = slog.Default()
	}
	defer n.cancelAllRetries()
	n.once.Do(func() { n.wake = make(chan struct{}, 1) })
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.wake:
		}
		for n.drainPass(ctx, log) {
		}
	}
}

func (n *Notifier) signalWake() {
	n.once.Do(func() { n.wake = make(chan struct{}, 1) })
	select {
	case n.wake <- struct{}{}:
	default:
	}
}

func (n *Notifier) retryClock() retryClock {
	if n.clock != nil {
		return n.clock
	}
	return systemRetryClock{}
}

func (n *Notifier) scheduleRetry(ctx context.Context, sub *Subscription, txID int64, changes []Change, failures int) {
	if ctx.Err() != nil || sub.isCancelled() {
		return
	}
	if len(changes) > maxTrackedChanges {
		changes = nil
	}

	n.mu.Lock()
	if n.retries == nil {
		n.retries = map[string]*retryState{}
	}
	// A notification can arrive while Refresh is running. Fold that newer
	// epoch into the one retry state, so the timer retries the latest tx once.
	if pendingTx, ok := n.pending[sub.ID]; ok {
		if pendingTx > txID {
			txID = pendingTx
		}
		if n.pendingCh == nil {
			changes = nil
		} else if pendingChanges, known := n.pendingCh[sub.ID]; !known {
			changes = nil
		} else {
			changes = mergeRetryChanges(changes, pendingChanges)
		}
		delete(n.pending, sub.ID)
		delete(n.pendingCh, sub.ID)
		if n.pendingAttempt != nil {
			delete(n.pendingAttempt, sub.ID)
		}
		n.gauge.Add(-1)
	}
	if existing, ok := n.retries[sub.ID]; ok {
		if txID > existing.txID {
			existing.txID = txID
		}
		existing.changes = mergeRetryChanges(existing.changes, changes)
		n.mu.Unlock()
		return
	}

	state := &retryState{
		txID:     txID,
		changes:  changes,
		failures: failures,
		sub:      sub,
		ctx:      ctx,
		done:     make(chan struct{}),
	}
	delay := retryDelay(failures)
	jitter := n.retryJitter
	if jitter == nil {
		jitter = stableRetryJitter
	}
	delay = jitter(sub.ID, delay)
	if delay > retryDelays[len(retryDelays)-1] {
		delay = retryDelays[len(retryDelays)-1]
	}
	if delay < time.Nanosecond {
		delay = time.Nanosecond
	}
	state.timer = n.retryClock().AfterFunc(delay, func() {
		n.retryDue(sub.ID, state)
	})
	n.retries[sub.ID] = state
	sub.cancelMu.Lock()
	if sub.cancelled.Load() {
		delete(n.retries, sub.ID)
		sub.cancelMu.Unlock()
		state.timer.Stop()
		n.mu.Unlock()
		return
	}
	sub.cancelHookState = state
	sub.cancelHook = func() { n.cancelRetry(sub.ID, state) }
	sub.cancelMu.Unlock()
	n.mu.Unlock()

	go n.watchRetryCancellation(state)
}

func (n *Notifier) watchRetryCancellation(state *retryState) {
	select {
	case <-state.ctx.Done():
		n.cancelRetry(state.sub.ID, state)
	case <-state.sub.cancelCh:
		n.cancelRetry(state.sub.ID, state)
	case <-state.done:
	}
}

func (n *Notifier) retryDue(id string, state *retryState) {
	n.mu.Lock()
	if current, ok := n.retries[id]; !ok || current != state {
		n.mu.Unlock()
		return
	}
	delete(n.retries, id)
	n.clearCancelHook(state)
	close(state.done)
	if state.ctx.Err() != nil || state.sub.isCancelled() {
		n.mu.Unlock()
		return
	}
	if n.pending == nil {
		n.pending = map[string]int64{}
	}
	if n.pendingAttempt == nil {
		n.pendingAttempt = map[string]int{}
	}
	n.pending[id] = state.txID
	n.pendingAttempt[id] = state.failures
	if state.changes == nil {
		delete(n.pendingCh, id)
	} else {
		if n.pendingCh == nil {
			n.pendingCh = map[string][]Change{}
		}
		n.pendingCh[id] = state.changes
	}
	n.gauge.Add(1)
	n.mu.Unlock()
	n.signalWake()
}

func (n *Notifier) cancelRetry(id string, state *retryState) {
	n.mu.Lock()
	current, ok := n.retries[id]
	if !ok || current != state {
		n.mu.Unlock()
		return
	}
	delete(n.retries, id)
	if state.timer != nil {
		state.timer.Stop()
	}
	n.clearCancelHook(state)
	close(state.done)
	n.mu.Unlock()
}

func (n *Notifier) cancelAllRetries() {
	n.mu.Lock()
	for id, state := range n.retries {
		delete(n.retries, id)
		if state.timer != nil {
			state.timer.Stop()
		}
		n.clearCancelHook(state)
		close(state.done)
	}
	n.mu.Unlock()
}

func (n *Notifier) clearCancelHook(state *retryState) {
	state.sub.cancelMu.Lock()
	if state.sub.cancelHookState == state {
		state.sub.cancelHook = nil
		state.sub.cancelHookState = nil
	}
	state.sub.cancelMu.Unlock()
}

// drainPass processes one snapshot of the pending set through a bounded
// worker pool, reporting whether any work existed. Extracted from Run so
// tests can drive drains synchronously — the incremental differential needs
// deterministic interleaving (docs/09 §T2.5 acceptance).
func (n *Notifier) drainPass(ctx context.Context, log *slog.Logger) bool {
	// Snapshot one drain batch.
	n.mu.Lock()
	if len(n.pending) == 0 {
		n.mu.Unlock()
		return false
	}
	batch := n.pending
	batchChanges := n.pendingCh
	batchAttempts := n.pendingAttempt
	n.pending = map[string]int64{}
	n.pendingCh = nil
	n.pendingAttempt = nil
	n.gauge.Add(-int64(len(batch)))
	n.mu.Unlock()

	// Nothing subscribed anywhere: record decode/dispatch upstream
	// is wasted motion — drop the batch without touching workers.
	if n.Store.Len() == 0 {
		return true
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, Workers)
	for id, txID := range batch {
		wg.Add(1)
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Done()
			continue
		}
		go func(id string, txID int64) {
			defer func() { <-sem; wg.Done() }()
			n.refreshOneAttempt(ctx, log, id, txID, batchChanges[id], batchAttempts[id])
		}(id, txID)
	}
	wg.Wait()
	return true
}

func (n *Notifier) refreshOneAttempt(ctx context.Context, log *slog.Logger, id string, txID int64, changes []Change, failures int) {
	// Covers requery/splice + the synchronous Emit → group fanout, so one
	// waterfall span shows query cost and per-member dispatch together.
	ctx, span := tracing.Tracer.Start(ctx, "notifier.refresh")
	defer span.End()
	sub, ok := n.Store.Get(id)
	if ctx.Err() != nil || !ok || sub.isCancelled() || txID <= sub.TxID.Load() {
		return
	}
	// Stamp the rule-generation epoch before computing (RT-001): the
	// transport refuses frames from attempts overtaken by a concurrent
	// re-gate. Stamping early (rather than after refreshResult) is what
	// makes the check meaningful — a swap landing anywhere in this
	// attempt, including inside incremental computation, is detected.
	gen := sub.Gen.Load()
	result, err := n.refreshResult(ctx, sub, changes)
	if err != nil {
		log.Error("reactive: refresh failed", "sub", id, "err", err)
		// A failed refresh is re-armed by one bounded timer. Any newer
		// invalidation that arrived while this refresh was running is folded
		// into the same timer-owned state by scheduleRetry.
		n.scheduleRetry(ctx, sub, txID, changes, failures+1)
		return
	}
	if ctx.Err() != nil || sub.isCancelled() {
		return
	}

	// Delta-refresh: diff against the previous full snapshot. Any ambiguity
	// (aggregates, reordering, >MaxTouchedRatio churn, malformed shapes)
	// leaves patchJSON empty and the client gets the full envelope.
	var patchJSON json.RawMessage
	if sub.Delta.Load() {
		if prev := sub.Snapshot(); prev != nil {
			if p, ok := DiffResults(prev, result); ok {
				if b, merr := json.Marshal(p); merr == nil {
					patchJSON = b
				}
			}
		}
	}
	// RT-002a/RT-002b: nothing about this generation becomes observable
	// until the fan-out outcome is known. Snapshot first, watermark second
	// (see SnapshotPair for why this order plus read order keeps any skew
	// conservative). On emit failure neither advances and the bounded retry
	// re-arms the same txID — identical to the refresh-error path, so a
	// poisoned generation can neither leak nor starve newer invalidations
	// (newer txIDs always pass the progress guard above).
	if sub.Emit != nil {
		if err := sub.Emit(Frame{
			SubID:         id,
			QueryJSON:     sub.Query,
			ResultJSON:    result,
			PatchJSON:     patchJSON,
			ProcessedTxID: txID,
			Gen:           gen,
		}); err != nil {
			log.Error("reactive: emit failed; generation dropped, retry armed", "sub", id, "err", err)
			n.scheduleRetry(ctx, sub, txID, changes, failures+1)
			return
		}
	}
	// RT-001: the commit itself is coordinated. A re-gate that landed
	// after the fan-out check must refuse this stale generation rather
	// than restore superseded state over a cleared (or newer) snapshot.
	if n.Publish != nil {
		if !n.Publish(sub, gen, result, txID) {
			log.Error("reactive: generation superseded before publish; retry armed", "sub", id)
			n.scheduleRetry(ctx, sub, txID, changes, failures+1)
			return
		}
	} else {
		sub.SetSnapshot(result)
		sub.TxID.Store(txID)
	}
}

// refreshResult produces one drain's envelope. When the invalidation carried
// a known change set and the engine is wired, incremental maintenance gets
// first crack (docs/reference/09-tier2-architecture.md §T2.5); any bail-out falls back
func (n *Notifier) refreshResult(ctx context.Context, sub *Subscription, changes []Change) (json.RawMessage, error) {
	forceFull := false
	if n.Revalidate != nil {
		changed, rerr := n.Revalidate(ctx, sub)
		if rerr != nil {
			metrics.Refreshes.WithLabelValues("error").Inc()
			return nil, rerr
		}
		forceFull = changed
	}
	if !forceFull && changes != nil && n.Inc != nil {
		if out, ok := n.Inc.apply(ctx, sub, changes); ok {
			metrics.Refreshes.WithLabelValues("spliced").Inc()
			return out, nil
		}
	}
	result, err := n.Refresh(ctx, sub)
	if err != nil {
		metrics.Refreshes.WithLabelValues("error").Inc()
		return result, err
	}
	metrics.Refreshes.WithLabelValues("full").Inc()
	if n.Inc != nil {
		n.Inc.materialize(sub, result)
	}
	return result, nil
}
