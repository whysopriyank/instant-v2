// Package reactive owns live query subscriptions: registration, topic-indexed
// invalidation, and coalesced refresh fan-out. Port surface of v1's
// reactive/store + reactive/invalidator with the DataScript session state
// replaced by plain structures (docs/02-architecture.md §5.3).
//
// Invalidation sources: the direct post-commit notifier (single-instance
// default) and waltail records (multi-instance). Both funnel into Invalidate.
package reactive

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"
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
}

// Subscription is one registered live query.
type Subscription struct {
	ID     string
	AppID  string
	Query  json.RawMessage
	Topics map[string]bool // attr-id set from the compiled plan
	// TxID is the last processed transaction watermark. Atomic: Notify
	// reads it on caller goroutines while drain workers write it.
	TxID atomic.Int64
	// Delta marks groups with at least one delta-refresh member; eligible
	// refreshes ship patches. Atomic: upgraded from session goroutines at
	// member attach while drain workers read it.
	Delta     atomic.Bool
	Emit      func(Frame)
	Cancelled bool

	mu sync.Mutex
	// mat is the optional incremental-engine state (docs/09-tier2-architecture.md
	// §T2.5): materialized member lists per top-level form, seeded only by full
	// refresh results. Guarded by mu alongside last. Zero value = disabled.
	mat  matState
	last json.RawMessage // last full snapshot; diff baseline for delta-refresh
}

// Snapshot returns the last full result emitted for this subscription
// (nil before the first snapshot). Diff baseline for delta-refresh.
func (s *Subscription) Snapshot() json.RawMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

// SetSnapshot records the full result as the diff baseline after it has been
// delivered. Called by the notifier after each refresh and by the sync layer
// after seeding an initial snapshot.
func (s *Subscription) SetSnapshot(result json.RawMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last = result
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
	defer s.mu.Unlock()
	sub, ok := s.byID[id]
	if !ok {
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
	return true
}

// Get returns a copy-on-read view of the subscription.
func (s *Store) Get(id string) (*Subscription, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sub, ok := s.byID[id]
	return sub, ok
}

// SubsForTopics returns the affected subscription ids for a changed attr.
func (s *Store) SubsForTopics(attrIDs []string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seen := map[string]struct{}{}
	var out []string
	for _, t := range attrIDs {
		for id := range s.topics[t] {
			if _, dup := seen[id]; !dup {
				seen[id] = struct{}{}
				out = append(out, id)
			}
		}
	}
	sort.Strings(out)
	return out
}

// SubsForApp returns every active subscription id for an app — the routing
// set when an invalidation carries no topic information at all
// (NotifyChanges with unknown changes, docs/09-tier2-architecture.md §T2.5).
func (s *Store) SubsForApp(appID string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []string
	for id, sub := range s.byID {
		if sub.AppID == appID {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
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
	// Refresh seam (docs/09-tier2-architecture.md §T2.5). nil — the default —
	// keeps today's byte-for-byte full-recompute path.
	Inc *Incremental

	pending map[string]int64 // subID → latest tx-id awaiting refresh
	// pendingCh holds coalesced known change sets alongside pending; absent
	// entry = unknown changes = full recompute. Guarded by mu.
	pendingCh map[string][]Change

	mu   sync.Mutex
	wake chan struct{}
	once sync.Once

	gauge atomic.Int64 // pending refreshes; lock-free backpressure signal
}

// QueueDepth reports how many subscriptions currently await a refresh. The
// WAL invalidator can poll this as a backpressure gauge (e.g. log or shed
// checkpoint frequency when it climbs); it never blocks.
func (n *Notifier) QueueDepth() int { return int(n.gauge.Load()) }

// shedRetryAfter is the fixed denial hint (docs/09-tier2-architecture.md
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
// today's behavior). Degenerate corner: maxDepth == 1 has low-water 0, so
// once latched it never reopens — operators should set maxDepth >= 2.
func (n *Notifier) Gate(maxDepth int64) func(appID string) error {
	if maxDepth <= 0 {
		return func(string) error { return nil }
	}
	low := maxDepth / 2
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
		return &ShedError{RetryAfter: shedRetryAfter}
	}
}

// Notify enqueues invalidation for the given attrs after txID committed.
// Coalescing: multiple notifications collapse into one refresh per sub.
// Legacy path: no change records are attached, so drains take the full
// recompute exactly as before (docs/09 §T2.5).
func (n *Notifier) Notify(ctx context.Context, appID string, attrIDs []string, txID int64) {
	_ = ctx
	n.enqueue(appID, n.Store.SubsForTopics(attrIDs), txID, nil)
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
	n.pending = map[string]int64{}
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
			n.refreshOne(ctx, log, id, txID)
		}(id, txID)
	}
	wg.Wait()
	return true
}

func (n *Notifier) refreshOne(ctx context.Context, log *slog.Logger, id string, txID int64) {
	sub, ok := n.Store.Get(id)
	if !ok || sub.Cancelled || txID <= sub.TxID.Load() {
		n.dropChanges(id) // don't leak change bookkeeping for a skipped drain
		return
	}
	result, err := n.refreshResult(ctx, id, sub)
	if err != nil {
		log.Error("reactive: refresh failed", "sub", id, "err", err)
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
	sub.SetSnapshot(result) // baseline is ALWAYS the latest full result

	sub.TxID.Store(txID) // watermark AFTER successful emit materialization
	if sub.Emit != nil {
		sub.Emit(Frame{
			SubID:         id,
			QueryJSON:     sub.Query,
			ResultJSON:    result,
			PatchJSON:     patchJSON,
			ProcessedTxID: txID,
		})
	}
}

// refreshResult produces one drain's envelope. When the invalidation carried
// a known change set and the engine is wired, incremental maintenance gets
// first crack (docs/09-tier2-architecture.md §T2.5); any bail-out falls back
// to Refresh, which remains the oracle and re-seeds materialized state.
func (n *Notifier) refreshResult(ctx context.Context, id string, sub *Subscription) (json.RawMessage, error) {
	changes, known := n.takeChanges(id)
	if known && n.Inc != nil {
		if out, ok := n.Inc.apply(ctx, sub, changes); ok {
			return out, nil
		}
	}
	result, err := n.Refresh(ctx, sub)
	if err == nil && n.Inc != nil {
		n.Inc.materialize(sub, result)
	}
	return result, err
}
