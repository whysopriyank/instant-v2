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
	ID        string
	AppID     string
	Query     json.RawMessage
	Topics    map[string]bool // attr-id set from the compiled plan
	TxID      int64
	Delta     bool // session negotiated `delta-refresh`; eligible refreshes ship patches
	Emit      func(Frame)
	Cancelled bool

	mu   sync.Mutex
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

	Logger *slog.Logger

	mu      sync.Mutex
	pending map[string]int64 // subID → latest tx-id awaiting refresh
	wake    chan struct{}
	once    sync.Once

	gauge atomic.Int64 // pending refreshes; lock-free backpressure signal
}

// QueueDepth reports how many subscriptions currently await a refresh. The
// WAL invalidator can poll this as a backpressure gauge (e.g. log or shed
// checkpoint frequency when it climbs); it never blocks.
func (n *Notifier) QueueDepth() int { return int(n.gauge.Load()) }

// Notify enqueues invalidation for the given attrs after txID committed.
// Coalescing: multiple notifications collapse into one refresh per sub.
func (n *Notifier) Notify(ctx context.Context, appID string, attrIDs []string, txID int64) {
	n.mu.Lock()
	if n.pending == nil {
		n.pending = map[string]int64{}
	}
	added := 0
	for _, id := range n.Store.SubsForTopics(attrIDs) {
		if sub, ok := n.Store.Get(id); ok && sub.AppID == appID && txID > sub.TxID {
			n.pending[id] = txID
			added++
		}
	}
	if added > 0 {
		n.gauge.Add(int64(added))
	}
	n.mu.Unlock()
	n.once.Do(func() { n.wake = make(chan struct{}, 1) })
	select {
	case n.wake <- struct{}{}:
	default:
	}
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

		for {
			// Snapshot one drain batch.
			n.mu.Lock()
			if len(n.pending) == 0 {
				n.mu.Unlock()
				break
			}
			batch := n.pending
			n.pending = map[string]int64{}
			n.gauge.Add(-int64(len(batch)))

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
		}
	}
}

func (n *Notifier) refreshOne(ctx context.Context, log *slog.Logger, id string, txID int64) {
	sub, ok := n.Store.Get(id)
	if !ok || sub.Cancelled || txID <= sub.TxID {
		return
	}
	result, err := n.Refresh(ctx, sub)
	if err != nil {
		log.Error("reactive: refresh failed", "sub", id, "err", err)
		return
	}

	// Delta-refresh: diff against the previous full snapshot. Any ambiguity
	// (aggregates, reordering, >MaxTouchedRatio churn, malformed shapes)
	// leaves patchJSON empty and the client gets the full envelope.
	var patchJSON json.RawMessage
	if sub.Delta {
		if prev := sub.Snapshot(); prev != nil {
			if p, ok := DiffResults(prev, result); ok {
				if b, merr := json.Marshal(p); merr == nil {
					patchJSON = b
				}
			}
		}
	}
	sub.SetSnapshot(result) // baseline is ALWAYS the latest full result

	sub.TxID = txID // watermark AFTER successful emit materialization
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
