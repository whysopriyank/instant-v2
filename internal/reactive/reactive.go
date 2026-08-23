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
)

// Frame is one outbound refresh payload for a subscription.
type Frame struct {
	SubID         string          `json:"-"`
	QueryJSON     json.RawMessage `json:"instaql-query"`
	ResultJSON    json.RawMessage `json:"instaql-result"`
	ProcessedTxID int64           `json:"processed-tx-id"`
}

// Subscription is one registered live query.
type Subscription struct {
	ID      string
	AppID   string
	Query   json.RawMessage
	Topics  map[string]bool // attr-id set from the compiled plan
	TxID    int64
	Emit    func(Frame)
	Cancelled bool
}

// Store is a per-app subscription registry with an inverted topic index.
type Store struct {
	mu     sync.RWMutex
	byID   map[string]*Subscription
	topics map[string]map[string]struct{} // topicAttrID -> subIDs
	nextID int64
}

func NewStore() *Store {
	return &Store{byID: map[string]*Subscription{}, topics: map[string]map[string]struct{}{}}
}

// Add registers a subscription; topics is its attr-id set.
func (s *Store) Add(sub *Subscription) string {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	return sub.ID
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
}

// Notify enqueues invalidation for the given attrs after txID committed.
// Coalescing: multiple notifications collapse into one refresh per sub.
func (n *Notifier) Notify(ctx context.Context, appID string, attrIDs []string, txID int64) {
	n.mu.Lock()
	if n.pending == nil {
		n.pending = map[string]int64{}
	}
	for _, id := range n.Store.SubsForTopics(attrIDs) {
		if sub, ok := n.Store.Get(id); ok && sub.AppID == appID && txID > sub.TxID {
			n.pending[id] = txID
		}
	}
	n.mu.Unlock()
	n.once.Do(func() { n.wake = make(chan struct{}, 1) })
	select {
	case n.wake <- struct{}{}:
	default:
	}
}

// Run processes pending refreshes until ctx ends. One goroutine drains all
// apps; refreshes are serialized to keep per-app ordering deterministic.
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
			n.mu.Lock()
			id := ""
			txID := int64(0)
			for sid, t := range n.pending {
				id, txID = sid, t
				break
			}
			if id == "" {
				n.mu.Unlock()
				break
			}
			delete(n.pending, id)
			n.mu.Unlock()

			sub, ok := n.Store.Get(id)
			if !ok || sub.Cancelled || txID <= sub.TxID {
				continue
			}
			result, err := n.Refresh(ctx, sub)
			if err != nil {
				log.Error("reactive: refresh failed", "sub", id, "err", err)
				continue
			}
			sub.TxID = txID
			if sub.Emit != nil {
				sub.Emit(Frame{
					SubID:         id,
					QueryJSON:     sub.Query,
					ResultJSON:    result,
					ProcessedTxID: txID,
				})
			}
		}
	}
}
