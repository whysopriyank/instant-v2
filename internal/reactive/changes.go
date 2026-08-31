package reactive

// Entity-level change records riding along with invalidations, plus the
// shared enqueue plumbing they funnel through (docs/reference/09-tier2-architecture.md
// §T2.5). Legacy Notify callers attach no change records and keep the exact
// full-recompute path they had before.

import (
	"context"
	"sort"
)

// Change is one entity-level mutation observed by an invalidation source:
// the WS transact path (resolved steps know entity + attr), the HTTP
// transact handler, or the admin bridge.
//
// AttrIDs drive topic routing only — the incremental engine never trusts
// them, it re-checks membership and refetches payloads for every touched
// entity (docs/09 §T2.5). A Change missing Etype, EntityID, or AttrIDs is
// only partially identified and degrades its whole batch to "unknown".
type Change struct {
	Etype    string
	EntityID string
	AttrIDs  []string
}

// maxTrackedChanges bounds per-subscription change accumulation across
// coalesced notifications. Past it the pending set degrades to unknown and
// the next drain takes legacy full refresh: O(change) bookkeeping must not
// silently grow toward O(result).
const maxTrackedChanges = 1024

// NotifyChanges enqueues invalidation for appID after txID committed,
// carrying entity-level changes for the incremental engine
// (docs/reference/09-tier2-architecture.md §T2.5).
//
// A nil/empty changes slice means "changes unknown": every subscription of
// the app is dirtied with no change record — the legacy full-recompute path,
// identical in outcome to a Notify matching all possible topics.
func (n *Notifier) NotifyChanges(ctx context.Context, appID string, changes []Change, txID int64) {
	_ = ctx // parity with Notify: draining is decoupled via wake
	var topics []string
	for _, c := range changes {
		if c.Etype == "" || c.EntityID == "" || len(c.AttrIDs) == 0 {
			// One partially-identified change poisons the batch: dirty every
			// sub of the app and let the full-refresh oracle recompute.
			n.enqueue(appID, n.Store.SubsForApp(appID), txID, nil)
			return
		}
		topics = append(topics, c.AttrIDs...)
	}
	if len(changes) == 0 {
		n.enqueue(appID, n.Store.SubsForApp(appID), txID, nil)
		return
	}
	n.enqueue(appID, n.Store.SubsForTopics(dedupeTopicIDs(topics)), txID, dedupeChanges(changes))
}

// enqueue dirties subs at txID under n.mu, attaching ch (non-nil = known
// change set) for the drain loop. Shared by Notify (ch nil) and
// NotifyChanges; coalescing keeps the newest tx-id per sub and merges known
// change sets until an unknown notification or the cap degrades them to
// unknown. That full-refresh obligation lasts until the batch is detached.
//
// subs arrive as live *Subscription pointers straight from a Store snapshot
// (SubsForTopics/SubsForApp) — no per-id Store.Get re-locking on the
// transact hot path. A sub removed between snapshot and enqueue costs one
// skipped drain at most: refreshOne re-validates liveness and watermark.
func (n *Notifier) enqueue(appID string, subs []*Subscription, txID int64, ch []Change) {
	n.mu.Lock()
	if n.pending == nil {
		n.pending = map[string]int64{}
	}
	added := 0
	for _, sub := range subs {
		if sub.AppID != appID || txID <= sub.TxID.Load() {
			continue
		}
		id := sub.ID
		pendingTx, alreadyPending := n.pending[id]
		if !alreadyPending {
			n.pending[id] = txID
			added++
		} else if txID > pendingTx {
			// Never move a coalesced entry backwards when an older
			// notification races with a newer one.
			n.pending[id] = txID
		}
		switch ch {
		case nil:
			delete(n.pendingCh, id) // discard the currently accumulated knowledge
		default:
			if alreadyPending && n.pendingCh[id] == nil {
				continue // a known suffix cannot account for earlier unknown changes
			}
			merged := mergeChanges(n.pendingCh[id], ch)
			if len(merged) > maxTrackedChanges {
				delete(n.pendingCh, id) // degrade to unknown; oracle re-seeds cheaply
				continue
			}
			if n.pendingCh == nil {
				n.pendingCh = map[string][]Change{}
			}
			n.pendingCh[id] = merged
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

// dedupeChanges collapses repeated touches of the same entity; order is not
// meaningful to the engine (it re-probes storage), only determinism helps
// tests.
func dedupeChanges(ch []Change) []Change {
	if len(ch) <= 1 {
		return ch
	}
	seen := make(map[string]struct{}, len(ch))
	out := ch[:0]
	for _, c := range ch {
		k := changeKey(c)
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, c)
	}
	return out
}

// mergeChanges unions two change sets, keeping a's entries first.
func mergeChanges(a, b []Change) []Change {
	if len(a) == 0 {
		return dedupeChanges(b)
	}
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]Change, 0, len(a)+len(b))
	for _, c := range a {
		seen[changeKey(c)] = struct{}{}
		out = append(out, c)
	}
	for _, c := range b {
		k := changeKey(c)
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, c)
	}
	return out
}

func changeKey(c Change) string { return c.Etype + "\x00" + c.EntityID }

// changedIDs returns the sorted unique entity ids touched among changes for
// one etype.
func changedIDs(changes []Change, etype string) []string {
	seen := map[string]struct{}{}
	var ids []string
	for _, c := range changes {
		if c.Etype != etype {
			continue
		}
		if _, dup := seen[c.EntityID]; dup {
			continue
		}
		seen[c.EntityID] = struct{}{}
		ids = append(ids, c.EntityID)
	}
	sort.Strings(ids)
	return ids
}
