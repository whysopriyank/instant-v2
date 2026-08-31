package reactive

// Topic routing owns its temporary de-duplication storage. Returned slices
// belong to callers; the subscription pointers remain live Store entries.

import (
	"sort"
	"sync"
)

const maxPooledTopicSubs = 4096

// Only SubsForTopics borrows this scratch set, for the duration of its read
// lock. Neither the set nor its keys escape into the caller-owned result.
var topicSubsSeenPool sync.Pool

func acquireTopicSubsSeen() map[string]struct{} {
	if value := topicSubsSeenPool.Get(); value != nil {
		seen := value.(map[string]struct{})
		for id := range seen {
			delete(seen, id)
		}
		return seen
	}
	return make(map[string]struct{})
}

func releaseTopicSubsSeen(seen map[string]struct{}) {
	// Inspect the pre-clear size so a burst over the bound cannot retain a
	// large backing table indefinitely.
	if len(seen) > maxPooledTopicSubs {
		return
	}
	for id := range seen {
		delete(seen, id)
	}
	topicSubsSeenPool.Put(seen)
}

// SubsForTopics returns the subscriptions watching any of the changed attrs,
// as a snapshot sorted by subscription id (deterministic iteration parity
// with the previous id-slice form). Callers own the slice; the *Subscription
// pointers are the store's live entries.
func (s *Store) SubsForTopics(attrIDs []string) []*Subscription {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seen := acquireTopicSubsSeen()
	defer releaseTopicSubsSeen(seen)
	var out []*Subscription
	for _, t := range attrIDs {
		for id := range s.topics[t] {
			if _, dup := seen[id]; !dup {
				seen[id] = struct{}{}
				if sub, ok := s.byID[id]; ok {
					out = append(out, sub)
				}
			}
		}
	}
	sortSubsByID(out)
	return out
}

// SubsForApp returns every active subscription of an app — the routing
// set when an invalidation carries no topic information at all
// (NotifyChanges with unknown changes, docs/reference/09-tier2-architecture.md §T2.5).
// Snapshot sorted by id, same ownership contract as SubsForTopics.
func (s *Store) SubsForApp(appID string) []*Subscription {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*Subscription
	for _, sub := range s.byID {
		if sub.AppID == appID {
			out = append(out, sub)
		}
	}
	sortSubsByID(out)
	return out
}

// sortSubsByID keeps enqueue iteration order identical to the old
// sort.Strings(id-list) pipeline.
func sortSubsByID(subs []*Subscription) {
	sort.Slice(subs, func(i, j int) bool { return subs[i].ID < subs[j].ID })
}

// dedupeTopicIDs removes repeated topic IDs before the store index is
// traversed. It preserves first-seen order; Store.SubsForTopics continues to
// sort the resulting subscriptions by ID for deterministic fan-out. The
// common small-topic path avoids a temporary map when there are no repeats;
// larger lists use a map to avoid quadratic scans.
func dedupeTopicIDs(topicIDs []string) []string {
	if len(topicIDs) < 2 {
		return topicIDs
	}
	if len(topicIDs) <= 16 {
		for i := 1; i < len(topicIDs); i++ {
			for j := 0; j < i; j++ {
				if topicIDs[i] == topicIDs[j] {
					return dedupeTopicIDsMap(topicIDs)
				}
			}
		}
		return topicIDs
	}
	return dedupeTopicIDsMap(topicIDs)
}

func dedupeTopicIDsMap(topicIDs []string) []string {
	seen := make(map[string]struct{}, len(topicIDs))
	out := make([]string, 0, len(topicIDs))
	for _, topicID := range topicIDs {
		if _, dup := seen[topicID]; dup {
			continue
		}
		seen[topicID] = struct{}{}
		out = append(out, topicID)
	}
	if len(out) == len(topicIDs) {
		return topicIDs
	}
	return out
}
