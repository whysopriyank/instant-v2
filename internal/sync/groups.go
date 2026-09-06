package sync

// Query-group registry: one logical subscription per
// (app, wire-class, canonical query) shared by every session that asked for
// it. Spec: docs/archive/08-tier1-hotpath.md §T1.1.
//
// Without grouping, N clients holding the same query are N independent
// pipelines — N recomputes + N renders + N marshals of byte-identical frames
// per write. Grouping renders once per generation and fans pre-encoded bytes
// out to members.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"

	"github.com/instant-v2/instant-v2/internal/metrics"
	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
)

type wireClass uint8

const (
	wireNodelist wireClass = iota // WS path: v1 join-rows node-list envelopes
	wireTree                      // admin SSE path: bare object-tree semantics
)

func (c wireClass) String() string {
	if c == wireTree {
		return "tree"
	}
	return "nodelist"
}

// member is one attached session inside a group. delta marks sessions that
// negotiated delta-refresh and can receive refresh-ok-delta frames.
type member struct {
	sess  *Session
	delta bool
}

type queryGroup struct {
	key   string
	class wireClass
	appID string
	sub   *reactive.Subscription // ID == key; owned by reactive.Store
	cat   *platform.AttrCatalog  // captured at creation; same lifetime semantics as the old per-session closure

	mu      sync.Mutex
	members map[*Session]member
}

// snapshotFlight represents one in-progress initial refresh. The registry
// entry is removed only after the leader has published its result and closed
// done. This is important on failures: a waiter that already joined the old
// flight must observe that flight's error, while a caller arriving after the
// failure must be able to start a fresh attempt without overlapping the old
// refresh.
type snapshotFlight struct {
	done chan struct{}

	started      bool
	participants int
	result       json.RawMessage
	err          error
}

// snapshotFlightsMu serializes lifecycle operations on Manager.flight. The
// refresh itself never runs under this lock; it is held only while selecting
// or completing a flight, so different query keys remain independent.
var snapshotFlightsMu sync.Mutex

func (g *queryGroup) snapshotMembers() (members []member, anyDelta bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	members = make([]member, 0, len(g.members))
	for _, mem := range g.members {
		if mem.delta {
			anyDelta = true
		}
		members = append(members, mem)
	}
	return members, anyDelta
}

// groupKey derives the registry key. The query participates compacted so
// cosmetic whitespace differences share a group; key-order differences do
// not (documented limitation — clients emit stable JSON in practice).
//
// The caller class (admin vs public) is mixed in: admins bypass view gates,
// so a shared group across classes would leak denied etypes to public
// members through admin-seeded snapshots.
func groupKey(appID string, class wireClass, rawQ json.RawMessage, admin bool) string {
	var c bytes.Buffer
	_ = json.Compact(&c, rawQ)
	h := fnv.New64a()
	h.Write([]byte(appID))
	h.Write([]byte{0})
	h.Write([]byte{byte(class)})
	if admin {
		h.Write([]byte{1})
	} else {
		h.Write([]byte{0})
	}
	h.Write(c.Bytes())
	return fmt.Sprintf("grp-%s-%016x", class, h.Sum64())
}

// attachGroup registers sess's membership for the query, creating the group
// (and its Store subscription) on first use. Cap enforcement happens here:
// MaxSubsPerApp counts group MEMBERS (sockets × queries) — the resource that
// actually costs memory — checked at attach time. Breach returns the exact
// protocol frames the previous per-session check produced.
//
// Caller must have computed topics and cat already (same compilation the
// pre-group code performed).
func (m *Manager) attachGroup(ctx context.Context, sess *Session, rawQ json.RawMessage, topics map[string]bool,
	cat *platform.AttrCatalog, class wireClass, doc *perms.RuleDoc,
) (*queryGroup, error) {
	key := groupKey(sess.AppID, class, rawQ, sess.Admin)

	m.groupsMu.Lock()
	defer m.groupsMu.Unlock()

	capN := m.Deps.Store.MaxSubsPerApp
	if capN > 0 && m.appMembers[sess.AppID]+1 > capN {
		return nil, &reactive.SubLimitError{AppID: sess.AppID, Max: capN}
	}

	g := m.groups[key]
	if g == nil {
		gate := NewQueryGate(doc, sess.Admin)
		sub := &reactive.Subscription{
			ID:        key,
			AppID:     sess.AppID,
			Query:     rawQ,
			Topics:    topics,
			AttachCtx: gate,
		}
		sub.SetAuthGate(gate)
		sub.Delta.Store(sess.Features["delta-refresh"])
		// Coordinated publication (RT-001): refuse generations overtaken
		// by a concurrent re-gate between computation and fan-out. The
		// notifier stamps Frame.Gen at attempt start; a mismatch means
		// the rules moved underneath this generation, so it is dropped
		// through the existing emit-error retry path instead of
		// publishing stale-authorized state. Only the epoch comparison
		// is synchronized with swaps — dispatch itself stays outside
		// any lock, so a slow member cannot stall re-gating. Content
		// sent after a passing check raced a swap is still reserved-era
		// self-consistent state, re-gated on the next generation.
		sub.Emit = func(fr reactive.Frame) error {
			if sub.Gen.Load() != fr.Gen {
				return errGenerationSuperseded
			}
			return m.dispatchGroup(g, fr)
		}
		if _, err := m.Deps.Store.Add(sub); err != nil {
			return nil, err
		}
		g = &queryGroup{
			key: key, class: class, appID: sess.AppID,
			sub: sub, cat: cat,
			members: map[*Session]member{},
		}
		m.groups[key] = g
	}

	// Version-bound membership (RT-001d): a joiner admitted under different
	// persisted rules than the group must not share the group's snapshot or
	// its gate. The joiner's doc may itself predate a concurrent rebind, so
	// reload the newest doc at this decision point and adopt it. A reload
	// failure refuses the join exactly like an unloadable attach-time doc
	// (RT-001e) instead of pinning either possibly-stale snapshot.
	// Content-hash comparison keeps unrelated catalog invalidations from
	// disturbing the group. groupsMu is held throughout; rulesFor only
	// takes the leaf catalog lock, so no lock-order inversion is possible.
	if cur, ok := g.sub.AttachCtx.(*QueryGate); !ok || cur == nil || GateHash(cur.Rules) != GateHash(doc) {
		fresh, ferr := m.rulesFor(ctx, sess.AppID)
		if ferr != nil {
			return nil, fmt.Errorf("%w: %v", errRulesReload, ferr)
		}
		if cur2, ok2 := g.sub.AttachCtx.(*QueryGate); !ok2 || cur2 == nil || GateHash(cur2.Rules) != GateHash(fresh) {
			m.rebindGroupLocked(g, fresh)
		}
	}

	// Upgrade the group to delta-capable when any member negotiates it.
	if sess.Features["delta-refresh"] {
		g.sub.Delta.Store(true)
	}

	g.mu.Lock()
	g.members[sess] = member{sess: sess, delta: sess.Features["delta-refresh"]}
	g.mu.Unlock()
	m.appMembers[sess.AppID]++

	sess.Subs[key] = true
	return g, nil
}

// detachMember removes sess from the group backing id; the group unregisters
// when its last member leaves.
func (m *Manager) detachMember(sess *Session, id string) {
	m.groupsMu.Lock()
	defer m.groupsMu.Unlock()

	g, ok := m.groups[id]
	if !ok {
		delete(sess.Subs, id)
		return
	}
	g.mu.Lock()
	delete(g.members, sess)
	empty := len(g.members) == 0
	g.mu.Unlock()

	delete(sess.Subs, id)
	if m.appMembers[sess.AppID] > 0 {
		m.appMembers[sess.AppID]--
	}
	if empty {
		m.Deps.Store.Remove(id)
		delete(m.groups, id)
	}
}

// DetachAll tears down every group membership held by sess (disconnect path).
func (m *Manager) DetachAll(sess *Session) {
	ids := make([]string, 0, len(sess.Subs))
	for id := range sess.Subs {
		ids = append(ids, id)
	}
	for _, id := range ids {
		m.detachMember(sess, id)
	}
}

// snapshotOrRefresh produces the synchronous initial answer for one group's
// subscription under the per-key flight: the first arriver runs the full
// refresh and seeds the group snapshot; concurrent duplicates of the same
// group wait and reuse the baseline instead of stacking N recomputes on the
// read pool (docs/archive/08-tier1-hotpath.md §T1.1 single-flight).
//
// Reuse safety: the baseline covers exactly this group's query. Any commit
// newer than the baseline either already dirtied the group (the drain pushes
// refresh-ok to every attached member, including the waiter) or cannot exist
// (fresh group ⇒ the baseline just materialized). A failed refresh seeds
// nothing, so the next caller retries.
func (m *Manager) snapshotOrRefresh(
	ctx context.Context,
	refresh func(context.Context, *reactive.Subscription) (json.RawMessage, error),
	sub *reactive.Subscription,
) (json.RawMessage, error) {
	if snap := sub.Snapshot(); snap != nil {
		return snap, nil
	}

	snapshotFlightsMu.Lock()
	flightAny, ok := m.flight.Load(sub.ID)
	if !ok {
		flightAny = &snapshotFlight{done: make(chan struct{})}
		m.flight.Store(sub.ID, flightAny)
	}
	flight := flightAny.(*snapshotFlight)
	leader := !flight.started
	if leader {
		flight.started = true
	}
	flight.participants++
	snapshotFlightsMu.Unlock()
	defer m.releaseSnapshotFlight(flight)

	if !leader {
		select {
		case <-flight.done:
			return flight.result, flight.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// A notifier may have seeded the subscription between the fast-path check
	// above and flight acquisition. Treat that as a successful no-op and still
	// complete the flight so followers receive the same baseline.
	if snap := sub.Snapshot(); snap != nil {
		m.completeSnapshotFlight(sub.ID, flight, snap, nil)
		return snap, nil
	}
	if err := ctx.Err(); err != nil {
		m.completeSnapshotFlight(sub.ID, flight, nil, err)
		return nil, err
	}

	// Coordinate initial publication with re-gating (RT-001): settle the
	// gate before capturing the epoch, so the post-refresh check below
	// fires only on swaps concurrent with this flight — not on the
	// flight's own legitimate re-gate. A reload failure here is
	// deliberately ignored: refresh() performs its own load and reports
	// the failure itself, and failing here would break gate-unaware
	// refresh funcs whose contracts predate re-gating.
	_, _, _ = m.RefreshGate(ctx, sub)
	gen := sub.Gen.Load()
	result, err := refresh(ctx, sub)
	if err == nil {
		// Publish only if no re-gate landed mid-flight; otherwise fail
		// the flight so waiters recompute under the new gate. Epochs
		// are monotonic, so this has no ABA hazard. The lock serializes
		// the check with swaps (RefreshGate and rebindGroupLocked both
		// bump under groupsMu); refresh itself stays outside it.
		m.groupsMu.Lock()
		if sub.Gen.Load() != gen {
			m.groupsMu.Unlock()
			m.completeSnapshotFlight(sub.ID, flight, nil, errGenerationSuperseded)
			return nil, errGenerationSuperseded
		}
		sub.SetSnapshot(result)
		m.groupsMu.Unlock()
	}
	m.completeSnapshotFlight(sub.ID, flight, result, err)
	return result, err
}

// completeSnapshotFlight publishes a flight outcome before making the entry
// unavailable to later callers. The channel close is the synchronization
// edge for followers; deleting while holding the same registry lock prevents
// the old unlock-then-delete race from creating a second active refresh.
func (m *Manager) completeSnapshotFlight(id string, flight *snapshotFlight, result json.RawMessage, err error) {
	snapshotFlightsMu.Lock()
	flight.result = result
	flight.err = err
	close(flight.done)
	m.flight.Delete(id)
	snapshotFlightsMu.Unlock()
}

func (m *Manager) releaseSnapshotFlight(flight *snapshotFlight) {
	snapshotFlightsMu.Lock()
	flight.participants--
	snapshotFlightsMu.Unlock()
}

// dispatchGroup renders ONE full envelope (+ optional delta variant) for the
// generation and fans pre-encoded bytes to every member. Replaces the old
// per-session Emit body. It reports whether the generation was acceptably
// served (RT-002b/RT-002c): a render failure serves nothing and returns an
// error so the generation is retried, never published; a member send
// failure detaches and closes that member (reconnect re-establishes from a
// full snapshot) without failing its siblings.
func (m *Manager) dispatchGroup(g *queryGroup, fr reactive.Frame) error {
	members, anyDelta := g.snapshotMembers()
	if len(members) == 0 {
		return nil
	}
	logger := m.logger()

	var fullRaw []byte
	switch g.class {
	case wireTree:
		tree, terr := UnwrapTree(fr.ResultJSON)
		if terr != nil {
			logger.Error("groups: unwrap tree", "err", terr)
			return fmt.Errorf("groups: unwrap tree: %w", terr)
		}
		// Single render + single encode per generation. resultMetaOf returns
		// non-empty bytes for essentially every instaql result (at minimum
		// "{}"), so the previous assemble-encode-reassemble-reencode flow
		// paid the full frame copy TWICE on every tree-class refresh.
		meta := resultMetaOf(fr.ResultJSON)
		pairs := make([][2]json.RawMessage, 0, 3)
		pairs = append(pairs,
			[2]json.RawMessage{keyInstaqlQuery, json.RawMessage(fr.QueryJSON)},
			[2]json.RawMessage{keyInstaqlResult, tree},
		)
		out := Frame{
			"op":              json.RawMessage(`"refresh-ok"`),
			"processed-tx-id": json.RawMessage(mustJSON(fr.ProcessedTxID)),
		}
		if len(meta) > 0 {
			// v1 admin SSE carries result-meta on refreshes too. Pairs stay
			// in sorted key order (instaql-query < instaql-result <
			// result-meta) as computationEntry requires.
			pairs = append(pairs, [2]json.RawMessage{keyResultMeta, json.RawMessage(meta)})
			out["result-meta"] = json.RawMessage(meta)
		}
		out["computations"] = computationEntry(pairs...)
		fullRaw = encodeFrame(out)
	default:
		nodes, nerr := BuildNodeList(g.cat, fr.ResultJSON)
		if nerr != nil {
			logger.Error("groups: node-list render", "err", nerr)
			return fmt.Errorf("groups: node-list render: %w", nerr)
		}
		payload := computationEntry(
			[2]json.RawMessage{keyInstaqlQuery, json.RawMessage(fr.QueryJSON)},
			[2]json.RawMessage{keyInstaqlResult, nodes},
		)
		fullRaw = encodeFrame(Frame{
			"op":              json.RawMessage(`"refresh-ok"`),
			"computations":    payload,
			"processed-tx-id": json.RawMessage(mustJSON(fr.ProcessedTxID)),
		})
	}

	// RT-002e: an encoding failure must never reach the wire as an empty
	// frame. encodeFrame already logs; refuse the generation here so the
	// retry path recomputes it instead of certifying silence.
	if len(fullRaw) == 0 {
		return fmt.Errorf("groups: frame encode produced no bytes")
	}

	var deltaRaw []byte
	if anyDelta && len(fr.PatchJSON) > 0 {
		dpayload := computationEntry(
			[2]json.RawMessage{keyDelta, json.RawMessage(fr.PatchJSON)},
			[2]json.RawMessage{keyInstaqlQuery, json.RawMessage(fr.QueryJSON)},
		)
		deltaRaw = encodeFrame(Frame{
			"op":              json.RawMessage(`"refresh-ok-delta"`),
			"computations":    dpayload,
			"processed-tx-id": json.RawMessage(mustJSON(fr.ProcessedTxID)),
		})
	}

	for _, mem := range members {
		if mem.sess.SendRaw == nil {
			continue // transport without raw support can't join groups
		}
		var b []byte
		if mem.delta && deltaRaw != nil {
			b = deltaRaw
			metrics.RefreshFrames.WithLabelValues("delta").Inc()
		} else {
			b = fullRaw
			metrics.RefreshFrames.WithLabelValues("full").Inc()
		}
		if err := mem.sess.SendRaw(b); err != nil {
			// RT-002c: a member that was not served must not keep a
			// watermark for this generation. Detach it now and close its
			// transport so it reconnects and re-establishes from a full
			// snapshot; siblings are unaffected.
			if logger != nil {
				logger.Debug("groups: send failed; detaching member", "err", err)
			}
			m.failMember(g, mem.sess)
			continue
		}
		metrics.FanoutBytes.Add(float64(len(b)))
	}
	return nil
}

// failMember detaches one unserved member and tears down its transport
// (RT-002c). Detach is eager so concurrent dispatches stop routing to it;
// Close breaks the transport read loop, whose deferred teardown completes
// the cleanup. A nil Close (tests, non-socket transports) detaches only.
func (m *Manager) failMember(g *queryGroup, sess *Session) {
	m.detachMember(sess, g.key)
	if sess.Close != nil {
		sess.Close()
	}
}

// encodeFrame marshals once; failures are programmer errors (map values are
// pre-marshaled raw), surfaced as empty output guarded upstream.
func encodeFrame(f Frame) []byte {
	b, err := f.Encode()
	if err != nil {
		slog.Error("groups: frame encode", "err", err)
		return nil
	}
	return b
}
