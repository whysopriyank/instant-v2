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
func (m *Manager) newGroupLocked(key string, sess *Session, rawQ json.RawMessage, topics map[string]bool, cat *platform.AttrCatalog, class wireClass, doc *perms.RuleDoc) (*queryGroup, error) {
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
	// by a concurrent re-gate. The notifier stamps Frame.Gen at
	// attempt start; validity is enforced at three points: admission
	// in Emit, per member inside dispatch (SSE uses queued epoch drop),
	// and commit in PublishGeneration (a swap landing after fan-out refuses
	// the stale snapshot instead of restoring superseded state).
	// Defined boundary for partially served generations: direct-write members
	// served before the swap keep one superseded envelope — wire bytes cannot
	// be recalled — but the server certifies nothing (no snapshot, no watermark
	// advance), stays attached, and the armed retry re-serves every attached
	// member under the new gate at the same txID, healing the stale read.
	// Queued SSE members hold a per-subscription generation lease through
	// their bounded socket write and flush. A swap waits for admitted writes;
	// after the swap, old queued envelopes are dropped. No global registry
	// lock is held across network writes; dispatch itself stays unlocked.
	g := &queryGroup{
		key: key, class: class, appID: sess.AppID,
		sub: sub, cat: cat,
		members: map[*Session]member{},
	}
	sub.Emit = func(fr reactive.Frame) error {
		if sub.Gen.Load() != fr.Gen {
			return errGenerationSuperseded
		}
		return m.dispatchGroup(g, fr)
	}
	return g, nil
}

func (m *Manager) attachGroup(ctx context.Context, sess *Session, rawQ json.RawMessage, topics map[string]bool,
	cat *platform.AttrCatalog, class wireClass, doc *perms.RuleDoc,
) (*queryGroup, error) {
	key := groupKey(sess.AppID, class, rawQ, sess.Admin)

	m.groupsMu.Lock()
	// Admission is one registry transaction except for a necessary rule load.
	// The final resolution, cap check and both membership records stay locked.
	sess.mu.Lock()
	closing := sess.closing
	sess.mu.Unlock()
	if closing {
		m.groupsMu.Unlock()
		return nil, fmt.Errorf("%w: session is closing", ErrCloseSession)
	}
	capN := m.Deps.Store.MaxSubsPerApp
	if capN > 0 && m.appMembers[sess.AppID]+1 > capN {
		m.groupsMu.Unlock()
		return nil, &reactive.SubLimitError{AppID: sess.AppID, Max: capN}
	}
	g := m.groups[key]
	// Version-bound membership (RT-001d): an existing group must reload the
	// current document before admitting a member. The caller's doc may have
	// become stale after its admission read, so comparing against it cannot
	// close that window. Release across rule-store I/O so one wedged load
	// cannot stall revocation of other groups (H-01g); the swap rechecks under
	// the lock so concurrent re-gates still converge.
	var cur *QueryGate
	if g != nil {
		cur, _ = g.sub.AttachCtx.(*QueryGate)
	}
	if g != nil {
		observed := g
		m.groupsMu.Unlock()
		fresh, ferr := m.rulesFor(ctx, sess.AppID)
		if ferr != nil {
			return nil, fmt.Errorf("%w: %v", errRulesReload, ferr)
		}
		// Never wait for network delivery while holding the registry lock.
		observed.sub.LockDelivery()
		defer observed.sub.UnlockDelivery()
		m.groupsMu.Lock()
		g = m.groups[key]
		if g == observed {
			sess.mu.Lock()
			closing := sess.closing
			sess.mu.Unlock()
			if closing {
				m.groupsMu.Unlock()
				return nil, fmt.Errorf("%w: session is closing", ErrCloseSession)
			}
			// A completed swap or replacement wins over this unlocked load.
			// Hash inequality alone cannot tell which document is newer.
			cur2, _ := g.sub.AttachCtx.(*QueryGate)
			if cur2 == cur && (cur2 == nil || GateHash(cur2.Rules) != GateHash(fresh)) {
				m.rebindGroupLocked(g, fresh)
			}
		}
		doc = fresh // used only if the observed group was removed
	}
	defer m.groupsMu.Unlock()
	// Re-check the cap after releasing across rule-store I/O: concurrent
	// attaches could have filled it while we loaded (H-01g independence must
	// not break admission accounting).
	if capN > 0 && m.appMembers[sess.AppID]+1 > capN {
		return nil, &reactive.SubLimitError{AppID: sess.AppID, Max: capN}
	}
	if g == nil {
		var err error
		g, err = m.newGroupLocked(key, sess, rawQ, topics, cat, class, doc)
		if err != nil {
			return nil, err
		}
	}
	newGroup := m.groups[key] == nil
	if newGroup {
		// Keep the group private to this registry transaction until its member
		// and session ownership records are committed below. The Store is added
		// last so external reactive observers cannot see a half-admitted group.
		m.groups[key] = g
	}

	// Upgrade the group to delta-capable when any member negotiates it.
	if sess.Features["delta-refresh"] {
		g.sub.Delta.Store(true)
	}

	g.mu.Lock()
	sess.mu.Lock()
	if sess.closing {
		sess.mu.Unlock()
		g.mu.Unlock()
		if newGroup {
			delete(m.groups, key)
		}
		return nil, fmt.Errorf("%w: session is closing", ErrCloseSession)
	}
	g.members[sess] = member{sess: sess, delta: sess.Features["delta-refresh"]}
	sess.Subs[key] = true
	m.appMembers[sess.AppID]++
	if newGroup {
		if _, err := m.Deps.Store.Add(g.sub); err != nil {
			delete(sess.Subs, key)
			delete(g.members, sess)
			m.appMembers[sess.AppID]--
			delete(m.groups, key)
			sess.mu.Unlock()
			g.mu.Unlock()
			return nil, err
		}
	}
	sess.mu.Unlock()
	g.mu.Unlock()
	return g, nil
}

// detachMember removes sess from the group backing id; the group unregisters
// when its last member leaves.
func (m *Manager) detachMember(sess *Session, id string) {
	m.groupsMu.Lock()
	defer m.groupsMu.Unlock()

	g, ok := m.groups[id]
	if !ok {
		sess.mu.Lock()
		delete(sess.Subs, id)
		sess.mu.Unlock()
		return
	}
	g.mu.Lock()
	_, existed := g.members[sess]
	delete(g.members, sess)
	empty := len(g.members) == 0
	g.mu.Unlock()

	sess.mu.Lock()
	delete(sess.Subs, id)
	sess.mu.Unlock()
	if existed && m.appMembers[sess.AppID] > 0 {
		m.appMembers[sess.AppID]--
	}
	if empty {
		m.Deps.Store.Remove(id)
		delete(m.groups, id)
	}
}

// DetachAll tears down every group membership held by sess (disconnect path).
// The id snapshot is taken under sess.mu: a concurrent failMember on a
// refresh worker may detach (and delete) mid-iteration, and detachMember
// tolerates already-gone memberships, so the snapshot may safely go stale.
func (m *Manager) DetachAll(sess *Session) {
	sess.mu.Lock()
	sess.closing = true
	ids := make([]string, 0, len(sess.Subs))
	for id := range sess.Subs {
		ids = append(ids, id)
	}
	sess.mu.Unlock()
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
	// Version-bound fast path (RT-001d): a cached snapshot is reusable only
	// if no re-gate landed around the read. A swap clears the snapshot under
	// groupsMu, but this read takes only sub.mu; without the epoch check a
	// reader racing the clear could serve stale-allow as a new answer.
	gen0 := sub.Gen.Load()
	if snap := sub.Snapshot(); snap != nil && sub.Gen.Load() == gen0 {
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
// full snapshot) without failing its siblings — except when NOTHING was
// served and the generation is still current: then there is no certified
// gap to repair by reconnect, so the generation is withheld (error, no
// snapshot, no watermark) with members kept attached for the notifier's
// bounded same-tx retry, which heals in place.
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

	// Direct-write (SendRaw) failures withhold teardown until the fan-out
	// outcome is known: detaching eagerly would destroy the subscription
	// (and its bounded same-tx retry) for a generation nobody certified.
	var served int
	var failedDirect []*Session
	for _, mem := range members {
		if mem.sess.SendRaw == nil && mem.sess.SendRawGen == nil {
			m.failMember(g, mem.sess)
			continue
		}
		var b []byte
		if mem.delta && deltaRaw != nil {
			b = deltaRaw
			metrics.RefreshFrames.WithLabelValues("delta").Inc()
		} else {
			b = fullRaw
			metrics.RefreshFrames.WithLabelValues("full").Inc()
		}
		var serr error
		if mem.sess.SendRawGen != nil {
			// Generation-aware queue (RT-001f, SSE): the epoch travels
			// with the bytes so the dequeue writer drops superseded
			// envelopes instead of serving them post-revoke. No pre-send
			// drop here — enqueue always succeeds unless backpressured —
			// so every member gets a healing retry via the same txID.
			serr = mem.sess.SendRawGen(b, fr.Gen, g.sub)
			if serr != nil {
				// Queued transports own their teardown signaling
				// (SSE overflow ends the stream independently of
				// dispatch), so the established detach-and-continue
				// outcome is preserved verbatim here.
				if logger != nil {
					logger.Debug("groups: send failed; detaching member", "err", serr)
				}
				m.failMember(g, mem.sess)
				continue
			}
		} else {
			// Per-member admission (RT-001): a re-gate that landed after
			// Emit's check — or mid-fan-out — stops the stale spread here
			// by failing the generation (the retry path recomputes under
			// the new gate). One atomic load per member; no lock, no stall.
			// Bytes already sent are irreducible, but the refused commit
			// certifies nothing stale.
			if g.sub.Gen.Load() != fr.Gen {
				return errGenerationSuperseded
			}
			serr = mem.sess.SendRaw(b)
			if serr != nil {
				// RT-002c: a direct write that reached no member must
				// not certify this generation — and, when the
				// generation is still current, must not destroy the
				// subscription either. The failure is collected for
				// the fan-out-end decision below: withhold (error,
				// members kept) for a same-tx healing retry when
				// nothing was served, detach+close once a sibling has
				// certified the generation (the failed member then has
				// a real gap and must reconnect for full replay).
				if logger != nil {
					logger.Debug("groups: direct send failed; outcome deferred to fan-out end", "err", serr)
				}
				failedDirect = append(failedDirect, mem.sess)
				continue
			}
		}
		served++
		metrics.FanoutBytes.Add(float64(len(b)))
	}
	if len(failedDirect) > 0 {
		if served == 0 && g.sub.Gen.Load() == fr.Gen {
			// Nothing was served and nothing was certified, so no
			// member has a gap to repair by reconnect: withhold the
			// generation (the notifier commits nothing and re-arms
			// the same txID) and keep every member attached so the
			// retry heals in place.
			return fmt.Errorf("groups: no member served (%d direct delivery failures); withholding generation for retry", len(failedDirect))
		}
		for _, sess := range failedDirect {
			// RT-002c: a member that was not served must not keep a
			// watermark for this generation. Detach it now and close its
			// transport so it reconnects and re-establishes from a full
			// snapshot; siblings are unaffected.
			if logger != nil {
				logger.Debug("groups: send failed; detaching member")
			}
			m.failMember(g, sess)
		}
		if served == 0 {
			// All direct writes failed after a re-gate overtook the
			// generation mid-flight: revoked bytes must never be
			// re-served in place, so the detached members
			// re-establish under the new gate instead.
			return errGenerationSuperseded
		}
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
