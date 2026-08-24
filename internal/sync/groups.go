package sync

// Query-group registry: one logical subscription per
// (app, wire-class, canonical query) shared by every session that asked for
// it. Spec: docs/08-tier1-hotpath.md §T1.1.
//
// Without grouping, N clients holding the same query are N independent
// pipelines — N recomputes + N renders + N marshals of byte-identical frames
// per write. Grouping renders once per generation and fans pre-encoded bytes
// out to members.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"

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
func groupKey(appID string, class wireClass, rawQ json.RawMessage) string {
	var c bytes.Buffer
	_ = json.Compact(&c, rawQ)
	h := fnv.New64a()
	h.Write([]byte(appID))
	h.Write([]byte{0})
	h.Write([]byte{byte(class)})
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
func (m *Manager) attachGroup(sess *Session, rawQ json.RawMessage, topics map[string]bool,
	cat *platform.AttrCatalog, class wireClass,
) (*queryGroup, error) {
	key := groupKey(sess.AppID, class, rawQ)

	m.groupsMu.Lock()
	defer m.groupsMu.Unlock()

	capN := m.Deps.Store.MaxSubsPerApp
	if capN > 0 && m.appMembers[sess.AppID]+1 > capN {
		return nil, &reactive.SubLimitError{AppID: sess.AppID, Max: capN}
	}

	g := m.groups[key]
	if g == nil {
		sub := &reactive.Subscription{
			ID:     key,
			AppID:  sess.AppID,
			Query:  rawQ,
			Topics: topics,
		}
		sub.Delta.Store(sess.Features["delta-refresh"])
		sub.Emit = func(fr reactive.Frame) {
			m.dispatchGroup(g, fr)
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

// dispatchGroup renders ONE full envelope (+ optional delta variant) for the
// generation and fans pre-encoded bytes to every member. Replaces the old
// per-session Emit body.
func (m *Manager) dispatchGroup(g *queryGroup, fr reactive.Frame) {
	members, anyDelta := g.snapshotMembers()
	if len(members) == 0 {
		return
	}
	logger := m.logger()

	var fullRaw []byte
	switch g.class {
	case wireTree:
		tree, terr := UnwrapTree(fr.ResultJSON)
		if terr != nil {
			logger.Error("groups: unwrap tree", "err", terr)
			return
		}
		payload := computationEntry(
			[2]json.RawMessage{keyInstaqlQuery, json.RawMessage(fr.QueryJSON)},
			[2]json.RawMessage{keyInstaqlResult, tree},
		)
		fullRaw = encodeFrame(Frame{
			"op":              json.RawMessage(`"refresh-ok"`),
			"computations":    payload,
			"processed-tx-id": json.RawMessage(mustJSON(fr.ProcessedTxID)),
		})
		if meta := resultMetaOf(fr.ResultJSON); len(meta) > 0 {
			// v1 admin SSE carries result-meta on refreshes too; rebuild the
			// frame with it rather than short-circuit above.
			payload2 := computationEntry(
				[2]json.RawMessage{keyInstaqlQuery, json.RawMessage(fr.QueryJSON)},
				[2]json.RawMessage{keyInstaqlResult, tree},
				[2]json.RawMessage{keyResultMeta, json.RawMessage(meta)},
			)
			fullRaw = encodeFrame(Frame{
				"op":              json.RawMessage(`"refresh-ok"`),
				"computations":    payload2,
				"result-meta":     json.RawMessage(meta),
				"processed-tx-id": json.RawMessage(mustJSON(fr.ProcessedTxID)),
			})
		}
	default:
		nodes, nerr := BuildNodeList(g.cat, fr.ResultJSON)
		if nerr != nil {
			logger.Error("groups: node-list render", "err", nerr)
			return
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
		} else {
			b = fullRaw
		}
		if err := mem.sess.SendRaw(b); err != nil && logger != nil {
			logger.Debug("groups: send", "err", err)
		}
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
