package sync

import (
	"context"
	"encoding/json"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/reactive"
)

// Transparent re-gate (RT-001): every refresh generation runs under the
// newest LOADED rule doc, never under a stale attach-time snapshot.
//
// Declared policy (pending DEC-001 ratification): on a persisted rule change,
// existing subscriptions are transparently re-gated at the next refresh
// boundary — no disconnect, no resubscribe, no new protocol event. Deny takes
// effect from the first generation that loads it; allow likewise. Generations
// that cannot load the current doc are dropped, never served stale.
//
// Determinism note (RT-001c): concurrent generations for one subscription
// serialize their gate swaps on groupsMu and converge on identical content;
// two generations straddling one rule commit may each run self-consistently
// under a different version, but no generation ever runs under a doc older
// than the newest one loaded at its start, and versions only move forward.
// A commit landing mid-generation (between the reload below and the executor
// run) can still color that single in-flight generation; closing that
// microsecond window would require emit-side versioning, which is out of
// scope for this packet.

// RefreshGate returns the gate the current refresh generation must run
// under, plus whether this call re-gated the subscription. It reloads the
// app's persisted doc through the same loader the attach path uses
// (Deps.Rules), compares content with the admitted gate, and swaps the
// subscription to the new gate when they differ.
//
//   - Same content (including the same cached pointer) → (admitted, false).
//   - Changed content → (new gate, true); the swap is serialized on
//     groupsMu and concurrent generations converge on identical content.
//   - Load error → (nil, false, err): the caller must drop the generation.
//     The old gate is retained on the subscription but never served under
//     again without a successful reload, so a lookup failure fails closed
//     (RT-001e) instead of pinning a formerly permissive snapshot.
//   - No admitted gate (non-sync producers) → (nil, false, nil): base
//     runner, exactly the pre-RT-001 behavior.
func (m *Manager) RefreshGate(ctx context.Context, sub *reactive.Subscription) (*QueryGate, bool, error) {
	if sub == nil {
		return nil, false, nil
	}
	// RT-001c: read the admitted gate under the swap lock, then reload
	// outside it. Holding groupsMu across rule-store I/O stalled every
	// group on one wedged load; the swap below rechecks under the lock so
	// concurrent generations still converge on identical content. Lock
	// order matches attachGroup (groupsMu outermost, catalog leaf lock
	// inside rulesFor only when called under it — here rulesFor runs
	// unlocked, so no inversion).
	m.groupsMu.Lock()
	old, _ := sub.AttachCtx.(*QueryGate)
	m.groupsMu.Unlock()
	if old == nil {
		return nil, false, nil
	}
	doc, err := m.rulesFor(ctx, sub.AppID)
	if err != nil {
		return nil, false, err
	}
	m.groupsMu.Lock()
	defer m.groupsMu.Unlock()
	// A concurrent generation already re-gated while we loaded; converge.
	if cur, _ := sub.AttachCtx.(*QueryGate); cur != old {
		sub.SetAuthGate(cur)
		return cur, true, nil
	}
	if doc == old.Rules {
		sub.SetAuthGate(old)
		return old, false, nil
	}
	if GateHash(doc) == GateHash(old.Rules) {
		sub.SetAuthGate(old)
		return old, false, nil
	}
	next := NewQueryGate(doc, old.Admin)
	sub.AttachCtx = next
	sub.SetAuthGate(next)
	// RT-001d: clear baselines BEFORE bumping the epoch (clear-then-bump).
	// A swapped gate must never share the previous gate's snapshot,
	// delta baseline, or incremental state. Clearing first ensures readers
	// either see old Gen+old baselines (dropped at Emit/Publish) or cleared
	// baselines (bail to full) — never new Gen+stale baselines which would
	// splice under the wrong gate. See ClearServedState.
	sub.ClearServedState()
	// Advance the generation epoch last: any concurrent publisher holding
	// the old epoch is refused at publication from here on. Epochs are
	// monotonic, so comparison has no ABA hazard.
	sub.Gen.Add(1)
	return next, true, nil
}

// PublishGeneration commits one fanned-out generation as the
// subscription's served state, coordinated with re-gating (RT-001): the
// epoch comparison runs under groupsMu — the same lock every swap takes —
// so a re-gate landing between fan-out and publication refuses the stale
// commit instead of restoring superseded state over a cleared (or newer)
// snapshot. Snapshot-then-watermark order matches the notifier's
// conservative-skew contract. False drops the generation to the retry
// path, which recomputes under the new gate. Lock order groupsMu → sub.mu
// matches snapshotOrRefresh; no path takes them in reverse.
func (m *Manager) PublishGeneration(sub *reactive.Subscription, gen uint64, result json.RawMessage, txID int64) bool {
	if sub == nil {
		return false
	}
	m.groupsMu.Lock()
	defer m.groupsMu.Unlock()
	if sub.Gen.Load() != gen {
		return false
	}
	sub.SetSnapshot(result)
	sub.TxID.Store(txID)
	return true
}

// rebindGroupLocked swaps a group's admitted gate after a version-mismatched
// attach (RT-001d) and drops served state atomically so the joiner's initial
// answer refreshes under the new gate. Clear-then-bump order matches
// RefreshGate; see ClearServedState. Caller must hold groupsMu.
func (m *Manager) rebindGroupLocked(g *queryGroup, doc *perms.RuleDoc) {
	gate := NewQueryGate(doc, g.admittedAdmin())
	g.sub.AttachCtx = gate
	g.sub.SetAuthGate(gate)
	g.sub.ClearServedState()
	g.sub.Gen.Add(1)
}

// SpliceAuthorize reports whether the incremental engine may splice etypes
// under the subscription's admitted gate, mirroring instaql runForm's view
// semantics for the flat root etypes classify() admits (nested forms bail
// out of splicing entirely, so roots cover every etype a splice can
// touch):
//
//   - open (or absent) doc → splice; the probe matches the oracle;
//   - closed → never splice; the oracle renders empty;
//   - dynamic non-admin → never splice; the oracle refuses execution;
//   - dynamic admin → splice; the oracle serves unfiltered, as does the
//     rule-unaware probe.
//
// An unknown or missing gate fails closed to full refresh. Reactive owns
// the hook signature; sync owns the gate, so this lives here and not in
// the engine (which must not import the transport layer).
func SpliceAuthorize(sub *reactive.Subscription, etypes []string) bool {
	if sub == nil {
		return false
	}
	gate, _ := sub.AuthGate().(*QueryGate)
	if gate == nil {
		return false
	}
	for _, et := range etypes {
		switch perms.ViewGate(gate.Rules, et) {
		case perms.ViewClosed:
			return false
		case perms.ViewDynamic:
			if !gate.Admin {
				return false
			}
		}
	}
	return true
}

// admittedAdmin recovers the caller class the group was admitted under.
// Group keys mix the admin bit, so every member of one group shares it.
func (g *queryGroup) admittedAdmin() bool {
	if gate, ok := g.sub.AttachCtx.(*QueryGate); ok && gate != nil {
		return gate.Admin
	}
	return false
}
