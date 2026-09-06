package sync

import (
	"context"

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
	// RT-001c: the whole read-decide-swap serializes on groupsMu. Reading
	// AttachCtx outside the swap lock raced concurrent generations on the
	// interface word; holding one lock across the reload keeps installs
	// totally ordered. Lock order matches attachGroup (groupsMu outermost,
	// catalog leaf lock inside rulesFor); no caller holds groupsMu across
	// RefreshGate, so this cannot self-deadlock.
	m.groupsMu.Lock()
	defer m.groupsMu.Unlock()
	old, _ := sub.AttachCtx.(*QueryGate)
	if old == nil {
		return nil, false, nil
	}
	doc, err := m.rulesFor(ctx, sub.AppID)
	if err != nil {
		return nil, false, err
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
	if cur, _ := sub.AttachCtx.(*QueryGate); cur != old {
		// A concurrent generation already re-gated; converge on it.
		sub.SetAuthGate(cur)
		return cur, true, nil
	}
	sub.AttachCtx = next
	sub.SetAuthGate(next)
	// Advance the generation epoch first: any concurrent publisher
	// holding the old epoch is refused at publication from here on.
	// Epochs are monotonic, so comparison has no ABA hazard.
	sub.Gen.Add(1)
	// RT-001d: a swapped gate must never share the previous gate's
	// snapshot. Clear it so a joiner admitted under the new doc
	// recomputes instead of reading state rendered under the old one;
	// the current generation re-seeds it on success, and delta readers
	// fall back to a full frame on a nil baseline.
	sub.SetSnapshot(nil)
	return next, true, nil
}

// rebindGroupLocked swaps a group's admitted gate after a version-mismatched
// attach (RT-001d) and drops the possibly stale snapshot so the joiner's
// initial answer refreshes under the new gate. Caller must hold groupsMu.
func (m *Manager) rebindGroupLocked(g *queryGroup, doc *perms.RuleDoc) {
	gate := NewQueryGate(doc, g.admittedAdmin())
	g.sub.AttachCtx = gate
	g.sub.SetAuthGate(gate)
	g.sub.Gen.Add(1)
	g.sub.SetSnapshot(nil)
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
