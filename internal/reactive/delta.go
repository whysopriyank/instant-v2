package reactive

// Structural refresh deltas for the opt-in `delta-refresh` feature gate
// (docs/03-protocol.md §5: "A wire-level delta is an additive v2
// optimisation (new feature flag), not a breaking change").
//
// A negotiating session's refresh frame carries a Patch instead of the full
// instaql-result whenever the change between the previous snapshot and the
// new result is expressible as per-entity add/update/remove operations plus
// an optional page-info delta. Any ambiguity falls back to the full
// envelope, so correctness never depends on the fast path.

import (
	"encoding/json"
	"sort"
)

// MaxTouchedRatio is the fallback heuristic constant: if more than this
// fraction of the result's entities changed, the patch would approach (or
// exceed) the full envelope in size and lose its reason to exist — ship the
// full envelope instead. 0.5 means "more than half the entities touched".
const MaxTouchedRatio = 0.5

// PatchOpKind enumerates the per-entity operations of a structural patch.
const (
	OpAdd    = "add"
	OpUpdate = "update"
	OpRemove = "remove"
)

// PatchOp is one entity-level change scoped to an etype.
type PatchOp struct {
	Op     string          `json:"op"`               // add | update | remove
	Etype  string          `json:"etype"`            // entity's query level key
	ID     string          `json:"id"`               // entity id
	Entity json.RawMessage `json:"entity,omitempty"` // full new entity (add/update)
}

// Patch is the structural delta between two instaql results.
//
// Ops within one etype are ordered so that removes come first and
// add/update ops follow the NEW array order; applying them in order
// reconstructs the new membership. Relative order of surviving entities is
// guaranteed unchanged (see DiffResults), so no move op exists.
type Patch struct {
	Ops      []PatchOp       `json:"ops,omitempty"`
	PageInfo json.RawMessage `json:"page-info,omitempty"` // new page-info when pagination shifted
}

// envelope mirrors instaql.Result without forcing full decoding.
type envelope struct {
	Data      map[string]json.RawMessage `json:"data"`
	PageInfo  json.RawMessage            `json:"page-info"`
	Aggregate json.RawMessage            `json:"aggregate"`
}

// DiffResults computes the structural patch turning oldJSON into newJSON
// (both full instaql-result envelopes). ok=false means "not expressible as a
// patch — ship the full envelope". Fallback triggers on:
//
//   - malformed JSON anywhere (never guess);
//   - aggregate queries (a count shift isn't a per-entity op);
//   - reordering: the relative order of surviving entities changes (patch op
//     vocabulary has no move; positional shifts are ambiguous across nested
//     link arrays, i.e. cross-etype reordering);
//   - touched entities exceeding MaxTouchedRatio of the larger side;
//   - duplicate or missing entity ids inside any etype array.
func DiffResults(oldJSON, newJSON json.RawMessage) (p *Patch, ok bool) {
	var oldEnv, newEnv envelope
	if json.Unmarshal(oldJSON, &oldEnv) != nil || json.Unmarshal(newJSON, &newEnv) != nil {
		return nil, false
	}
	// Aggregates are whole-set numbers, not entity ops.
	if isSet(oldEnv.Aggregate) || isSet(newEnv.Aggregate) {
		return nil, false
	}

	// parseSide splits each etype array once. Every entity is unmarshaled
	// exactly ONE time here (targeted id probe); all later stages work off
	// the extracted ids plus raw bytes — no per-entity re-marshaling.
	type side map[string][]string // etype → ids aligned with env.Data raws
	parseSide := func(env *envelope) (side, bool) {
		out := make(side, len(env.Data))
		for etype, raw := range env.Data {
			if len(raw) == 0 || string(raw) == "null" {
				out[etype] = nil
				continue
			}
			var arr []json.RawMessage
			if err := json.Unmarshal(raw, &arr); err != nil {
				return nil, false // etype payload is not an entity array
			}
			ids := make([]string, len(arr))
			seen := make(map[string]struct{}, len(arr))
			for i, e := range arr {
				var probe struct {
					ID string `json:"id"`
				}
				if json.Unmarshal(e, &probe) != nil || probe.ID == "" {
					return nil, false // non-object or missing string id
				}
				if _, dup := seen[probe.ID]; dup {
					return nil, false // duplicate id: positional identity broken
				}
				seen[probe.ID] = struct{}{}
				ids[i] = probe.ID
			}
			out[etype] = ids
		}
		return out, true
	}
	oldSide, oldOK := parseSide(&oldEnv)
	newSide, newOK := parseSide(&newEnv)
	if !oldOK || !newOK {
		return nil, false
	}

	patch := &Patch{}
	touched := 0

	// Removes: in old but absent from new (deterministic id order).
	for etype, oldIDs := range oldSide {
		newArr, exists := newEnv.Data[etype]
		var newSet map[string]struct{}
		if exists && len(newArr) > 0 {
			newSet = make(map[string]struct{}, len(newSide[etype]))
			for _, id := range newSide[etype] {
				newSet[id] = struct{}{}
			}
		}
		var remIDs []string
		for _, id := range oldIDs {
			if _, live := newSet[id]; !live {
				remIDs = append(remIDs, id)
			}
		}
		sort.Strings(remIDs)
		for _, id := range remIDs {
			patch.Ops = append(patch.Ops, PatchOp{Op: OpRemove, Etype: etype, ID: id})
			touched++
		}
	}

	// Adds + updates walked in NEW array order per etype.
	for etype, newIDs := range newSide {
		oldEnts := make(map[string]json.RawMessage, len(oldSide[etype]))
		var oldArr []json.RawMessage
		if raw, exists := oldEnv.Data[etype]; exists && len(raw) > 0 && string(raw) != "null" {
			_ = json.Unmarshal(raw, &oldArr)
			for i, id := range oldSide[etype] {
				oldEnts[id] = oldArr[i]
			}
		}
		var newArr []json.RawMessage
		raw, _ := newEnv.Data[etype]
		if len(raw) > 0 && string(raw) != "null" {
			_ = json.Unmarshal(raw, &newArr)
		}
		for i, id := range newIDs {
			e := newArr[i]
			prev, wasOld := oldEnts[id]
			switch {
			case !wasOld:
				patch.Ops = append(patch.Ops, PatchOp{Op: OpAdd, Etype: etype, ID: id, Entity: e})
				touched++
			case !sameEntity(prev, e):
				patch.Ops = append(patch.Ops, PatchOp{Op: OpUpdate, Etype: etype, ID: id, Entity: e})
				touched++
			}
		}
	}

	// Reorder check: relative order of surviving ids must be identical on
	// both sides (per etype). Any positional shift of common entities is
	// not expressible without move semantics → full envelope.
	for etype, oldIDs := range oldSide {
		newIDs, exists := newSide[etype]
		if !exists {
			continue
		}
		newPos := make(map[string]int, len(newIDs))
		for i, id := range newIDs {
			newPos[id] = i
		}
		last := -1
		for _, id := range oldIDs { // survivors must keep relative order
			pos, keep := newPos[id]
			if !keep {
				continue
			}
			if pos < last {
				return nil, false // reorder / cross-etype shuffle ambiguity
			}
			last = pos
		}
	}

	// Size heuristic: too much churn ⇒ patch ≈ full envelope anyway.
	total := 0
	for etype, ids := range oldSide {
		n := len(ids)
		if m := len(newSide[etype]); m > n {
			n = m
		}
		total += n
	}
	for etype, ids := range newSide {
		if _, existed := oldSide[etype]; !existed {
			total += len(ids)
		}
	}
	if total > 0 && float64(touched)/float64(total) > MaxTouchedRatio {
		return nil, false
	}

	// Page-info delta: attach only when pagination actually shifted.
	if pageChanged(oldEnv.PageInfo, newEnv.PageInfo) {
		patch.PageInfo = newEnv.PageInfo
	}
	return patch, true
}

func isSet(raw json.RawMessage) bool {
	return len(raw) > 0 && string(raw) != "null" && string(raw) != ""
}

// sameEntity reports whether two entity payloads carry identical content.
// Byte equality short-circuits the common case (same producer, unchanged
// row); differing bytes fall back to a canonical map comparison so key-order
// differences don't count as updates.
func sameEntity(a, b json.RawMessage) bool {
	if string(a) == string(b) {
		return true
	}
	var am, bm any
	if json.Unmarshal(a, &am) != nil || json.Unmarshal(b, &bm) != nil {
		return false
	}
	ab, _ := json.Marshal(am) // sorted keys: canonical
	bb, _ := json.Marshal(bm)
	return string(ab) == string(bb)
}

func pageChanged(oldRaw, newRaw json.RawMessage) bool {
	if !isSet(oldRaw) && !isSet(newRaw) {
		return false
	}
	var o, n any
	_ = json.Unmarshal(oldRaw, &o)
	_ = json.Unmarshal(newRaw, &n)
	ob, _ := json.Marshal(o)
	nb, _ := json.Marshal(n)
	return string(ob) != string(nb)
}
