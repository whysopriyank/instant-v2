package reactive

// Structural refresh deltas for the opt-in `delta-refresh` feature gate
// (docs/reference/03-protocol.md §5: "A wire-level delta is an additive v2
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

// deltaEntities keeps array order, payloads, and identity lookup together.
// The index serves duplicate detection, membership, and survivor-order checks.
type deltaEntities struct {
	ids   []string
	raw   []json.RawMessage
	index map[string]int
}

type deltaResult struct {
	data     map[string]deltaEntities
	pageInfo json.RawMessage
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
	oldResult, oldOK := decodeDeltaResult(oldJSON)
	newResult, newOK := decodeDeltaResult(newJSON)
	if !oldOK || !newOK {
		return nil, false
	}

	ops, total, ok := diffEntities(oldResult.data, newResult.data)
	if !ok || (total > 0 && float64(len(ops))/float64(total) > MaxTouchedRatio) {
		return nil, false
	}
	patch := &Patch{Ops: ops}
	if pageChanged(oldResult.pageInfo, newResult.pageInfo) {
		patch.PageInfo = newResult.pageInfo
	}
	return patch, true
}

// decodeDeltaResult splits each result and etype array once. Keep raw
// payloads for patch emission and sameEntity's byte-equality fast path;
// only byte-different entities need the semantic comparison's full decode.
func decodeDeltaResult(raw json.RawMessage) (deltaResult, bool) {
	var env struct {
		Data      map[string]json.RawMessage `json:"data"`
		PageInfo  json.RawMessage            `json:"page-info"`
		Aggregate json.RawMessage            `json:"aggregate"`
	}
	if json.Unmarshal(raw, &env) != nil || isSet(env.Aggregate) {
		// Aggregates describe the whole result, not entity operations.
		return deltaResult{}, false
	}
	out := deltaResult{data: make(map[string]deltaEntities, len(env.Data)), pageInfo: env.PageInfo}
	for etype, rawArray := range env.Data {
		var entities deltaEntities
		if json.Unmarshal(rawArray, &entities.raw) != nil {
			return deltaResult{}, false // not an entity array
		}
		entities.ids = make([]string, len(entities.raw))
		entities.index = make(map[string]int, len(entities.raw))
		for i, entity := range entities.raw {
			var probe struct {
				ID string `json:"id"`
			}
			if json.Unmarshal(entity, &probe) != nil || probe.ID == "" {
				return deltaResult{}, false // non-object or missing string id
			}
			if _, duplicate := entities.index[probe.ID]; duplicate {
				return deltaResult{}, false
			}
			entities.ids[i] = probe.ID
			entities.index[probe.ID] = i
		}
		out.data[etype] = entities
	}
	return out, true
}

// diffEntities emits removals first (sorted IDs), then adds/updates in new
// array order. It also checks survivor order and counts the larger side of
// each etype for the churn heuristic, reusing the decode-time indexes.
func diffEntities(oldData, newData map[string]deltaEntities) ([]PatchOp, int, bool) {
	var ops []PatchOp
	total := 0
	for etype, old := range oldData {
		new := newData[etype]
		total += max(len(old.ids), len(new.ids))
		last := -1
		var remIDs []string
		for _, id := range old.ids {
			pos, live := new.index[id]
			if !live {
				remIDs = append(remIDs, id)
				continue
			}
			if pos < last {
				return nil, 0, false // surviving entities reordered
			}
			last = pos
		}
		sort.Strings(remIDs)
		for _, id := range remIDs {
			ops = append(ops, PatchOp{Op: OpRemove, Etype: etype, ID: id})
		}
	}

	for etype, new := range newData {
		old, existed := oldData[etype]
		if !existed {
			total += len(new.ids)
		}
		for i, id := range new.ids {
			entity := new.raw[i]
			oldPos, wasOld := old.index[id]
			switch {
			case !wasOld:
				ops = append(ops, PatchOp{Op: OpAdd, Etype: etype, ID: id, Entity: entity})
			case !sameEntity(old.raw[oldPos], entity):
				ops = append(ops, PatchOp{Op: OpUpdate, Etype: etype, ID: id, Entity: entity})
			}
		}
	}
	return ops, total, true
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
