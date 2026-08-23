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
	"fmt"
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

	patch := &Patch{}
	oldTotal, newTotal := 0, 0

	for _, env := range []*envelope{&oldEnv, &newEnv} {
		for _, raw := range env.Data {
			arr, err := decodeEntities(raw)
			if err != nil {
				return nil, false
			}
			if env == &oldEnv {
				oldTotal += len(arr)
			} else {
				newTotal += len(arr)
			}
		}
	}

	type ent struct {
		id    string
		raw   json.RawMessage // original bytes as produced by instaql
		canon json.RawMessage // map-reserialized bytes for content comparison
	}
	index := func(env *envelope) map[string]map[string]ent {
		out := map[string]map[string]ent{}
		for etype, raw := range env.Data {
			var arr []json.RawMessage
			_ = json.Unmarshal(raw, &arr)
			m := make(map[string]ent, len(arr))
			for _, e := range arr {
				var obj map[string]any
				if json.Unmarshal(e, &obj) != nil {
					continue // caught by decodeEntities above
				}
				id, _ := obj["id"].(string)
				canon, _ := json.Marshal(obj)
				if dup, exists := m[id]; exists && string(dup.canon) != string(canon) {
					continue // duplicates resolved at fallback below via count check
				}
				m[id] = ent{id: id, raw: e, canon: canon}
			}
			out[etype] = m
		}
		return out
	}
	oldIdx, newIdx := index(&oldEnv), index(&newEnv)

	// Duplicate ids with differing bodies make position ambiguous → fallback.
	countCheck := func(env *envelope) bool {
		for etype, raw := range env.Data {
			var arr []json.RawMessage
			_ = json.Unmarshal(raw, &arr)
			seen := map[string]bool{}
			for _, e := range arr {
				var obj map[string]any
				if json.Unmarshal(e, &obj) != nil {
					return false
				}
				id, _ := obj["id"].(string)
				if seen[id] {
					return false
				}
				seen[id] = true
			}
		}
		return true
	}
	if !countCheck(&oldEnv) || !countCheck(&newEnv) {
		return nil, false
	}

	touched := 0
	for etype, oldEnts := range oldIdx {
		newEnts, exists := newIdx[etype]
		if !exists {
			newEnts = map[string]ent{}
		}
		// Removes: present before, absent now (deterministic id order).
		var remIDs []string
		for id := range oldEnts {
			if _, live := newEnts[id]; !live {
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
	etypeOrder := sortedKeys(newIdx)
	for _, etype := range etypeOrder {
		newEnts := newIdx[etype]
		oldEnts, existed := oldIdx[etype]
		if !existed {
			oldEnts = map[string]ent{}
		}
		var arr []json.RawMessage
		if raw, okData := newEnv.Data[etype]; okData {
			_ = json.Unmarshal(raw, &arr)
		}
		for _, e := range arr {
			var obj map[string]any
			if json.Unmarshal(e, &obj) != nil {
				return nil, false
			}
			id, _ := obj["id"].(string)
			prev, wasOld := oldEnts[id]
			if !wasOld {
				patch.Ops = append(patch.Ops, PatchOp{Op: OpAdd, Etype: etype, ID: id, Entity: e})
				touched++
				continue
			}
			if string(prev.canon) != string(objBytes(obj)) {
				patch.Ops = append(patch.Ops, PatchOp{Op: OpUpdate, Etype: etype, ID: id, Entity: e})
				touched++
			}
		}
	}

	// Reorder check: relative order of surviving ids must be identical on
	// both sides (per etype). Any positional shift of common entities is not
	// expressible without move semantics → full envelope.
	for etype, oldEnts := range oldIdx {
		newEnts, exists := newIdx[etype]
		if !exists {
			continue
		}
		var oldCommon, newCommon []string
		forEachID(oldEnv.Data[etype], func(id string) {
			if _, keep := newEnts[id]; keep {
				oldCommon = append(oldCommon, id)
			}
		})
		forEachID(newEnv.Data[etype], func(id string) {
			if _, keep := oldEnts[id]; keep {
				newCommon = append(newCommon, id)
			}
		})
		if len(oldCommon) != len(newCommon) {
			return nil, false
		}
		for i := range oldCommon {
			if oldCommon[i] != newCommon[i] {
				return nil, false // reorder / cross-etype shuffle ambiguity
			}
		}
	}

	// Size heuristic: too much churn ⇒ patch ≈ full envelope anyway.
	total := oldTotal
	if newTotal > total {
		total = newTotal
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

// decodeEntities parses one etype's value as an array of objects carrying
// string ids; anything else is a shape we refuse to diff.
func decodeEntities(raw json.RawMessage) ([]json.RawMessage, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil {
		return nil, fmt.Errorf("delta: etype payload is not an array: %w", err)
	}
	for _, e := range arr {
		var obj map[string]any
		if err := json.Unmarshal(e, &obj); err != nil {
			return nil, fmt.Errorf("delta: entity is not an object: %w", err)
		}
		if id, _ := obj["id"].(string); id == "" {
			return nil, fmt.Errorf("delta: entity missing string id")
		}
	}
	return arr, nil
}

func forEachID(raw json.RawMessage, fn func(string)) {
	var arr []json.RawMessage
	if json.Unmarshal(raw, &arr) != nil {
		return
	}
	for _, e := range arr {
		var obj map[string]any
		if json.Unmarshal(e, &obj) != nil {
			continue
		}
		if id, _ := obj["id"].(string); id != "" {
			fn(id)
		}
	}
}

func objBytes(obj map[string]any) json.RawMessage {
	b, _ := json.Marshal(obj) // Go marshals maps with sorted keys: canonical
	return b
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

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
