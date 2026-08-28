package benchharness

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// Materialized is a canonical semantic query result. Entities and attribute
// keys are sorted only when encoded; the in-memory maps stay convenient for
// applying full and delta refreshes.
type Materialized struct {
	QueryID  string
	Entities map[string]Entity
}

// Digest is the stable SHA-256 digest of a canonical materialized result.
func (m Materialized) Digest() (string, error) {
	b, err := CanonicalJSON(m)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

// CanonicalJSON emits deterministic JSON for arbitrary JSON-like values. Map
// keys are sorted, entities are sorted by id, and arrays retain query order.
// It strips only fields explicitly documented as volatile by the caller.
func CanonicalJSON(v any) ([]byte, error) {
	normalized, err := canonicalValue(v)
	if err != nil {
		return nil, err
	}
	return json.Marshal(normalized)
}

func canonicalValue(v any) (any, error) {
	switch x := v.(type) {
	case Materialized:
		ids := make([]string, 0, len(x.Entities))
		for id := range x.Entities {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		entities := make([]any, 0, len(ids))
		for _, id := range ids {
			e := x.Entities[id]
			attrs, err := canonicalValue(e.Attributes)
			if err != nil {
				return nil, err
			}
			entities = append(entities, map[string]any{"id": e.ID, "bucket": e.Bucket, "rank": e.Rank, "attrs": attrs})
		}
		return map[string]any{"query-id": x.QueryID, "entities": entities}, nil
	case Entity:
		attrs, err := canonicalValue(x.Attributes)
		if err != nil {
			return nil, err
		}
		return map[string]any{"id": x.ID, "bucket": x.Bucket, "rank": x.Rank, "attrs": attrs}, nil
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make(map[string]any, len(x))
		for _, k := range keys {
			cv, err := canonicalValue(x[k])
			if err != nil {
				return nil, err
			}
			out[k] = cv
		}
		return out, nil
	case []any:
		out := make([]any, len(x))
		for i := range x {
			cv, err := canonicalValue(x[i])
			if err != nil {
				return nil, err
			}
			out[i] = cv
		}
		return out, nil
	case json.RawMessage:
		var decoded any
		if err := json.Unmarshal(x, &decoded); err != nil {
			return nil, err
		}
		return canonicalValue(decoded)
	case nil, string, bool, float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return x, nil
	default:
		// Marshal/unmarshal supports typed aliases without introducing a
		// reflection-heavy canonicalizer into the hot benchmark loop.
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		var decoded any
		if err := json.Unmarshal(b, &decoded); err != nil {
			return nil, err
		}
		return canonicalValue(decoded)
	}
}

// Delta is the semantic equivalent of a delta refresh.
type Delta struct {
	QueryID      string
	Adds         []Entity
	Updates      []Entity
	Removes      []string
	StateVersion int64
}

type RefreshKind string

const (
	RefreshFull  RefreshKind = "full"
	RefreshDelta RefreshKind = "delta"
)

// Refresh is the common semantic envelope accepted from either full or delta
// targets. ProcessedTransactionID is evidence only; it is not synthesized by
// this package when the target omits it.
type Refresh struct {
	QueryID                string
	Kind                   RefreshKind
	Full                   *Materialized
	Delta                  *Delta
	ProcessedTransactionID string
	StateVersion           int64
	At                     time.Time
}

func ApplyRefresh(previous Materialized, refresh Refresh) (Materialized, error) {
	switch refresh.Kind {
	case RefreshFull:
		if refresh.Full == nil {
			return Materialized{}, fmt.Errorf("full refresh has no materialized state")
		}
		out := Materialized{QueryID: refresh.Full.QueryID, Entities: cloneEntities(refresh.Full.Entities)}
		if refresh.QueryID != "" && out.QueryID != "" && refresh.QueryID != out.QueryID {
			return Materialized{}, fmt.Errorf("full refresh query mismatch")
		}
		return out, nil
	case RefreshDelta:
		if refresh.Delta == nil {
			return Materialized{}, fmt.Errorf("delta refresh has no delta")
		}
		return ApplyDelta(previous, *refresh.Delta)
	default:
		return Materialized{}, fmt.Errorf("unknown refresh kind %q", refresh.Kind)
	}
}

// ApplyDelta applies a delta to a prior materialized result, returning a copy.
func ApplyDelta(previous Materialized, delta Delta) (Materialized, error) {
	if previous.QueryID != "" && delta.QueryID != "" && previous.QueryID != delta.QueryID {
		return Materialized{}, fmt.Errorf("delta query %q does not match %q", delta.QueryID, previous.QueryID)
	}
	out := Materialized{QueryID: previous.QueryID, Entities: make(map[string]Entity, len(previous.Entities)+len(delta.Adds)+len(delta.Updates))}
	if delta.QueryID != "" {
		out.QueryID = delta.QueryID
	}
	for id, e := range previous.Entities {
		out.Entities[id] = cloneEntity(e)
	}
	for _, id := range delta.Removes {
		delete(out.Entities, id)
	}
	for _, e := range delta.Adds {
		if e.ID == "" {
			return Materialized{}, errorsFor("delta add has empty entity id")
		}
		out.Entities[e.ID] = cloneEntity(e)
	}
	for _, e := range delta.Updates {
		if e.ID == "" {
			return Materialized{}, errorsFor("delta update has empty entity id")
		}
		out.Entities[e.ID] = cloneEntity(e)
	}
	return out, nil
}

func cloneEntity(e Entity) Entity {
	attrs := make(map[string]any, len(e.Attributes))
	for k, v := range e.Attributes {
		attrs[k] = v
	}
	e.Attributes = attrs
	return e
}

type stringError string

func (e stringError) Error() string { return string(e) }
func errorsFor(s string) error      { return stringError(s) }

// Materialize computes the expected query state from a fixture and a mutation
// prefix. Prefix zero is the seeded state; negative or overrun prefixes fail.
func (o *PrefixOracle) Materialize(queryID string, prefix int) (Materialized, error) {
	o.mu.RLock()
	events := append([]Mutation(nil), o.events...)
	initial := cloneEntities(o.initial)
	o.mu.RUnlock()
	if prefix < 0 || prefix > len(events) {
		return Materialized{}, fmt.Errorf("prefix %d outside [0,%d]", prefix, len(events))
	}
	q, ok := o.query(queryID)
	if !ok {
		return Materialized{}, errMissingQuery
	}
	state := initial
	for _, event := range events[:prefix] {
		applyMutation(state, event)
	}
	result := Materialized{QueryID: queryID, Entities: make(map[string]Entity)}
	for id, e := range state {
		if q.Bucket >= 0 && e.Bucket != q.Bucket {
			continue
		}
		result.Entities[id] = cloneEntity(e)
	}
	if q.TopN > 0 && len(result.Entities) > q.TopN {
		ids := make([]string, 0, len(result.Entities))
		for id := range result.Entities {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool {
			a, b := result.Entities[ids[i]], result.Entities[ids[j]]
			if a.Rank == b.Rank {
				return ids[i] < ids[j]
			}
			return a.Rank < b.Rank
		})
		for len(ids) > q.TopN {
			delete(result.Entities, ids[len(ids)-1])
			ids = ids[:len(ids)-1]
		}
	}
	return result, nil
}

func cloneEntities(in map[string]Entity) map[string]Entity {
	out := make(map[string]Entity, len(in))
	for id, e := range in {
		out[id] = cloneEntity(e)
	}
	return out
}

func applyMutation(state map[string]Entity, m Mutation) {
	switch m.Kind {
	case MutationAppend:
		state[m.EntityID] = Entity{ID: m.EntityID, Bucket: m.Bucket, Rank: m.Rank, Attributes: map[string]any{"value": m.Marker}}
	case MutationUpdate:
		e, ok := state[m.EntityID]
		if !ok {
			e = Entity{ID: m.EntityID, Bucket: m.Bucket, Attributes: map[string]any{}}
		}
		if e.Attributes == nil {
			e.Attributes = map[string]any{}
		}
		e.Attributes["value"] = m.Marker
		state[m.EntityID] = e
	case MutationRetract:
		delete(state, m.EntityID)
	case MutationReorder:
		e, ok := state[m.EntityID]
		if ok {
			e.Rank = m.Rank
			state[m.EntityID] = e
		}
	}
}
