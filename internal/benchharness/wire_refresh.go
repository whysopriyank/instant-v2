package benchharness

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

// wireComputation is the protocol-level computation object used by both V1
// and V2 refresh frames. Keeping this shape here lets the decoder inspect all
// computations before deciding whether a frame is relevant to a subscription.
type wireComputation struct {
	Query  json.RawMessage `json:"instaql-query"`
	Result json.RawMessage `json:"instaql-result"`
	Delta  json.RawMessage `json:"delta"`
}

// DecodeWireRefresh parses the actual V1/V2 envelope using label-shaped
// attributes. A TargetSession supplies the explicit UUID alias map and the
// subscribed wire query through decodeWireRefresh below.
func DecodeWireRefresh(ev SessionEvent, queryID string) (Refresh, error) {
	return decodeWireRefresh(ev, queryID, nil, nil)
}

// DecodeWireRefreshWithAliases parses a refresh where the target may encode
// semantic attributes as UUIDs. aliases maps semantic names (id, value,
// bucket, rank) to their wire labels/UUIDs. Unknown attributes remain lossless.
func DecodeWireRefreshWithAliases(ev SessionEvent, queryID string, aliases map[string]string) (Refresh, error) {
	return decodeWireRefresh(ev, queryID, aliases, nil)
}

// DecodeWireRefreshForQuery additionally filters a multi-computation frame to
// the computation(s) matching wireQuery. This is the form used by live
// sessions, where unrelated subscriptions can share a refresh envelope.
func DecodeWireRefreshForQuery(ev SessionEvent, queryID string, wireQuery json.RawMessage, aliases map[string]string) (Refresh, error) {
	return decodeWireRefresh(ev, queryID, aliases, wireQuery)
}

// V1 and V2 return the initial materialized result in add-query-ok. Some test
// adapters emit a separate refresh frame instead, so absence of result is an
// explicit fallback rather than a fabricated empty snapshot.
func decodeAddQuerySnapshot(ev SessionEvent, queryID string, aliases map[string]string, expectedQuery json.RawMessage) (Refresh, bool, error) {
	if ev.Op != "add-query-ok" {
		return Refresh{}, false, nil
	}
	if len(bytes.TrimSpace(ev.Payload)) == 0 {
		return Refresh{}, false, nil
	}
	var envelope struct {
		Query     json.RawMessage `json:"q"`
		Result    json.RawMessage `json:"result"`
		Processed json.RawMessage `json:"processed-tx-id"`
	}
	if err := json.Unmarshal(ev.Payload, &envelope); err != nil {
		return Refresh{}, true, fmt.Errorf("decode add-query snapshot: %w", err)
	}
	if len(envelope.Result) == 0 || rawIsNull(envelope.Result) {
		return Refresh{}, false, nil
	}
	query := envelope.Query
	if len(query) == 0 || rawIsNull(query) {
		query = expectedQuery
	}
	if len(query) == 0 || rawIsNull(query) {
		return Refresh{}, true, errors.New("add-query snapshot missing query")
	}
	payload, err := json.Marshal(struct {
		Computations []wireComputation `json:"computations"`
		Processed    json.RawMessage   `json:"processed-tx-id,omitempty"`
	}{Computations: []wireComputation{{Query: query, Result: envelope.Result}}, Processed: envelope.Processed})
	if err != nil {
		return Refresh{}, true, err
	}
	ev.Op = "refresh-ok"
	ev.Payload = payload
	refresh, err := decodeWireRefresh(ev, queryID, aliases, expectedQuery)
	return refresh, true, err
}

func decodeWireRefresh(ev SessionEvent, queryID string, aliases map[string]string, expectedQuery json.RawMessage) (Refresh, error) {
	if ev.Op != "refresh-ok" && ev.Op != "refresh-ok-delta" {
		return Refresh{}, fmt.Errorf("not a refresh event: %s", ev.Op)
	}
	var envelope struct {
		Computations []wireComputation `json:"computations"`
		Processed    json.RawMessage   `json:"processed-tx-id"`
	}
	if err := json.Unmarshal(ev.Payload, &envelope); err != nil {
		return Refresh{}, err
	}
	if len(envelope.Computations) == 0 {
		return Refresh{}, fmt.Errorf("refresh missing computations")
	}
	computations, err := matchingComputations(envelope.Computations, expectedQuery)
	if err != nil {
		return Refresh{}, err
	}
	if len(expectedQuery) == 0 || rawIsNull(expectedQuery) {
		for i, computation := range computations {
			if len(computation.Query) == 0 || rawIsNull(computation.Query) {
				return Refresh{}, fmt.Errorf("computation %d missing instaql-query", i)
			}
		}
	}
	refresh := Refresh{QueryID: queryID, Kind: RefreshFull, ProcessedTransactionID: scalarString(envelope.Processed), At: ev.At}
	if n, err := strconv.ParseInt(refresh.ProcessedTransactionID, 10, 64); err == nil && n > 0 {
		refresh.StateVersion = n
	}

	hasDelta := ev.Op == "refresh-ok-delta"
	for _, computation := range computations {
		if len(computation.Delta) > 0 && !rawIsNull(computation.Delta) {
			hasDelta = true
		}
	}
	if hasDelta {
		refresh.Kind = RefreshDelta
		delta := &Delta{QueryID: queryID, StateVersion: refresh.StateVersion}
		for _, computation := range computations {
			if len(computation.Delta) == 0 || rawIsNull(computation.Delta) {
				return Refresh{}, fmt.Errorf("delta refresh computation missing delta")
			}
			if len(computation.Result) > 0 && !rawIsNull(computation.Result) {
				return Refresh{}, fmt.Errorf("delta refresh computation contains both result and delta")
			}
			var patch struct {
				Ops []struct {
					Op     string          `json:"op"`
					ID     string          `json:"id"`
					Entity json.RawMessage `json:"entity"`
				} `json:"ops"`
			}
			if err := json.Unmarshal(computation.Delta, &patch); err != nil {
				return Refresh{}, fmt.Errorf("decode refresh delta: %w", err)
			}
			for _, op := range patch.Ops {
				switch op.Op {
				case "remove":
					if op.ID == "" {
						return Refresh{}, fmt.Errorf("refresh delta remove has empty entity id")
					}
					delta.Removes = append(delta.Removes, op.ID)
				case "add", "update":
					entity, err := wireEntity(op.ID, op.Entity, aliases)
					if err != nil {
						return Refresh{}, err
					}
					if op.Op == "add" {
						delta.Adds = append(delta.Adds, entity)
					} else {
						delta.Updates = append(delta.Updates, entity)
					}
				default:
					return Refresh{}, fmt.Errorf("unknown refresh delta op %q", op.Op)
				}
			}
		}
		refresh.Delta = delta
		return refresh, nil
	}

	result := Materialized{QueryID: queryID, Entities: map[string]Entity{}}
	for _, computation := range computations {
		if len(computation.Query) == 0 || rawIsNull(computation.Query) {
			return Refresh{}, fmt.Errorf("computation missing instaql-query")
		}
		part, err := decodeWireResult(computation.Result, queryID, aliases)
		if err != nil {
			return Refresh{}, err
		}
		for id, entity := range part.Entities {
			result.Entities[id] = entity
		}
	}
	refresh.Full = &result
	return refresh, nil
}

func matchingComputations(all []wireComputation, expectedQuery json.RawMessage) ([]wireComputation, error) {
	if len(expectedQuery) == 0 || rawIsNull(expectedQuery) {
		return all, nil
	}
	var matches []wireComputation
	for i, computation := range all {
		if len(computation.Query) == 0 || rawIsNull(computation.Query) {
			return nil, fmt.Errorf("computation %d missing instaql-query", i)
		}
		equal, err := sameWireJSON(expectedQuery, computation.Query)
		if err != nil {
			return nil, fmt.Errorf("compare computation %d query: %w", i, err)
		}
		if equal {
			matches = append(matches, computation)
		}
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("refresh has no computation matching subscribed query")
	}
	return matches, nil
}

func rawIsNull(raw []byte) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func sameWireJSON(left, right []byte) (bool, error) {
	l, err := rawValue(left)
	if err != nil {
		return false, err
	}
	r, err := rawValue(right)
	if err != nil {
		return false, err
	}
	// encoding/json deterministically orders object keys, so this comparison
	// ignores harmless whitespace/key-order differences in wire frames.
	lb, err := json.Marshal(l)
	if err != nil {
		return false, err
	}
	rb, err := json.Marshal(r)
	if err != nil {
		return false, err
	}
	return bytes.Equal(lb, rb), nil
}
