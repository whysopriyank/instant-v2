package main

import (
	"encoding/json"
	"errors"
	"fmt"
)

// frame is a decoded wire envelope (mirror of internal/sync.Frame's JSON).
type frame map[string]json.RawMessage

func (f frame) op() string {
	var s string
	if raw, ok := f["op"]; ok && json.Unmarshal(raw, &s) == nil {
		return s
	}
	return ""
}

// extractEntities pulls the entity ids out of a refresh-ok payload's
// instaql-result. Two wire shapes are handled:
//
//   - v1 node list (current): an array of nodes whose
//     data['datalog-result']['join-rows'] rows are [entity-id, attr-id, value]
//     triples (internal/sync/nodelist.go);
//   - flat envelope (legacy fallback): {data:{etype:[{id,...}]}}.
func extractEntities(f frame) (map[string]bool, error) {
	rawComp, ok := f["computations"]
	if !ok {
		return nil, errors.New("refresh-ok missing computations")
	}
	var comps []struct {
		Result json.RawMessage `json:"instaql-result"`
	}
	if err := json.Unmarshal(rawComp, &comps); err != nil {
		return nil, err
	}
	if len(comps) == 0 {
		return nil, errors.New("empty computations")
	}
	return extractEntitiesFromResult(comps[0].Result)
}

// extractEntitiesFromResult decodes entity ids from either the node-list
// instaql-result (WS path) or the bare object-tree (SSE/admin path).
func extractEntitiesFromResult(raw json.RawMessage) (map[string]bool, error) {
	out := map[string]bool{}

	// Node-list shape: try first; fall back to the flat envelope.
	// v1 collect-instaql-results-for-client emits ONE flat join-row whose
	// elements are [entity-id, attr-id, value] triples (query.clj:100).
	var nodes []struct {
		Data struct {
			DatalogResult struct {
				JoinRows [][]json.RawMessage `json:"join-rows"`
			} `json:"datalog-result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &nodes); err == nil && len(nodes) > 0 {
		for _, n := range nodes {
			for _, row := range n.Data.DatalogResult.JoinRows {
				for _, t := range row {
					var triple []any
					if json.Unmarshal(t, &triple) != nil || len(triple) == 0 {
						continue
					}
					if eid, ok := triple[0].(string); ok {
						out[eid] = true
					}
				}
			}
		}
		return out, nil
	}

	var res struct {
		Data map[string][]struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, fmt.Errorf("unmarshal %s: %w", raw, err)
	}
	for _, es := range res.Data {
		for _, e := range es {
			out[e.ID] = true
		}
	}
	return out, nil
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
