package sync

// v1-conformant result shaping for refresh frames (docs/reference/03-protocol.md §5).
//
// The frozen TS client consumes refresh-ok `instaql-result` as a NODE LIST
// (client/packages/core/src/Reactor.js:761-777 → model/instaqlResult.js):
//
//	extractTriples(result) walks idNodes.forEach over the array, reading
//	node.data['datalog-result']['join-rows'] as [[triple,…],…] and
//	result?.[0]?.data?.['page-info'] / ['aggregate'].
//
// v1 produces that shape in reactive/query.clj:76-96
// (collect-instaql-results-for-client): one entry per datalog sub-result,
// triples as [entity-id, attr-id, value] rows, page-info/aggregate merged in
// keyed by etype. This file ports that projection over instaql.Result's flat
// envelope so every full frame on the wire is byte-compatible with what the
// frozen SDK expects.
import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// BuildNodeList converts a flat instaql-result envelope
// ({data:{etype:[entities…]},page-info?,aggregate?}) into v1's node-list:
// one node per top-level etype, each carrying its triples under
// data['datalog-result']['join-rows'] plus empty child-nodes.
//
// The envelope carries a single level-0 page-info/aggregate; it is attached
// to the FIRST node (sorted etype order) keyed by that node's etype — the
// position the frozen client reads (result[0].data). Multi-form paginated
// queries lose per-form attribution inside the executor already; this keeps
// the single-form case exact.
//
// Values use json.Number to preserve numeric precision and lexical spelling.
func BuildNodeList(cat *platform.AttrCatalog, result json.RawMessage) (json.RawMessage, error) {
	var env struct {
		Data      map[string]json.RawMessage `json:"data"`
		PageInfo  json.RawMessage            `json:"page-info"`
		Aggregate json.RawMessage            `json:"aggregate"`
	}
	if err := json.Unmarshal(result, &env); err != nil {
		return nil, fmt.Errorf("nodelist: bad instaql-result envelope: %w", err)
	}

	etypes := make([]string, 0, len(env.Data))
	for etype := range env.Data {
		etypes = append(etypes, etype)
	}
	sort.Strings(etypes)

	nodes := make([]map[string]any, 0, len(etypes))
	for i, etype := range etypes {
		var ents []map[string]any
		raw := env.Data[etype]
		if len(raw) > 0 && string(raw) != "null" {
			// UseNumber: values re-marshal byte-exactly through
			// json.Number (see func comment).
			dec := json.NewDecoder(bytes.NewReader(raw))
			dec.UseNumber()
			if err := dec.Decode(&ents); err != nil {
				return nil, fmt.Errorf("nodelist: %s payload: %w", etype, err)
			}
		}

		// v1 collect-instaql-results-for-client (query.clj:91) wraps ALL
		// deduped triples as ONE flat row: join-rows = [triples…], where
		// each triple is [entity-id, attr-id, value]. The implicit
		// <etype>/id attr participates: TS clients assemble entities from
		// the primary-key triple, so omitting it renders empty lists.
		idAttr := cat.FindByEtypeLabel(etype, "id")
		triples := make([]any, 0, len(ents))
		for _, e := range ents {
			id, _ := e["id"].(string)
			if id == "" {
				return nil, fmt.Errorf("nodelist: %s entity missing id", etype)
			}
			if idAttr != nil {
				triples = append(triples, []any{id, idAttr.UUID(), id})
			}
			labels := make([]string, 0, len(e))
			for label := range e {
				if label == "id" {
					continue
				}
				labels = append(labels, label)
			}
			sort.Strings(labels)
			for _, label := range labels {
				attr := cat.FindByEtypeLabel(etype, label)
				switch {
				case attr != nil && attr.ValueType == "ref":
					// Link attrs project one triple per linked child id;
					// child bodies arrive via the child etype's own node.
					for _, cid := range linkedIDs(e[label]) {
						triples = append(triples, []any{id, attr.UUID(), cid})
					}
				case attr != nil:
					triples = append(triples, []any{id, attr.UUID(), e[label]})
				default:
					// Unknown label (defensive): keep it verbatim so the
					// projection stays lossless.
					triples = append(triples, []any{id, label, e[label]})
				}
			}
		}

		// Node shape mirrors v1 (query.clj): data carries ONLY the
		// datalog-result — v1 clients never read etype/k keys, and the
		// differential treats extra keys as divergence.
		data := map[string]any{
			"datalog-result": map[string]any{
				"join-rows": []any{triples}, // one flat row of all triples
			},
		}
		if i == 0 && isRawSet(env.PageInfo) {
			data["page-info"] = map[string]any{etype: json.RawMessage(env.PageInfo)}
		}
		if i == 0 && isRawSet(env.Aggregate) {
			data["aggregate"] = map[string]any{etype: json.RawMessage(env.Aggregate)}
		}
		nodes = append(nodes, map[string]any{
			"data":        data,
			"child-nodes": []any{},
		})
	}
	return json.Marshal(nodes)
}

// linkedIDs normalizes a ref attr value into linked child ids; values may be
// nested child objects ({id:…}) or bare id strings. Values come from the
// UseNumber decode — ref payloads are strings, so number fidelity is moot
// here.
func linkedIDs(v any) []string {
	arr, ok := v.([]any)
	if !ok {
		if s := asLinkedID(v); s != "" {
			return []string{s}
		}
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, cv := range arr {
		if s := asLinkedID(cv); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func asLinkedID(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case map[string]any:
		s, _ := t["id"].(string)
		return s
	default:
		return ""
	}
}

func isRawSet(raw json.RawMessage) bool {
	return len(raw) > 0 && string(raw) != "null"
}
