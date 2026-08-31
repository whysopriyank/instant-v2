package transact

import (
	"encoding/json"
	"strings"

	"github.com/instant-v2/instant-v2/internal/platform"
)

func collectTouchedEntities(steps []Step) [][16]byte {
	seen := make(map[[16]byte]struct{})
	var out [][16]byte
	for _, st := range steps {
		switch st.Op {
		case "add-triple", "deep-merge-triple", "retract-triple", "delete-entity":
			if len(st.Args) == 0 {
				continue
			}
			var eidStr string
			_ = json.Unmarshal(st.Args[0], &eidStr)
			var u [16]byte
			if err := parseUUID(eidStr, &u); err == nil {
				if _, ok := seen[u]; !ok {
					seen[u] = struct{}{}
					out = append(out, u)
				}
			}
		}
	}
	return out
}

func appendDistinctEntities(base [][16]byte, extra ...[16]byte) [][16]byte {
	seen := make(map[[16]byte]struct{}, len(base)+len(extra))
	for _, id := range base {
		seen[id] = struct{}{}
	}
	for _, id := range extra {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		base = append(base, id)
	}
	return base
}

// TouchedTriple describes one resolved triple-level write for change-routed
// invalidation (docs/reference/09-tier2-architecture.md §T2.5).
type TouchedTriple struct {
	EntityID string // canonical uuid text
	AttrID   string
	Etype    string
}

// ResolveTriples maps triple-class steps onto the entities they touch. The
// second return is false when any step is NOT a plain triple op — delete-entity
// cascades, schema ops, unknown ops — and callers must fall back to
// topic-wide invalidation instead of pretending to know the change set.
func ResolveTriples(steps []Step, cat *platform.AttrCatalog) ([]TouchedTriple, bool) {
	out := make([]TouchedTriple, 0, len(steps))
	for _, st := range steps {
		switch st.Op {
		case "add-triple", "deep-merge-triple", "retract-triple":
			ta, err := parseTripleArgs(st, cat)
			if err != nil {
				return nil, false
			}
			eid := strings.Trim(string(ta.EID), `"`)
			a, ok := cat.ByID(ta.AttrID)
			if !ok {
				return nil, false
			}
			attrID := platform.UUIDToStr(ta.AttrID)
			etype := "$default"
			if a.Etype != nil {
				etype = *a.Etype
			}
			out = append(out, TouchedTriple{EntityID: eid, AttrID: attrID, Etype: etype})
		default:
			return nil, false
		}
	}
	return out, true
}
