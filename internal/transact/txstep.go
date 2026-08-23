// Package transact implements the instaml → storage pipeline: tx-step parsing,
// lookup-ref resolution, the op-ordered execution from db/transaction.clj,
// and required-field validation. Permissioned gating is layered by the caller
// via perms.Check; this package is responsible for doing what the permissioned
// transaction says *will* happen.
//
// Validates inputs against the spec in db/transaction.clj (the frozen grammar
// in docs/03 section 4) and against the live AttrCatalog so that later phases
// see coherent attribute metadata.
package transact

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// Step is a wire tx-step after being classified by op.
type Step struct {
	Op   string
	Raw  json.RawMessage
	Args []json.RawMessage
}

// ParseSteps classifies each wire JSON array into a Step and validates the
// tuple arity. Unknown ops are accepted here and flagged during Apply so that
// unknown-round-trip fidelity (forward compat) is elsewhere preserved.
func ParseSteps(rawSteps []json.RawMessage) ([]Step, error) {
	out := make([]Step, 0, len(rawSteps))
	for _, raw := range rawSteps {
		var arr []json.RawMessage
		if err := json.Unmarshal(raw, &arr); err != nil {
			return nil, fmt.Errorf("transact: step not an array: %w", err)
		}
		if len(arr) == 0 {
			return nil, fmt.Errorf("transact: step must be non-empty array")
		}
		var op string
		if err := json.Unmarshal(arr[0], &op); err != nil {
			return nil, err
		}
		if err := validateArity(op, len(arr)-1); err != nil {
			return nil, err
		}
		out = append(out, Step{Op: op, Raw: raw, Args: arr[1:]})
	}
	return out, nil
}

func validateArity(op string, nargs int) error {
	switch op {
	case "add-triple", "deep-merge-triple", "retract-triple":
		if nargs < 3 || nargs > 4 {
			return fmt.Errorf("transact: %s: want 3 or 4 args, got %d", op, nargs)
		}
	case "add-attr", "update-attr":
		if nargs != 1 {
			return fmt.Errorf("transact: %s: want 1 arg", op)
		}
	case "delete-attr", "restore-attr":
		if nargs != 1 {
			return fmt.Errorf("transact: %s: want 1 arg", op)
		}
	case "delete-entity":
		if nargs < 1 || nargs > 2 {
			return fmt.Errorf("transact: delete-entity: want 1 or 2 args")
		}
	case "rule-params":
		if nargs < 1 {
			return fmt.Errorf("transact: rule-params: want 1+ args")
		}
	default:
		// forward-compat: accept unknown ops
	}
	return nil
}

// KnownOps is the full frozen grammar (docs/03 section 4); used for
// reordering (first-appearance grouping) and for the "unknown op" lint.
var KnownOps = map[string]bool{
	"add-triple": true, "deep-merge-triple": true, "retract-triple": true,
	"delete-entity": true,
	"add-attr":      true, "update-attr": true, "delete-attr": true, "restore-attr": true,
	"rule-params": true,
}

// Mode is the add-triple option bag ({mode:create|update|upsert}).
type Mode string

const (
	ModeCreate Mode = "create"
	ModeUpdate Mode = "update"
	ModeUpsert Mode = ""
)

// tripleArgs decodes one (eid, attr-id, value, [opts]) four-tuple.
// Lookup-ref decoding (Phase 2) is done later in lookup.go; here we keep the
// raws so that resolution can happen inside the tx.
type tripleArgs struct {
	EID    json.RawMessage
	AttrID [16]byte
	Value  json.RawMessage
	Opts   map[string]any // mode only so far
}

func parseTripleArgs(st Step, catalog *platform.AttrCatalog) (tripleArgs, error) {
	var t tripleArgs
	t.EID = st.Args[0]
	var attrIDStr string
	if err := json.Unmarshal(st.Args[1], &attrIDStr); err != nil {
		return t, fmt.Errorf("attr id: %w", err)
	}
	if err := parseUUID(attrIDStr, &t.AttrID); err != nil {
		return t, fmt.Errorf("attr id %q: %w", attrIDStr, err)
	}
	if _, ok := catalog.ByID(t.AttrID); !ok {
		return t, fmt.Errorf("unknown attr %s", attrIDStr)
	}
	t.Value = st.Args[2]
	if len(st.Args) == 4 {
		var opts map[string]any
		if err := json.Unmarshal(st.Args[3], &opts); err != nil {
			return t, fmt.Errorf("opts: %w", err)
		}
		t.Opts = opts
	}
	return t, nil
}

func parseUUID(s string, dst *[16]byte) error {
	if len(s) != 36 {
		return fmt.Errorf("uuid: bad length %d", len(s))
	}
	hexOnly := strings.ReplaceAll(s, "-", "")
	if len(hexOnly) != 32 {
		return fmt.Errorf("uuid: bad hex length")
	}
	var b []byte
	for i := 0; i < 32; i += 2 {
		hi, ok1 := hexVal(hexOnly[i])
		lo, ok2 := hexVal(hexOnly[i+1])
		if !ok1 || !ok2 {
			return fmt.Errorf("uuid: bad hex char")
		}
		b = append(b, byte(hi<<4|lo))
	}
	copy(dst[:], b)
	return nil
}

func hexVal(c byte) (int, bool) {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0'), true
	case c >= 'a' && c <= 'f':
		return int(c - 'a' + 10), true
	case c >= 'A' && c <= 'F':
		return int(c - 'A' + 10), true
	}
	return 0, false
}
