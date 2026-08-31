package transact

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// validationErrf builds v1-shaped user-facing errors ("Validation failed for
// <input-type>: <msg>"). Capitalization and trailing punctuation intentionally
// mirror ex/throw-validation-err!'s client-visible message shape.
func validationErrf(inputType, format string, args ...any) error {
	return fmt.Errorf("Validation failed for "+inputType+": "+format, args...)
}

// highLevelOps are the admin-plane ops that need lowering; anything else
// passes through untouched (existing low-level passthrough is preserved).
var highLevelOps = map[string]bool{
	"create": true, "update": true, "merge": true,
	"link": true, "unlink": true, "delete": true, "ruleParams": true,
}

// HasHighLevelOps reports whether any wire step is a high-level admin op.
// Malformed steps report false and are left for ParseSteps to reject.
func HasHighLevelOps(rawSteps []json.RawMessage) bool {
	for _, raw := range rawSteps {
		var arr []json.RawMessage
		if json.Unmarshal(raw, &arr) != nil || len(arr) == 0 {
			continue
		}
		var op string
		if json.Unmarshal(arr[0], &op) != nil {
			continue
		}
		if highLevelOps[op] {
			return true
		}
	}
	return false
}

type parsedEID struct {
	isUUID bool
	uuid   string
	ident  string // lookup attr name (before ref-lookup resolution)
	value  json.RawMessage
}

type parsedStep struct {
	raw     json.RawMessage
	op      string
	high    bool
	etype   string
	eid     parsedEID
	obj     map[string]json.RawMessage // create/update/merge/link/unlink
	objKeys []string                   // sorted for determinism
	opts    map[string]any             // update/merge 4th arg
	params  json.RawMessage            // ruleParams
	hasOpts bool
}

// ---- parsing ----------------------------------------------------------------

func parseAdminStep(raw json.RawMessage) (parsedStep, error) {
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil {
		return parsedStep{}, fmt.Errorf("transact: step not an array: %w", err)
	}
	if len(arr) == 0 {
		return parsedStep{}, fmt.Errorf("transact: step must be non-empty array")
	}
	var op string
	if err := json.Unmarshal(arr[0], &op); err != nil {
		return parsedStep{}, fmt.Errorf("transact: step op must be a string")
	}
	if !highLevelOps[op] {
		return parsedStep{raw: raw, op: op}, nil
	}
	// args exclude the op itself: [etype, eid, obj(, opts)] for the
	// obj-taking ops; delete is [etype, eid]; ruleParams is [etype, eid, params].
	minArgs := map[string]int{
		"create": 3, "update": 3, "merge": 3, "link": 3, "unlink": 3,
		"delete":     2,
		"ruleParams": 3,
	}[op]
	if len(arr)-1 < minArgs {
		return parsedStep{}, fmt.Errorf("transact: %s: want %d+ args, got %d", op, minArgs, len(arr)-1)
	}
	ps := parsedStep{raw: raw, op: op, high: true}
	if err := json.Unmarshal(arr[1], &ps.etype); err != nil {
		return parsedStep{}, fmt.Errorf("transact: %s: etype must be a string", op)
	}
	eid, err := parseAdminEID(arr[2])
	if err != nil {
		return parsedStep{}, err
	}
	ps.eid = eid

	switch op {
	case "create", "update", "merge", "link", "unlink":
		obj := map[string]json.RawMessage{}
		if err := json.Unmarshal(arr[3], &obj); err != nil {
			return parsedStep{}, fmt.Errorf("transact: %s: obj must be an object", op)
		}
		ps.obj = obj
		ps.objKeys = sortedKeys(obj)
		if op == "update" || op == "merge" {
			if len(arr) >= 5 {
				opts := map[string]any{}
				if err := json.Unmarshal(arr[4], &opts); err != nil {
					return parsedStep{}, fmt.Errorf("transact: %s: opts must be an object", op)
				}
				ps.opts, ps.hasOpts = opts, true
			}
		}
	case "ruleParams":
		ps.params = arr[3]
	}
	return ps, nil
}

// parseAdminEID accepts a uuid string, an encoded lookup string
// "lookup__<attr>__<json>", an [attr, value] pair, or a single-entry
// {attr: value} object — v1's ::lookup spec.
func parseAdminEID(raw json.RawMessage) (parsedEID, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if strings.HasPrefix(s, "lookup__") {
			return parseEncodedLookup(s)
		}
		var u [16]byte
		if parseUUID(s, &u) == nil {
			return parsedEID{isUUID: true, uuid: s}, nil
		}
		return parsedEID{}, validationErrf("steps",
			"Invalid entity ID '%s'. Entity IDs must be UUIDs or lookup references.", s)
	}
	var pair []json.RawMessage
	if err := json.Unmarshal(raw, &pair); err == nil {
		if len(pair) != 2 {
			return parsedEID{}, validationErrf("lookup",
				"Invalid entity ID '%s'. Entity IDs must be UUIDs or lookup references.", raw)
		}
		var ident string
		if err := json.Unmarshal(pair[0], &ident); err != nil {
			return parsedEID{}, fmt.Errorf("transact: lookup attr name must be a string")
		}
		return parsedEID{ident: ident, value: pair[1]}, nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err == nil {
		if len(obj) != 1 {
			return parsedEID{}, validationErrf("lookup",
				"lookup must be an object with a single unique attr and value.")
		}
		for k, v := range obj {
			return parsedEID{ident: k, value: v}, nil
		}
	}
	return parsedEID{}, validationErrf("steps",
		"Invalid entity ID '%s'. Entity IDs must be UUIDs or lookup references.", raw)
}

// parseEncodedLookup ports parse-lookup: split on "__", the second component
// is the attr name, everything after re-joins into the JSON-encoded value
// (so JSON payloads containing "__" survive).
func parseEncodedLookup(s string) (parsedEID, error) {
	parts := strings.Split(s, "__")
	if len(parts) < 3 {
		return parsedEID{}, validationErrf("lookup", "lookup value is invalid")
	}
	joined := strings.Join(parts[2:], "__")
	if !json.Valid([]byte(joined)) {
		return parsedEID{}, validationErrf("lookup", "lookup value is invalid")
	}
	return parsedEID{ident: parts[1], value: json.RawMessage(joined)}, nil
}

func sortedKeys(m map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// decodeLinkValue splits a link obj value into its one-or-many eid raws.
// A JSON array always means a LIST of eids (v1 coll? semantics): bare
// two-element pairs are ambiguous in that position and must be spelled as
// "lookup__…" strings or single-entry objects instead.
func decodeLinkValue(raw json.RawMessage) []json.RawMessage {
	var many []json.RawMessage
	if err := json.Unmarshal(raw, &many); err == nil {
		return many
	}
	return []json.RawMessage{raw}
}
