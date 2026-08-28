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
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/instant-v2/instant-v2/internal/perms"
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

// hasOp reports whether any step carries the given op.
func hasOp(steps []Step, op string) bool {
	for _, st := range steps {
		if st.Op == op {
			return true
		}
	}
	return false
}

// wireAttr is the client-supplied add-attr payload (frozen shape, docs/03 §4):
//
//	{"id": uuid, "forward-identity": [identId, etype, label],
//	 "value-type": "blob"|"number"|"ref", cardinality: "one"|"many",
//	 "reverse-identity"?: [revIdentId, etype, label],
//	 "unique?": bool?, "index?": bool?}
type wireAttr struct {
	ID              string         `json:"id"`
	ForwardIdentity []string       `json:"forward-identity"`
	ValueType       string         `json:"value-type"`
	Cardinality     string         `json:"cardinality"`
	ReverseIdentity []string       `json:"reverse-identity"`
	Unique          bool           `json:"unique?"`
	Indexed         bool           `json:"index?"`
	Required        *bool          `json:"required?"`
	CheckedDataType string         `json:"checked-data-type"`
	Rest            map[string]any `json:"-"`
}

// validIdentName enforces the v1 ident grammar on client-minted attr names:
// ASCII letters, digits, `_`, `-`, `$`, `.`; 1..256 bytes. Names surface as
// JSON keys in query results and ride every init-ok handshake, so unbounded
// or control-bearing strings would become persistent amplification payload.
func validIdentName(s string) bool {
	if s == "" || len(s) > 256 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '_' || c == '-' || c == '$' || c == '.':
		default:
			return false
		}
	}
	return true
}

var validValueTypes = map[string]bool{"blob": true, "ref": true, "number": true}

// checkedDataTypes mirrors the DB enum plus the wire's "json" spelling,
// which maps to NULL (unconstrained) since jsonb accepts any value.
var checkedDataTypes = map[string]bool{
	"string": true, "number": true, "boolean": true, "date": true,
}

// parseWireAttr converts an add-attr payload into a platform.Attr the server
// can adopt verbatim. Clients mint attr + ident uuids and reference them in
// later same-batch steps (docs/03 §4; Reactor.js instaml), so id preservation
// is load-bearing.
func parseWireAttr(raw json.RawMessage) (platform.Attr, error) {
	var wa wireAttr
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&wa); err != nil {
		// Unknown fields are forward-compat: re-decode permissively.
		wa = wireAttr{}
		if err := json.Unmarshal(raw, &wa); err != nil {
			return platform.Attr{}, fmt.Errorf("payload not an object: %w", err)
		}
	}
	if wa.Required == nil {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err == nil {
			if value, present := fields["required?"]; present && string(value) == "null" {
				return platform.Attr{}, fmt.Errorf("required? must be a boolean")
			}
		}
	}
	var attrID [16]byte
	if wa.ID == "" || parseUUID(wa.ID, &attrID) != nil {
		return platform.Attr{}, fmt.Errorf("id must be a uuid")
	}
	if len(wa.ForwardIdentity) < 3 {
		return platform.Attr{}, fmt.Errorf("forward-identity must be [identId, etype, label]")
	}
	etype := wa.ForwardIdentity[1]
	label := wa.ForwardIdentity[2]
	if !validIdentName(etype) || !validIdentName(label) {
		return platform.Attr{}, fmt.Errorf(
			"forward-identity etype/label must be 1..256 chars of [A-Za-z0-9_$.-], got %q/%q", etype, label)
	}
	if perms.ReservedNamespaces[etype] {
		// System namespaces ($users, $files, $default, …) are provisioned by
		// the server. Client-minted attrs there would let any session inject
		// data into auth/user projections.
		return platform.Attr{}, fmt.Errorf(
			"transact: add-attr: %q is a reserved namespace", etype)
	}
	var fwdIdent [16]byte
	if parseUUID(wa.ForwardIdentity[0], &fwdIdent) != nil {
		// Tolerant fallback: some clients reuse the attr id here.
		fwdIdent = attrID
	}
	vt := wa.ValueType
	if vt == "" {
		vt = "blob"
	}
	if !validValueTypes[vt] {
		return platform.Attr{}, fmt.Errorf("value-type must be blob|ref|number, got %q", vt)
	}
	card := wa.Cardinality
	switch card {
	case "", "one":
		card = "one"
	case "many":
	default:
		return platform.Attr{}, fmt.Errorf("cardinality must be one|many, got %q", card)
	}

	a := platform.Attr{
		ID:           attrID,
		Etype:        &etype,
		Label:        &label,
		ValueType:    vt,
		Cardinality:  card,
		IsUnique:     wa.Unique,
		IsIndexed:    wa.Indexed || wa.Unique, // unique implies index (v1 semantics)
		IsRequired:   wa.Required != nil && *wa.Required,
		ForwardIdent: fwdIdent,
	}
	if len(wa.ReverseIdentity) >= 3 {
		revEtype, revLabel := wa.ReverseIdentity[1], wa.ReverseIdentity[2]
		// Mirror the forward-side guards: reserved namespaces and malformed
		// names are equally dangerous on the reverse edge (instaql resolves
		// nested links through reverse metadata).
		if !validIdentName(revEtype) || !validIdentName(revLabel) {
			return platform.Attr{}, fmt.Errorf(
				"reverse-identity etype/label must be 1..256 chars of [A-Za-z0-9_$.-], got %q/%q", revEtype, revLabel)
		}
		if perms.ReservedNamespaces[revEtype] {
			return platform.Attr{}, fmt.Errorf(
				"transact: add-attr: reverse namespace %q is reserved", revEtype)
		}
		var revIdent [16]byte
		if parseUUID(wa.ReverseIdentity[0], &revIdent) == nil {
			a.ReverseIdent = &revIdent
		}
		a.ReverseEtype = &revEtype
		a.ReverseLabel = &revLabel
	}
	if cdt := wa.CheckedDataType; cdt != "" {
		if !checkedDataTypes[cdt] {
			return platform.Attr{}, fmt.Errorf(
				"checked-data-type must be string|number|boolean|date|json, got %q", cdt)
		}
		if cdt != "json" { // "json" is wire spelling for unconstrained (NULL)
			a.CheckedDataType = &cdt
		}
	}
	return a, nil
}

// requiredAttrUpdate is the bounded update-attr surface currently supported
// by the v2 transactor. V1 accepts a full attr patch, but requiredness is the
// only metadata mutation whose validation and persistence semantics are
// implemented here. Rejecting other keys keeps callers from believing a
// silently ignored index/cardinality/value-type update succeeded.
type requiredAttrUpdate struct {
	ID       string
	Required *bool
}

func parseRequiredAttrUpdate(raw json.RawMessage) (requiredAttrUpdate, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return requiredAttrUpdate{}, fmt.Errorf("payload must be an object: %w", err)
	}
	var out requiredAttrUpdate
	for key, value := range fields {
		switch key {
		case "id":
			if err := json.Unmarshal(value, &out.ID); err != nil {
				return requiredAttrUpdate{}, fmt.Errorf("id must be a uuid: %w", err)
			}
		case "required?":
			var required *bool
			if err := json.Unmarshal(value, &required); err != nil {
				return requiredAttrUpdate{}, fmt.Errorf("required? must be a boolean: %w", err)
			}
			if required == nil {
				return requiredAttrUpdate{}, fmt.Errorf("required? must be a boolean")
			}
			out.Required = required
		default:
			return requiredAttrUpdate{}, fmt.Errorf("unsupported update-attr field %q (only id and required? are supported)", key)
		}
	}
	if out.ID == "" {
		return requiredAttrUpdate{}, fmt.Errorf("update-attr: id is required")
	}
	var id [16]byte
	if err := parseUUID(out.ID, &id); err != nil {
		return requiredAttrUpdate{}, fmt.Errorf("update-attr: id %q: %w", out.ID, err)
	}
	if out.Required == nil {
		return requiredAttrUpdate{}, fmt.Errorf("update-attr: required? is required by the supported v2 patch")
	}
	return out, nil
}

// rewriteAttrRefs replaces client-minted attr ids in triple steps with the
// server-side ids adopted during add-attr processing. Only Args[1] of
// triple-shaped ops participates — the position parseTripleArgs reads.
func rewriteAttrRefs(steps []Step, alias map[[16]byte][16]byte) {
	if len(alias) == 0 {
		return
	}
	remap := func(raw json.RawMessage) (json.RawMessage, bool) {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return raw, false
		}
		var id [16]byte
		if parseUUID(s, &id) != nil {
			return raw, false
		}
		serverID, ok := alias[id]
		if !ok {
			return raw, false
		}
		out, err := json.Marshal(uuidToStr(serverID))
		if err != nil {
			return raw, false
		}
		return out, true
	}
	for i := range steps {
		switch steps[i].Op {
		case "add-triple", "deep-merge-triple", "retract-triple":
			if len(steps[i].Args) >= 3 {
				if repl, ok := remap(steps[i].Args[1]); ok {
					steps[i].Args[1] = repl
				}
			}
		}
	}
}
