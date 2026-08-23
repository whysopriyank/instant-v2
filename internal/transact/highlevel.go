package transact

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// High-level admin tx-step lowering: a port of instant.admin.model.clj
// (@ a4d2ef33). The Python SDK POSTs /admin/transact with high-level ops
//
//	[["update","todos",<eid>,{"text":"hi"}], ["link",...], ...]
//
// while the frozen WS grammar stays low-level (the TS client lowers
// client-side). This file transforms those high-level ops into the low-level
// wire steps ParseSteps understands:
//
//	create/update/merge → add-triple / deep-merge-triple (+ {mode} opts)
//	link/unlink         → add-triple / retract-triple through ref attrs
//	delete              → delete-entity (dropped when its lookup misses)
//	ruleParams          → rule-params
//
// Lookup entity ids — "lookup__<attr>__<json-value>" strings, [attr, value]
// pairs, or single-entry {attr: value} objects — resolve against committed
// state (plus entities minted earlier in the same batch). Missing lookups on
// create/update/merge/link/unlink mint a fresh entity carrying the unique
// value (mirroring v1's lookup-ref-inserts CTE); on delete they silently drop
// the step (v1 resolve-lookups-for-delete-entity).
//
// When "throw-on-missing-attrs?" is falsy (SDK default), attrs referenced by
// the batch that don't exist are created first (v1 create-missing-attrs):
// id attrs unique, lookup attrs unique+indexed, link attrs ref/many,
// ref-lookup attrs ref/one/unique/indexed, plain obj attrs blob/one. Like v1's
// create-object-attr, no data-type inference happens — everything is created
// untyped (blob) — so the caller must Invalidate its catalog afterwards.
//
// DEVIATIONS from v1 (documented at the affected sites): resolution reads
// committed state outside the write tx, so a unique value written by an
// earlier step in the same batch is visible only when it was introduced
// through this lowering (mint/resolution cache), and attr provisioning
// commits in its own transaction (same pattern as authn system attrs).
const (
	createModeJSON = `{"mode":"create"}`
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

// AttrSpec describes one missing attr to provision (v1 create-object-attr /
// create-ref-attr shapes).
type AttrSpec struct {
	Etype, Label string
	Ref          bool // ref attr (link / ref-lookup) vs blob obj attr
	ValueType    string
	Cardinality  string
	Unique       bool
	Indexed      bool
	// Reverse naming follows v1's create-ref-attr: rev identity is
	// [(uuid) label etype], i.e. reverse_etype=label, reverse_label=etype.
	ReverseEtype *string
	ReverseLabel *string

	IdentName string // "etype.label", for throw-on-missing-attrs reporting
}

// LowerHooks supplies the DB surface lowering needs. Reads see committed
// state; CreateAttr provisions missing attrs (adminapi wraps
// platform.GetOrCreateAttr(+Rev) and invalidates its catalog afterwards).
type LowerHooks struct {
	// ResolveUnique returns the entity owning the (attrID, value) triple
	// when one exists. value arrives as canonical jsonb-encoded JSON.
	ResolveUnique func(ctx context.Context, appID [16]byte, attrID [16]byte, value json.RawMessage) ([16]byte, bool, error)
	// EntityExists reports whether any triple exists for the entity.
	EntityExists func(ctx context.Context, appID [16]byte, eid [16]byte) (bool, error)
	// CreateAttr provisions one missing attr and returns its stored row
	// (including the assigned ID).
	CreateAttr func(ctx context.Context, appID [16]byte, spec AttrSpec) (platform.Attr, error)
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

type lowerer struct {
	ctx      context.Context
	appID    [16]byte
	cat      *platform.AttrCatalog
	hooks    LowerHooks
	extra    map[[16]byte]platform.Attr // attrs provisioned during this run
	resolved map[string]resolvedEID     // lookupKey -> cached resolution
	missing  map[string]AttrSpec
	missingI []string // insertion-ordered missing-attr keys
	out      []json.RawMessage
}

type resolvedEID struct {
	eid     string
	found   bool // resolved to an entity (pre-existing or minted)
	existed bool // entity already existed before this batch
}

// LowerAdminSteps ports instant.admin.model/->tx-steps!'s transform phase:
// it validates entity ids, provisions missing attrs (unless
// throwOnMissingAttrs, which fails with v1's message shape), and rewrites
// high-level ops into low-level wire steps. Non-high-level steps pass
// through byte-for-byte.
func LowerAdminSteps(
	ctx context.Context,
	appID [16]byte,
	cat *platform.AttrCatalog,
	rawSteps []json.RawMessage,
	hooks LowerHooks,
	throwOnMissingAttrs bool,
) ([]json.RawMessage, error) {
	l := &lowerer{
		ctx:      ctx,
		appID:    appID,
		cat:      cat,
		hooks:    hooks,
		extra:    map[[16]byte]platform.Attr{},
		resolved: map[string]resolvedEID{},
		missing:  map[string]AttrSpec{},
	}

	parsed := make([]parsedStep, 0, len(rawSteps))
	for _, raw := range rawSteps {
		ps, err := parseAdminStep(raw)
		if err != nil {
			return nil, err
		}
		parsed = append(parsed, ps)
		if ps.high {
			l.collectMissingAttrs(ps)
		}
	}

	if len(l.missingI) > 0 {
		if throwOnMissingAttrs {
			return nil, validationErrf("steps", "Attributes are missing in your schema")
		}
		if hooks.CreateAttr == nil {
			return nil, fmt.Errorf("transact: cannot lower admin steps without an attr creator")
		}
		for _, key := range l.missingI {
			spec := l.missing[key]
			at, err := hooks.CreateAttr(ctx, appID, spec)
			if err != nil {
				return nil, err
			}
			l.extra[at.ID] = at
		}
	}

	for _, ps := range parsed {
		if !ps.high {
			l.out = append(l.out, ps.raw)
			continue
		}
		if err := l.expand(ps); err != nil {
			return nil, err
		}
	}
	return l.out, nil
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
	case "create", "update", "merge":
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
	case "link", "unlink":
		obj := map[string]json.RawMessage{}
		if err := json.Unmarshal(arr[3], &obj); err != nil {
			return parsedStep{}, fmt.Errorf("transact: %s: obj must be an object", op)
		}
		ps.obj = obj
		ps.objKeys = sortedKeys(obj)
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

// ---- catalog seeking --------------------------------------------------------

func (l *lowerer) seek(etype, label string) *platform.Attr {
	for _, a := range l.extra {
		if a.Etype != nil && *a.Etype == etype && a.Label != nil && *a.Label == label {
			cp := a
			return &cp
		}
	}
	return l.cat.FindByEtypeLabel(etype, label)
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// isRefLookup ports ref-lookup?: the ident contains "." and no attr matches
// it directly, so it must name a ref attr plus ".id".
func (l *lowerer) isRefLookup(etype string, e parsedEID) bool {
	return strings.Contains(e.ident, ".") && l.seek(etype, e.ident) == nil
}

// refLookupFwdName ports extract-ref-lookup-fwd-name: "<fwd>.id" only.
func refLookupFwdName(ident string) (string, error) {
	parts := strings.Split(ident, ".")
	if len(parts) != 2 || parts[1] != "id" {
		return "", validationErrf("lookup", "%s is not a valid lookup attribute.", ident)
	}
	return parts[0], nil
}

// lookupLabel resolves the effective attr label for a lookup eid.
func (l *lowerer) lookupLabel(etype string, e parsedEID) (string, error) {
	if l.isRefLookup(etype, e) {
		return refLookupFwdName(e.ident)
	}
	return e.ident, nil
}

// ---- missing-attr collection (v1 create-missing-attrs) ----------------------

func (l *lowerer) markMissing(spec AttrSpec) {
	key := spec.Etype + "\x00" + spec.Label
	if _, ok := l.missing[key]; ok {
		return
	}
	spec.IdentName = spec.Etype + "." + spec.Label
	l.missing[key] = spec
	l.missingI = append(l.missingI, key)
}

// addAttrsForObj ports add-attrs-for-obj: every obj-action ensures the etype
// has an id attr, plus one attr per obj key.
func (l *lowerer) addAttrsForObj(etype, action string, obj map[string]json.RawMessage) {
	if l.seek(etype, "id") == nil {
		l.markMissing(AttrSpec{Etype: etype, Label: "id", ValueType: "blob",
			Cardinality: "one", Unique: true})
	}
	refAction := action == "link" || action == "unlink"
	for _, label := range sortedKeys(obj) {
		if l.seek(etype, label) != nil {
			continue
		}
		if refAction {
			revE, revL := label, etype // v1 create-ref-attr rev naming
			l.markMissing(AttrSpec{Etype: etype, Label: label, Ref: true,
				ValueType: "ref", Cardinality: "many",
				ReverseEtype: &revE, ReverseLabel: &revL})
		} else {
			l.markMissing(AttrSpec{Etype: etype, Label: label, ValueType: "blob",
				Cardinality: "one", Unique: label == "id"})
		}
	}
}

// addAttrsForRefLookup ports add-attrs-for-ref-lookup: a lookup used as a
// link target needs a unique, indexed, cardinality-one ref attr.
func (l *lowerer) addAttrsForRefLookup(label, etype string) {
	if l.seek(etype, label) != nil {
		return
	}
	revE, revL := label, etype // v1 create-ref-attr rev naming
	l.markMissing(AttrSpec{Etype: etype, Label: label, Ref: true,
		ValueType: "ref", Cardinality: "one", Unique: true, Indexed: true,
		ReverseEtype: &revE, ReverseLabel: &revL})
}

// addAttrsForLookup ports add-attrs-for-lookup: whatever attr a lookup names
// must exist and be unique (+indexed for fresh creations).
func (l *lowerer) addAttrsForLookup(etype string, e parsedEID) error {
	if l.isRefLookup(etype, e) {
		label, err := refLookupFwdName(e.ident)
		if err != nil {
			return err
		}
		l.addAttrsForRefLookup(label, etype)
		return nil
	}
	if l.seek(etype, e.ident) == nil {
		l.markMissing(AttrSpec{Etype: etype, Label: e.ident, ValueType: "blob",
			Cardinality: "one", Unique: true, Indexed: true})
	}
	return nil
}

// addAttrsForLinkLookup ports add-attrs-for-link-lookup: the lookup pair of a
// link VALUE resolves against the link's target etype.
func (l *lowerer) addAttrsForLinkLookup(pair parsedEID, linkLabel, etype string) error {
	fwd := l.seek(etype, linkLabel)
	linkEtype := linkLabel
	if fwd != nil {
		linkEtype = derefStr(fwd.Label)
	}
	return l.addAttrsForLookup(linkEtype, pair)
}

func (l *lowerer) collectMissingAttrs(ps parsedStep) {
	switch ps.op {
	case "delete":
		if !ps.eid.isUUID {
			_ = l.addAttrsForLookup(ps.etype, ps.eid)
		}
	case "create", "update", "merge", "link", "unlink":
		l.addAttrsForObj(ps.etype, ps.op, ps.obj)
		if !ps.eid.isUUID {
			_ = l.addAttrsForLookup(ps.etype, ps.eid)
		}
		if ps.op == "link" {
			for _, label := range ps.objKeys {
				for _, eid := range decodeLinkValue(ps.obj[label]) {
					var pair parsedEID
					if errPair(&pair, eid) == nil && !pair.isUUID {
						l.addAttrsForRefLookup(label, ps.etype)
						_ = l.addAttrsForLinkLookup(pair, label, ps.etype)
					}
				}
			}
		}
	}
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

func errPair(dst *parsedEID, raw json.RawMessage) error {
	p, err := parseAdminEID(raw)
	if err != nil {
		return err
	}
	*dst = p
	return nil
}

// ---- eid resolution ---------------------------------------------------------

func lookupCacheKey(attrID [16]byte, value json.RawMessage) string {
	norm, err := normalizeValue(value)
	if err != nil {
		norm = value
	}
	return string(attrID[:]) + "\x00" + string(norm)
}

func newEID() string {
	var u [16]byte
	_, _ = rand.Read(u[:])
	u[6] = (u[6] & 0x0f) | 0x40
	u[8] = (u[8] & 0x3f) | 0x80
	return uuidToStr(u)
}

// resolveEID ports extract-lookup plus the storage layer's lookup-ref entity
// creation. allowMint mirrors which ops v1 lets create entities: the main eid
// of create/update/merge/link/unlink mints; link VALUES raise; delete drops.
func (l *lowerer) resolveEID(etype string, e parsedEID, allowMint bool) (resolvedEID, error) {
	if e.isUUID {
		exists := false
		if l.hooks.EntityExists != nil {
			var err error
			exists, err = l.hooks.EntityExists(l.ctx, l.appID, mustParseUUID(e.uuid))
			if err != nil {
				return resolvedEID{}, err
			}
		}
		return resolvedEID{eid: e.uuid, found: true, existed: exists}, nil
	}
	attr, err := l.lookupAttr(etype, e)
	if err != nil {
		return resolvedEID{}, err
	}
	key := lookupCacheKey(attr.ID, e.value)
	if r, ok := l.resolved[key]; ok {
		return r, nil
	}

	norm, err := normalizeValue(e.value)
	if err != nil {
		return resolvedEID{}, err
	}
	eid := [16]byte{}
	found := false
	if l.hooks.ResolveUnique != nil {
		var err error
		eid, found, err = l.hooks.ResolveUnique(l.ctx, l.appID, attr.ID, json.RawMessage(norm))
		if err != nil {
			return resolvedEID{}, err
		}
	}
	if found {
		r := resolvedEID{eid: uuidToStr(eid), found: true, existed: true}
		l.resolved[key] = r
		return r, nil
	}
	if !allowMint {
		return resolvedEID{}, nil
	}

	// Mint a fresh entity carrying the unique value (v1 lookup-ref-inserts),
	// including its id triple so /admin/query sees a well-formed entity.
	newID := newEID()
	l.emitTriple("add-triple", newID, attr.ID, e.value, "")
	if ida := l.seek(etype, "id"); ida != nil {
		l.emitTriple("add-triple", newID, ida.ID, mustMarshalJSON(newID), "")
	}
	r := resolvedEID{eid: newID, found: true, existed: false}
	l.resolved[key] = r
	return r, nil
}

// lookupAttr resolves the concrete attr a lookup eid names, enforcing v1's
// uniqueness and id-lookup-uuid rules.
func (l *lowerer) lookupAttr(etype string, e parsedEID) (*platform.Attr, error) {
	label, err := l.lookupLabel(etype, e)
	if err != nil {
		return nil, err
	}
	attr := l.seek(etype, label)
	if attr == nil {
		return nil, validationErrf("lookup", "unknown attribute %s on %s", label, etype)
	}
	if derefStr(attr.Label) == "id" {
		var u [16]byte
		var s string
		if json.Unmarshal(e.value, &s) != nil || parseUUID(s, &u) != nil {
			return nil, validationErrf("tx-steps",
				"Invalid lookup '[%s %s]'. The lookup attribute is '%s.%s', but the lookup value is not a valid UUID.",
				label, e.value, etype, label)
		}
	}
	if !attr.IsUnique {
		return nil, validationErrf("lookup", "%s is not a unique attribute on %s", e.ident, etype)
	}
	return attr, nil
}

// ---- expansion (v1 to-tx-steps) ---------------------------------------------

func (l *lowerer) expand(ps parsedStep) error {
	switch ps.op {
	case "create":
		return l.expandCreateUpdateMerge(ps, true)
	case "update", "merge":
		return l.expandCreateUpdateMerge(ps, false)
	case "link":
		return l.expandLinkUnlink(ps, false)
	case "unlink":
		return l.expandLinkUnlink(ps, true)
	case "delete":
		return l.expandDelete(ps)
	case "ruleParams":
		return l.expandRuleParams(ps)
	}
	return validationErrf("action", "Unsupported action %s", ps.op)
}

// convertOpts ports convert-opts: opts surface as a mode bag only when the
// "upsert" key is present (v1 validate-mode only enforces modes that exist).
func convertOpts(opts map[string]any, hasOpts bool) json.RawMessage {
	if !hasOpts {
		return nil
	}
	upsert, present := opts["upsert"]
	if !present {
		return nil
	}
	mode := "update"
	if b, ok := upsert.(bool); ok && b {
		mode = "upsert"
	}
	b, _ := json.Marshal(map[string]string{"mode": mode})
	return b
}

func (l *lowerer) expandCreateUpdateMerge(ps parsedStep, isCreate bool) error {
	r, err := l.resolveEID(ps.etype, ps.eid, true)
	if err != nil {
		return err
	}
	if isCreate && r.existed {
		return validationErrf("tx-step", "Creating entities that exist: %s", r.eid)
	}
	if !isCreate && !r.existed {
		if mode, ok := ps.opts["upsert"]; ok {
			if b, isBool := mode.(bool); isBool && !b {
				return validationErrf("tx-step", "Updating entities that don't exist: %s", r.eid)
			}
		}
	}

	opts := createModeJSON
	if !isCreate {
		opts = ""
		if cv := convertOpts(ps.opts, ps.hasOpts); cv != nil {
			opts = string(cv)
		}
	}

	idAttr := l.seek(ps.etype, "id")
	if idAttr != nil {
		// id first so we don't clobber updates on the lookup field (v1 head).
		l.emitTriple("add-triple", r.eid, idAttr.ID, mustMarshalJSON(r.eid), opts)
	}
	for _, label := range ps.objKeys {
		if label == "id" {
			continue // remove-id-from-step: the id triple above carries it
		}
		attr := l.seek(ps.etype, label)
		if attr == nil {
			return validationErrf("steps", "unknown attribute %s on %s", label, ps.etype)
		}
		op := "add-triple"
		if ps.op == "merge" {
			op = "deep-merge-triple"
		}
		l.emitTriple(op, r.eid, attr.ID, ps.obj[label], opts)
	}
	return nil
}

func (l *lowerer) expandLinkUnlink(ps parsedStep, retract bool) error {
	r, err := l.resolveEID(ps.etype, ps.eid, true)
	if err != nil {
		return err
	}
	op := "add-triple"
	if retract {
		op = "retract-triple"
	}
	for _, label := range ps.objKeys {
		fwd := l.seek(ps.etype, label)
		if fwd == nil {
			return validationErrf("steps", "unknown attribute %s on %s", label, ps.etype)
		}
		targetEtype := derefStr(fwd.Label)
		for _, raw := range decodeLinkValue(ps.obj[label]) {
			var vb parsedEID
			if err := errPair(&vb, raw); err != nil {
				return err
			}
			br, err := l.resolveEID(targetEtype, vb, false)
			if err != nil {
				return err
			}
			if !br.found {
				return validationErrf("lookup", "The entity for the lookup does not exist.")
			}
			l.emitTriple(op, r.eid, fwd.ID, mustMarshalJSON(br.eid), "")
		}
	}
	return nil
}

func (l *lowerer) expandDelete(ps parsedStep) error {
	r, err := l.resolveEID(ps.etype, ps.eid, false)
	if err != nil {
		return err
	}
	if !r.found {
		return nil // v1 drops deletes whose lookup doesn't resolve
	}
	l.out = append(l.out, mustMarshalRaw([]any{"delete-entity", r.eid, ps.etype}))
	return nil
}

func (l *lowerer) expandRuleParams(ps parsedStep) error {
	r, err := l.resolveEID(ps.etype, ps.eid, false)
	if err != nil {
		return err
	}
	if !r.found {
		return nil // unresolved entities can't carry params
	}
	l.out = append(l.out, mustMarshalRaw([]any{"rule-params", r.eid, ps.etype, ps.params}))
	return nil
}

// ---- emit helpers -----------------------------------------------------------

func (l *lowerer) emitTriple(op, eid string, attrID [16]byte, value json.RawMessage, opts string) {
	parts := []any{op, eid, uuidToStr(attrID), value}
	if opts != "" {
		parts = append(parts, json.RawMessage(opts))
	}
	l.out = append(l.out, mustMarshalRaw(parts))
}

func mustMarshalRaw(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("transact: marshal lowered step: %v", err))
	}
	return b
}

func mustParseUUID(s string) [16]byte {
	var u [16]byte
	if err := parseUUID(s, &u); err != nil {
		panic(fmt.Sprintf("transact: bad uuid %q: %v", s, err))
	}
	return u
}
