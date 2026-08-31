package transact

import (
	"context"
	"encoding/json"
	"fmt"

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
