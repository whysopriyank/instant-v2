package transact

import (
	"crypto/rand"
	"encoding/json"
	"fmt"

	"github.com/instant-v2/instant-v2/internal/platform"
)

type resolvedEID struct {
	eid     string
	found   bool // resolved to an entity (pre-existing or minted)
	existed bool // entity already existed before this batch
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

func mustParseUUID(s string) [16]byte {
	var u [16]byte
	if err := parseUUID(s, &u); err != nil {
		panic(fmt.Sprintf("transact: bad uuid %q: %v", s, err))
	}
	return u
}
