package transact

import (
	"strings"

	"github.com/instant-v2/instant-v2/internal/platform"
)

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
func (l *lowerer) addAttrsForObj(etype, action string, labels []string) {
	if l.seek(etype, "id") == nil {
		l.markMissing(AttrSpec{Etype: etype, Label: "id", ValueType: "blob",
			Cardinality: "one", Unique: true})
	}
	refAction := action == "link" || action == "unlink"
	for _, label := range labels {
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
		l.addAttrsForObj(ps.etype, ps.op, ps.objKeys)
		if !ps.eid.isUUID {
			_ = l.addAttrsForLookup(ps.etype, ps.eid)
		}
		if ps.op == "link" {
			for _, label := range ps.objKeys {
				for _, eid := range decodeLinkValue(ps.obj[label]) {
					if pair, err := parseAdminEID(eid); err == nil && !pair.isUUID {
						l.addAttrsForRefLookup(label, ps.etype)
						_ = l.addAttrsForLinkLookup(pair, label, ps.etype)
					}
				}
			}
		}
	}
}
