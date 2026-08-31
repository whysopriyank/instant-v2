package transact

import (
	"encoding/json"
	"fmt"
)

const (
	createModeJSON = `{"mode":"create"}`
)

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
			vb, err := parseAdminEID(raw)
			if err != nil {
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
