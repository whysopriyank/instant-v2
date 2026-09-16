package transact

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
)

func etypeFor(cat *platform.AttrCatalog, attrID [16]byte) string {
	if a, ok := cat.ByID(attrID); ok {
		if a.Etype != nil {
			return *a.Etype
		}
	}
	return "$default"
}

// entityProjectionWithFetcher projects one entity's committed triples into a
// label→value map (the `data` binding v1 rules see), including its stable id.
// Cardinality-many attrs accumulate as arrays; unknown attrs are skipped.
type permissionFetcher func(context.Context, pgx.Tx, [16]byte, storage.FetchFilter) ([]storage.Enhanced, error)

type permissionProjectionKey struct {
	eid   [16]byte
	etype string
}

func entityProjectionWithFetcher(ctx context.Context, tx pgx.Tx, appID [16]byte, eid [16]byte, cat *platform.AttrCatalog, fetch permissionFetcher) (map[string]any, error) {
	out := map[string]any{}
	rows, err := fetch(ctx, tx, appID, storage.FetchFilter{
		EntityIDs: [][16]byte{eid},
	})
	if err != nil {
		return nil, fmt.Errorf("fetch entity projection: %w", err)
	}
	if len(rows) > 0 {
		// v1's entity-model projection includes the stable entity id.
		out["id"] = uuidToStr(eid)
	}
	for _, r := range rows {
		a, ok := cat.ByID(r.Triple.A)
		if !ok || a.Label == nil {
			continue
		}
		label := *a.Label
		if a.Cardinality == "many" {
			arr, _ := out[label].([]any)
			out[label] = append(arr, normalizedProjectionValue(r.Triple.V))
		} else {
			out[label] = normalizedProjectionValue(r.Triple.V)
		}
	}
	return out, nil
}

func permBindings(opts Options, data, newData map[string]any) perms.Bindings {
	return perms.Bindings{
		Data:       data,
		NewData:    newData,
		Auth:       opts.AuthUser,
		RuleParams: opts.RuleParams,
		Request:    opts.Request,
	}
}

type permissionStoredValue struct {
	value any
	md5   string
}

// fingerprintValuesTx is the transaction-local value fingerprint seam. Keep
// storage.ValueMD5sTx as the production default; package tests can replace it
// to prove fingerprint failures abort the journaled Transact atomically.
var fingerprintValuesTx = storage.ValueMD5sTx

func firstStoredValue(values []permissionStoredValue) (permissionStoredValue, bool) {
	if len(values) == 0 {
		return permissionStoredValue{}, false
	}
	first := values[0]
	for _, value := range values[1:] {
		if value.md5 < first.md5 {
			first = value
		}
	}
	return first, true
}

func normalizedProjectionValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for key, value := range x {
			out[key] = normalizedProjectionValue(value)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, value := range x {
			out[i] = normalizedProjectionValue(value)
		}
		return out
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i
		}
		value, _ := x.Float64()
		return value
	default:
		return v
	}
}

// enforcePerms checks every mutating step in the batch against doc. Runs
// inside the write transaction after lookup resolution so existence probes
// and data projections see same-batch state. Any check error or denial
// aborts the whole transaction (fail-closed).
//
// Initial projections for all touched entities are batch-fetched in ONE query.
// Permission checks use the original entity image for update/delete/unlink and
// the final same-batch image for new entities, matching v1's pre/post split.
func enforcePerms(ctx context.Context, tx pgx.Tx, appID [16]byte,
	cat *platform.AttrCatalog, steps []Step, opts Options, doc *perms.RuleDoc, fetch permissionFetcher, evaluations ...*PermissionEvaluation,
) error {
	var evaluation *PermissionEvaluation
	if len(evaluations) > 0 {
		evaluation = evaluations[0]
	}
	seenEntities := make(map[[16]byte]bool)
	var touchedEntities [][16]byte
	stepKeys := make([]permissionProjectionKey, len(steps))
	for stepIndex, st := range steps {
		var eid [16]byte
		var etype string
		switch st.Op {
		case "add-triple", "deep-merge-triple", "retract-triple":
			ta, err := parseTripleArgs(st, cat)
			if err != nil {
				return err
			}
			if err := parseUUID(strings.Trim(string(ta.EID), `"`), &eid); err != nil {
				return fmt.Errorf("perms: eid %s: %w", ta.EID, err)
			}
			etype = etypeFor(cat, ta.AttrID)
		case "delete-entity":
			var eidStr string
			if err := json.Unmarshal(st.Args[0], &eidStr); err != nil {
				continue
			}
			if err := parseUUID(eidStr, &eid); err != nil {
				continue
			}
			etype = "$default"
			if len(st.Args) == 2 {
				if err := json.Unmarshal(st.Args[1], &etype); err != nil {
					return fmt.Errorf("perms: delete-entity etype: %w", err)
				}
			}
		default:
			continue
		}
		stepKeys[stepIndex] = permissionProjectionKey{eid: eid, etype: etype}
		if !seenEntities[eid] {
			seenEntities[eid] = true
			touchedEntities = append(touchedEntities, eid)
		}
	}

	projMap := make(map[permissionProjectionKey]map[string]any, len(steps))
	attrExists := make(map[permissionProjectionKey]map[[16]byte]bool, len(steps))
	storedValues := make(map[permissionProjectionKey]map[[16]byte][]permissionStoredValue, len(steps))
	for _, key := range stepKeys {
		if key == (permissionProjectionKey{}) {
			continue
		}
		if _, ok := projMap[key]; !ok {
			projMap[key] = make(map[string]any)
			attrExists[key] = make(map[[16]byte]bool)
			storedValues[key] = make(map[[16]byte][]permissionStoredValue)
		}
	}
	if len(touchedEntities) > 0 {
		rows, err := fetch(ctx, tx, appID, storage.FetchFilter{
			EntityIDs: touchedEntities,
		})
		if err != nil {
			return fmt.Errorf("fetch entity projections: %w", err)
		}
		for _, r := range rows {
			eid := r.Triple.E
			a, ok := cat.ByID(r.Triple.A)
			if !ok || a.Etype == nil {
				continue
			}
			key := permissionProjectionKey{eid: eid, etype: *a.Etype}
			if _, ok := projMap[key]; !ok {
				// This entity/namespace was not touched by this batch. Avoid
				// leaking its fields into another namespace's permission data.
				continue
			}
			if attrExists[key] == nil {
				attrExists[key] = make(map[[16]byte]bool)
			}
			// v1 entity maps always carry the entity id, even when the
			// namespace has no user-defined id attribute.
			if _, exists := projMap[key]["id"]; !exists {
				projMap[key]["id"] = uuidToStr(eid)
			}
			attrExists[key][r.Triple.A] = true
			if storedValues[key] == nil {
				storedValues[key] = make(map[[16]byte][]permissionStoredValue)
			}
			storedValues[key][r.Triple.A] = append(storedValues[key][r.Triple.A], permissionStoredValue{
				value: r.Triple.V,
				md5:   r.MD5,
			})
			if a.Label == nil {
				continue
			}
			label := *a.Label
			if a.Cardinality == "many" {
				arr, _ := projMap[key][label].([]any)
				projMap[key][label] = append(arr, normalizedProjectionValue(r.Triple.V))
			} else {
				projMap[key][label] = normalizedProjectionValue(r.Triple.V)
			}
		}
	}

	stepMD5s := make([]string, len(steps))
	var fingerprintValues []any
	var fingerprintSteps []int
	for i, st := range steps {
		switch st.Op {
		case "add-triple", "deep-merge-triple", "retract-triple":
			ta, err := parseTripleArgs(st, cat)
			if err != nil {
				return err
			}
			value, err := parseValueJSON(ta.Value)
			if err != nil {
				return err
			}
			fingerprintSteps = append(fingerprintSteps, i)
			fingerprintValues = append(fingerprintValues, value)
		}
	}
	fingerprints, err := fingerprintValuesTx(ctx, tx, fingerprintValues)
	if err != nil {
		return fmt.Errorf("fingerprint permission values: %w", err)
	}
	for i, stepIndex := range fingerprintSteps {
		stepMD5s[stepIndex] = fingerprints[i]
	}

	finalProjMap := cloneProjectionMap(projMap)
	finalAttrExists := cloneAttrExists(attrExists)
	finalStoredValues := cloneStoredValues(storedValues)
	originalStoredValues := cloneStoredValues(storedValues)
	updateEligible := make(map[permissionProjectionKey]bool, len(storedValues))
	for key, values := range storedValues {
		updateEligible[key] = len(values) > 0
	}
	stepUsesUpdate := make([]bool, len(steps))
	for stepIndex, st := range steps {
		key := stepKeys[stepIndex]
		stepUsesUpdate[stepIndex] = updateEligible[key]
		if err := projectPermissionStep(ctx, tx, cat, st, stepMD5s[stepIndex], finalProjMap, finalAttrExists, finalStoredValues); err != nil {
			return err
		}
		if err := removeOriginalPermissionValue(cat, st, stepMD5s[stepIndex], originalStoredValues); err != nil {
			return err
		}
		// Once the original entity image is fully removed, later additions are
		// creates. Values added by this batch cannot keep update eligibility.
		if len(originalStoredValues[key]) == 0 {
			updateEligible[key] = false
		}
	}

	for stepIndex, st := range steps {
		switch st.Op {
		case "add-triple", "deep-merge-triple":
			ta, err := parseTripleArgs(st, cat)
			if err != nil {
				return err
			}
			var eid [16]byte
			if err := parseUUID(strings.Trim(string(ta.EID), `"`), &eid); err != nil {
				return fmt.Errorf("perms: eid %s: %w", ta.EID, err)
			}
			key := permissionProjectionKey{eid: eid, etype: etypeFor(cat, ta.AttrID)}
			action := "create"
			if stepUsesUpdate[stepIndex] {
				action = "update"
			}
			etype := etypeFor(cat, ta.AttrID)
			data := cloneMap(projMap[key])
			newData := cloneMap(finalProjMap[key])
			if action == "create" {
				// v1 runs create checks against the final entity image, and
				// binds that same image as both data and newData.
				data = cloneMap(finalProjMap[key])
			}
			bindings := permBindings(opts, data, newData)
			allow, err := perms.Check(etype, action, doc, bindings)
			recordPermissionCheck(evaluation, stepIndex, etype, action, allow, err, bindings)
			if err != nil {
				return fmt.Errorf("perms %s %s: %w", etype, action, err)
			}
			if !allow {
				return fmt.Errorf("transact: permission denied (%s %s)", action, etype)
			}

		case "retract-triple":
			ta, err := parseTripleArgs(st, cat)
			if err != nil {
				return err
			}
			var eid [16]byte
			if err := parseUUID(strings.Trim(string(ta.EID), `"`), &eid); err != nil {
				return fmt.Errorf("perms: eid %s: %w", ta.EID, err)
			}
			key := permissionProjectionKey{eid: eid, etype: etypeFor(cat, ta.AttrID)}
			etype := etypeFor(cat, ta.AttrID)
			data := cloneMap(projMap[key])
			newData := cloneMap(finalProjMap[key])
			action := "update"
			if a, ok := cat.ByID(ta.AttrID); ok && a.ValueType == "ref" && doc.ResolveExpr(etype, "unlink") != "" {
				action = "unlink"
			}
			bindings := permBindings(opts, data, newData)
			allow, err := perms.Check(etype, action, doc, bindings)
			recordPermissionCheck(evaluation, stepIndex, etype, action, allow, err, bindings)
			if err != nil {
				return fmt.Errorf("perms %s %s: %w", etype, action, err)
			}
			if !allow {
				return fmt.Errorf("transact: permission denied (%s %s)", action, etype)
			}

		case "delete-entity":
			var eidStr string
			if err := json.Unmarshal(st.Args[0], &eidStr); err != nil {
				continue // non-uuid eids never reached applyDeleteEntity either
			}
			var eid [16]byte
			if err := parseUUID(eidStr, &eid); err != nil {
				continue
			}
			etype := "$default"
			if len(st.Args) == 2 {
				if err := json.Unmarshal(st.Args[1], &etype); err != nil {
					return fmt.Errorf("perms: delete-entity etype: %w", err)
				}
			}
			key := permissionProjectionKey{eid: eid, etype: etype}
			data := cloneMap(projMap[key])
			bindings := permBindings(opts, data, map[string]any{})
			allow, err := perms.Check(etype, "delete", doc, bindings)
			recordPermissionCheck(evaluation, stepIndex, etype, "delete", allow, err, bindings)
			if err != nil {
				return fmt.Errorf("perms %s delete: %w", etype, err)
			}
			if !allow {
				return fmt.Errorf("transact: permission denied (delete %s)", etype)
			}
		}
	}
	return nil
}

func removeOriginalPermissionValue(cat *platform.AttrCatalog, st Step, stepMD5 string, values map[permissionProjectionKey]map[[16]byte][]permissionStoredValue) error {
	switch st.Op {
	case "retract-triple":
		ta, err := parseTripleArgs(st, cat)
		if err != nil {
			return err
		}
		var eid [16]byte
		if err := parseUUID(strings.Trim(string(ta.EID), `"`), &eid); err != nil {
			return fmt.Errorf("perms: eid %s: %w", ta.EID, err)
		}
		key := permissionProjectionKey{eid: eid, etype: etypeFor(cat, ta.AttrID)}
		stored := values[key][ta.AttrID]
		for i, item := range stored {
			if item.md5 != stepMD5 {
				continue
			}
			stored = append(stored[:i:i], stored[i+1:]...)
			if len(stored) == 0 {
				delete(values[key], ta.AttrID)
			} else {
				values[key][ta.AttrID] = stored
			}
			break
		}
	case "delete-entity":
		var eidStr, etype string
		if err := json.Unmarshal(st.Args[0], &eidStr); err != nil {
			return nil
		}
		var eid [16]byte
		if err := parseUUID(eidStr, &eid); err != nil {
			return nil
		}
		etype = "$default"
		if len(st.Args) == 2 {
			if err := json.Unmarshal(st.Args[1], &etype); err != nil {
				return fmt.Errorf("perms: delete-entity etype: %w", err)
			}
		}
		values[permissionProjectionKey{eid: eid, etype: etype}] = nil
	}
	return nil
}

func cloneProjectionMap(src map[permissionProjectionKey]map[string]any) map[permissionProjectionKey]map[string]any {
	out := make(map[permissionProjectionKey]map[string]any, len(src))
	for key, values := range src {
		out[key] = cloneMap(values)
	}
	return out
}

func cloneAttrExists(src map[permissionProjectionKey]map[[16]byte]bool) map[permissionProjectionKey]map[[16]byte]bool {
	out := make(map[permissionProjectionKey]map[[16]byte]bool, len(src))
	for key, attrs := range src {
		copyAttrs := make(map[[16]byte]bool, len(attrs))
		for attrID, exists := range attrs {
			copyAttrs[attrID] = exists
		}
		out[key] = copyAttrs
	}
	return out
}

func cloneStoredValues(src map[permissionProjectionKey]map[[16]byte][]permissionStoredValue) map[permissionProjectionKey]map[[16]byte][]permissionStoredValue {
	out := make(map[permissionProjectionKey]map[[16]byte][]permissionStoredValue, len(src))
	for key, attrs := range src {
		copyAttrs := make(map[[16]byte][]permissionStoredValue, len(attrs))
		for attrID, values := range attrs {
			copyAttrs[attrID] = append([]permissionStoredValue(nil), values...)
		}
		out[key] = copyAttrs
	}
	return out
}

// projectPermissionStep builds the final post-transaction entity image used
// by permission checks. It deliberately mirrors storage's exact-md5 matching,
// cardinality-one overwrite, cardinality-many deduplication, and deep merge
// behavior without evaluating a rule or changing the database.
func projectPermissionStep(ctx context.Context, tx pgx.Tx, cat *platform.AttrCatalog, st Step, stepMD5 string,
	projMap map[permissionProjectionKey]map[string]any,
	attrExists map[permissionProjectionKey]map[[16]byte]bool,
	storedValues map[permissionProjectionKey]map[[16]byte][]permissionStoredValue,
) error {
	switch st.Op {
	case "add-triple", "deep-merge-triple":
		ta, err := parseTripleArgs(st, cat)
		if err != nil {
			return err
		}
		var eid [16]byte
		if err := parseUUID(strings.Trim(string(ta.EID), `"`), &eid); err != nil {
			return fmt.Errorf("perms: eid %s: %w", ta.EID, err)
		}
		key := permissionProjectionKey{eid: eid, etype: etypeFor(cat, ta.AttrID)}
		if projMap[key] == nil {
			projMap[key] = make(map[string]any)
		}
		if _, exists := projMap[key]["id"]; !exists {
			projMap[key]["id"] = uuidToStr(eid)
		}
		if attrExists[key] == nil {
			attrExists[key] = make(map[[16]byte]bool)
		}
		if storedValues[key] == nil {
			storedValues[key] = make(map[[16]byte][]permissionStoredValue)
		}
		incoming, err := parseValueJSON(ta.Value)
		if err != nil {
			return err
		}
		a, hasAttr := cat.ByID(ta.AttrID)
		label := attrLabel(cat, ta.AttrID)
		candidate := incoming
		candidateMD5 := stepMD5
		alreadyExists := false
		if hasAttr && a.Cardinality == "many" {
			stored := storedValues[key][ta.AttrID]
			if st.Op == "deep-merge-triple" {
				if first, ok := firstStoredValue(stored); ok {
					candidate = deepMergeJSON(normalizedProjectionValue(first.value), incoming)
					hashes, err := fingerprintValuesTx(ctx, tx, []any{candidate})
					if err != nil {
						return fmt.Errorf("fingerprint deep-merge value: %w", err)
					}
					candidateMD5 = hashes[0]
				}
			}
			for _, item := range stored {
				if item.md5 == candidateMD5 {
					alreadyExists = true
					break
				}
			}
		}
		if label != "" {
			if hasAttr && a.Cardinality == "many" {
				arr, _ := projMap[key][label].([]any)
				if !alreadyExists {
					projMap[key][label] = append(append([]any(nil), arr...), candidate)
				}
			} else {
				if st.Op == "deep-merge-triple" {
					candidate = deepMergeJSON(projMap[key][label], incoming)
				}
				projMap[key][label] = candidate
			}
		}
		attrExists[key][ta.AttrID] = true
		if hasAttr && a.Cardinality == "many" {
			if !alreadyExists {
				storedValues[key][ta.AttrID] = append(storedValues[key][ta.AttrID], permissionStoredValue{value: candidate, md5: candidateMD5})
			}
		} else {
			storedValues[key][ta.AttrID] = []permissionStoredValue{{value: candidate, md5: candidateMD5}}
		}

	case "retract-triple":
		ta, err := parseTripleArgs(st, cat)
		if err != nil {
			return err
		}
		var eid [16]byte
		if err := parseUUID(strings.Trim(string(ta.EID), `"`), &eid); err != nil {
			return fmt.Errorf("perms: eid %s: %w", ta.EID, err)
		}
		key := permissionProjectionKey{eid: eid, etype: etypeFor(cat, ta.AttrID)}
		values := storedValues[key][ta.AttrID]
		matchedIndex := -1
		for i, item := range values {
			if item.md5 == stepMD5 {
				matchedIndex = i
				break
			}
		}
		if matchedIndex < 0 {
			return nil
		}
		a, hasAttr := cat.ByID(ta.AttrID)
		label := attrLabel(cat, ta.AttrID)
		remaining := append([]permissionStoredValue(nil), values[:matchedIndex]...)
		remaining = append(remaining, values[matchedIndex+1:]...)
		if len(remaining) == 0 {
			delete(storedValues[key], ta.AttrID)
			if attrExists[key] != nil {
				delete(attrExists[key], ta.AttrID)
			}
		} else {
			storedValues[key][ta.AttrID] = remaining
		}
		if label != "" && hasAttr {
			if a.Cardinality == "many" {
				arr, _ := projMap[key][label].([]any)
				if matchedIndex < len(arr) {
					arr = append(arr[:matchedIndex:matchedIndex], arr[matchedIndex+1:]...)
				}
				if len(arr) == 0 {
					delete(projMap[key], label)
				} else {
					projMap[key][label] = arr
				}
			} else {
				delete(projMap[key], label)
			}
		}

	case "delete-entity":
		var eidStr string
		if err := json.Unmarshal(st.Args[0], &eidStr); err != nil {
			return nil
		}
		var eid [16]byte
		if err := parseUUID(eidStr, &eid); err != nil {
			return nil
		}
		etype := "$default"
		if len(st.Args) == 2 {
			if err := json.Unmarshal(st.Args[1], &etype); err != nil {
				return fmt.Errorf("perms: delete-entity etype: %w", err)
			}
		}
		key := permissionProjectionKey{eid: eid, etype: etype}
		projMap[key] = make(map[string]any)
		attrExists[key] = make(map[[16]byte]bool)
		storedValues[key] = make(map[[16]byte][]permissionStoredValue)
	}
	return nil
}

func cloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func attrLabel(cat *platform.AttrCatalog, id [16]byte) string {
	if a, ok := cat.ByID(id); ok && a.Label != nil {
		return *a.Label
	}
	return ""
}
