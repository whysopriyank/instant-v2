package adminapi

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/transact"
)

// ---- Perms checks (dry-run) ------------------------------------------------

// rulesFromBody parses the optional "rules" JSON object. DEVIATION: v1 loads
// persisted rules and accepts "rules-override"; v2 Phase 5 has no rules
// persistence, so evaluation runs solely against this body-passed doc,
// defaulting to the permissive empty doc.
func rulesFromBody(body map[string]any) (*perms.RuleDoc, error) {
	v, present := body["rules"]
	if !present || v == nil {
		return perms.ParseRuleDoc(nil)
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return perms.ParseRuleDoc(raw)
}

// handleQueryPermsCheck ports query-perms-check: evaluate the view rule per
// root etype WITHOUT mutating anything, returning {check-results, result}.
// DEVIATION: v1 evaluates permissioned-query-check with as-guest/as-token
// identity overrides, ip/origin overrides and a rule-wheres echo; none of
// those inputs exist yet, so bindings are empty and rule-wheres is omitted.
func (h *Handler) handleQueryPermsCheck(w http.ResponseWriter, r *http.Request, a *authedReq) {
	doc, err := rulesFromBody(a.body)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid `rules`: "+err.Error())
		return
	}
	raw, _ := a.body["query"].(map[string]any)
	if raw == nil {
		writeErr(w, http.StatusBadRequest, "missing or invalid `query` object")
		return
	}
	q, err := instaql.Coerce(raw)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	checks := make([]map[string]any, 0, len(q.Forms))
	for _, f := range q.Forms {
		allowed, cerr := perms.Check(f.Etype, "view", doc, perms.Bindings{})
		entry := map[string]any{"etype": f.Etype, "action": "view", "allowed": allowed}
		if cerr != nil {
			entry["error"] = cerr.Error()
		}
		checks = append(checks, entry)
	}
	res, err := (&instaql.Executor{DB: h.Pool, Admin: true}).Run(r.Context(), q, a.cat, a.appID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"check-results": checks,
		"result":        res.Data, // object-tree, matching v1's :result
	})
}

// tripleAction maps one wire step to the permission action v1 evaluates.
// add-triple resolves to update vs create by probing current state, mirroring
// permissioned-tx's data-dependent choice.
func (h *Handler) tripleAction(ctx context.Context, a *authedReq, op, eidStr, attrStr string) (etype, action string) {
	switch op {
	case "add-triple", "deep-merge-triple":
		action = "create"
		var attrID [16]byte
		if platform.ScanUUID(strings.Trim(attrStr, `"`), &attrID) == nil {
			if at, ok := a.cat.ByID(attrID); ok && at.Etype != nil {
				etype = *at.Etype
			}
			var eid [16]byte
			if platform.ScanUUID(strings.Trim(eidStr, `"`), &eid) == nil {
				rows, err := h.DB.FetchTriples(ctx, a.appID, storage.FetchFilter{
					EntityIDs: [][16]byte{eid}, AttrIDs: [][16]byte{attrID},
				})
				if err == nil && len(rows) > 0 {
					action = "update"
				}
			}
		}
	case "retract-triple":
		action = "delete"
		var attrID [16]byte
		if platform.ScanUUID(strings.Trim(attrStr, `"`), &attrID) == nil {
			if at, ok := a.cat.ByID(attrID); ok && at.Etype != nil {
				etype = *at.Etype
			}
		}
	case "delete-entity":
		action = "delete"
	case "add-attr", "restore-attr":
		return "attrs", "create"
	case "update-attr":
		return "attrs", "update"
	case "delete-attr":
		return "attrs", "delete"
	default:
		return "", "" // rule-params and unknown ops carry no permission gate
	}
	if etype == "" {
		etype = "$default"
	}
	return etype, action
}

// handleTransactPermsCheck ports transact-perms-check as a pure dry-run: it
// validates the steps and evaluates the resolved permission programs WITHOUT
// committing anything. DEVIATION: v1 honors "dangerously-commit-tx" to run the
// permissioned transaction for real; that flag is deliberately ignored here —
// committing belongs to the wired permission layer, not the admin plane.
// Response mirrors v1's cleaned-result envelope ({tx-id, all-checks-ok?,
// committed?, check-results}) with tx-id null and committed? false.
func (h *Handler) handleTransactPermsCheck(w http.ResponseWriter, r *http.Request, a *authedReq) {
	doc, err := rulesFromBody(a.body)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid `rules`: "+err.Error())
		return
	}
	stepsRaw, err := stepsFromBody(a.body)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	steps, err := transact.ParseSteps(stepsRaw)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx := r.Context()
	checks := []map[string]any{}
	seen := map[[2]string]bool{}
	allOK := true
	for _, st := range steps {
		var eidStr, attrStr string
		if len(st.Args) > 0 {
			_ = json.Unmarshal(st.Args[0], &eidStr)
		}
		if len(st.Args) > 1 {
			_ = json.Unmarshal(st.Args[1], &attrStr)
		}
		etype, action := h.tripleAction(ctx, a, st.Op, eidStr, attrStr)
		if action == "" {
			continue
		}
		key := [2]string{etype, action}
		if seen[key] {
			continue
		}
		seen[key] = true
		allowed, cerr := perms.Check(etype, action, doc, perms.Bindings{})
		if cerr != nil || !allowed {
			allOK = false
		}
		entry := map[string]any{"etype": etype, "action": action, "allowed": allowed}
		if cerr != nil {
			entry["error"] = cerr.Error()
		}
		checks = append(checks, entry)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"tx-id":          nil,
		"all-checks-ok?": allOK,
		"committed?":     false,
		"check-results":  checks,
	})
}
