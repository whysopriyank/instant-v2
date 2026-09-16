package adminapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/transact"
)

// ---- Perms checks (dry-run) ------------------------------------------------

type selectedRules struct {
	doc     *perms.RuleDoc
	source  string
	version any
}

// rulesForCheck uses an explicit request rule document when present; otherwise
// it loads the persisted document and version. Missing and explicit-null rules
// fail closed because a permission-check endpoint must never silently switch to
// default-open behavior.
func (h *Handler) rulesForCheck(r *http.Request, a *authedReq) (selectedRules, error) {
	body := a.body
	v, present := body["rules"]
	if present {
		if v == nil {
			return selectedRules{}, errors.New("explicit `rules` must not be null")
		}
		raw, err := json.Marshal(v)
		if err != nil {
			return selectedRules{}, err
		}
		doc, err := perms.ParseRuleDoc(raw)
		return selectedRules{doc: doc, source: "request-override"}, err
	}

	var raw []byte
	var version int
	err := h.Pool.QueryRow(r.Context(),
		`SELECT code, version FROM rules WHERE app_id=$1::uuid`, a.appID,
	).Scan(&raw, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return selectedRules{}, errors.New("no persisted rules for app")
	}
	if err != nil {
		return selectedRules{}, err
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return selectedRules{}, errors.New("persisted rules must not be null")
	}
	doc, err := perms.ParseRuleDoc(raw)
	return selectedRules{doc: doc, source: "persisted", version: version}, err
}

func appendViewChecks(forms []*instaql.Form, doc *perms.RuleDoc, checks *[]map[string]any) bool {
	allOpen := true
	for _, f := range forms {
		mode := perms.ViewGate(doc, f.Etype)
		entry := map[string]any{"etype": f.Etype, "action": "view", "allowed": mode == perms.ViewOpen}
		if mode == perms.ViewDynamic {
			entry["error"] = (&instaql.ErrRuleFilterUnsupported{Etype: f.Etype}).Error()
		}
		*checks = append(*checks, entry)
		if mode != perms.ViewOpen {
			allOpen = false
		}
		if !appendViewChecks(f.Children, doc, checks) {
			allOpen = false
		}
	}
	return allOpen
}

// handleQueryPermsCheck ports query-perms-check: evaluate the view rule per
// root etype WITHOUT mutating anything, returning {check-results, result}.
// DEVIATION: v1 evaluates permissioned-query-check with as-guest/as-token
// identity overrides, ip/origin overrides and a rule-wheres echo; none of
// those inputs exist yet, so bindings are empty and rule-wheres is omitted.
func (h *Handler) handleQueryPermsCheck(w http.ResponseWriter, r *http.Request, a *authedReq) {
	rules, err := h.rulesForCheck(r, a)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid rules: "+err.Error())
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
	response := map[string]any{
		"check-results": checks,
		"rule-source":   rules.source,
		"rule-version":  rules.version,
	}
	if !appendViewChecks(q.Forms, rules.doc, &checks) {
		response["check-results"] = checks
		writeJSON(w, http.StatusOK, response)
		return
	}
	res, err := (&instaql.Executor{DB: h.Pool, Rules: rules.doc}).Run(r.Context(), q, a.cat, a.appID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	response["check-results"] = checks
	response["result"] = res.Data // object-tree, matching v1's :result
	writeJSON(w, http.StatusOK, response)
}

// handleTransactPermsCheck ports transact-perms-check as a pure dry-run: it
// validates the steps and evaluates them through the transact coordinator,
// which always rolls back. DEVIATION: v1 honors "dangerously-commit-tx" to run
// the permissioned transaction for real; that flag is deliberately ignored
// here — committing belongs to the wired permission layer, not the admin plane.
// Response mirrors v1's cleaned-result envelope ({tx-id, all-checks-ok?,
// committed?, check-results}) with tx-id null and committed? false.
func (h *Handler) handleTransactPermsCheck(w http.ResponseWriter, r *http.Request, a *authedReq) {
	rules, err := h.rulesForCheck(r, a)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid rules: "+err.Error())
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

	evaluation, err := transact.EvaluatePermissions(r.Context(), h.DB, a.cat, a.appID, steps, transact.Options{}, rules.doc)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	checks := make([]map[string]any, 0, len(evaluation.Checks))
	for _, check := range evaluation.Checks {
		entry := map[string]any{
			"etype":   check.Etype,
			"action":  check.Action,
			"allowed": check.Allowed,
		}
		if check.Err != nil {
			entry["error"] = check.Err.Error()
		}
		checks = append(checks, entry)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"tx-id":          nil,
		"all-checks-ok?": evaluation.AllAllowed,
		"committed?":     false,
		"check-results":  checks,
		"rule-source":    rules.source,
		"rule-version":   rules.version,
	})
}
