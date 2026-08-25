// Package adminapi is the admin REST plane keyed by app admin tokens. Port of
// instant.admin.routes (server/src/instant/admin/routes.clj @ a4d2ef33).
//
// Every route authenticates the caller against app_admin_tokens (v1
// req->app-id-authed! restricted to app-admin tokens) and bypasses ALL
// permission checks: admin callers act with :admin? true semantics.
//
// Known deviations from v1 (documented at the affected handlers):
//   - Rules persistence does not exist yet in v2, so *_perms_check routes
//     evaluate against an optional "rules" object from the request body
//     instead of rule-model/get-by-app-id + "rules-override".
//   - transact_perms_check never commits (dry-run only); v1's
//     "dangerously-commit-tx" flag is intentionally ignored.
//   - /admin/rooms/presence returns {} because RoomHub lives in internal/sync
//     which this package must not import (orchestrator wires Phase 6).
package adminapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/instant-v2/instant-v2/internal/authn"
	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/metrics"
	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/tracing"
	"github.com/instant-v2/instant-v2/internal/transact"
	"github.com/instant-v2/instant-v2/internal/triple"
)

// Handler mounts the /admin/* routes.
type Handler struct {
	Pool     *pgxpool.Pool
	DB       *storage.DB
	Catalogs *platform.CatalogCache
	Logger   *slog.Logger
	// OnCommit, when set, fires after every successful /admin/transact so
	// the reactive invalidator learns about admin-plane writes (same hook
	// the WS and /runtime/transact paths use). attrIDs are the triple-step
	// targets; empty means no invalidatable writes.
	OnCommit func(ctx context.Context, appID [16]byte, attrIDs []string, txID int64, attrsChanged bool)
	// OnCommitChanges, when set, is preferred over OnCommit when every
	// step resolves to a plain triple write: entity-annotated events let
	// the incremental engine splice instead of recompute
	// (docs/09-tier2-architecture.md §T2.5).
	OnCommitChanges func(ctx context.Context, appID [16]byte, changes []reactive.Change, txID int64, attrsChanged bool)
}

// authedReq carries the authenticated request context through routing.
type authedReq struct {
	appID  [16]byte
	appStr string
	cat    *platform.AttrCatalog
	body   map[string]any // decoded JSON body; nil when absent
}

func (h *Handler) logger() *slog.Logger {
	if h.Logger != nil {
		return h.Logger
	}
	return slog.Default()
}

// ServeHTTP authenticates then dispatches by path. Wire spellings are copied
// verbatim from routes.clj (kebab/snake mix included).
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimSuffix(r.URL.Path, "/")
	if !strings.HasPrefix(path, "/admin/") {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	route := path[len("/admin"):]

	a, ok := h.authenticate(w, r)
	if !ok {
		return
	}

	post, get, del := r.Method == http.MethodPost, r.Method == http.MethodGet, r.Method == http.MethodDelete
	switch {
	case post && route == "/query":
		h.handleQuery(w, r, a)
	case post && route == "/transact":
		h.handleTransact(w, r, a)
	case post && route == "/query_perms_check":
		h.handleQueryPermsCheck(w, r, a)
	case post && route == "/transact_perms_check":
		h.handleTransactPermsCheck(w, r, a)
	case post && route == "/sign_out":
		h.handleSignOut(w, r, a)
	case post && route == "/refresh_tokens":
		h.handleRefreshTokens(w, r, a)
	case post && route == "/magic_code", post && route == "/send_magic_code":
		h.handleMagicCode(w, r, a)
	case post && route == "/verify_magic_code":
		h.handleVerifyMagicCode(w, r, a)
	case post && route == "/sign_in_guest":
		h.handleSignInGuest(w, r, a)
	case get && route == "/users":
		h.handleUsersList(w, r, a)
	case del && route == "/users":
		h.handleUsersDelete(w, r, a)
	case get && route == "/schema":
		h.handleSchema(w, r, a)
	case get && route == "/soft_deleted_attrs":
		h.handleSoftDeletedAttrs(w, r, a)
	case get && route == "/rooms/presence":
		h.handlePresence(w, r, a)
	default:
		writeErr(w, http.StatusNotFound, "not found")
	}
}

// authenticate ports req->app-id-authed! restricted to app-admin tokens:
//
//   - token: "Authorization: Bearer <t>" or "X-admin-token: <t>" header
//   - app-id: JSON body "app-id" (assignment-frozen), falling back to the
//     "X-app-id" header or "app-id"/"app_id" query params for body-less GETs
//
// Errors: 400 malformed app-id/body, 401 bad token, 404 unknown app.
func (h *Handler) authenticate(w http.ResponseWriter, r *http.Request) (*authedReq, bool) {
	token := bearerOrAdminToken(r)
	if token == "" {
		writeErr(w, http.StatusUnauthorized, "missing admin token")
		return nil, false
	}

	ctx := r.Context()
	var body map[string]any
	if r.Body != nil {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "unreadable request body")
			return nil, false
		}
		r.Body = io.NopCloser(bytes.NewReader(raw)) // downstream handlers reuse it
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &body); err != nil {
				writeErr(w, http.StatusBadRequest, "invalid JSON body")
				return nil, false
			}
		}
	}

	appStr := ""
	for _, v := range []any{body["app-id"], body["app_id"]} {
		if s, ok := v.(string); ok && s != "" {
			appStr = s
			break
		}
	}
	if appStr == "" {
		q := r.URL.Query()
		appStr = firstNonEmpty(r.Header.Get("X-app-id"),
			r.Header.Get("app-id"), q.Get("app-id"), q.Get("app_id"))
	}
	var appID [16]byte
	if appStr == "" || platform.ScanUUID(appStr, &appID) != nil {
		writeErr(w, http.StatusBadRequest, "missing or invalid app-id")
		return nil, false
	}

	cat, err := h.Catalogs.For(ctx, appStr)
	if err != nil {
		if errors.Is(err, platform.ErrNoRows) {
			writeErr(w, http.StatusNotFound, "unknown app")
			return nil, false
		}
		h.logger().Error("adminapi: catalog load", "err", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
		return nil, false
	}
	if platform.ScanUUID(token, new([16]byte)) != nil {
		// Non-uuid tokens can never match app_admin_tokens; 401 without
		// reaching Postgres (uuid cast would raise a 500).
		writeErr(w, http.StatusUnauthorized, "Invalid admin token")
		return nil, false
	}
	// Catalogs.For returns an empty (not erroring) catalog for unknown apps,
	// so probe the apps table for the 404-unknown-app contract.
	var one int
	if err := h.Pool.QueryRow(ctx, `SELECT 1 FROM apps WHERE id=$1::uuid`, appStr).Scan(&one); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeErr(w, http.StatusNotFound, "unknown app")
			return nil, false
		}
		h.logger().Error("adminapi: app lookup", "err", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
		return nil, false
	}
	okTok, err := h.Catalogs.CheckAdminToken(ctx, appStr, token)
	if err != nil {
		h.logger().Error("adminapi: token check", "err", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
		return nil, false
	}
	if !okTok {
		writeErr(w, http.StatusUnauthorized, "Invalid admin token")
		return nil, false
	}
	return &authedReq{appID: appID, appStr: appStr, cat: cat, body: body}, true
}

func bearerOrAdminToken(r *http.Request) string {
	authz := r.Header.Get("Authorization")
	if len(authz) >= 7 && strings.EqualFold(authz[:7], "bearer ") {
		if t := strings.TrimSpace(authz[7:]); t != "" {
			return t
		}
	}
	return r.Header.Get("X-admin-token")
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func strField(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeErr ports response/error envelopes: {"message": "..."}.
func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"message": msg})
}

// ---- Query -----------------------------------------------------------------

// handleQuery ports query-post: {"query": {...}} → the InstaQL result
// envelope. Admin callers run the bare executor (v1 permissioned-query with
// :admin? true short-circuits rule wheres).
func (h *Handler) handleQuery(w http.ResponseWriter, r *http.Request, a *authedReq) {
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
	ex := &instaql.Executor{DB: h.Pool, Admin: true} // admin bypasses perms
	res, err := ex.Run(r.Context(), q, a.cat, a.appID)
	if err != nil {
		h.logger().Error("adminapi: query", "err", err)
		writeErr(w, http.StatusInternalServerError, "query failed")
		return
	}
	// v1 query-post responds (response/ok object-tree): the bare etype map,
	// no envelope key — the Python/TS SDKs index result[<etype>] directly.
	writeJSON(w, http.StatusOK, res.Data)
}

// ---- Transact --------------------------------------------------------------

// handleTransact ports transact-post: {"steps": [...]} → {"tx-id": N} with
// transact.Options{Admin: true}. DEVIATION: v1 loads persisted rules
// (rule-model/get-by-app-id) into the permissioned transaction; rules
// persistence does not exist yet in v2, so the admin path runs without a
func (h *Handler) handleTransact(w http.ResponseWriter, r *http.Request, a *authedReq) {
	ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	ctx, span := tracing.Tracer.Start(ctx, "transact.admin")
	defer span.End()
	stepsRaw, err := stepsFromBody(a.body)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	throwOnMissing := false
	if v, ok := a.body["throw-on-missing-attrs?"].(bool); ok {
		throwOnMissing = v
	}
	cat := a.cat
	if transact.HasHighLevelOps(stepsRaw) {
		var lerr error
		stepsRaw, cat, lerr = h.lowerAdminSteps(ctx, a, stepsRaw, throwOnMissing)
		if lerr != nil {
			writeErr(w, http.StatusBadRequest, lerr.Error())
			return
		}
	}
	steps, err := transact.ParseSteps(stepsRaw)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	started := time.Now()
	res, err := transact.Transact(ctx, h.DB, cat, a.appID, steps,
		transact.Options{Admin: true}, nil)
	metrics.TransactDuration.WithLabelValues("admin").Observe(time.Since(started).Seconds())
	if err != nil {
		h.logger().Error("adminapi: transact", "err", err)
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if res.AttrsChanged {
		h.Catalogs.Invalidate(a.appStr)
	}
	if h.OnCommitChanges != nil {
		if triples, ok := transact.ResolveTriples(steps, cat); ok && len(triples) > 0 {
			changes := make([]reactive.Change, 0, len(triples))
			for _, tt := range triples {
				changes = append(changes, reactive.Change{
					Etype: tt.Etype, EntityID: tt.EntityID, AttrIDs: []string{tt.AttrID},
				})
			}
			h.OnCommitChanges(ctx, a.appID, changes, res.TxID, res.AttrsChanged)
		} else if h.OnCommit != nil {
			h.OnCommit(r.Context(), a.appID, touchedAttrs(steps), res.TxID, res.AttrsChanged)
		}
	} else if h.OnCommit != nil {
		h.OnCommit(r.Context(), a.appID, touchedAttrs(steps), res.TxID, res.AttrsChanged)
	}
	writeJSON(w, http.StatusOK, map[string]any{"tx-id": res.TxID})
}

// lowerAdminSteps runs the instant.admin.model lowering (see
// internal/transact/highlevel.go): it resolves lookup eids against committed
// state and provisions missing attrs via platform.GetOrCreateAttr(+Rev),
// mirroring how authn creates system attrs. Provisioning commits in its own
// transaction and the catalog cache is invalidated + reloaded so Transact's
// parseTripleArgs sees the fresh attr ids. Pure-low-level batches never reach
// this path.
func (h *Handler) lowerAdminSteps(
	ctx context.Context, a *authedReq, raw []json.RawMessage, throwOnMissing bool,
) ([]json.RawMessage, *platform.AttrCatalog, error) {
	var (
		tx      pgx.Tx
		created int
	)
	hooks := transact.LowerHooks{
		ResolveUnique: func(ctx context.Context, appID [16]byte, attrID [16]byte, value json.RawMessage) ([16]byte, bool, error) {
			var eid [16]byte
			err := h.Pool.QueryRow(ctx,
				`SELECT entity_id FROM triples
				  WHERE app_id=$1 AND attr_id=$2 AND av AND value=$3::jsonb
				  LIMIT 1`, appID, attrID, string(value)).Scan(&eid)
			if errors.Is(err, pgx.ErrNoRows) {
				return [16]byte{}, false, nil
			}
			if err != nil {
				return [16]byte{}, false, err
			}
			return eid, true, nil
		},
		EntityExists: func(ctx context.Context, appID [16]byte, eid [16]byte) (bool, error) {
			var one int
			err := h.Pool.QueryRow(ctx,
				`SELECT 1 FROM triples WHERE app_id=$1 AND entity_id=$2 LIMIT 1`,
				appID, eid).Scan(&one)
			if errors.Is(err, pgx.ErrNoRows) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			return true, nil
		},
		CreateAttr: func(ctx context.Context, appID [16]byte, spec transact.AttrSpec) (platform.Attr, error) {
			if tx == nil {
				var err error
				tx, err = h.Pool.Begin(ctx)
				if err != nil {
					return platform.Attr{}, err
				}
			}
			created++
			if spec.Ref {
				return platform.GetOrCreateAttrRev(ctx, tx, appID,
					spec.Etype, spec.Label, spec.ReverseEtype, spec.ReverseLabel,
					spec.ValueType, spec.Cardinality, spec.Unique, spec.Indexed)
			}
			return platform.GetOrCreateAttr(ctx, tx, appID,
				spec.Etype, spec.Label, spec.ValueType, spec.Cardinality,
				spec.Unique, spec.Indexed)
		},
	}

	lowered, err := transact.LowerAdminSteps(ctx, a.appID, a.cat, raw, hooks, throwOnMissing)
	if tx != nil {
		if err != nil {
			_ = tx.Rollback(ctx)
			return nil, nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, nil, err
		}
	}
	if err != nil {
		return nil, nil, err
	}
	if created == 0 {
		return lowered, a.cat, nil
	}
	// Attrs were provisioned: drop the stale cached catalog and reload so the
	// freshly minted attr ids resolve inside Transact.
	h.Catalogs.Invalidate(a.appStr)
	fresh, err := h.Catalogs.For(ctx, a.appStr)
	if err != nil {
		return nil, nil, err
	}
	return lowered, fresh, nil
}

func stepsFromBody(body map[string]any) ([]json.RawMessage, error) {
	arr, ok := body["steps"].([]any)
	if !ok {
		return nil, errors.New("missing or invalid `steps` array")
	}
	out := make([]json.RawMessage, 0, len(arr))
	for _, el := range arr {
		b, err := json.Marshal(el)
		if err != nil {
			return nil, fmt.Errorf("invalid step: %w", err)
		}
		out = append(out, b)
	}
	return out, nil
}

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

// touchedAttrs extracts attr-id strings from triple-shaped steps so callers
// can invalidate reactive subscriptions (mirrors cmd's touchedAttrs).
func touchedAttrs(steps []transact.Step) []string {
	var out []string
	for _, s := range steps {
		switch s.Op {
		case "add-triple", "deep-merge-triple", "retract-triple":
			if len(s.Args) >= 2 {
				var attrStr string
				if json.Unmarshal(s.Args[1], &attrStr) == nil {
					out = append(out, attrStr)
				}
			}
		}
	}
	return out
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

// ---- Auth routes -----------------------------------------------------------

// service builds the authn facade from explicit deps (no globals; cmd wiring
// stays with the orchestrator).
func (h *Handler) service() *authn.Service {
	return &authn.Service{DB: h.DB, Pool: h.Pool, Catalogs: h.Catalogs, Logger: h.Logger}
}

// captureMailer recovers the generated code so the admin envelope can include
// it — v1 magic-code-post/send-magic-code-post both return {:keys [code]}.
type captureMailer struct{ Code *string }

func (m captureMailer) SendMagicCode(_ context.Context, _ string, code string) error {
	*m.Code = code
	return nil
}

// handleMagicCode ports magic-code-post and send-magic-code-post →
// {"code": "..."} (identical envelopes in v1). DEVIATION: v1 send_magic_code
// additionally emails through the configured provider; here delivery is the
// Mailer hook's job (nil Mailer → code logged by authn), and we capture it
// purely to preserve the response shape.
func (h *Handler) handleMagicCode(w http.ResponseWriter, r *http.Request, a *authedReq) {
	email := strField(a.body, "email")
	if email == "" {
		writeErr(w, http.StatusBadRequest, "missing `email`")
		return
	}
	code := ""
	svc := h.service()
	svc.Mailer = captureMailer{&code}
	if err := svc.SendMagicCode(r.Context(), a.appID, email); err != nil {
		h.logger().Error("adminapi: send magic code", "err", err)
		writeErr(w, http.StatusInternalServerError, "could not create magic code")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"code": code})
}

// handleVerifyMagicCode ports verify-magic-code-post →
// {"user": {..., "refresh_token"}, "created": bool}.
func (h *Handler) handleVerifyMagicCode(w http.ResponseWriter, r *http.Request, a *authedReq) {
	email := strField(a.body, "email")
	code := strField(a.body, "code")
	if email == "" || code == "" {
		writeErr(w, http.StatusBadRequest, "missing `email` or `code`")
		return
	}
	guestToken := strField(a.body, "refresh-token")
	extra, _ := a.body["extra-fields"].(map[string]any)

	res, err := h.service().VerifyMagicCode(r.Context(), a.appID, email, code, guestToken, extra, true)
	switch {
	case errors.Is(err, authn.ErrInvalidCode), errors.Is(err, authn.ErrExpiredCode):
		writeErr(w, http.StatusUnauthorized, "invalid or expired magic code")
	case errors.Is(err, authn.ErrSignupDenied):
		writeErr(w, http.StatusForbidden, "signup denied by permissions")
	case errors.Is(err, authn.ErrLocked):
		writeErr(w, http.StatusTooManyRequests, "too many failed attempts")
	case err != nil:
		h.logger().Error("adminapi: verify magic code", "err", err)
		writeErr(w, http.StatusInternalServerError, "verify failed")
	default:
		writeJSON(w, http.StatusOK, res)
	}
}

// handleSignInGuest ports sign-in-guest-post → {"user": {..., "refresh_token"}}.
func (h *Handler) handleSignInGuest(w http.ResponseWriter, r *http.Request, a *authedReq) {
	extra, _ := a.body["extra-fields"].(map[string]any)
	res, err := h.service().SignInGuest(r.Context(), a.appID, extra)
	if err != nil {
		h.logger().Error("adminapi: sign in guest", "err", err)
		writeErr(w, http.StatusInternalServerError, "sign-in failed")
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleSignOut ports sign-out-post: accepts `id`, `email`, or `refresh_token`
// and deletes the matching refresh token(s) → {"ok": true}. The raw-token
// variant delegates to authn.Service.SignOut; id/email variants resolve the
// user then drop every $userRefreshTokens entity linked to it (v1
// delete-by-user-id!).
func (h *Handler) handleSignOut(w http.ResponseWriter, r *http.Request, a *authedReq) {
	if rt := strField(a.body, "refresh_token"); rt != "" {
		// authn.SignOut is a no-op for unknown tokens, matching v1's row delete.
		if err := h.service().SignOut(r.Context(), a.appID, rt); err != nil {
			h.logger().Error("adminapi: sign out", "err", err)
			writeErr(w, http.StatusInternalServerError, "sign-out failed")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
		return
	}

	var userID [16]byte
	switch {
	case strField(a.body, "id") != "":
		idStr := strField(a.body, "id")
		if platform.ScanUUID(idStr, &userID) != nil {
			writeErr(w, http.StatusBadRequest, "invalid `id`")
			return
		}
	case strField(a.body, "email") != "":
		found, err := h.userByEmail(r.Context(), a, strField(a.body, "email"))
		if err != nil {
			h.logger().Error("adminapi: sign out lookup", "err", err)
			writeErr(w, http.StatusInternalServerError, "lookup failed")
			return
		}
		if found == "" {
			writeErr(w, http.StatusNotFound, "user not found")
			return
		}
		if platform.ScanUUID(found, &userID) != nil {
			writeErr(w, http.StatusNotFound, "user not found")
			return
		}
	default:
		// v1 message verbatim.
		writeErr(w, http.StatusBadRequest, "Please provide an `id`, `email`, or `refresh_token`")
		return
	}

	if err := h.deleteUserTokens(r.Context(), a, userID); err != nil {
		h.logger().Error("adminapi: sign out delete", "err", err)
		writeErr(w, http.StatusInternalServerError, "sign-out failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// userByEmail resolves a $users entity id by its email triple.
func (h *Handler) userByEmail(ctx context.Context, a *authedReq, email string) (string, error) {
	attr := a.cat.FindByEtypeLabel("$users", "email")
	if attr == nil {
		return "", nil // no auth attrs provisioned yet → no users
	}
	var eid string
	err := h.Pool.QueryRow(ctx,
		`SELECT entity_id FROM triples WHERE app_id=$1 AND attr_id=$2 AND value=to_jsonb($3::text) LIMIT 1`,
		a.appID, attr.ID, email).Scan(&eid)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return eid, err
}

// deleteUserTokens deletes every token entity whose $user link points at uid.
func (h *Handler) deleteUserTokens(ctx context.Context, a *authedReq, userID [16]byte) error {
	link := a.cat.FindByEtypeLabel("$userRefreshTokens", "$user")
	if link == nil {
		return nil
	}
	rows, err := h.Pool.Query(ctx,
		`SELECT entity_id FROM triples WHERE app_id=$1 AND attr_id=$2 AND value=to_jsonb($3::text)`,
		a.appID, link.ID, platform.UUIDToStr(userID))
	if err != nil {
		return err
	}
	defer rows.Close()
	var ids [][16]byte
	for rows.Next() {
		var id [16]byte
		if err := rows.Scan(&id); err != nil {
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil || len(ids) == 0 {
		return err
	}
	_, err = h.Pool.Exec(ctx,
		`DELETE FROM triples WHERE app_id=$1 AND entity_id = ANY($2::uuid[])`, a.appID, ids)
	return err
}

// handleRefreshTokens ports refresh-tokens-post: find-or-create the user by
// `email` or `id`, mint a fresh refresh token → {"user": {..., "refresh_token"},
// "created": bool}. User/token rows are written as triples exactly the way
// authn writes them ($users.email/$users.type, $userRefreshTokens.hashedToken
// + $user link), so runtime verify/sign-out see identical state.
func (h *Handler) handleRefreshTokens(w http.ResponseWriter, r *http.Request, a *authedReq) {
	email := strField(a.body, "email")
	idStr := strField(a.body, "id")
	if email == "" && idStr == "" {
		// v1 message verbatim.
		writeErr(w, http.StatusBadRequest, "Please provide an `email` or `id`")
		return
	}
	extra, _ := a.body["extra-fields"].(map[string]any)

	ctx := r.Context()
	var providedID [16]byte
	if idStr != "" && platform.ScanUUID(idStr, &providedID) != nil {
		writeErr(w, http.StatusBadRequest, "invalid `id`")
		return
	}

	var userID [16]byte
	created := false
	if email != "" {
		eid, err := h.userByEmail(ctx, a, email)
		if err != nil {
			h.logger().Error("adminapi: refresh lookup", "err", err)
			writeErr(w, http.StatusInternalServerError, "lookup failed")
			return
		}
		if eid != "" {
			_ = platform.ScanUUID(eid, &userID)
		} else {
			created = true
			userID = orUUID(providedID, newUUID())
		}
	} else if h.userExists(ctx, a, providedID) {
		userID = providedID
	} else {
		created = true
		userID = providedID
	}

	token := ""
	if created {
		if err := h.createUser(ctx, a, userID, email, extra); err != nil {
			h.logger().Error("adminapi: create user", "err", err)
			writeErr(w, http.StatusInternalServerError, "could not create user")
			return
		}
	}
	token, err := h.mintRefreshToken(ctx, a, userID)
	if err != nil {
		h.logger().Error("adminapi: mint token", "err", err)
		writeErr(w, http.StatusInternalServerError, "could not create refresh token")
		return
	}

	user := map[string]any{"id": platform.UUIDToStr(userID)}
	if email != "" || created {
		user["type"] = "user"
	}
	if email != "" {
		user["email"] = email
	}
	for k, v := range extra {
		if _, exists := user[k]; !exists {
			user[k] = v
		}
	}
	user["refresh_token"] = token
	writeJSON(w, http.StatusOK, map[string]any{"user": user, "created": created})
}

func (h *Handler) userExists(ctx context.Context, a *authedReq, uid [16]byte) bool {
	var one int
	err := h.Pool.QueryRow(ctx,
		`SELECT 1 FROM triples WHERE app_id=$1 AND entity_id=$2 LIMIT 1`,
		a.appID, uid).Scan(&one)
	return err == nil
}

// createUser writes $users triples for a fresh admin-created user. Attr ids
// resolve through GetOrCreateAttr — the same deterministic ids authn uses.
func (h *Handler) createUser(ctx context.Context, a *authedReq, userID [16]byte, email string, extra map[string]any) error {
	err := h.DB.WithTx(ctx, func(tx pgx.Tx) error {
		if _, _, err := h.userAttrsTx(ctx, tx, a); err != nil {
			return err
		}
		for k := range extra {
			if isReservedUserLabel(k) {
				continue
			}
			if _, err := platform.GetOrCreateAttr(ctx, tx, a.appID, "$users", k, "blob", "one", false, false); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	h.Catalogs.Invalidate(a.appStr)
	cat, err := h.Catalogs.For(ctx, a.appStr)
	if err != nil {
		return err
	}

	var ts []triple.Triple
	add := func(etype, label string, v any) error {
		at := cat.FindByEtypeLabel(etype, label)
		if at == nil {
			return fmt.Errorf("missing attr %s.%s", etype, label)
		}
		ts = append(ts, triple.Triple{E: userID, A: at.ID, V: v})
		return nil
	}
	if err := add("$users", "type", "user"); err != nil {
		return err
	}
	if email != "" {
		if err := add("$users", "email", email); err != nil {
			return err
		}
	}
	for k, v := range extra {
		if isReservedUserLabel(k) {
			continue
		}
		if err := add("$users", k, v); err != nil {
			return err
		}
	}
	_, err = h.DB.InsertTriples(ctx, a.appID, cat, ts, false)
	return err
}

func isReservedUserLabel(label string) bool {
	switch label {
	case "email", "type", "id":
		return true
	}
	return false
}

// mintRefreshToken creates a $userRefreshTokens entity and returns the raw
// uuid bearer token (storage keeps only sha256(uuid-text), like authn).
func (h *Handler) mintRefreshToken(ctx context.Context, a *authedReq, userID [16]byte) (string, error) {
	var hashedAttr, linkAttr [16]byte
	err := h.DB.WithTx(ctx, func(tx pgx.Tx) error {
		ha, err := platform.GetOrCreateAttr(ctx, tx, a.appID, "$userRefreshTokens", "hashedToken", "blob", "one", true, true)
		if err != nil {
			return err
		}
		la, err := platform.GetOrCreateAttr(ctx, tx, a.appID, "$userRefreshTokens", "$user", "ref", "one", false, false)
		if err != nil {
			return err
		}
		hashedAttr, linkAttr = ha.ID, la.ID
		return nil
	})
	if err != nil {
		return "", err
	}
	h.Catalogs.Invalidate(a.appStr)
	cat, err := h.Catalogs.For(ctx, a.appStr)
	if err != nil {
		return "", err
	}

	raw := newUUID()
	ts := []triple.Triple{
		{E: raw, A: hashedAttr, V: authn.HashToken(platform.UUIDToStr(raw))},
		{E: raw, A: linkAttr, V: platform.UUIDToStr(userID)},
	}
	if _, err := h.DB.InsertTriples(ctx, a.appID, cat, ts, false); err != nil {
		return "", err
	}
	return platform.UUIDToStr(raw), nil
}

// userAttrsTx ensures the $users email/type attrs exist inside tx.
func (h *Handler) userAttrsTx(ctx context.Context, tx pgx.Tx, a *authedReq) ([16]byte, [16]byte, error) {
	ea, err := platform.GetOrCreateAttr(ctx, tx, a.appID, "$users", "email", "blob", "one", true, true)
	if err != nil {
		return [16]byte{}, [16]byte{}, err
	}
	ta, err := platform.GetOrCreateAttr(ctx, tx, a.appID, "$users", "type", "blob", "one", false, false)
	if err != nil {
		return [16]byte{}, [16]byte{}, err
	}
	return ea.ID, ta.ID, nil
}

// ---- Users -----------------------------------------------------------------

// handleUsersList lists the app's $users entities. limit/offset come from the
// query params (v1's GET /admin/users fetched a single user; the list shape
// with `{"users": [...]}` is this package's addition per the Phase 5 spec).
func (h *Handler) handleUsersList(w http.ResponseWriter, r *http.Request, a *authedReq) {
	limit, offset := 100, 0
	q := r.URL.Query()
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeErr(w, http.StatusBadRequest, "invalid `limit`")
			return
		}
		limit = n
	}
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeErr(w, http.StatusBadRequest, "invalid `offset`")
			return
		}
		offset = n
	}

	emailAttr := a.cat.FindByEtypeLabel("$users", "email")
	typeAttr := a.cat.FindByEtypeLabel("$users", "type")
	if emailAttr == nil && typeAttr == nil {
		writeJSON(w, http.StatusOK, map[string]any{"users": []any{}})
		return
	}
	attrIDs := [][16]byte{}
	for _, at := range []*platform.Attr{emailAttr, typeAttr} {
		if at != nil {
			attrIDs = append(attrIDs, at.ID)
		}
	}
	rows, err := h.Pool.Query(r.Context(), `
		SELECT entity_id, attr_id, value FROM triples
		WHERE app_id=$1 AND attr_id = ANY($2::uuid[])
		ORDER BY entity_id`,
		a.appID, attrIDs)
	if err != nil {
		h.logger().Error("adminapi: users list", "err", err)
		writeErr(w, http.StatusInternalServerError, "query failed")
		return
	}
	defer rows.Close()

	byID := map[string]map[string]any{}
	var order []string
	for rows.Next() {
		var eid string
		var attrID [16]byte
		var value json.RawMessage
		if err := rows.Scan(&eid, &attrID, &value); err != nil {
			h.logger().Error("adminapi: users scan", "err", err)
			writeErr(w, http.StatusInternalServerError, "query failed")
			return
		}
		u, ok := byID[eid]
		if !ok {
			u = map[string]any{"id": eid}
			byID[eid] = u
			order = append(order, eid)
		}
		var v any
		_ = json.Unmarshal(value, &v)
		var emailID, typeID [16]byte
		if emailAttr != nil {
			emailID = emailAttr.ID
		}
		if typeAttr != nil {
			typeID = typeAttr.ID
		}
		switch attrID {
		case emailID:
			u["email"] = v
		case typeID:
			u["type"] = v
		}
	}
	if err := rows.Err(); err != nil {
		writeErr(w, http.StatusInternalServerError, "query failed")
		return
	}

	out := []any{}
	for i, eid := range order {
		if i < offset || i >= offset+limit {
			continue
		}
		out = append(out, byID[eid])
	}
	writeJSON(w, http.StatusOK, map[string]any{"users": out})
}

// handleUsersDelete bulk-deletes users by `ids` (triples + reverse refs),
// mirroring applyDeleteEntity semantics → {"deleted": N}.
func (h *Handler) handleUsersDelete(w http.ResponseWriter, r *http.Request, a *authedReq) {
	arr, ok := a.body["ids"].([]any)
	if !ok || len(arr) == 0 {
		writeErr(w, http.StatusBadRequest, "missing or invalid `ids` array")
		return
	}
	ids := make([][16]byte, 0, len(arr))
	for _, el := range arr {
		s, _ := el.(string)
		var id [16]byte
		if platform.ScanUUID(s, &id) != nil {
			writeErr(w, http.StatusBadRequest, "invalid user id: "+s)
			return
		}
		ids = append(ids, id)
	}

	ctx := r.Context()
	var deleted int64
	err := h.DB.WithTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`DELETE FROM triples WHERE app_id=$1 AND entity_id = ANY($2::uuid[])`, a.appID, ids)
		if err != nil {
			return err
		}
		deleted += tag.RowsAffected()
		for _, id := range ids {
			tag, err := tx.Exec(ctx,
				`DELETE FROM triples WHERE app_id=$1 AND vae AND value = to_jsonb($2::text)`,
				a.appID, platform.UUIDToStr(id))
			if err != nil {
				return err
			}
			deleted += tag.RowsAffected()
		}
		return nil
	})
	if err != nil {
		h.logger().Error("adminapi: users delete", "err", err)
		writeErr(w, http.StatusInternalServerError, "delete failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": deleted})
}

// ---- Schema / soft deletions / presence ------------------------------------

// handleSchema ports schema-get. v1 returns {:schema {:attrs [...] :refs ...}};
// the catalog exposes only the flat wire-attr list today, so refs are omitted.
func (h *Handler) handleSchema(w http.ResponseWriter, r *http.Request, a *authedReq) {
	writeJSON(w, http.StatusOK, map[string]any{
		"schema": map[string]any{"attrs": a.cat.WireAttrs()},
	})
}

// handleSoftDeletedAttrs ports soft-deleted-attrs-get: direct SQL over attrs
// WHERE deletion_marked_at IS NOT NULL → {"attrs": [...], "grace-period-days"}.
func (h *Handler) handleSoftDeletedAttrs(w http.ResponseWriter, r *http.Request, a *authedReq) {
	rows, err := h.Pool.Query(r.Context(), `
		SELECT id, etype, label, deletion_marked_at FROM attrs
		WHERE app_id=$1 AND deletion_marked_at IS NOT NULL`, a.appID)
	if err != nil {
		h.logger().Error("adminapi: soft deleted attrs", "err", err)
		writeErr(w, http.StatusInternalServerError, "query failed")
		return
	}
	defer rows.Close()

	attrs := []any{}
	for rows.Next() {
		var id [16]byte
		var etype, label *string
		var markedAt time.Time
		if err := rows.Scan(&id, &etype, &label, &markedAt); err != nil {
			writeErr(w, http.StatusInternalServerError, "scan failed")
			return
		}
		entry := map[string]any{"id": platform.UUIDToStr(id)}
		if etype != nil {
			entry["etype"] = *etype
		}
		if label != nil {
			entry["label"] = *label
		}
		entry["deletion-marked-at"] = markedAt.UTC().Format(time.RFC3339Nano)
		attrs = append(attrs, entry)
	}
	if err := rows.Err(); err != nil {
		writeErr(w, http.StatusInternalServerError, "query failed")
		return
	}
	// sweeper/grace-period-days (hard_deletion_sweeper.clj L17).
	writeJSON(w, http.StatusOK, map[string]any{"attrs": attrs, "grace-period-days": 2})
}

// handlePresence is a stub until Phase 6 wires RoomHub (internal/sync cannot
// be imported from this package). Returns {} for now.
func (h *Handler) handlePresence(w http.ResponseWriter, r *http.Request, a *authedReq) {
	// TODO(Phase 6): return real presence sessions once the orchestrator wires
	// sync.RoomHub into this handler.
	writeJSON(w, http.StatusOK, map[string]any{})
}

// ---- small helpers ---------------------------------------------------------

func orUUID(preferred, fallback [16]byte) [16]byte {
	if preferred != ([16]byte{}) {
		return preferred
	}
	return fallback
}

func newUUID() [16]byte {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return b
}
