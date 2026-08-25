// Package runtimeapi serves the end-user REST plane endpoints that
// internal/authn's Handler does not already cover, ported from v1
// server/src/instant/runtime/routes.clj @ a4d2ef33:
//
//	POST /runtime/auth/refresh_tokens                 {refresh_tokens: [uuid…], app-id}
//	                                                  → {"users":[{…,"refresh_token"}…]}
//	POST /runtime/signout                             {app_id, refresh_token} → {}
//	                                                  (snake_case quirk preserved, L161-165)
//	POST /runtime/framework/query                     {"query": {…instaql…}} (L726)
//	GET  /runtime/:app_id/.well-known/openid-configuration (L718)
//	GET  /runtime/openid-configuration                convenience alias
//
// /runtime/auth/{send_magic_code,verify_magic_code,sign_out,
// verify_refresh_token,sign_in_guest} and /runtime/oauth/* are served by
// internal/authn.Handler and intentionally NOT reimplemented here.
package runtimeapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/instant-v2/instant-v2/internal/authn"
	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
)

// Handler mounts the remaining runtime REST routes.
type Handler struct {
	Pool     *pgxpool.Pool
	DB       *storage.DB
	Catalogs *platform.CatalogCache
	Auth     *authn.Service
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/runtime/auth/refresh_tokens":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		h.refreshTokens(w, r)
	case "/runtime/signout":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		h.signout(w, r)
	case "/runtime/framework/query":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		h.frameworkQuery(w, r)
	case "/runtime/openid-configuration":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		h.openIDConfiguration(w, r, "")
	default:
		// GET /runtime/:app_id/.well-known/openid-configuration (routes.clj L776).
		const suffix = "/.well-known/openid-configuration"
		if strings.HasPrefix(r.URL.Path, "/runtime/") && strings.HasSuffix(r.URL.Path, suffix) && r.Method == http.MethodGet {
			appID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/runtime/"), suffix)
			h.openIDConfiguration(w, r, appID)
			return
		}
		http.NotFound(w, r)
	}
}

func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"message": "method not allowed"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func readBody(r *http.Request) map[string]any {
	var m map[string]any
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&m)
	}
	return m
}

func str(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

// parseAppID accepts both "app-id" and legacy "app_id" keys.
func parseAppID(m map[string]any) ([16]byte, bool) {
	for _, k := range []string{"app-id", "app_id"} {
		if s := str(m, k); s != "" {
			if id, err := platform.ScanUUIDErr(s); err == nil {
				return id, true
			}
		}
	}
	return [16]byte{}, false
}

// refreshTokens resolves a batch of raw refresh tokens to their users.
// Wire contract pinned to v1's user projection (routes.clj L154-159:
// {:user (assoc user :refresh_token refresh-token)}), batched under "users":
//
//	POST /runtime/auth/refresh_tokens
//	{"refresh_tokens": ["<uuid>", …], "app-id": "<uuid>"}
//	→ {"users": [{"id":…,"email":…,"type":…,…,"refresh_token":"<uuid>"}…]}
//
// Any unresolvable token fails the whole batch with 401 {"message": …},
// mirroring v1's single-token verify (get-by-refresh-token! throws).
func (h *Handler) refreshTokens(w http.ResponseWriter, r *http.Request) {
	m := readBody(r)
	appID, ok := parseAppID(m)
	raws, _ := m["refresh_tokens"].([]any)
	if !ok || len(raws) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": "refresh_tokens and app-id are required"})
		return
	}
	tokens := make([]string, 0, len(raws))
	valid := true
	for _, raw := range raws {
		s, _ := raw.(string)
		if s == "" {
			valid = false
			break
		}
		tokens = append(tokens, s)
	}
	if !valid {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": "refresh_tokens must be a non-empty list of uuids"})
		return
	}
	users := make([]map[string]any, 0, len(tokens))
	for _, tok := range tokens {
		user, err := h.Auth.VerifyRefreshToken(r.Context(), appID, tok)
		if err != nil {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"message": err.Error()})
			return
		}
		users = append(users, user.Full(tok, false)["user"].(map[string]any))
	}
	writeJSON(w, http.StatusOK, map[string]any{"users": users})
}

// signout ports signout-post (routes.clj L161-165): snake_case body keys and
// an empty ok envelope — distinct from authn's /runtime/auth/sign_out only in
// path; kept here because clients hit both spellings.
func (h *Handler) signout(w http.ResponseWriter, r *http.Request) {
	m := readBody(r)
	token := str(m, "refresh_token")
	if token == "" { // tolerate kebab-case like the sibling endpoint
		token = str(m, "refresh-token")
	}
	appID, ok := parseAppID(m)
	if !ok || token == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": "refresh_token and app_id are required"})
		return
	}
	if err := h.Auth.SignOut(r.Context(), appID, token); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"message": platform.ClientMessage(err)})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

// frameworkQuery ports framework-query-triples (routes.clj L726-743):
//
//	POST /runtime/framework/query
//	headers: app-id: <uuid>            (or ?app_id=<uuid>, req->app-id-untrusted!)
//	body:    {"query": {…instaql…}, "refresh-token": "<uuid>"?}
//	→ {"data": {etype: […]}} (+ "page-info"/"aggregate" when requested)
//
// The optional refresh-token resolves the caller like v1's
// get-by-refresh-token (non-bang): an unknown token degrades to anonymous —
// it never fails the request. The app's view rules gate execution: closed
// etypes return empty results; dynamic rules are refused until rule-where
// pushdown exists (fail-closed — never serve unfiltered).
func (h *Handler) frameworkQuery(w http.ResponseWriter, r *http.Request) {
	m := readBody(r)
	query, _ := m["query"].(map[string]any)
	if query == nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": "query is required"})
		return
	}
	appStr := r.Header.Get("app-id")
	if appStr == "" {
		appStr = r.URL.Query().Get("app_id")
	}
	appID, err := platform.ScanUUIDErr(appStr)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": "missing or invalid app-id"})
		return
	}
	var authUser map[string]any
	if tok := str(m, "refresh-token"); tok != "" {
		// Anonymous-on-failure mirrors v1's non-bang get-by-refresh-token.
		if user, verr := h.Auth.VerifyRefreshToken(r.Context(), appID, tok); verr == nil && user != nil {
			authUser = map[string]any{"id": user.ID}
			if user.Email != "" {
				authUser["email"] = user.Email
			}
			if user.Type != "" {
				authUser["type"] = user.Type
			}
			for k, v := range user.Extra {
				authUser[k] = v
			}
		}
	}
	doc, derr := h.Catalogs.RuleDocFor(r.Context(), platform.UUIDToStr(appID))
	if derr != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"message": "internal error"})
		return
	}
	cat, err := h.Catalogs.For(r.Context(), platform.UUIDToStr(appID))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"message": platform.ClientMessage(err)})
		return
	}
	q, err := instaql.Coerce(query)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	ex := &instaql.Executor{DB: h.Pool, Rules: doc, Auth: authUser}
	res, err := ex.Run(r.Context(), q, cat, appID)
	if err != nil {
		var unsupported *instaql.ErrRuleFilterUnsupported
		if errors.As(err, &unsupported) {
			writeJSON(w, http.StatusForbidden, map[string]any{"message": err.Error()})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"message": platform.ClientMessage(err)})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// openIDConfiguration ports openid-configuration-get (routes.clj L718-724):
// {"authorization_endpoint": "<origin>/runtime/<app>/oauth/start",
//
//	"token_endpoint":         "<origin>/runtime/<app>/oauth/token"}.
//
// Origin comes from the Host header over http (self-hosted/local installs);
// appID may arrive in the path (:app_id) or, for the alias route, the
// app-id header / app_id query parameter.
func (h *Handler) openIDConfiguration(w http.ResponseWriter, r *http.Request, appIDInPath string) {
	appStr := appIDInPath
	if appStr == "" {
		appStr = r.Header.Get("app-id")
		if appStr == "" {
			appStr = r.URL.Query().Get("app_id")
		}
	}
	if _, err := platform.ScanUUIDErr(appStr); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": "missing or invalid app-id"})
		return
	}
	// Sanitize the reflected host: only scheme-less authority material with
	// sane characters may ride into discovery URLs (defends naive proxies
	// against Host-header poisoning of OAuth endpoint discovery).
	host := r.Host
	if _, ok := platform.OriginOf("http://" + host); !ok {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": "invalid Host header"})
		return
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	origin := scheme + "://" + host
	writeJSON(w, http.StatusOK, map[string]any{
		"authorization_endpoint": origin + "/runtime/" + appStr + "/oauth/start",
		"token_endpoint":         origin + "/runtime/" + appStr + "/oauth/token",
	})
}
