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
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/instant-v2/instant-v2/internal/authn"
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
