package authn

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// Handler mounts the /runtime/auth/* routes with v1's exact wire shapes
// (runtime/routes.clj @ a4d2ef33):
//
//	POST /runtime/auth/send_magic_code       {email, app-id}            → {"sent":true}
//	POST /runtime/auth/verify_magic_code     {email, code, app-id,
//	                                          extra-fields?, refresh-token?} → {"user":{...,"refresh_token"},"created":bool}
//	POST /runtime/auth/sign_in_guest         {app-id, extra-fields?}    → {"user":{...,"refresh_token"}}
//	POST /runtime/auth/verify_refresh_token  {refresh-token, app-id}    → {"user":{...,"refresh_token"}}
//	POST /runtime/auth/sign_out              {app_id, refresh_token}    → {}   // snake_case quirk preserved
type Handler struct {
	Service *Service
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/runtime/auth/send_magic_code":
		h.sendMagicCode(w, r)
	case "/runtime/auth/verify_magic_code":
		h.verifyMagicCode(w, r)
	case "/runtime/auth/sign_in_guest":
		h.signInGuest(w, r)
	case "/runtime/auth/verify_refresh_token":
		h.verifyRefreshToken(w, r)
	case "/runtime/auth/sign_out":
		h.signOut(w, r)
	case "/runtime/oauth/start":
		h.oauthStart(w, r)
	case "/runtime/oauth/callback":
		h.oauthCallback(w, r)
	case "/runtime/oauth/token":
		h.oauthToken(w, r)
	case "/runtime/oauth/id_token":
		h.oauthIDToken(w, r)
	default:
		http.NotFound(w, r)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
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
func (h *Handler) parseAppID(m map[string]any) ([16]byte, error) {
	for _, k := range []string{"app-id", "app_id"} {
		if s := str(m, k); s != "" {
			return platform.ScanUUIDErr(s)
		}
	}
	return [16]byte{}, errBadAppID
}

var errBadAppID = errStr("missing or invalid app-id")

type errStr string

func (e errStr) Error() string { return string(e) }

func (h *Handler) sendMagicCode(w http.ResponseWriter, r *http.Request) {
	m := readBody(r)
	email := strings.TrimSpace(str(m, "email"))
	appID, err := h.parseAppID(m)
	if err != nil || email == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": "email and app-id are required"})
		return
	}
	if err := h.Service.SendMagicCode(r.Context(), appID, email); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"message": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sent": true})
}

func (h *Handler) verifyMagicCode(w http.ResponseWriter, r *http.Request) {
	m := readBody(r)
	email := strings.TrimSpace(str(m, "email"))
	code := strings.TrimSpace(str(m, "code"))
	appID, err := h.parseAppID(m)
	if err != nil || email == "" || code == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": "email, code and app-id are required"})
		return
	}
	extra, _ := m["extra-fields"].(map[string]any)
	res, err := h.Service.VerifyMagicCode(context.Background(), appID,
		email, code, str(m, "refresh-token"), extra, false)
	if err != nil {
		status := http.StatusUnauthorized
		switch err {
		case ErrSignupDenied:
			status = http.StatusForbidden
		case ErrExpiredCode:
			status = http.StatusUnauthorized
		case ErrLocked:
			status = http.StatusTooManyRequests
		}
		writeJSON(w, status, map[string]any{"message": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (h *Handler) signInGuest(w http.ResponseWriter, r *http.Request) {
	m := readBody(r)
	appID, err := h.parseAppID(m)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	extra, _ := m["extra-fields"].(map[string]any)
	res, err := h.Service.SignInGuest(r.Context(), appID, extra)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"message": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (h *Handler) verifyRefreshToken(w http.ResponseWriter, r *http.Request) {
	m := readBody(r)
	token := str(m, "refresh-token")
	appID, err := h.parseAppID(m)
	if err != nil || token == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": "refresh-token and app-id are required"})
		return
	}
	user, err := h.Service.VerifyRefreshToken(r.Context(), appID, token)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"message": err.Error()})
		return
	}
	full := user.Full(token, false)["user"]
	writeJSON(w, http.StatusOK, map[string]any{"user": full})
}

func (h *Handler) signOut(w http.ResponseWriter, r *http.Request) {
	m := readBody(r)
	// v1 signout-post reads :refresh_token / :app_id (snake_case).
	token := str(m, "refresh_token")
	if token == "" {
		token = str(m, "refresh-token")
	}
	appID, err := h.parseAppID(m)
	if err != nil || token == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": "refresh_token and app_id are required"})
		return
	}
	if err := h.Service.SignOut(r.Context(), appID, token); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"message": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

// --- OAuth endpoints (v1 runtime/routes.clj shapes) ---

func (h *Handler) oauthStart(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	appIDStr := q.Get("app_id")
	if appIDStr == "" {
		appIDStr = q.Get("app-id")
	}
	appID, err := platform.ScanUUIDErr(appIDStr)
	if err != nil {
		http.Error(w, "missing app_id", 400)
		return
	}
	clientName := q.Get("client_name")
	if clientName == "" {
		clientName = q.Get("client_id")
	}
	authURL, cookieValue, err := h.Service.OAuthStart(r.Context(), OAuthStartParams{
		AppID:               appID,
		ClientName:          clientName,
		RedirectURI:         q.Get("redirect_uri"),
		CodeChallenge:       q.Get("code_challenge"),
		CodeChallengeMethod: q.Get("code_challenge_method"),
		State:               q.Get("state"),
	})
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	http.SetCookie(w, &http.Cookie{
		Name:     oauthCookieName,
		Value:    cookieValue,
		Path:     "/runtime/oauth",
		HttpOnly: true,
		Secure:   scheme == "https",
		SameSite: http.SameSiteLaxMode,
		MaxAge:   3600, // v1: 1h
	})
	http.Redirect(w, r, authURL, http.StatusFound)
}

func (h *Handler) oauthCallback(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	appIDStr := q.Get("app_id")
	if appIDStr == "" {
		appIDStr = q.Get("app-id")
	}
	// v1 packs app-id into the first 36 chars of the 72-char state.
	state := q.Get("state")
	if len(state) == 72 && appIDStr == "" {
		appIDStr = state[:36]
	}
	appID, err := platform.ScanUUIDErr(appIDStr)
	if err != nil {
		http.Error(w, "missing app id", 400)
		return
	}
	var cookieValue string
	if c, err := r.Cookie(oauthCookieName); err == nil {
		// v1 throws away anything without the prefix; the hash covers the
		// FULL prefixed value.
		cookieValue = c.Value
		if !strings.HasPrefix(cookieValue, oauthCookiePrefix) {
			cookieValue = ""
		}
	}
	// v1 state = app-id(36) + client-state(36) = 72 chars.
	oauthState := state
	if len(state) == 72 {
		oauthState = state[36:]
	}
	target, err := h.Service.OAuthCallback(r.Context(), appID, oauthState, cookieValue, q.Get("code"))
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	http.Redirect(w, r, target, http.StatusFound)
}

func (h *Handler) oauthToken(w http.ResponseWriter, r *http.Request) {
	m := readBody(r)
	appID, err := h.parseAppID(m)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	res, err := h.Service.OAuthToken(r.Context(), appID,
		str(m, "code"), str(m, "code_verifier"))
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"message": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// oauthIDToken ports oauth-id-token-callback: the client presents a
// third-party id_token directly; we verify it against the provider JWKS.
func (h *Handler) oauthIDToken(w http.ResponseWriter, r *http.Request) {
	m := readBody(r)
	appID, err := h.parseAppID(m)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	clientName := str(m, "client_name")
	if clientName == "" {
		clientName = "google"
	}
	res, err := h.Service.IDTokenSignIn(r.Context(), appID,
		clientName, str(m, "id_token"), str(m, "nonce"))
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"message": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, res)
}
