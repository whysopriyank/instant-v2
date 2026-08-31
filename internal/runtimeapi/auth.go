package runtimeapi

import (
	"net/http"

	"github.com/instant-v2/instant-v2/internal/platform"
)

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
	req, err := readRefreshTokens(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	users := make([]map[string]any, 0, len(req.Tokens))
	for _, tok := range req.Tokens {
		user, err := h.Auth.VerifyRefreshToken(r.Context(), req.AppID, tok)
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
	req, err := readSignout(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	if err := h.Auth.SignOut(r.Context(), req.AppID, req.Token); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"message": platform.ClientMessage(err)})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}
