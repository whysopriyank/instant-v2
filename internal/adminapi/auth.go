package adminapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/instant-v2/instant-v2/internal/authn"
	"github.com/instant-v2/instant-v2/internal/platform"
)

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
// Mailer hook's job, and a nil Mailer means NO delivery anywhere — authn logs
// only an app-id/email notice, never the code itself. The captureMailer below
// recovers the generated code purely so the response envelope matches v1.
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
