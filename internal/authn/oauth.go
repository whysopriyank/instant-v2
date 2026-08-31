package authn

// OAuth port surface of v1 runtime/routes.clj + auth/oauth.clj @ a4d2ef33,
// collapsed onto the triples store (same choice v1 made for $oauthRedirects /
// $oauthCodes / $oauthLinks).
//
// Supported now: Google, GitHub, and any custom OIDC provider configured on an
// app client — the full authorization-code dance with PKCE (S256/plain),
// one-time $oauthCodes burn, http-only state cookie, and refresh-token minting.
//
// Former deviations, corrected as of the 2026-08-27 drift pass:
//   - JWKS id_token verification (POST /runtime/oauth/id_token) IS implemented
//     in idtoken.go: hand-rolled RS256/ES256 against provider JWKS, alg-pinned,
//     sig-before-claims, mandatory iss/aud/exp/nonce handling.
//   - Apple ES256 assertion signing material ships (AppleSigner/.p8 loading in
//     idtoken.go), but end-to-end Apple token-exchange wiring remains deferred
//     — resolveProvider has no Apple client_assertion plumbing yet; Google/
//     GitHub/custom-OIDC run through the userinfo exchange today.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/triple"
)

// OAuthStartParams mirror GET /runtime/oauth/start query params.
type OAuthStartParams struct {
	AppID               [16]byte
	ClientName          string // "google" | "github" | ... | custom OIDC name
	RedirectURI         string
	CodeChallenge       string
	CodeChallengeMethod string // S256 | plain
	State               string
}

// validateRedirectOrigin ensures the caller-supplied redirect_uri is
// registered on the app. An empty allowlist permits nothing (fail-closed):
// this is what stops the phishing chain where an attacker starts an OAuth
// flow with their own redirect target and harvests the minted instant code.
func (s *Service) validateRedirectOrigin(ctx context.Context, appID [16]byte, redirectURI string) error {
	origin, ok := platform.OriginOf(redirectURI)
	if !ok {
		return fmt.Errorf("authn: invalid redirect_uri %q", redirectURI)
	}
	allowed, err := platform.RedirectOrigins(ctx, s.Catalogs.RowQ, appID)
	if err != nil {
		return fmt.Errorf("authn: redirect origins load: %w", err)
	}
	for _, a := range allowed {
		if a == origin {
			return nil
		}
	}
	return fmt.Errorf("authn: redirect_uri origin %q is not registered for this app", origin)
}

// OAuthStart performs start: persists the redirect record, returns the
// provider authorize URL and the cookie value to set (http-only, 1h).
func (s *Service) OAuthStart(ctx context.Context, p OAuthStartParams) (authorizeURL, cookieValue string, err error) {
	if err := s.validateRedirectOrigin(ctx, p.AppID, p.RedirectURI); err != nil {
		return "", "", err
	}
	if p.State == "" {
		p.State = randToken()
	}
	if p.CodeChallengeMethod == "" {
		p.CodeChallengeMethod = "S256"
	}
	prov, err := s.resolveProvider(ctx, p.AppID, p.ClientName)
	if err != nil {
		return "", "", err
	}
	a, err := s.oaAttrs(ctx, p.AppID)
	if err != nil {
		return "", "", err
	}
	cookieValue = oauthCookiePrefix + randToken()
	cat, err := s.catalog(ctx, p.AppID)
	if err != nil {
		return "", "", err
	}
	stateEntity := newRandUUID()
	if _, err := s.DB.InsertTriples(ctx, p.AppID, cat, []triple.Triple{
		{E: stateEntity, A: a.state, V: p.State},
		{E: stateEntity, A: a.cookieHash, V: hashHex(cookieValue)},
		{E: stateEntity, A: a.clientID, V: p.ClientName},
		{E: stateEntity, A: a.redirectURL, V: p.RedirectURI},
		{E: stateEntity, A: a.codeChallenge, V: p.CodeChallenge},
		{E: stateEntity, A: a.ccMethod, V: p.CodeChallengeMethod},
	}, false); err != nil {
		return "", "", err
	}
	q := url.Values{}
	q.Set("client_id", prov.ClientID)
	q.Set("redirect_uri", p.RedirectURI)
	q.Set("response_type", "code")
	q.Set("scope", prov.scope)
	q.Set("state", p.State)
	for k, vs := range prov.extraAuth {
		for _, v := range vs {
			q.Set(k, v)
		}
	}
	return prov.authURL + "?" + q.Encode(), cookieValue, nil
}

// OAuthCallback runs callback: validates state+cookie, exchanges the provider
// code for userinfo, mints a one-time instant code, returns the app redirect
// target (`redirect_uri?code=...&_instant_oauth_redirect=true`).
func (s *Service) OAuthCallback(ctx context.Context, appID [16]byte,
	state, cookieValue, providerCode string,
) (redirectTarget string, err error) {
	a, err := s.oaAttrs(ctx, appID)
	if err != nil {
		return "", err
	}
	rows, err := s.DB.FetchTriples(ctx, appID, storage.FetchFilter{
		AttrIDs: [][16]byte{a.state},
		Value:   state,
	})
	if err != nil || len(rows) == 0 {
		return "", ErrOAuthState
	}
	values, err := s.loadTriplesByEntity(ctx, appID, rows[0].Triple.E)
	if err != nil {
		return "", err
	}
	record, err := decodeOAuthRedirect(values, a)
	if err != nil {
		return "", err
	}
	if record.cookieHash != hashHex(cookieValue) {
		return "", ErrOAuthState
	}

	prov, err := s.resolveProvider(ctx, appID, record.clientName)
	if err != nil {
		return "", err
	}
	// Defense in depth: the stored redirectURI was validated at start; the
	// origin check runs again before anything is redirected there.
	if err := s.validateRedirectOrigin(ctx, appID, record.redirectURI); err != nil {
		return "", err
	}
	userInfo, err := exchangeUserInfo(ctx, prov, providerCode, record.redirectURI)
	if err != nil {
		return "", err
	}
	// Enforce the 10-minute $oauthRedirects expiry before consuming.
	if expiredAt(s.entityCreatedAt(ctx, appID, rows[0].Triple.E), oauthStateTTL) {
		return "", ErrOAuthState
	}
	if err := s.burnRedirect(ctx, appID, rows[0].Triple.E); err != nil {
		return "", err
	}

	// Mint one-time code carrying the verified identity.
	instantCode := randToken()
	cat, err := s.catalog(ctx, appID)
	if err != nil {
		return "", err
	}
	codeEntity := newRandUUID()
	uiJSON, err := json.Marshal(userInfo)
	if err != nil {
		return "", fmt.Errorf("authn: encode oauth userInfo: %w", err)
	}
	if _, err := s.DB.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: codeEntity, A: a.oauthCodeHash, V: hashHex(instantCode)},
		{E: codeEntity, A: a.oauthCC, V: record.challenge},
		{E: codeEntity, A: a.oauthCCM, V: record.method},
		{E: codeEntity, A: a.oauthUserInfo, V: string(uiJSON)},
	}, false); err != nil {
		return "", err
	}
	sep := "?"
	if strings.Contains(record.redirectURI, "?") {
		sep = "&"
	}
	return fmt.Sprintf("%s%scode=%s&_instant_oauth_redirect=true", record.redirectURI, sep,
		url.QueryEscape(instantCode)), nil
}

// OAuthToken runs POST /runtime/oauth/token: burns the instant code, verifies
// PKCE, upserts the user by email/sub, mints a refresh token.
func (s *Service) OAuthToken(ctx context.Context, appID [16]byte,
	instantCode, codeVerifier string,
) (map[string]any, error) {
	a, err := s.oaAttrs(ctx, appID)
	if err != nil {
		return nil, err
	}
	rows, err := s.DB.FetchTriples(ctx, appID, storage.FetchFilter{
		AttrIDs: [][16]byte{a.oauthCodeHash},
		Value:   hashHex(instantCode),
	})
	if err != nil || len(rows) == 0 {
		return nil, ErrOAuthCode
	}
	values, err := s.loadTriplesByEntity(ctx, appID, rows[0].Triple.E)
	if err != nil {
		return nil, err
	}
	record, err := decodeOAuthCode(values, a)
	if err != nil {
		return nil, err
	}
	if !verifyPKCE(record.method, record.challenge, codeVerifier) {
		return nil, errors.New("authn: pkce verification failed")
	}
	email, _ := record.userInfo["email"].(string)
	if email == "" {
		return nil, errors.New("authn: provider returned no email")
	}
	// Burn the one-time code first (single-use regardless of what follows).
	if _, err := s.DB.DeleteTriples(ctx, appID, []triple.Triple{
		{E: rows[0].Triple.E, A: a.oauthCodeHash, V: hashHex(instantCode)},
	}); err != nil {
		return nil, err
	}
	// Upsert user through the same machinery magic codes use.
	res, verr := s.VerifyMagicCodeTrusted(ctx, appID, email, record.userInfo)
	if verr != nil {
		return nil, verr
	}
	return res, nil
}
