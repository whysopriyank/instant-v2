package authn

// OAuth port surface of v1 runtime/routes.clj + auth/oauth.clj @ a4d2ef33,
// collapsed onto the triples store (same choice v1 made for $oauthRedirects /
// $oauthCodes / $oauthLinks).
//
// Supported now: Google OIDC and GitHub OAuth authorization-code flows with
// PKCE (S256/plain), one-time $oauthCodes burn, http-only state cookie, and
// refresh-token minting. Apple and custom providers are explicit exclusions.
//
// Former deviations, corrected as of the 2026-08-27 drift pass:
//   - JWKS id_token verification for Google's authorization-code response is
//     implemented in idtoken.go: hand-rolled RS256 against provider JWKS,
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
	"github.com/instant-v2/instant-v2/internal/triple"
	"github.com/jackc/pgx/v5"
)

// OAuthStartParams mirror GET /runtime/oauth/start query params.
type OAuthStartParams struct {
	AppID               [16]byte
	ClientName          string // "google" | "github"
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
	return validateRedirectOrigin(ctx, s.Catalogs.RowQ, appID, redirectURI)
}

func validateRedirectOrigin(ctx context.Context, q platform.RowQueryer, appID [16]byte, redirectURI string) error {
	origin, ok := platform.OriginOf(redirectURI)
	if !ok {
		return fmt.Errorf("authn: invalid redirect_uri %q", redirectURI)
	}
	allowed, err := platform.RedirectOrigins(ctx, q, appID)
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
	if p.CodeChallenge == "" || (p.CodeChallengeMethod != "S256" && p.CodeChallengeMethod != "plain") {
		return "", "", errors.New("authn: PKCE requires a non-empty challenge and S256 or plain method")
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
	nonceHash := ""
	if p.ClientName == "google" {
		// Keep only the hash in local state. The hash itself is sent as the
		// provider nonce, so a returned claim can be checked without storing
		// the raw nonce in the database.
		nonceHash = hashHex(randToken())
	}
	cat, err := s.catalog(ctx, p.AppID)
	if err != nil {
		return "", "", err
	}
	stateEntity := newRandUUID()
	stateTriples := []triple.Triple{
		{E: stateEntity, A: a.state, V: p.State},
		{E: stateEntity, A: a.cookieHash, V: hashHex(cookieValue)},
		{E: stateEntity, A: a.clientID, V: p.ClientName},
		{E: stateEntity, A: a.redirectURL, V: p.RedirectURI},
		{E: stateEntity, A: a.codeChallenge, V: p.CodeChallenge},
		{E: stateEntity, A: a.ccMethod, V: p.CodeChallengeMethod},
	}
	if nonceHash != "" {
		stateTriples = append(stateTriples, triple.Triple{E: stateEntity, A: a.nonceHash, V: nonceHash})
	}
	if _, err := s.DB.InsertTriples(ctx, p.AppID, cat, stateTriples, false); err != nil {
		return "", "", err
	}
	q := url.Values{}
	q.Set("client_id", prov.ClientID)
	q.Set("redirect_uri", p.RedirectURI)
	q.Set("response_type", "code")
	q.Set("scope", prov.scope)
	q.Set("state", p.State)
	if nonceHash != "" {
		q.Set("nonce", nonceHash)
	}
	for k, vs := range prov.extraAuth {
		for _, v := range vs {
			q.Set(k, v)
		}
	}
	return prov.authURL + "?" + q.Encode(), cookieValue, nil
}

// OAuthCallback runs callback: validates and burns state in a short local
// transaction, then exchanges the provider code outside that transaction and
// mints a one-time instant code. Provider failure therefore requires a fresh
// authorization start.
func (s *Service) OAuthCallback(ctx context.Context, appID [16]byte,
	state, cookieValue, providerCode string,
) (redirectTarget string, err error) {
	a, err := s.oaAttrs(ctx, appID)
	if err != nil {
		return "", err
	}
	cat, err := s.catalog(ctx, appID)
	if err != nil {
		return "", err
	}
	var record oauthRedirectRecord
	var prov *ResolvedProvider
	err = s.DB.WithTx(ctx, func(tx pgx.Tx) error {
		eid, values, created, err := lockOAuthRecord(ctx, tx, appID, a.state, state, ErrOAuthState)
		if err != nil {
			return err
		}
		record, err = decodeOAuthRedirect(values, a)
		if err != nil {
			return err
		}
		if record.cookieHash != hashHex(cookieValue) || !s.now().Before(created.Add(oauthStateTTL)) {
			return ErrOAuthState
		}
		prov, err = s.resolveProvider(ctx, appID, record.clientName)
		if err != nil {
			return err
		}
		if record.clientName == "google" && record.nonceHash == "" {
			return fmt.Errorf("%w: missing Google nonce binding", ErrOAuthState)
		}
		if err := validateRedirectOrigin(ctx, tx, appID, record.redirectURI); err != nil {
			return err
		}
		var consumed []triple.Triple
		for aid, value := range values {
			consumed = append(consumed, triple.Triple{E: eid, A: aid, V: value})
		}
		n, err := s.DB.DeleteTx(ctx, tx, appID, consumed)
		if err != nil {
			return err
		}
		if n != int64(len(consumed)) {
			return ErrOAuthState
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	providerCtx, cancel := context.WithTimeout(ctx, oauthProviderDeadline)
	userInfo, err := exchangeUserInfoWithNonce(providerCtx, prov, providerCode, record.redirectURI,
		record.nonceHash, &s.jwks, s.now)
	cancel()
	if err != nil {
		return "", err
	}
	uiJSON, err := json.Marshal(userInfo)
	if err != nil {
		return "", fmt.Errorf("authn: encode oauth userInfo: %w", err)
	}
	instantCode := randToken()
	codeEntity := newRandUUID()
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
	var record oauthCodeRecord
	var email string
	err = s.DB.WithTx(ctx, func(tx pgx.Tx) error {
		eid, values, created, err := lockOAuthRecord(ctx, tx, appID, a.oauthCodeHash, hashHex(instantCode), ErrOAuthCode)
		if err != nil {
			return err
		}
		record, err = decodeOAuthCode(values, a)
		if err != nil {
			return err
		}
		if !s.now().Before(created.Add(oauthCodeTTL)) {
			return ErrOAuthCode
		}
		if !verifyPKCE(record.method, record.challenge, codeVerifier) {
			return errors.New("authn: pkce verification failed")
		}
		email, _ = record.userInfo["email"].(string)
		if email == "" {
			return errors.New("authn: provider returned no email")
		}
		n, err := s.DB.DeleteTx(ctx, tx, appID, []triple.Triple{{E: eid, A: a.oauthCodeHash, V: hashHex(instantCode)}})
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrOAuthCode
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Preserve burn-before-issuance: a valid code is single-use even when
	// the following user creation or refresh-token issuance fails.
	// Upsert user through the same machinery magic codes use.
	res, verr := s.VerifyMagicCodeTrusted(ctx, appID, email, record.userInfo)
	if verr != nil {
		return nil, verr
	}
	return res, nil
}
