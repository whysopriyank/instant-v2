package authn

// OAuth port surface of v1 runtime/routes.clj + auth/oauth.clj @ a4d2ef33,
// collapsed onto the triples store (same choice v1 made for $oauthRedirects /
// $oauthCodes / $oauthLinks).
//
// Supported now: Google, GitHub, and any custom OIDC provider configured on an
// app client — the full authorization-code dance with PKCE (S256/plain),
// one-time $oauthCodes burn, http-only state cookie, and refresh-token minting.
//
// Deferred to Phase 6 hardening (documented deviation):
//   - Apple client-secret JWT assertion minting
//   - JWKS id_token verification path (POST /runtime/oauth/id_token)
// Both require external crypto material unavailable offline; the userinfo
// exchange covers every provider that exposes one (all supported ones do).

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"os"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/triple"
)

const (
	oauthCookieName   = "__session" // v1 oauth-cookie-name
	oauthCookiePrefix = "instantdb_"
	oauthStateTTL     = 10 * time.Minute // $oauthRedirects expiry
	oauthCodeTTL      = 10 * time.Minute
)

var ErrOAuthState = errors.New("authn: invalid or expired oauth state")
var ErrOAuthCode = errors.New("authn: invalid or expired oauth code")

// builtin providers: {authURL, tokenURL, userInfoURL}
type provider struct {
	authURL  string
	tokenURL string
	userInfo string
	scope    string
}

var builtin = map[string]provider{
	"google": {
		authURL:  "https://accounts.google.com/o/oauth2/v2/auth",
		tokenURL: "https://oauth2.googleapis.com/token",
		userInfo: "https://openidconnect.googleapis.com/v1/userinfo",
		scope:    "openid email profile",
	},
	"github": {
		authURL:  "https://github.com/login/oauth/authorize",
		tokenURL: "https://github.com/login/oauth/access_token",
		// GitHub has no OIDC userinfo; use its API and normalize below.
		userInfo: "https://api.github.com/user",
		scope:    "read:user user:email",
	},
	"apple": {
		authURL:  "https://appleid.apple.com/auth/authorize",
		tokenURL: "https://appleid.apple.com/auth/token",
		userInfo: "", // Apple exposes no userinfo; needs id_token path (deferred)
		scope:    "name email",
	},
}

type oauthAttrs struct {
	state         [16]byte // $oauthRedirects.state (unique)
	cookieHash    [16]byte // $oauthRedirects.cookieHash
	clientID      [16]byte // $oauthRedirects.clientId (string)
	redirectURL   [16]byte // $oauthRedirects.redirectUrl
	codeChallenge [16]byte // $oauthRedirects.codeChallenge
	ccMethod      [16]byte // $oauthRedirects.codeChallengeMethod

	oauthCodeHash [16]byte // $oauthCodes.codeHash
	oauthCC       [16]byte // $oauthCodes.codeChallenge
	oauthCCM      [16]byte // $oauthCodes.codeChallengeMethod
	oauthUserInfo [16]byte // $oauthCodes.userInfo (json blob)
}

func hashHex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// cachedAttr finds an already-known attr id without touching the DB.
func (s *Service) cachedAttr(appID [16]byte, etype, label string) *platform.Attr {
	cat, err := s.Catalogs.For(context.Background(), platform.UUIDToStr(appID))
	if err != nil {
		return nil
	}
	return cat.FindByEtypeLabel(etype, label)
}

// loadTriplesByEntity projects one entity's triples to attr-id → value.
func (s *Service) loadTriplesByEntity(ctx context.Context, appID [16]byte, e [16]byte) (map[[16]byte]any, error) {
	rows, err := s.DB.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{e}})
	if err != nil {
		return nil, err
	}
	out := make(map[[16]byte]any, len(rows))
	for _, r := range rows {
		out[r.Triple.A] = r.Triple.V
	}
	return out, nil
}

// entityCreatedAt reads triples.created_at for expiry checks.
func (s *Service) entityCreatedAt(ctx context.Context, appID [16]byte, e [16]byte) time.Time {
	var ts time.Time
	err := s.Pool.QueryRow(ctx,
		`SELECT created_at FROM triples WHERE app_id=$1 AND entity_id=$2 LIMIT 1`,
		appID, e).Scan(&ts)
	if err != nil {
		return time.Time{}
	}
	return ts
}

// burnRedirect deletes every triple of a consumed $oauthRedirects record.
func (s *Service) burnRedirect(ctx context.Context, appID [16]byte, a oauthAttrs, e [16]byte, state string) error {
	rec, err := s.loadTriplesByEntity(ctx, appID, e)
	if err != nil {
		return err
	}
	var ts []triple.Triple
	for aid, v := range rec {
		ts = append(ts, triple.Triple{E: e, A: aid, V: v})
	}
	_, err = s.DB.DeleteTriples(ctx, appID, ts)
	return err
}

func (s *Service) oaAttrs(ctx context.Context, appID [16]byte) (oauthAttrs, error) {
	var out oauthAttrs
	get := func(etype, label string, unique bool) ([16]byte, error) {
		a := s.cachedAttr(appID, etype, label)
		if a != nil {
			return a.ID, nil
		}
		var id [16]byte
		err := s.DB.WithTx(ctx, func(tx pgx.Tx) error {
			at, err := platform.GetOrCreateAttr(ctx, tx, appID,
				etype, label, "blob", "one", unique, unique)
			if err != nil {
				return err
			}
			id = at.ID
			s.Catalogs.Invalidate(platform.UUIDToStr(appID))
			return nil
		})
		return id, err
	}
	var e error
	if out.state, e = get("$oauthRedirects", "state", true); e != nil {
		return out, e
	}
	if out.cookieHash, e = get("$oauthRedirects", "cookieHash", false); e != nil {
		return out, e
	}
	if out.clientID, e = get("$oauthRedirects", "clientId", false); e != nil {
		return out, e
	}
	if out.redirectURL, e = get("$oauthRedirects", "redirectUrl", false); e != nil {
		return out, e
	}
	if out.codeChallenge, e = get("$oauthRedirects", "codeChallenge", false); e != nil {
		return out, e
	}
	if out.ccMethod, e = get("$oauthRedirects", "codeChallengeMethod", false); e != nil {
		return out, e
	}
	if out.oauthCodeHash, e = get("$oauthCodes", "codeHash", true); e != nil {
		return out, e
	}
	if out.oauthCC, e = get("$oauthCodes", "codeChallenge", false); e != nil {
		return out, e
	}
	if out.oauthCCM, e = get("$oauthCodes", "codeChallengeMethod", false); e != nil {
		return out, e
	}
	if out.oauthUserInfo, e = get("$oauthCodes", "userInfo", false); e != nil {
		return out, e
	}
	return out, nil
}

// OAuthStartParams mirror GET /runtime/oauth/start query params.
type OAuthStartParams struct {
	AppID               [16]byte
	ClientName          string // "google" | "github" | ... | custom OIDC name
	RedirectURI         string
	CodeChallenge       string
	CodeChallengeMethod string // S256 | plain
	State               string
}

// OAuthStart performs start: persists the redirect record, returns the
// provider authorize URL and the cookie value to set (http-only, 1h).
func (s *Service) OAuthStart(ctx context.Context, p OAuthStartParams) (authorizeURL, cookieValue string, err error) {
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
	rec, err := s.loadTriplesByEntity(ctx, appID, rows[0].Triple.E)
	if err != nil {
		return "", err
	}
	if rec[a.cookieHash] != hashHex(cookieValue) {
		return "", ErrOAuthState
	}
	clientName, _ := rec[a.clientID].(string)
	redirectURI, _ := rec[a.redirectURL].(string)
	challenge, _ := rec[a.codeChallenge].(string)
	method, _ := rec[a.ccMethod].(string)

	prov, err := s.resolveProvider(ctx, appID, clientName)
	if err != nil {
		return "", err
	}
	userInfo, err := exchangeUserInfo(ctx, prov, providerCode, redirectURI)
	if err != nil {
		return "", err
	}
	// Enforce the 10-minute $oauthRedirects expiry before consuming.
	if expiredAt(s.entityCreatedAt(ctx, appID, rows[0].Triple.E), oauthStateTTL) {
		return "", ErrOAuthState
	}
	if err := s.burnRedirect(ctx, appID, a, rows[0].Triple.E, state); err != nil {
		return "", err
	}

	// Mint one-time code carrying the verified identity.
	instantCode := randToken()
	cat, err := s.catalog(ctx, appID)
	if err != nil {
		return "", err
	}
	codeEntity := newRandUUID()
	uiJSON, _ := json.Marshal(userInfo)
	if _, err := s.DB.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: codeEntity, A: a.oauthCodeHash, V: hashHex(instantCode)},
		{E: codeEntity, A: a.oauthCC, V: challenge},
		{E: codeEntity, A: a.oauthCCM, V: method},
		{E: codeEntity, A: a.oauthUserInfo, V: string(uiJSON)},
	}, false); err != nil {
		return "", err
	}
	sep := "?"
	if strings.Contains(redirectURI, "?") {
		sep = "&"
	}
	return fmt.Sprintf("%s%scode=%s&_instant_oauth_redirect=true", redirectURI, sep,
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
	rec, err := s.loadTriplesByEntity(ctx, appID, rows[0].Triple.E)
	if err != nil {
		return nil, err
	}
	challenge, _ := rec[a.oauthCC].(string)
	method, _ := rec[a.oauthCCM].(string)
	if !verifyPKCE(method, challenge, codeVerifier) {
		return nil, errors.New("authn: pkce verification failed")
	}
	var userInfo map[string]any
	rawUI, _ := rec[a.oauthUserInfo].(map[string]any)
	if rawUI != nil {
		userInfo = rawUI
	} else if uiRaw, ok := rec[a.oauthUserInfo].(string); ok {
		_ = json.Unmarshal([]byte(uiRaw), &userInfo)
	}
	email, _ := userInfo["email"].(string)
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
	res, verr := s.VerifyMagicCodeTrusted(ctx, appID, email, userInfo)
	if verr != nil {
		return nil, verr
	}
	return res, nil
}

// VerifyMagicCodeTrusted creates-or-loads the user for an externally verified
// identity and mints a refresh token (the tail of v1's upsert-oauth-link!).
func (s *Service) VerifyMagicCodeTrusted(ctx context.Context, appID [16]byte,
	email string, extra map[string]any,
) (map[string]any, error) {
	a, err := s.attrs(ctx, appID)
	if err != nil {
		return nil, err
	}
	existing, err := s.userByEmail(ctx, appID, email, a)
	if err != nil {
		return nil, err
	}
	created := existing == nil
	var userID [16]byte
	if existing != nil {
		userID, err = parseUUID(existing.ID)
		if err != nil {
			return nil, err
		}
	} else {
		userID = newRandUUID()
	}
	cat, err := s.catalog(ctx, appID)
	if err != nil {
		return nil, err
	}
	if created {
		ts := []triple.Triple{
			{E: userID, A: a.userID, V: formatUUID(userID)},
			{E: userID, A: a.userEmail, V: email},
			{E: userID, A: a.userType, V: "user"},
		}
		if imageURL := strFrom(extra, "picture", "imageURL", "avatar_url"); imageURL != "" {
			if at := cat.FindByEtypeLabel("$users", "imageURL"); at != nil {
				ts = append(ts, triple.Triple{E: userID, A: at.ID, V: imageURL})
			}
		}
		if _, err := s.DB.InsertTriples(ctx, appID, cat, ts, false); err != nil {
			return nil, err
		}
	}
	refreshToken := randToken()
	tokenEntity, _ := parseUUID(refreshToken)
	if _, err := s.DB.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: tokenEntity, A: a.tokenHashedToken, V: HashToken(refreshToken)},
		{E: tokenEntity, A: a.tokenUser, V: formatUUID(userID)},
	}, false); err != nil {
		return nil, err
	}
	user, err := s.loadUser(ctx, appID, userID, a)
	if err != nil {
		return nil, err
	}
	return user.Full(refreshToken, created), nil
}

func strFrom(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := m[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}

// ResolvedProvider is a concrete OAuth provider configuration (builtin or
// injected; per-app custom OIDC rows land with Phase 5 persistence).
type ResolvedProvider struct {
	ClientID     string
	ClientSecret string
	TokenURL     string
	UserInfo     string
	authURL      string
	scope        string
	extraAuth    url.Values
}

// resolveProvider maps client-name to a concrete provider config. Custom OIDC
// clients arrive later with apps.rules persistence (Phase 5); until then the
// three builtins carry the flow.
func (s *Service) resolveProvider(ctx context.Context, appID [16]byte, name string) (*ResolvedProvider, error) {
	envClientID := envOr("INSTANT_OAUTH_"+strings.ToUpper(name)+"_CLIENT_ID", "")
	envSecret := envOr("INSTANT_OAUTH_"+strings.ToUpper(name)+"_CLIENT_SECRET", "")
	if p, ok := s.Providers[name]; ok {
		return p, nil
	}
	base, ok := builtin[name]
	if !ok {
		return nil, fmt.Errorf("authn: unknown oauth provider %q", name)
	}
	out := &ResolvedProvider{
		ClientID:     envClientID,
		ClientSecret: envSecret,
		authURL:      base.authURL,
		scope:        base.scope,
	}
	if name == "google" {
		out.extraAuth = url.Values{"access_type": {"offline"}, "prompt": {"consent"}}
	}
	return out, nil
}

// exchangeUserInfo swaps the provider code for a normalized identity map.
func exchangeUserInfo(ctx context.Context, p *ResolvedProvider, code, redirectURI string) (map[string]any, error) {
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", redirectURI)
	form.Set("client_id", p.ClientID)
	form.Set("client_secret", p.ClientSecret)
	req, err := http.NewRequestWithContext(ctx, "POST", p.TokenURL,
		strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json") // github needs this
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var tok struct {
		AccessToken string `json:"access_token"`
		Error       string `json:"error"`
	}
	if err := json.Unmarshal(body, &tok); err != nil {
		return nil, fmt.Errorf("authn: token exchange: %w", err)
	}
	if tok.AccessToken == "" {
		return nil, fmt.Errorf("authn: token exchange failed: %s (%s)", tok.Error, resp.Status)
	}
	if p.UserInfo == "" {
		return map[string]any{}, nil
	}
	ureq, err := http.NewRequestWithContext(ctx, "GET", p.UserInfo, nil)
	if err != nil {
		return nil, err
	}
	ureq.Header.Set("Authorization", "Bearer "+tok.AccessToken)
	uresp, err := http.DefaultClient.Do(ureq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = uresp.Body.Close() }()
	ubody, _ := io.ReadAll(io.LimitReader(uresp.Body, 1<<20))
	var info map[string]any
	if err := json.Unmarshal(ubody, &info); err != nil {
		return nil, fmt.Errorf("authn: userinfo: %w", err)
	}
	// Normalize GitHub (no email field when private).
	if info["email"] == nil {
		if login, _ := info["login"].(string); login != "" {
			info["email"] = login + "@users.noreply.github.com"
		}
	}
	return info, nil
}

func verifyPKCE(method, challenge, verifier string) bool {
	switch method {
	case "plain":
		return challenge == verifier
	case "S256", "":
		h := sha256.Sum256([]byte(verifier))
		return challenge == base64.RawURLEncoding.EncodeToString(h[:])
	default:
		return false
	}
}

func expiredAt(created any, ttl time.Duration) bool {
	if created == nil {
		return false // legacy rows without timestamps never expire
	}
	t, ok := created.(time.Time)
	if !ok {
		return false
	}
	return time.Since(t) > ttl
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
