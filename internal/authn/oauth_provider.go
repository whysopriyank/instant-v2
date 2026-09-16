package authn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// builtin providers: {authURL, tokenURL, userInfoURL}
type provider struct {
	authURL  string
	tokenURL string
	userInfo string
	scope    string
	issuer   string
	jwksURL  string
}

var builtin = map[string]provider{
	"google": {
		authURL:  "https://accounts.google.com/o/oauth2/v2/auth",
		tokenURL: "https://oauth2.googleapis.com/token",
		userInfo: "https://openidconnect.googleapis.com/v1/userinfo",
		scope:    "openid email profile",
		issuer:   "https://accounts.google.com",
		jwksURL:  "https://www.googleapis.com/oauth2/v3/certs",
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

// ResolvedProvider is a concrete Google or GitHub authorization-code
// configuration (builtin or test-injected).
type ResolvedProvider struct {
	ClientID        string
	ClientSecret    string
	TokenURL        string
	UserInfo        string
	Issuer          string // OIDC issuer for id_token verification
	JWKSURL         string // JWKS discovery URL
	AcceptedIssuers []string

	authURL   string
	scope     string
	extraAuth url.Values
}

// resolveProvider maps client-name to a concrete provider config. Custom OIDC
// clients arrive later with apps.rules persistence (Phase 5). Google and GitHub
// support the userinfo flow; Apple's separate exchange path remains deferred.
func (s *Service) resolveProvider(ctx context.Context, appID [16]byte, name string) (*ResolvedProvider, error) {
	if name != "google" && name != "github" {
		return nil, fmt.Errorf("authn: oauth provider %q is unsupported; only Google and GitHub authorization-code flows are enabled", name)
	}
	clientID, secret := s.GoogleClientID, s.GoogleClientSecret
	if name == "github" {
		clientID, secret = s.GitHubClientID, s.GitHubClientSecret
	}
	if p, ok := s.Providers[name]; ok {
		if p == nil {
			return nil, fmt.Errorf("authn: oauth provider %q has no configuration", name)
		}
		if err := validateProviderCredentials(name, p.ClientID, p.ClientSecret); err != nil {
			return nil, err
		}
		return p, nil
	}
	base, ok := builtin[name]
	if !ok {
		return nil, fmt.Errorf("authn: unknown oauth provider %q", name)
	}
	out := &ResolvedProvider{
		ClientID:     clientID,
		ClientSecret: secret,
		TokenURL:     base.tokenURL,
		UserInfo:     base.userInfo,
		authURL:      base.authURL,
		scope:        base.scope,
		Issuer:       base.issuer,
		JWKSURL:      base.jwksURL,
	}
	if name == "google" {
		out.AcceptedIssuers = []string{"https://accounts.google.com", "accounts.google.com"}
	}
	if err := validateProviderCredentials(name, out.ClientID, out.ClientSecret); err != nil {
		return nil, err
	}
	if name == "google" {
		out.extraAuth = url.Values{"access_type": {"offline"}, "prompt": {"consent"}}
	}
	return out, nil
}

func validateProviderCredentials(name, clientID, clientSecret string) error {
	if strings.TrimSpace(clientID) == "" || strings.TrimSpace(clientSecret) == "" {
		return fmt.Errorf("authn: oauth provider %q requires non-empty client id and client secret", name)
	}
	return nil
}

// exchangeUserInfo swaps the provider code for a normalized identity map.
func exchangeUserInfo(ctx context.Context, p *ResolvedProvider, code, redirectURI string) (map[string]any, error) {
	return exchangeUserInfoWithNonce(ctx, p, code, redirectURI, "", nil, time.Now)
}

func exchangeUserInfoWithNonce(ctx context.Context, p *ResolvedProvider, code, redirectURI, nonceHash string,
	jwks *jwksCache, now func() time.Time,
) (map[string]any, error) {
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
	resp, err := boundedHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("authn: token exchange failed: %s", resp.Status)
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var tok struct {
		AccessToken string `json:"access_token"`
		IDToken     string `json:"id_token"`
		Error       string `json:"error"`
	}
	if err := json.Unmarshal(body, &tok); err != nil {
		return nil, fmt.Errorf("authn: token exchange: %w", err)
	}
	if tok.AccessToken == "" {
		return nil, fmt.Errorf("authn: token exchange failed: %s (%s)", tok.Error, resp.Status)
	}
	if nonceHash != "" {
		if tok.IDToken == "" {
			return nil, errors.New("authn: Google token exchange returned no id_token")
		}
		if jwks == nil {
			return nil, errors.New("authn: Google token verification unavailable")
		}
		claims, err := verifyIDTokenWithCache(ctx, p, tok.IDToken, nonceHash, jwks, now)
		if err != nil {
			return nil, fmt.Errorf("authn: Google id_token: %w", err)
		}
		return claims, nil
	}
	if p.UserInfo == "" {
		return map[string]any{}, nil
	}
	ureq, err := http.NewRequestWithContext(ctx, "GET", p.UserInfo, nil)
	if err != nil {
		return nil, err
	}
	ureq.Header.Set("Authorization", "Bearer "+tok.AccessToken)
	uresp, err := boundedHTTPClient.Do(ureq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = uresp.Body.Close() }()
	if uresp.StatusCode < http.StatusOK || uresp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("authn: userinfo failed: %s", uresp.Status)
	}
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

// boundedHTTPClient caps IdP exchanges so a hung provider can't pin
// callback goroutines indefinitely (audit F10). Mirrors the JWKS client.
const oauthProviderDeadline = 10 * time.Second

// boundedHTTPClient is a transport-level safety net. OAuthCallback supplies
// the authoritative shared deadline across token, JWKS, and user-info calls.
var boundedHTTPClient = &http.Client{Timeout: oauthProviderDeadline}
