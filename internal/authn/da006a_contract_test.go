package authn_test

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/authn"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
)

func TestDA006AOAuthStateIsCommittedBeforeProviderIO(t *testing.T) {
	svc, _, appID, cleanup := env(t)
	defer cleanup()

	var stateRows atomic.Int64
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/token" {
			_, _ = w.Write([]byte(`{"email":"oauth@example.com"}`))
			return
		}
		var count int64
		if err := svc.Pool.QueryRow(r.Context(), `
			SELECT count(*)
			  FROM triples t
			  JOIN attrs a ON a.app_id=t.app_id AND a.id=t.attr_id
			 WHERE t.app_id=$1 AND a.etype='$oauthRedirects' AND a.label='state'
			   AND t.value=$2::jsonb`, appID, `"claim-before-provider"`).Scan(&count); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		stateRows.Store(count)
		_, _ = w.Write([]byte(`{"access_token":"access"}`))
	}))
	defer provider.Close()
	svc.Providers = map[string]*authn.ResolvedProvider{
		"github": {
			ClientID: "client", ClientSecret: "secret",
			TokenURL: provider.URL + "/token", UserInfo: provider.URL + "/userinfo",
		},
	}

	_, cookie, err := svc.OAuthStart(context.Background(), authn.OAuthStartParams{
		AppID: appID, ClientName: "github", RedirectURI: "http://app/cb",
		State: "claim-before-provider", CodeChallenge: "verifier", CodeChallengeMethod: "plain",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.OAuthCallback(context.Background(), appID, "claim-before-provider", cookie, "provider-code"); err != nil {
		t.Fatal(err)
	}
	if got := stateRows.Load(); got != 0 {
		t.Fatalf("provider observed %d persisted state rows; want committed burn before I/O", got)
	}
}

func TestDA006AOAuthStartRequiresPKCE(t *testing.T) {
	svc, _, appID, cleanup := env(t)
	defer cleanup()
	for _, params := range []authn.OAuthStartParams{
		{AppID: appID, ClientName: "github", RedirectURI: "http://app/cb", State: "missing-challenge", CodeChallengeMethod: "plain"},
		{AppID: appID, ClientName: "github", RedirectURI: "http://app/cb", State: "bad-method", CodeChallenge: "challenge", CodeChallengeMethod: "unknown"},
	} {
		if _, _, err := svc.OAuthStart(context.Background(), params); err == nil || !strings.Contains(err.Error(), "PKCE") {
			t.Fatalf("OAuthStart accepted invalid PKCE %+v: %v", params, err)
		}
	}
}

func TestDA006AConcurrentCallbacksHaveOneClaimantAndExchange(t *testing.T) {
	svc, _, appID, cleanup := env(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	providerStarted := make(chan struct{})
	releaseProvider := make(chan struct{})
	var tokenCalls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			tokenCalls.Add(1)
			close(providerStarted)
			<-releaseProvider
			_, _ = w.Write([]byte(`{"access_token":"access"}`))
			return
		}
		tokenCalls.Add(1)
		_, _ = w.Write([]byte(`{"email":"oauth@example.com"}`))
	}))
	defer provider.Close()
	svc.Providers = map[string]*authn.ResolvedProvider{
		"github": {ClientID: "client", ClientSecret: "secret", TokenURL: provider.URL + "/token", UserInfo: provider.URL + "/userinfo"},
	}
	_, cookie, err := svc.OAuthStart(ctx, authn.OAuthStartParams{
		AppID: appID, ClientName: "github", RedirectURI: "http://app/cb",
		State: "concurrent-claim", CodeChallenge: "verifier", CodeChallengeMethod: "plain",
	})
	if err != nil {
		t.Fatal(err)
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := svc.OAuthCallback(ctx, appID, "concurrent-claim", cookie, "provider-code")
		firstDone <- err
	}()
	select {
	case <-providerStarted:
	case <-ctx.Done():
		t.Fatal("first callback did not reach provider")
	}

	secondCtx, secondCancel := context.WithTimeout(ctx, time.Second)
	defer secondCancel()
	secondDone := make(chan error, 1)
	go func() {
		_, err := svc.OAuthCallback(secondCtx, appID, "concurrent-claim", cookie, "provider-code")
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		if !errors.Is(err, authn.ErrOAuthState) {
			t.Fatalf("losing callback error = %v, want ErrOAuthState", err)
		}
	case <-time.After(250 * time.Millisecond):
		close(releaseProvider)
		t.Fatal("losing callback remained behind provider I/O; state was not committed before exchange")
	}
	close(releaseProvider)
	if err := <-firstDone; err != nil {
		t.Fatalf("winning callback: %v", err)
	}
	if got := tokenCalls.Load(); got != 2 {
		t.Fatalf("provider requests = %d, want one token and one userinfo request", got)
	}
}

func TestDA006AProviderFailureRequiresRestart(t *testing.T) {
	svc, _, appID, cleanup := env(t)
	defer cleanup()
	var fail atomic.Bool
	fail.Store(true)
	var tokenCalls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			tokenCalls.Add(1)
			if fail.Load() {
				http.Error(w, `{"error":"temporarily unavailable"}`, http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte(`{"access_token":"access"}`))
			return
		}
		_, _ = w.Write([]byte(`{"email":"oauth@example.com"}`))
	}))
	defer provider.Close()
	svc.Providers = map[string]*authn.ResolvedProvider{
		"github": {ClientID: "client", ClientSecret: "secret", TokenURL: provider.URL + "/token", UserInfo: provider.URL + "/userinfo"},
	}
	_, cookie, err := svc.OAuthStart(context.Background(), authn.OAuthStartParams{
		AppID: appID, ClientName: "github", RedirectURI: "http://app/cb",
		State: "provider-failure", CodeChallenge: "verifier", CodeChallengeMethod: "plain",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.OAuthCallback(context.Background(), appID, "provider-failure", cookie, "provider-code"); err == nil {
		t.Fatal("provider failure unexpectedly succeeded")
	}
	fail.Store(false)
	if _, err := svc.OAuthCallback(context.Background(), appID, "provider-failure", cookie, "provider-code"); !errors.Is(err, authn.ErrOAuthState) {
		t.Fatalf("retry after provider failure = %v, want ErrOAuthState/restart required", err)
	}
	if got := tokenCalls.Load(); got != 1 {
		t.Fatalf("provider token calls = %d, want one attempt", got)
	}
}

func TestDA006AGoogleNonceIsHashedPersistedAndRequired(t *testing.T) {
	svc, _, appID, cleanup := env(t)
	defer cleanup()
	var tokenCalls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenCalls.Add(1)
		if r.URL.Path == "/token" {
			_, _ = w.Write([]byte(`{"access_token":"access","id_token":"not-a-valid-bound-token"}`))
			return
		}
		_, _ = w.Write([]byte(`{"email":"oauth@example.com"}`))
	}))
	defer provider.Close()
	svc.Providers = map[string]*authn.ResolvedProvider{
		"google": {
			ClientID: "client", ClientSecret: "secret",
			TokenURL: provider.URL + "/token", UserInfo: provider.URL + "/userinfo",
		},
	}
	authURL, cookie, err := svc.OAuthStart(context.Background(), authn.OAuthStartParams{
		AppID: appID, ClientName: "google", RedirectURI: "http://app/cb",
		State: "google-nonce", CodeChallenge: "verifier", CodeChallengeMethod: "plain",
	})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(authURL)
	if err != nil {
		t.Fatal(err)
	}
	sentNonce := parsed.Query().Get("nonce")
	if sentNonce == "" {
		t.Fatal("Google authorize URL omitted nonce")
	}
	if len(sentNonce) != 64 || strings.Contains(sentNonce, "-") {
		t.Fatalf("Google nonce is not a cryptographic hash-shaped value: %q", sentNonce)
	}

	cat, err := svc.Catalogs.For(context.Background(), platform.UUIDToStr(appID))
	if err != nil {
		t.Fatal(err)
	}
	stateAttr := cat.FindByEtypeLabel("$oauthRedirects", "state")
	nonceAttr := cat.FindByEtypeLabel("$oauthRedirects", "nonceHash")
	if nonceAttr == nil {
		t.Fatal("Google nonce hash attribute was not provisioned")
	}
	rows, err := svc.DB.FetchTriples(context.Background(), appID, storage.FetchFilter{AttrIDs: [][16]byte{stateAttr.ID}, Value: "google-nonce"})
	if err != nil || len(rows) != 1 {
		t.Fatalf("state record: %v %v", err, rows)
	}
	state := rows[0].Triple.E
	nonceRows, err := svc.DB.FetchTriples(context.Background(), appID, storage.FetchFilter{EntityIDs: [][16]byte{state}, AttrIDs: [][16]byte{nonceAttr.ID}})
	if err != nil || len(nonceRows) != 1 || nonceRows[0].Triple.V != sentNonce {
		t.Fatalf("persisted nonce hash = %v, want sent hash %q", nonceRows, sentNonce)
	}

	if _, err := svc.OAuthCallback(context.Background(), appID, "google-nonce", cookie, "provider-code"); err == nil {
		t.Fatal("Google authorization-code callback accepted a response without a verifiable nonce-bound ID token")
	}
	if got := tokenCalls.Load(); got != 1 {
		t.Fatalf("provider token calls = %d, want one failed attempt", got)
	}
}

func TestDA006ADirectIDTokenRouteIsExplicitlyUnsupported(t *testing.T) {
	_, h, appID, cleanup := env(t)
	defer cleanup()
	status, body := post(t, h, "/runtime/oauth/id_token", map[string]any{
		"app-id": platform.UUIDToStr(appID), "client_name": "google", "id_token": "anything", "nonce": "anything",
	})
	if status != http.StatusNotImplemented {
		t.Fatalf("direct id_token status = %d body=%v, want 501 explicit unsupported", status, body)
	}
	if !strings.Contains(body["message"].(string), "unsupported") {
		t.Fatalf("direct id_token message = %v, want unsupported explanation", body["message"])
	}
}

func TestDA006AGoogleJWKSRulesAndUnknownKidRefresh(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	claims := map[string]any{
		"iss": "accounts.google.com", "aud": "client", "sub": "subject",
		"email": "oauth@example.com", "exp": float64(time.Now().Add(time.Hour).Unix()),
	}
	sign := func(kid string) string {
		header := b64url(mustJSON(t, map[string]any{"alg": "RS256", "kid": kid}))
		payload := b64url(mustJSON(t, claims))
		si := header + "." + payload
		digest := sha256.Sum256([]byte(si))
		sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
		if err != nil {
			t.Fatal(err)
		}
		return si + "." + b64url(sig)
	}
	t.Run("accepted issuer and live exp", func(t *testing.T) {
		jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]any{{
				"kty": "RSA", "kid": "good", "alg": "RS256",
				"n": b64url(key.N.Bytes()), "e": b64url(big.NewInt(int64(key.E)).Bytes()),
			}}})
		}))
		defer jwks.Close()
		svc := &authn.Service{}
		if _, err := svc.VerifyIDToken(context.Background(), &authn.ResolvedProvider{
			ClientID: "client", Issuer: "https://accounts.google.com", JWKSURL: jwks.URL,
		}, sign("good"), ""); err != nil {
			t.Fatalf("accepted Google issuer form rejected: %v", err)
		}
		claims["exp"] = float64(time.Now().Add(-30 * time.Second).Unix())
		if _, err := (&authn.Service{}).VerifyIDToken(context.Background(), &authn.ResolvedProvider{
			ClientID: "client", Issuer: "https://accounts.google.com", JWKSURL: jwks.URL,
		}, sign("good"), ""); err == nil {
			t.Fatal("expired ID token within old leeway was accepted")
		}
	})
	t.Run("reject non RS256 JWK", func(t *testing.T) {
		jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]any{{
				"kty": "RSA", "kid": "bad-alg", "alg": "HS256",
				"n": b64url(key.N.Bytes()), "e": b64url(big.NewInt(int64(key.E)).Bytes()),
			}}})
		}))
		defer jwks.Close()
		if _, err := (&authn.Service{}).VerifyIDToken(context.Background(), &authn.ResolvedProvider{
			ClientID: "client", Issuer: "accounts.google.com", JWKSURL: jwks.URL,
		}, sign("bad-alg"), ""); err == nil {
			t.Fatal("non-RS256 JWK was accepted")
		}
	})
	t.Run("reject missing kid", func(t *testing.T) {
		jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]any{{
				"kty": "RSA", "kid": "", "alg": "RS256",
				"n": b64url(key.N.Bytes()), "e": b64url(big.NewInt(int64(key.E)).Bytes()),
			}}})
		}))
		defer jwks.Close()
		if _, err := (&authn.Service{}).VerifyIDToken(context.Background(), &authn.ResolvedProvider{
			ClientID: "client", Issuer: "accounts.google.com", JWKSURL: jwks.URL,
		}, sign(""), ""); err == nil || !strings.Contains(err.Error(), "missing kid") {
			t.Fatalf("missing-kid token was not rejected explicitly: %v", err)
		}
	})
	t.Run("unknown kid forces one refresh", func(t *testing.T) {
		var requests atomic.Int32
		jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if requests.Add(1) == 1 {
				_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]any{{
					"kty": "RSA", "kid": "old", "alg": "RS256",
					"n": b64url(key.N.Bytes()), "e": b64url(big.NewInt(int64(key.E)).Bytes()),
				}}})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]any{{
				"kty": "RSA", "kid": "new", "alg": "RS256",
				"n": b64url(key.N.Bytes()), "e": b64url(big.NewInt(int64(key.E)).Bytes()),
			}}})
		}))
		defer jwks.Close()
		claims["exp"] = float64(time.Now().Add(time.Hour).Unix())
		if _, err := (&authn.Service{}).VerifyIDToken(context.Background(), &authn.ResolvedProvider{
			ClientID: "client", Issuer: "accounts.google.com", JWKSURL: jwks.URL,
		}, sign("new"), ""); err != nil {
			t.Fatalf("unknown-kid refresh did not recover: %v", err)
		}
		if got := requests.Load(); got != 2 {
			t.Fatalf("JWKS requests = %d, want initial plus one forced refresh", got)
		}
	})
}
