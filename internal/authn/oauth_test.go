package authn_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/instant-v2/instant-v2/internal/authn"
	"github.com/instant-v2/instant-v2/internal/platform"
)

// TestOAuthRedirectOriginGate pins the redirect allowlist: an unregistered
// origin is refused at start (fail-closed, empty allowlist included), and a
// registered origin passes. This closes the phishing chain where an attacker
// starts the flow with their own redirect_uri and harvests the minted code.
func TestOAuthRedirectOriginGate(t *testing.T) {
	_, h, appID, cleanup := env(t)
	defer cleanup()
	appStr := platform.UUIDToStr(appID)

	start := func(redirect string) int {
		req := httptest.NewRequest("GET",
			"/runtime/oauth/start?app_id="+appStr+"&client_name=google&redirect_uri="+url.QueryEscape(redirect), nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}

	// env registers http://app — evil origins must be refused.
	if code := start("http://evil.example/cb"); code != 400 {
		t.Fatalf("unregistered origin must be refused at start, got %d", code)
	}
	// Same host, wrong scheme → still refused (origin = scheme://host).
	if code := start("https://app/cb"); code != 400 {
		t.Fatalf("scheme mismatch must be refused, got %d", code)
	}
	// Registered origin → proceeds to provider 302.
	if code := start("http://app/other-path?x=1"); code != 302 {
		t.Fatalf("registered origin should pass, got %d", code)
	}

	// Service-level: empty allowlist permits nothing.
	svc2, _, appB, cleanup2 := env(t)
	defer cleanup2()
	if _, err := svc2.Pool.Exec(context.Background(),
		`UPDATE apps SET redirect_origins='[]'::jsonb WHERE id=$1`, appB); err != nil {
		t.Fatal(err)
	}
	_, _, err := svc2.OAuthStart(context.Background(), authn.OAuthStartParams{
		AppID: appB, ClientName: "google", RedirectURI: "http://app/cb",
	})
	if err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("empty allowlist must deny all redirects, got: %v", err)
	}
}

// TestOAuthFullFlow runs the complete start → callback → token dance against
// a stub OAuth provider, proving PKCE (S256), state+cookie binding, one-time
// code burn, and refresh-token minting.
func TestOAuthFullFlow(t *testing.T) {
	svc, h, appID, cleanup := env(t)
	defer cleanup()
	appStr := platform.UUIDToStr(appID)

	// Stub provider: /authorize echoes the code; /token issues one;
	// /userinfo returns the identity.
	var issuedCode = "prov-code-123"
	mux := http.NewServeMux()
	tokenHit := false
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil || r.Form.Get("code") != issuedCode {
			http.Error(w, "bad code", 400)
			return
		}
		tokenHit = true
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "at-1"})
	})
	mux.HandleFunc("/userinfo", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer at-1" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"email": "oauth@user", "sub": "sub-1", "picture": "http://p",
		})
	})
	provSrv := httptest.NewServer(mux)
	defer provSrv.Close()

	svc.Providers = map[string]*authn.ResolvedProvider{
		"google": {
			ClientID:     "cid",
			ClientSecret: "secret",
			TokenURL:     provSrv.URL + "/token",
			UserInfo:     provSrv.URL + "/userinfo",
		},
	}

	challenge := pkceS256("verifier-xyz")
	// 1. start
	startReq := httptest.NewRequest("GET",
		"/runtime/oauth/start?app_id="+appStr+"&client_name=google"+
			"&redirect_uri=http://app/cb&code_challenge="+challenge+
			"&code_challenge_method=S256&state=mystate", nil)
	startRec := httptest.NewRecorder()
	h.ServeHTTP(startRec, startReq)
	if startRec.Code != 302 {
		t.Fatalf("start: %d %s", startRec.Code, startRec.Body.String())
	}
	loc, _ := url.Parse(startRec.Header().Get("Location"))
	if loc.Query().Get("state") != "mystate" || loc.Query().Get("client_id") != "cid" {
		t.Fatalf("authorize URL wrong: %s", loc)
	}
	cookies := startRec.Result().Cookies()
	if len(cookies) == 0 || !strings.HasPrefix(cookies[0].Value, "instantdb_") {
		t.Fatalf("cookie missing: %v", cookies)
	}

	// 2. callback (provider redirects back with its code)
	cbReq := httptest.NewRequest("GET",
		"/runtime/oauth/callback?app_id="+appStr+"&state=mystate&code="+issuedCode, nil)
	cbReq.AddCookie(cookies[0])
	cbRec := httptest.NewRecorder()
	h.ServeHTTP(cbRec, cbReq)
	if cbRec.Code != 302 {
		t.Fatalf("callback: %d %s", cbRec.Code, cbRec.Body.String())
	}
	target, _ := url.Parse(cbRec.Header().Get("Location"))
	instantCode := target.Query().Get("code")
	if instantCode == "" || target.Query().Get("_instant_oauth_redirect") != "true" {
		t.Fatalf("redirect target wrong: %s", target)
	}
	if !tokenHit {
		t.Fatal("provider token endpoint never hit")
	}

	// 3. callback replay must fail (one-time state)
	replay := httptest.NewRequest("GET",
		"/runtime/oauth/callback?app_id="+appStr+"&state=mystate&code="+issuedCode, nil)
	replay.AddCookie(cookies[0])
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, replay)
	if rec.Code != 400 {
		t.Fatalf("state replay must fail, got %d", rec.Code)
	}

	// 4. token exchange with wrong verifier fails
	code, resp := post(t, h, "/runtime/oauth/token",
		map[string]any{"app-id": appStr, "code": instantCode, "code_verifier": "wrong"})
	if code != http.StatusUnauthorized {
		t.Fatalf("pkce bypass: %d %v", code, resp)
	}

	// 5. correct verifier → user + refresh_token
	code, resp = post(t, h, "/runtime/oauth/token",
		map[string]any{"app-id": appStr, "code": instantCode, "code_verifier": "verifier-xyz"})
	if code != 200 {
		t.Fatalf("token: %d %v", code, resp)
	}
	userObj := resp["user"].(map[string]any)
	if userObj["email"] != "oauth@user" {
		t.Fatalf("email = %v", userObj["email"])
	}
	rt := userObj["refresh_token"].(string)
	u, err := svc.VerifyRefreshToken(context.Background(), appID, rt)
	if err != nil || u.Email != "oauth@user" {
		t.Fatalf("refresh verify after oauth: %v %+v", err, u)
	}

	// 6. instant code burned — reuse rejected
	code, _ = post(t, h, "/runtime/oauth/token",
		map[string]any{"app-id": appStr, "code": instantCode, "code_verifier": "verifier-xyz"})
	if code != http.StatusUnauthorized {
		t.Fatalf("code reuse must fail, got %d", code)
	}
}

func pkceS256(verifier string) string {
	h := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(h[:])
}
