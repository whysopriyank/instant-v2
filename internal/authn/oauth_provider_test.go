package authn

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestQualityBuiltinOAuthProviders(t *testing.T) {
	for _, tc := range []struct{ name, authURL, tokenURL, userInfo, scope string }{
		{"google", "https://accounts.google.com/o/oauth2/v2/auth", "https://oauth2.googleapis.com/token", "https://openidconnect.googleapis.com/v1/userinfo", "openid email profile"},
		{"github", "https://github.com/login/oauth/authorize", "https://github.com/login/oauth/access_token", "https://api.github.com/user", "read:user user:email"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("INSTANT_OAUTH_"+strings.ToUpper(tc.name)+"_CLIENT_ID", "client & id")
			t.Setenv("INSTANT_OAUTH_"+strings.ToUpper(tc.name)+"_CLIENT_SECRET", "fixture & secret")
			p, err := (&Service{}).resolveProvider(context.Background(), [16]byte{}, tc.name)
			if err != nil {
				t.Fatal(err)
			}
			if p.TokenURL != tc.tokenURL || p.UserInfo != tc.userInfo {
				t.Fatalf("configured builtin exchange endpoints = %q, %q; want %q, %q", p.TokenURL, p.UserInfo, tc.tokenURL, tc.userInfo)
			}
			if p.authURL != tc.authURL || p.scope != tc.scope || p.ClientID != "client & id" || p.ClientSecret != "fixture & secret" {
				t.Fatalf("provider configuration: %+v", p)
			}
			if tc.name == "google" && (p.extraAuth.Get("access_type") != "offline" || p.extraAuth.Get("prompt") != "consent") {
				t.Fatal("Google authorize options missing")
			}
			for _, response := range []struct {
				name, token, info string
				status            int
				wantErr           bool
			}{
				{"success", `{"access_token":"access-token"}`, `{"email":"oauth@example.com","sub":"subject-1"}`, 200, false},
				{"malformed token", `{`, `{}`, 200, true},
				{"token error", `{"error":"invalid_grant"}`, `{}`, 400, true},
				{"malformed identity", `{"access_token":"access-token"}`, `{`, 200, true},
				{"identity HTTP error", `{"access_token":"access-token"}`, `{"error":"unauthorized"}`, 401, true},
			} {
				t.Run(response.name, func(t *testing.T) {
					fixture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.Method == "POST" {
							if err := r.ParseForm(); err != nil {
								t.Error(err)
							}
							for key, value := range map[string]string{"grant_type": "authorization_code", "code": "code & +", "redirect_uri": "http://app/cb?x=1&y=2", "client_id": "client & id", "client_secret": "fixture & secret"} {
								if r.Form.Get(key) != value {
									t.Errorf("%s=%q, want %q", key, r.Form.Get(key), value)
								}
							}
							if r.Header.Get("Accept") != "application/json" || r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
								t.Error("token request headers")
							}
							if response.name == "token error" {
								w.WriteHeader(response.status)
							}
							_, _ = io.WriteString(w, response.token)
							return
						}
						if r.Header.Get("Authorization") != "Bearer access-token" {
							t.Error("userinfo bearer token missing")
						}
						w.WriteHeader(response.status)
						_, _ = io.WriteString(w, response.info)
					}))
					defer fixture.Close()
					local, _ := url.Parse(fixture.URL)
					previous := boundedHTTPClient
					client := *previous
					client.Transport = oauthFixtureTransport(func(r *http.Request) (*http.Response, error) {
						wantURL := tc.userInfo
						if r.Method == "POST" {
							wantURL = tc.tokenURL
						}
						if r.URL.String() != wantURL {
							t.Errorf("request endpoint %q, want %q", r.URL, wantURL)
						}
						r = r.Clone(r.Context())
						r.URL.Scheme, r.URL.Host = local.Scheme, local.Host
						return fixture.Client().Transport.RoundTrip(r)
					})
					boundedHTTPClient = &client
					defer func() { boundedHTTPClient = previous }()
					identity, err := exchangeUserInfo(context.Background(), p, "code & +", "http://app/cb?x=1&y=2")
					if response.wantErr {
						if err == nil {
							t.Fatalf("provider failure returned identity: %v", identity)
						}
					} else if err != nil || identity["email"] != "oauth@example.com" || identity["sub"] != "subject-1" {
						t.Fatalf("identity=%v err=%v", identity, err)
					}
				})
			}
		})
	}
}

func TestQualityBuiltinOAuthAppleUnsupported(t *testing.T) {
	_, err := (&Service{}).resolveProvider(context.Background(), [16]byte{}, "apple")
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("unwired builtin Apple must be explicitly unsupported: %v", err)
	}
}

func TestQualityOAuthProviderHTTPError(t *testing.T) {
	for _, failedPath := range []string{"/token", "/userinfo"} {
		t.Run(failedPath, func(t *testing.T) {
			fixture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == failedPath {
					w.WriteHeader(http.StatusUnauthorized)
				}
				if r.URL.Path == "/token" {
					_, _ = io.WriteString(w, `{"access_token":"must-not-accept"}`)
				} else {
					_, _ = io.WriteString(w, `{"email":"must-not-accept@example.com"}`)
				}
			}))
			defer fixture.Close()
			p := &ResolvedProvider{TokenURL: fixture.URL + "/token", UserInfo: fixture.URL + "/userinfo"}
			if identity, err := exchangeUserInfo(context.Background(), p, "code", "http://app/cb"); err == nil {
				t.Fatalf("HTTP failure accepted identity: %v", identity)
			}
		})
	}
}

type oauthFixtureTransport func(*http.Request) (*http.Response, error)

func (f oauthFixtureTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
