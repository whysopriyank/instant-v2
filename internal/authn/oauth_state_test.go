package authn

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestOAuthRecordDecoding(t *testing.T) {
	a := oauthAttrs{
		state: [16]byte{1}, cookieHash: [16]byte{2}, clientID: [16]byte{3},
		redirectURL: [16]byte{4}, codeChallenge: [16]byte{5}, ccMethod: [16]byte{6},
		oauthCodeHash: [16]byte{7}, oauthCC: [16]byte{8}, oauthCCM: [16]byte{9}, oauthUserInfo: [16]byte{10},
	}
	t.Run("redirect strings retain empty values", func(t *testing.T) {
		values := map[[16]byte]any{
			a.state: "state", a.cookieHash: "hash", a.clientID: "test", a.redirectURL: "http://app/cb",
			a.codeChallenge: "", a.ccMethod: "",
		}
		got, err := decodeOAuthRedirect(values, a)
		want := oauthRedirectRecord{state: "state", cookieHash: "hash", clientName: "test", redirectURI: "http://app/cb"}
		if err != nil || got != want {
			t.Fatalf("decode = %+v, %v; want %+v", got, err, want)
		}
		for _, invalid := range []any{nil, false, float64(1), []any{}, map[string]any{}} {
			values[a.state] = invalid
			if _, err := decodeOAuthRedirect(values, a); !errors.Is(err, ErrOAuthState) || !strings.Contains(err.Error(), "state") {
				t.Errorf("invalid state type %T: %v", invalid, err)
			}
		}
		delete(values, a.state)
		if _, err := decodeOAuthRedirect(values, a); !errors.Is(err, ErrOAuthState) {
			t.Fatalf("missing state: %v", err)
		}
	})
	info := map[string]any{"email": "user@example.com", "provider_claim": map[string]any{"number": float64(3)}}
	for name, persisted := range map[string]any{
		"object":      info,
		"JSON string": `{"email":"user@example.com","provider_claim":{"number":3}}`,
	} {
		t.Run(name, func(t *testing.T) {
			values := map[[16]byte]any{a.oauthCodeHash: "hash", a.oauthCC: "", a.oauthCCM: "", a.oauthUserInfo: persisted}
			got, err := decodeOAuthCode(values, a)
			want := oauthCodeRecord{codeHash: "hash", userInfo: info}
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("decode = %+v, %v; want %+v", got, err, want)
			}
			delete(values, a.oauthCodeHash)
			if _, err := decodeOAuthCode(values, a); !errors.Is(err, ErrOAuthCode) || !strings.Contains(err.Error(), "codeHash") {
				t.Fatalf("missing codeHash: %v", err)
			}
		})
	}
	t.Run("userinfo errors do not expose stored claims", func(t *testing.T) {
		for _, value := range []any{nil, map[string]any(nil), `null`, `[]`, `{"private_claim":"secret"} trailing`} {
			_, err := decodeOAuthCode(map[[16]byte]any{
				a.oauthCodeHash: "hash", a.oauthCC: "", a.oauthCCM: "plain", a.oauthUserInfo: value,
			}, a)
			if !errors.Is(err, ErrOAuthCode) || !strings.Contains(err.Error(), "userInfo") || strings.Contains(err.Error(), "secret") {
				t.Errorf("unexpected userInfo error for %T: %v", value, err)
			}
		}
	})
}

func TestOAuthPKCECompatibility(t *testing.T) {
	challenge := "ungWv48Bz-pBQUDeXa4iI7ADYaOWF3qctBD_YfIAFa0" // SHA-256("abc")
	for _, tc := range []struct {
		method, challenge, verifier string
		want                        bool
	}{
		{"plain", "abc", "abc", true}, {"plain", "", "", false},
		{"S256", challenge, "abc", true}, {"", challenge, "abc", true},
		{"plain", "abc", "wrong", false}, {"S256", challenge, "wrong", false},
		{"unknown", "abc", "abc", false},
	} {
		if got := verifyPKCE(tc.method, tc.challenge, tc.verifier); got != tc.want {
			t.Errorf("verifyPKCE(%q, %q, %q) = %v; want %v", tc.method, tc.challenge, tc.verifier, got, tc.want)
		}
	}
}
