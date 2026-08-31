package authn_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/instant-v2/instant-v2/internal/authn"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/triple"
)

func TestOAuthRejectsCorruptRedirectRecords(t *testing.T) {
	svc, _, appID, cleanup := env(t)
	defer cleanup()
	ctx := context.Background()
	var providerCalls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerCalls.Add(1)
		if r.URL.Path == "/token" {
			_, _ = w.Write([]byte(`{"access_token":"test-token"}`))
			return
		}
		_, _ = w.Write([]byte(`{"email":"test@example.com"}`))
	}))
	defer provider.Close()
	svc.Providers = map[string]*authn.ResolvedProvider{
		"test": {TokenURL: provider.URL + "/token", UserInfo: provider.URL + "/userinfo"},
	}
	for _, field := range []string{"cookieHash", "clientId", "redirectUrl", "codeChallenge", "codeChallengeMethod"} {
		for _, missing := range []bool{false, true} {
			name := field + "/wrong-type"
			if missing {
				name = field + "/missing"
			}
			t.Run(name, func(t *testing.T) {
				_, cookie, err := svc.OAuthStart(ctx, authn.OAuthStartParams{
					AppID: appID, ClientName: "test", RedirectURI: "http://app/cb", State: name,
					CodeChallenge: "verifier", CodeChallengeMethod: "plain",
				})
				if err != nil {
					t.Fatal(err)
				}
				cat, err := svc.Catalogs.For(ctx, platform.UUIDToStr(appID))
				if err != nil {
					t.Fatal(err)
				}
				stateAttr := cat.FindByEtypeLabel("$oauthRedirects", "state")
				rows, err := svc.DB.FetchTriples(ctx, appID, storage.FetchFilter{AttrIDs: [][16]byte{stateAttr.ID}, Value: name})
				if err != nil || len(rows) != 1 {
					t.Fatalf("state lookup: %v %v", rows, err)
				}
				eid := rows[0].Triple.E
				attr := cat.FindByEtypeLabel("$oauthRedirects", field)
				if missing {
					_, err = svc.Pool.Exec(ctx, `DELETE FROM triples WHERE app_id=$1 AND entity_id=$2 AND attr_id=$3`, appID, eid, attr.ID)
				} else {
					_, err = svc.DB.InsertTriples(ctx, appID, cat, []triple.Triple{{E: eid, A: attr.ID, V: float64(42)}}, false)
				}
				if err != nil {
					t.Fatal(err)
				}
				before := oauthEntitySnapshot(t, svc, appID, eid)
				callsBefore := providerCalls.Load()
				_, err = svc.OAuthCallback(ctx, appID, name, cookie, "provider-code")
				if !errors.Is(err, authn.ErrOAuthState) || !strings.Contains(err.Error(), field) {
					t.Errorf("corrupt %s must fail at record boundary with field-specific state error, got %v", field, err)
				}
				if providerCalls.Load() != callsBefore {
					t.Error("corrupt state must not reach provider exchange")
				}
				if after := oauthEntitySnapshot(t, svc, appID, eid); !reflect.DeepEqual(before, after) {
					t.Errorf("rejected record changed: before=%v after=%v", before, after)
				}
			})
		}
	}
}

func TestOAuthRejectsCorruptCodeRecords(t *testing.T) {
	svc, _, appID, cleanup := env(t)
	defer cleanup()
	ctx := context.Background()
	// Start provisions the existing OAuth attributes; fixtures retain their
	// actual stored representation instead of testing a copied decoder.
	if _, _, err := svc.OAuthStart(ctx, authn.OAuthStartParams{
		AppID: appID, ClientName: "google", RedirectURI: "http://app/cb",
	}); err != nil {
		t.Fatal(err)
	}
	cat, err := svc.Catalogs.For(ctx, platform.UUIDToStr(appID))
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name, field string
		value       any
		missing     bool
	}{
		{"missing challenge", "codeChallenge", nil, true},
		{"wrong challenge", "codeChallenge", float64(42), false},
		{"missing method", "codeChallengeMethod", nil, true},
		{"wrong method", "codeChallengeMethod", float64(42), false},
		{"missing userinfo", "userInfo", nil, true},
		{"wrong userinfo", "userInfo", float64(42), false},
		{"null userinfo", "userInfo", nil, false},
		{"malformed userinfo JSON", "userInfo", `{"email":`, false},
		{"array userinfo JSON", "userInfo", `[]`, false},
		{"null userinfo JSON", "userInfo", `null`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eid := newUUID()
			fields := map[string]any{
				"codeHash": authn.HashToken(tc.name), "codeChallenge": "", "codeChallengeMethod": "plain",
				"userInfo": `{"email":"persisted@example.com"}`,
			}
			if tc.missing {
				delete(fields, tc.field)
			} else {
				fields[tc.field] = tc.value
			}
			var ts []triple.Triple
			for field, value := range fields {
				ts = append(ts, triple.Triple{E: eid, A: cat.FindByEtypeLabel("$oauthCodes", field).ID, V: value})
			}
			if _, err := svc.DB.InsertTriples(ctx, appID, cat, ts, false); err != nil {
				t.Fatal(err)
			}
			before := oauthEntitySnapshot(t, svc, appID, eid)
			_, err := svc.OAuthToken(ctx, appID, tc.name, "")
			if !errors.Is(err, authn.ErrOAuthCode) || !strings.Contains(err.Error(), tc.field) {
				t.Errorf("corrupt %s must fail at record boundary with field-specific code error, got %v", tc.field, err)
			}
			if after := oauthEntitySnapshot(t, svc, appID, eid); !reflect.DeepEqual(before, after) {
				t.Errorf("rejected record changed: before=%v after=%v", before, after)
			}
		})
	}
}

func TestOAuthCodeRecordCompatibility(t *testing.T) {
	svc, _, appID, cleanup := env(t)
	defer cleanup()
	ctx := context.Background()
	if _, _, err := svc.OAuthStart(ctx, authn.OAuthStartParams{
		AppID: appID, ClientName: "google", RedirectURI: "http://app/cb",
	}); err != nil {
		t.Fatal(err)
	}
	cat, err := svc.Catalogs.For(ctx, platform.UUIDToStr(appID))
	if err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{"plain", "S256", ""} {
		for _, asObject := range []bool{false, true} {
			name := method + "/string"
			if asObject {
				name = method + "/object"
			}
			t.Run(name, func(t *testing.T) {
				eid := newUUID()
				challenge := "verifier"
				if method != "plain" {
					challenge = pkceS256(challenge)
				}
				var info any = `{"email":"compat@example.com","sub":"provider-sub"}`
				if asObject {
					info = map[string]any{"email": "compat@example.com", "sub": "provider-sub"}
				}
				fields := map[string]any{"codeHash": authn.HashToken(name), "codeChallenge": challenge, "codeChallengeMethod": method, "userInfo": info}
				var ts []triple.Triple
				for field, value := range fields {
					ts = append(ts, triple.Triple{E: eid, A: cat.FindByEtypeLabel("$oauthCodes", field).ID, V: value})
				}
				if _, err := svc.DB.InsertTriples(ctx, appID, cat, ts, false); err != nil {
					t.Fatal(err)
				}
				// A fresh Service must decode the persisted record without any
				// state from the writer. Wrong PKCE leaves the exact record intact.
				reader := &authn.Service{DB: svc.DB, Pool: svc.Pool, Catalogs: platform.NewCatalogCache(svc.Pool, svc.Pool)}
				before := oauthEntitySnapshot(t, svc, appID, eid)
				if _, err := reader.OAuthToken(ctx, appID, name, "wrong"); err == nil || err.Error() != "authn: pkce verification failed" {
					t.Fatalf("wrong verifier: %v", err)
				}
				if after := oauthEntitySnapshot(t, svc, appID, eid); !reflect.DeepEqual(before, after) {
					t.Fatal("wrong verifier consumed or changed the code record")
				}
				res, err := reader.OAuthToken(ctx, appID, name, "verifier")
				if err != nil {
					t.Fatal(err)
				}
				user := res["user"].(map[string]any)
				if user["email"] != "compat@example.com" || user["refresh_token"] == "" {
					t.Fatalf("unexpected identity: %v", user)
				}
				if _, err := reader.VerifyRefreshToken(ctx, appID, user["refresh_token"].(string)); err != nil {
					t.Fatalf("minted refresh token: %v", err)
				}
				if _, err := reader.OAuthToken(ctx, appID, name, "verifier"); !errors.Is(err, authn.ErrOAuthCode) {
					t.Fatalf("code replay: %v", err)
				}
			})
		}
	}
}

func oauthEntitySnapshot(t *testing.T, svc *authn.Service, appID, eid [16]byte) map[[16]byte]string {
	t.Helper()
	rows, err := svc.DB.FetchTriples(context.Background(), appID, storage.FetchFilter{EntityIDs: [][16]byte{eid}})
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[[16]byte]string, len(rows))
	for _, row := range rows {
		value, err := json.Marshal(row.Triple.V)
		if err != nil {
			t.Fatal(err)
		}
		out[row.Triple.A] = string(value)
	}
	return out
}
