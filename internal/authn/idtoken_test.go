package authn_test

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/authn"
	"github.com/instant-v2/instant-v2/internal/platform"
)

// b64url encodes without padding.
func b64url(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestIDTokenVerification spins a stub JWKS + issuer, signs an RS256
// id_token, and proves the full verify → sign-in path — plus rejection of a
// tampered token.
func TestIDTokenVerification(t *testing.T) {
	svc, h, appID, cleanup := env(t)
	defer cleanup()
	appStr := platform.UUIDToStr(appID)

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	kid := "test-key-1"

	// Stub JWKS endpoint serving the public key.
	jwksSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nB64 := b64url(key.N.Bytes())
		eBig := big.NewInt(int64(key.E))
		eB64 := b64url(eBig.Bytes())
		_ = json.NewEncoder(w).Encode(map[string]any{
			"keys": []map[string]any{{
				"kty": "RSA", "kid": kid, "alg": "RS256",
				"n": nB64, "e": eB64,
			}},
		})
	}))
	defer jwksSrv.Close()

	svc.Providers = map[string]*authn.ResolvedProvider{
		"google": {
			ClientID: "cid", Issuer: jwksSrv.URL,
			JWKSURL:               jwksSrv.URL + "/certs",
			TokenURL:              "http://unused/token",
			UserInfo:              "",
			TrustUnsignedFallback: false,
		},
	}

	signToken := func(claims map[string]any, mutateSig bool) string {
		header := map[string]any{"alg": "RS256", "kid": kid}
		si := b64url(mustJSON(t, header)) + "." + b64url(mustJSON(t, claims))
		digest := sha256.Sum256([]byte(si))
		sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
		if err != nil {
			t.Fatal(err)
		}
		if mutateSig {
			sig[0] ^= 0xFF
		}
		return si + "." + b64url(sig)
	}

	goodClaims := map[string]any{
		"iss": jwksSrv.URL, "aud": "cid", "sub": "sub-7",
		"email": "idtok@user", "email_verified": true,
		"exp": float64(time.Now().Add(time.Hour).Unix()),
	}
	good := signToken(goodClaims, false)

	// Happy path.
	code, resp := post(t, h, "/runtime/oauth/id_token",
		map[string]any{"app-id": appStr, "client_name": "google", "id_token": good})
	if code != 200 {
		t.Fatalf("id_token: %d %v", code, resp)
	}
	userObj := resp["user"].(map[string]any)
	if userObj["email"] != "idtok@user" {
		t.Fatalf("email = %v", userObj["email"])
	}
	rt := userObj["refresh_token"].(string)
	u, err := svc.VerifyRefreshToken(context.Background(), appID, rt)
	if err != nil || u.Email != "idtok@user" {
		t.Fatalf("refresh after id_token: %v %+v", err, u)
	}

	// Tampered signature rejected.
	code, _ = post(t, h, "/runtime/oauth/id_token",
		map[string]any{"app-id": appStr, "client_name": "google",
			"id_token": signToken(goodClaims, true)})
	if code != http.StatusUnauthorized {
		t.Fatalf("tampered token must be rejected, got %d", code)
	}

	// Wrong issuer rejected.
	badIss := map[string]any{}
	for k, v := range goodClaims {
		badIss[k] = v
	}
	badIss["iss"] = "https://evil.example"
	code, _ = post(t, h, "/runtime/oauth/id_token",
		map[string]any{"app-id": appStr, "client_name": "google", "id_token": signToken(badIss, false)})
	if code != http.StatusUnauthorized {
		t.Fatalf("issuer mismatch must be rejected, got %d", code)
	}

	// Expired rejected.
	expired := map[string]any{}
	for k, v := range goodClaims {
		expired[k] = v
	}
	expired["exp"] = float64(time.Now().Add(-time.Hour).Unix())
	code, _ = post(t, h, "/runtime/oauth/id_token",
		map[string]any{"app-id": appStr, "client_name": "google", "id_token": signToken(expired, false)})
	if code != http.StatusUnauthorized {
		t.Fatalf("expired token must be rejected, got %d", code)
	}
}

// TestAppleSignerMint proves .p8 loading and ES256 assertion minting with
// verifiable signature.
func TestAppleSignerMint(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	pemStr := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	signer, err := authn.LoadAppleSigner(pemStr, "KID123", "TEAM456")
	if err != nil {
		t.Fatal(err)
	}
	assertion, err := signer.Mint(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(assertion, ".")
	if len(parts) != 3 {
		t.Fatal("not a JWS")
	}
	payloadRaw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var claims map[string]any
	_ = json.Unmarshal(payloadRaw, &claims)
	if claims["iss"] != "TEAM456" || claims["aud"] != "https://appleid.apple.com" {
		t.Fatalf("claims wrong: %v", claims)
	}
	// Verify signature.
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(sig) != 64 {
		t.Fatalf("bad signature bytes: %v", err)
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	r := new(big.Int).SetBytes(sig[:32])
	sv := new(big.Int).SetBytes(sig[32:])
	if !ecdsa.Verify(&key.PublicKey, digest[:], r, sv) {
		t.Fatal("signature does not verify")
	}
}
