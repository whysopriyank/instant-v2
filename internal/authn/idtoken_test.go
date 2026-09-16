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

// Direct id_token exchange stays disabled until the server owns a one-time
// nonce lifecycle; provider callback tests cover ID-token verification.
func TestIDTokenVerification(t *testing.T) {
	_, h, _, cleanup := env(t)
	defer cleanup()
	code, resp := post(t, h, "/runtime/oauth/id_token", map[string]any{
		"app-id": "11111111-1111-4111-8111-111111111111", "id_token": "anything",
	})
	if code != http.StatusNotImplemented || !strings.Contains(resp["message"].(string), "unsupported") {
		t.Fatalf("direct id_token response = %d %v, want explicit unsupported", code, resp)
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

// Audit F2a: when the client binds a nonce, an id_token WITHOUT a nonce
// claim must be rejected (previously it silently passed — a stolen token
// replayed without knowledge of the nonce still minted a refresh token).
func TestIDTokenNonceRequiredWhenBound(t *testing.T) {
	svc, _, _, cleanup := env(t)
	defer cleanup()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	kid := "nonce-key"
	jwksSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"keys": []map[string]any{{
				"kty": "RSA", "kid": kid, "alg": "RS256",
				"n": b64url(key.N.Bytes()), "e": b64url(big.NewInt(int64(key.E)).Bytes()),
			}},
		})
	}))
	defer jwksSrv.Close()

	svc.Providers = map[string]*authn.ResolvedProvider{
		"google": {ClientID: "cid", Issuer: jwksSrv.URL, JWKSURL: jwksSrv.URL + "/certs"},
	}
	sign := func(claims map[string]any) string {
		si := b64url(mustJSON(t, map[string]any{"alg": "RS256", "kid": kid})) + "." + b64url(mustJSON(t, claims))
		digest := sha256.Sum256([]byte(si))
		sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
		if err != nil {
			t.Fatal(err)
		}
		return si + "." + b64url(sig)
	}
	noNonce := sign(map[string]any{
		"iss": jwksSrv.URL, "aud": "cid", "sub": "s1",
		"email": "nn@user", "email_verified": true,
		"exp": float64(time.Now().Add(time.Hour).Unix()),
	})

	if _, err := svc.VerifyIDToken(context.Background(), svc.Providers["google"], noNonce, "bound-value"); err == nil {
		t.Fatal("nonce-bound verification accepted a nonce-less token")
	}

	if _, err := svc.VerifyIDToken(context.Background(), svc.Providers["google"], noNonce, ""); err != nil {
		t.Fatalf("unbound verifier rejected the otherwise valid token: %v", err)
	}
}
