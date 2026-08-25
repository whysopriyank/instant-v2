package authn

// Third-party identity-token verification: the POST /runtime/oauth/id_token
// path (runtime/routes.clj L661) plus Apple's client-secret JWT assertion
// minting (auth/jwt.clj + jwt/apple_client_secret.clj equivalents).
//
// JWS verification is hand-rolled on stdlib crypto (rsa/ecdsa + sha256) —
// the only algorithms v1's JWKS providers advertise are RS256/ES256.
// Deviation note: v1 also honors HS256 in discovery defaults; HS256 with a
// shared secret is not part of any supported provider flow here.

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
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// jwksKey is one JWK we can verify with.
type jwksKey struct {
	KID string
	Alg string
	RSA *rsa.PublicKey
	EC  *ecdsa.PublicKey
}

type jwksCache struct {
	mu    sync.Mutex
	keys  []jwksKey
	fetch time.Time
}

// jwksHTTPClient bounds JWKS discovery so a slow issuer cannot stall sign-in
// requests indefinitely.
var jwksHTTPClient = &http.Client{Timeout: 10 * time.Second}

func fetchJWKS(ctx context.Context, url string) ([]jwksKey, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := jwksHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("authn: jwks fetch status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	var doc struct {
		Keys []struct {
			KID string `json:"kid"`
			Kty string `json:"kty"`
			Alg string `json:"alg"`
			N   string `json:"n"`
			E   string `json:"e"`
			Crv string `json:"crv"`
			X   string `json:"x"`
			Y   string `json:"y"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("authn: jwks decode: %w", err)
	}
	out := make([]jwksKey, 0, len(doc.Keys))
	for _, k := range doc.Keys {
		jk := jwksKey{KID: k.KID, Alg: k.Alg}
		switch k.Kty {
		case "RSA":
			nb, err1 := base64.RawURLEncoding.DecodeString(k.N)
			eb, err2 := base64.RawURLEncoding.DecodeString(k.E)
			if err1 != nil || err2 != nil || len(nb) == 0 || len(eb) == 0 {
				continue
			}
			jk.RSA = &rsa.PublicKey{N: new(big.Int).SetBytes(nb), E: int(new(big.Int).SetBytes(eb).Int64())}
		case "EC":
			if k.Crv != "P-256" {
				continue
			}
			xb, _ := base64.RawURLEncoding.DecodeString(k.X)
			yb, _ := base64.RawURLEncoding.DecodeString(k.Y)
			jk.EC = &ecdsa.PublicKey{Curve: elliptic.P256(), X: new(big.Int).SetBytes(xb), Y: new(big.Int).SetBytes(yb)}
		default:
			continue
		}
		out = append(out, jk)
	}
	return out, nil
}

// get returns cached keys or refreshes them. The network call happens OUTSIDE
// the mutex (double-check) so a slow issuer blocks one refresh, not every
// concurrent verification.
func (c *jwksCache) get(ctx context.Context, url string, force bool) ([]jwksKey, error) {
	c.mu.Lock()
	if !force && c.keys != nil && time.Since(c.fetch) < time.Hour {
		keys := c.keys
		c.mu.Unlock()
		return keys, nil
	}
	c.mu.Unlock()

	keys, err := fetchJWKS(ctx, url)
	if err != nil && !force && c.keys != nil {
		// Serve stale keys on transient refresh failures; only a cold cache
		// propagates the error (verification fails closed without keys).
		c.mu.Lock()
		stale := c.keys
		c.mu.Unlock()
		return stale, nil
	}
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.keys = keys
	c.fetch = time.Now()
	c.mu.Unlock()
	return keys, nil
}

// b64 segments use padded-or-raw tolerance; providers vary.
func b64(s string) ([]byte, error) {
	if strings.ContainsRune(s, '+') || strings.ContainsRune(s, '/') {
		return base64.StdEncoding.WithPadding(base64.NoPadding).DecodeString(strings.TrimRight(s, "="))
	}
	return base64.RawURLEncoding.DecodeString(s)
}

// VerifyIDToken validates a third-party OIDC id_token against the issuer's
// JWKS and returns its claims. Signature + kid + exp + iss + aud are ALL
// mandatory: a JWKS fetch failure fails closed (there is no unsigned
// acceptance path), exp must be present and live (60s leeway), the issuer is
// compared against the configured provider issuer, and the audience must
// contain the provider client id (string or array form).
func (s *Service) VerifyIDToken(ctx context.Context, p *ResolvedProvider, idToken, nonce string) (map[string]any, error) {
	if p.Issuer == "" || p.ClientID == "" {
		return nil, errors.New("authn: provider lacks issuer/client-id; refusing unverifiable tokens")
	}
	parts := strings.Split(idToken, ".")
	if len(parts) != 3 {
		return nil, errors.New("authn: malformed id_token")
	}
	headerRaw, err := b64(parts[0])
	if err != nil {
		return nil, err
	}
	var header struct {
		Alg string `json:"alg"`
		KID string `json:"kid"`
	}
	if err := json.Unmarshal(headerRaw, &header); err != nil {
		return nil, err
	}
	switch header.Alg {
	case "RS256", "ES256":
	default:
		return nil, fmt.Errorf("authn: unsupported alg %q", header.Alg)
	}

	keys, kerr := s.jwks.get(ctx, p.JWKSURL, false)
	if kerr != nil && header.KID != "" {
		// Unknown-kid / cold-cache rotation retry.
		keys, kerr = s.jwks.get(ctx, p.JWKSURL, true)
	}
	if kerr != nil {
		return nil, fmt.Errorf("authn: jwks fetch: %w", kerr)
	}
	var match *jwksKey
	for i := range keys {
		if keys[i].KID == header.KID {
			match = &keys[i]
			break
		}
	}
	if match == nil {
		return nil, errors.New("authn: no JWKS key for kid")
	}
	sig, serr := b64(parts[2])
	if serr != nil {
		return nil, serr
	}
	signingInput := idToken[:len(parts[0])+1+len(parts[1])]
	digest := sha256.Sum256([]byte(signingInput))
	ok := false
	switch {
	case header.Alg == "RS256" && match.RSA != nil:
		err = rsa.VerifyPKCS1v15(match.RSA, crypto.SHA256, digest[:], sig)
		ok = err == nil
	case header.Alg == "ES256" && match.EC != nil:
		if len(sig) == 64 {
			r := new(big.Int).SetBytes(sig[:32])
			sv := new(big.Int).SetBytes(sig[32:])
			ok = ecdsa.Verify(match.EC, digest[:], r, sv)
		}
	}
	if !ok {
		if err != nil {
			return nil, fmt.Errorf("authn: signature: %w", err)
		}
		return nil, errors.New("authn: signature verification failed")
	}

	payloadRaw, err := b64(parts[1])
	if err != nil {
		return nil, err
	}
	var claims map[string]any
	if err := json.Unmarshal(payloadRaw, &claims); err != nil {
		return nil, err
	}

	// exp: REQUIRED.
	exp, ok := claims["exp"].(float64)
	if !ok {
		return nil, errors.New("authn: id_token missing exp")
	}
	if time.Now().Add(-60 * time.Second).After(time.Unix(int64(exp), 0)) {
		return nil, errors.New("authn: id_token expired")
	}

	// iss: REQUIRED and must equal the configured provider issuer.
	if iss, _ := claims["iss"].(string); iss == "" || iss != p.Issuer {
		return nil, fmt.Errorf("authn: issuer mismatch %q", claims["iss"])
	}

	// aud: REQUIRED; string or array form must contain the client id.
	if !audContains(claims["aud"], p.ClientID) {
		return nil, errors.New("authn: audience mismatch")
	}

	if nonce != "" {
		got, _ := claims["nonce"].(string)
		// Audit F2a: when the caller requested a nonce binding, an id_token
		// without a nonce claim is a replay vector (stolen tokens pass any
		// check). Absence is now rejected; callers that don't bind a nonce
		// are unaffected. Apple-style sha256(nonce) stays accepted.
		if got == "" {
			return nil, errors.New("authn: id_token missing required nonce claim")
		}
		if got != nonce && hashHex(got) != nonce {
			return nil, errors.New("authn: nonce mismatch")
		}
	}
	if ev, ok := claims["email_verified"]; ok {
		if verified, ok := ev.(bool); ok && !verified {
			return nil, errors.New("authn: email not verified")
		}
	}
	return claims, nil
}

// audContains reports whether the aud claim (string, [string], or mixed
// JSON array) contains want. Non-string entries are ignored per OIDC core
// §3.1.3.7 (aud arrays may carry authorized-party values).
func audContains(aud any, want string) bool {
	switch v := aud.(type) {
	case string:
		return v == want
	case []any:
		for _, el := range v {
			if s, ok := el.(string); ok && s == want {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// IDTokenSignIn ports oauth-id-token-callback: verify the third-party token,
// upsert the user, mint an app refresh token.
func (s *Service) IDTokenSignIn(ctx context.Context, appID [16]byte,
	clientName, idToken, nonce string,
) (map[string]any, error) {
	prov, err := s.resolveProvider(ctx, appID, clientName)
	if err != nil {
		return nil, err
	}
	if prov.Issuer == "" || prov.JWKSURL == "" {
		return nil, fmt.Errorf("authn: provider %q lacks issuer/jwks config", clientName)
	}
	claims, err := s.VerifyIDToken(ctx, prov, idToken, nonce)
	if err != nil {
		return nil, err
	}
	email, _ := claims["email"].(string)
	if email == "" {
		return nil, errors.New("authn: id_token carries no email")
	}
	return s.VerifyMagicCodeTrusted(ctx, appID, email, claims)
}

// --- Apple client-secret assertion ---

// AppleSigner mints the ES256 client_secret JWT Apple requires at the token
// endpoint. Key material arrives as PKCS#8 PEM (.p8).
type AppleSigner struct {
	Key    *ecdsa.PrivateKey
	KeyID  string // kid header
	TeamID string // iss claim
}

// LoadAppleSigner parses a .p8 PEM block.
func LoadAppleSigner(pemBytes []byte, keyID, teamID string) (*AppleSigner, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("authn: apple key: no PEM block")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("authn: apple key: %w", err)
	}
	ec, ok := key.(*ecdsa.PrivateKey)
	if !ok {
		return nil, errors.New("authn: apple key: not EC")
	}
	return &AppleSigner{Key: ec, KeyID: keyID, TeamID: teamID}, nil
}

// Mint produces a client_assertion valid for 30 minutes (Apple caps at 6mo;
// short-lived matches v1 behavior of minting per exchange).
func (a *AppleSigner) Mint(now time.Time) (string, error) {
	header := map[string]any{"alg": "ES256", "kid": a.KeyID}
	claims := map[string]any{
		"iss": a.TeamID,
		"iat": now.Unix(),
		"exp": now.Add(30 * time.Minute).Unix(),
		"aud": "https://appleid.apple.com",
	}
	hb, _ := json.Marshal(header)
	cb, _ := json.Marshal(claims)
	b64e := func(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }
	signingInput := b64e(hb) + "." + b64e(cb)
	digest := sha256.Sum256([]byte(signingInput))
	r, sv, err := ecdsa.Sign(rand.Reader, a.Key, digest[:])
	if err != nil {
		return "", err
	}
	sig := make([]byte, 64)
	copy(sig[:32], r.Bytes())
	copy(sig[32:], sv.Bytes())
	return signingInput + "." + b64e(sig), nil
}

// AppleSignerFor resolves signing material from config when the static
// client secret is absent. Env-driven until Phase 5 persists per-app keys.
func (s *Service) AppleSignerFor() *AppleSigner {
	if s.Apple != nil {
		return s.Apple
	}
	pemStr := os.Getenv("INSTANT_OAUTH_APPLE_KEY_P8")
	if pemStr == "" {
		return nil
	}
	keyID := os.Getenv("INSTANT_OAUTH_APPLE_KEY_ID")
	teamID := os.Getenv("INSTANT_OAUTH_APPLE_TEAM_ID")
	if keyID == "" || teamID == "" {
		return nil
	}
	as, err := LoadAppleSigner([]byte(strings.ReplaceAll(pemStr, `\n`, "\n")), keyID, teamID)
	if err != nil {
		s.logger().Error("authn: apple signer load failed", "err", err)
		return nil
	}
	s.Apple = as
	return as
}
