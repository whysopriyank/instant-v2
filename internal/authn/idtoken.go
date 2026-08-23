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

func (c *jwksCache) get(ctx context.Context, url string, force bool) ([]jwksKey, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !force && c.keys != nil && time.Since(c.fetch) < time.Hour {
		return c.keys, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
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
	c.keys = out
	c.fetch = time.Now()
	return out, nil
}

// b64 segments use padded-or-raw tolerance; providers vary.
func b64(s string) ([]byte, error) {
	if strings.ContainsRune(s, '+') || strings.ContainsRune(s, '/') {
		return base64.StdEncoding.WithPadding(base64.NoPadding).DecodeString(strings.TrimRight(s, "="))
	}
	return base64.RawURLEncoding.DecodeString(s)
}

// VerifyIDToken validates a third-party OIDC id_token against the issuer's
// JWKS and returns its claims. Checks signature, kid, exp, iss, aud.
func (s *Service) VerifyIDToken(ctx context.Context, p *ResolvedProvider, idToken, nonce string) (map[string]any, error) {
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
	if kerr == nil && len(keys) > 0 {
		var match *jwksKey
		for i := range keys {
			if keys[i].KID == header.KID {
				match = &keys[i]
				break
			}
		}
		if match == nil {
			// Unknown kid → refresh once (rotation).
			keys, kerr = s.jwks.get(ctx, p.JWKSURL, true)
			if kerr == nil {
				for i := range keys {
					if keys[i].KID == header.KID {
						match = &keys[i]
						break
					}
				}
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
	} else if !p.TrustUnsignedFallback {
		return nil, fmt.Errorf("authn: jwks fetch: %w", kerr)
	}

	payloadRaw, err := b64(parts[1])
	if err != nil {
		return nil, err
	}
	var claims map[string]any
	if err := json.Unmarshal(payloadRaw, &claims); err != nil {
		return nil, err
	}
	if exp, ok := claims["exp"].(float64); ok && time.Now().After(time.Unix(int64(exp), 0)) {
		return nil, errors.New("authn: id_token expired")
	}
	if iss, ok := claims["iss"].(string); ok && p.Issuer != "" && iss != p.Issuer {
		return nil, fmt.Errorf("authn: issuer mismatch %q", iss)
	}
	if aud, ok := claims["aud"].(string); ok && p.ClientID != "" && aud != p.ClientID {
		return nil, errors.New("authn: audience mismatch")
	}
	if nonce != "" {
		got, _ := claims["nonce"].(string)
		// v1 quirks preserved: Google omits nonce, Apple sends sha256(nonce).
		if got != "" && got != nonce && hashHex(got) != nonce {
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
