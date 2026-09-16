package authn

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const (
	oauthCookieName   = "__session" // v1 oauth-cookie-name
	oauthCookiePrefix = "instantdb_"
	oauthStateTTL     = 10 * time.Minute // $oauthRedirects expiry
	oauthCodeTTL      = 10 * time.Minute
)

var ErrOAuthState = errors.New("authn: invalid or expired oauth state")
var ErrOAuthCode = errors.New("authn: invalid or expired oauth code")

// These records describe existing triples, not a new persisted format. Empty
// strings remain valid representations (including the legacy PKCE method);
// missing or incorrectly typed fields must not become those empty values.
type oauthRedirectRecord struct {
	state, cookieHash, clientName, redirectURI, challenge, method, nonceHash string
}

type oauthCodeRecord struct {
	codeHash, challenge, method string
	userInfo                    map[string]any // provider-owned dynamic claims
}

func decodeOAuthRedirect(values map[[16]byte]any, a oauthAttrs) (oauthRedirectRecord, error) {
	var record oauthRedirectRecord
	if err := decodeOAuthStrings(values, ErrOAuthState,
		oauthStringField{a.state, "state", &record.state},
		oauthStringField{a.cookieHash, "cookieHash", &record.cookieHash},
		oauthStringField{a.clientID, "clientId", &record.clientName},
		oauthStringField{a.redirectURL, "redirectUrl", &record.redirectURI},
		oauthStringField{a.codeChallenge, "codeChallenge", &record.challenge},
		oauthStringField{a.ccMethod, "codeChallengeMethod", &record.method},
	); err != nil {
		return record, err
	}
	// The nonce binding is required only for newly-created Google records;
	// leaving it optional here preserves decoding of older GitHub records.
	if value, ok := values[a.nonceHash]; ok {
		text, ok := value.(string)
		if !ok {
			return record, fmt.Errorf("%w: nonceHash must be a string", ErrOAuthState)
		}
		record.nonceHash = text
	}
	return record, nil
}

func decodeOAuthCode(values map[[16]byte]any, a oauthAttrs) (oauthCodeRecord, error) {
	var record oauthCodeRecord
	if err := decodeOAuthStrings(values, ErrOAuthCode,
		oauthStringField{a.oauthCodeHash, "codeHash", &record.codeHash},
		oauthStringField{a.oauthCC, "codeChallenge", &record.challenge},
		oauthStringField{a.oauthCCM, "codeChallengeMethod", &record.method},
	); err != nil {
		return record, err
	}
	// Current writers persist a JSON string; older records may hold the object
	// directly. Accept both without reinterpreting provider-specific claims.
	switch value := values[a.oauthUserInfo].(type) {
	case map[string]any:
		record.userInfo = value
	case string:
		if err := json.Unmarshal([]byte(value), &record.userInfo); err != nil {
			return record, fmt.Errorf("%w: userInfo must contain a JSON object", ErrOAuthCode)
		}
	default:
		return record, fmt.Errorf("%w: userInfo must be an object or JSON object string", ErrOAuthCode)
	}
	if record.userInfo == nil {
		return record, fmt.Errorf("%w: userInfo must contain a non-null object", ErrOAuthCode)
	}
	return record, nil
}

type oauthStringField struct {
	attr [16]byte
	name string
	dest *string
}

func decodeOAuthStrings(values map[[16]byte]any, kind error, fields ...oauthStringField) error {
	for _, field := range fields {
		value, exists := values[field.attr]
		if !exists {
			return fmt.Errorf("%w: missing %s", kind, field.name)
		}
		text, ok := value.(string)
		if !ok {
			return fmt.Errorf("%w: %s must be a string", kind, field.name)
		}
		*field.dest = text
	}
	return nil
}

func hashHex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func verifyPKCE(method, challenge, verifier string) bool {
	if challenge == "" || verifier == "" {
		return false
	}
	switch method {
	case "plain":
		return challenge == verifier
	case "S256", "":
		h := sha256.Sum256([]byte(verifier))
		return challenge == base64.RawURLEncoding.EncodeToString(h[:])
	default:
		return false
	}
}
