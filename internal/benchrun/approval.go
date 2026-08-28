package benchrun

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"os"
)

// TrustedApprovalPublicKeyEnv is deliberately out-of-band: a bundle or live
// config may carry a key for display, but it can never choose the trust root.
const TrustedApprovalPublicKeyEnv = "INSTANT_BENCH_TRUSTED_APPROVAL_PUBLIC_KEY"

// ApprovalTuple is the immutable provenance payload signed by an external
// operator. It includes the content root, manifest identity/provenance, and
// derived artifact limits; only approval transport fields are excluded.
func ApprovalTuple(m Manifest) ([]byte, error) {
	m.ApprovalPublicKey, m.ApprovalSignature = "", ""
	return CanonicalJSON(m)
}

func VerifyApproval(m Manifest) error {
	if m.ApprovalSignature == "" {
		return errors.New("detached approval signature is missing")
	}
	pub, err := trustedApprovalKey()
	if err != nil {
		return err
	}
	sig, err := decodeApproval(m.ApprovalSignature, ed25519.SignatureSize)
	if err != nil {
		return errors.New("invalid approval signature")
	}
	if m.ContentRoot == "" {
		return errors.New("content root is missing")
	}
	tuple, err := ApprovalTuple(m)
	if err != nil {
		return errors.New("approval tuple is invalid")
	}
	if !ed25519.Verify(ed25519.PublicKey(pub), tuple, sig) {
		return errors.New("approval signature verification failed")
	}
	return nil
}

func trustedApprovalKey() (ed25519.PublicKey, error) {
	raw := os.Getenv(TrustedApprovalPublicKeyEnv)
	b, err := decodeApproval(raw, ed25519.PublicKeySize)
	if err != nil {
		return nil, errors.New("trusted approval public key is missing or invalid")
	}
	return ed25519.PublicKey(b), nil
}

// LiveConfigAuthorizationTuple excludes only the signature fields. It
// therefore binds executable argv/hash, target endpoints, and all fixed
// benchmark settings to the external authorization.
func LiveConfigAuthorizationTuple(c LiveConfig) ([]byte, error) {
	c.AuthorizationPublicKey, c.AuthorizationSignature = "", ""
	return CanonicalJSON(c)
}

// CanonicalConfigEvidence returns the redacted, self-independent config
// representation persisted in a bundle.  ConfigHash is excluded from its own
// digest, as are the authorization fields which are transport metadata rather
// than benchmark configuration.
func CanonicalConfigEvidence(c LiveConfig) LiveConfig {
	c.ConfigHash = ""
	c.AuthorizationPublicKey = ""
	c.AuthorizationSignature = ""
	return c
}

func VerifyLiveConfigAuthorization(c LiveConfig) error {
	if c.AuthorizationSignature == "" {
		return errors.New("live config authorization signature is missing")
	}
	pub, err := trustedApprovalKey()
	if err != nil {
		return err
	}
	sig, err := decodeApproval(c.AuthorizationSignature, ed25519.SignatureSize)
	if err != nil {
		return errors.New("invalid live config authorization signature")
	}
	tuple, err := LiveConfigAuthorizationTuple(c)
	if err != nil || !ed25519.Verify(pub, tuple, sig) {
		return errors.New("live config authorization verification failed")
	}
	return nil
}

func decodeApproval(value string, size int) ([]byte, error) {
	if b, err := hex.DecodeString(value); err == nil && len(b) == size {
		return b, nil
	}
	b, err := base64.StdEncoding.DecodeString(value)
	if err != nil || len(b) != size {
		return nil, errors.New("invalid approval encoding")
	}
	return b, nil
}
