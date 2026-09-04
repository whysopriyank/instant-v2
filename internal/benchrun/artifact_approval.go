package benchrun

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var derivedBundleFiles = map[string]bool{"report.md": true, "checksums.sha256": true, "raw-index.json": true, "approval.json": true}

const ApprovalPrivateKeyEnv = "INSTANT_BENCH_APPROVAL_PRIVATE_KEY"

// ComputeContentRoot hashes only immutable raw evidence. Derived reports,
// indexes, checksums, and approval metadata are excluded to avoid circular
// signing and to make replay independent of renderer output.
func ComputeContentRoot(root string) (string, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	m, err := LoadManifest(root)
	if err != nil {
		return "", err
	}
	return computeContentRoot(root, m)
}

func computeContentRoot(root string, m Manifest) (string, error) {
	totalLimit, fileLimit, err := bundleArtifactLimits(root)
	if err != nil {
		return "", err
	}
	return computeContentRootWithLimits(root, m, totalLimit, fileLimit)
}

func computeContentRootWithLimits(root string, m Manifest, totalLimit, fileLimit int64) (string, error) {
	if err := validateBundleSize(root, totalLimit, fileLimit); err != nil {
		return "", err
	}
	files, err := bundleFiles(root)
	if err != nil {
		return "", err
	}
	paths := make([]string, 0, len(files))
	for path := range files {
		if derivedBundleFiles[path] {
			continue
		}
		paths = append(paths, path)
	}
	sort.Strings(paths)
	h := sha256.New()
	for _, path := range paths {
		var sum [32]byte
		if path == "manifest.json" {
			m.ApprovalPublicKey, m.ApprovalSignature, m.ContentRoot = "", "", ""
			manifestBytes, marshalErr := marshalRedactedJSON(m)
			if marshalErr != nil {
				return "", fmt.Errorf("serialize manifest for content root: %w", marshalErr)
			}
			sum = sha256.Sum256(append(manifestBytes, '\n'))
		} else {
			sum, err = hashFileBounded(filepath.Join(root, path), fileLimit)
			if err != nil {
				return "", err
			}
		}
		_, _ = fmt.Fprintf(h, "%s\x00%s\n", path, hex.EncodeToString(sum[:]))
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func VerifyContentRoot(root string, m Manifest) error {
	if m.ContentRoot == "" {
		return errors.New("content root is missing")
	}
	got, err := computeContentRoot(root, m)
	if err != nil {
		return err
	}
	if !strings.EqualFold(got, m.ContentRoot) {
		return errors.New("content root mismatch")
	}
	return nil
}

// ApproveBundle is the local post-run approval operation. It requires an
// already integrity-checked bundle, signs the raw-evidence root with the
// out-of-band trusted key, then atomically updates indexes and checksums.
func ApproveBundle(root string) error {
	m, err := LoadManifest(root)
	if err != nil {
		return err
	}
	pub, err := trustedApprovalKey()
	if err != nil {
		return err
	}
	private, err := approvalPrivateKey()
	if err != nil {
		return err
	}
	if !bytes.Equal(private.Public().(ed25519.PublicKey), pub) {
		return errors.New("approval private key does not match trusted public key")
	}
	totalLimit, fileLimit := DefaultMaxArtifactBytes, DefaultMaxArtifactBytes
	if validateArtifactLimits(m.ArtifactMaxTotalBytes, m.ArtifactMaxFileBytes) == nil {
		totalLimit, fileLimit = m.ArtifactMaxTotalBytes, m.ArtifactMaxFileBytes
	}
	if err := verifyChecksumsWithLimits(root, totalLimit, fileLimit); err != nil {
		return err
	}
	contentRoot, err := computeContentRootWithLimits(root, m, totalLimit, fileLimit)
	if err != nil {
		return err
	}
	m.ContentRoot = contentRoot
	m.ApprovalPublicKey = hex.EncodeToString(pub)
	tuple, err := ApprovalTuple(m)
	if err != nil {
		return err
	}
	m.ApprovalSignature = hex.EncodeToString(ed25519.Sign(private, tuple))
	data, err := marshalRedactedJSON(m)
	if err != nil {
		return fmt.Errorf("serialize approved manifest: %w", err)
	}
	data = append(data, '\n')
	tmp, err := os.CreateTemp(root, ".manifest-approval-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	// After a successful rename this temporary path no longer exists.
	defer func() { _ = os.Remove(tmpName) }()
	if err = tmp.Chmod(0o640); err == nil {
		_, err = tmp.Write(data)
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmpName, filepath.Join(root, "manifest.json")); err != nil {
		return err
	}
	if err = RebuildBundleIndexes(root); err != nil {
		return err
	}
	if err = VerifyChecksums(root); err != nil {
		return err
	}
	return VerifyContentRoot(root, m)
}

func approvalPrivateKey() (ed25519.PrivateKey, error) {
	b, err := decodeApproval(os.Getenv(ApprovalPrivateKeyEnv), ed25519.PrivateKeySize)
	if err != nil {
		return nil, errors.New("approval private key is missing or invalid")
	}
	return ed25519.PrivateKey(b), nil
}
