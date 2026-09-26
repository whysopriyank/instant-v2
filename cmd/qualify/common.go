package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// CanonicalEndpoint is the single canonical instantd session endpoint string
// shared by all lanes of one campaign. endpoint_sha256 is the sha256 of this
// exact literal.
const CanonicalEndpoint = "ws://instantd:8080/runtime/session"

// CanonicalEndpointSHA256 returns hex(sha256(CanonicalEndpoint)).
func CanonicalEndpointSHA256() string {
	sum := sha256.Sum256([]byte(CanonicalEndpoint))
	return hex.EncodeToString(sum[:])
}

// ExpectedPackets is the exact packet inventory the gate requires in
// manifest handoffs (mirrors scripts/quality-release-gate.sh
// expected_packets). manifest refuses any other set.
var ExpectedPackets = []string{
	"CF-002", "CF-003", "DA-001", "DA-002", "DA-003", "DA-004", "DA-004V",
	"DA-005", "DA-006A", "DA-007", "DA-008A", "EV-001", "EV-002", "EV-003",
	"EV-004", "EV-005", "EV-006", "F-001", "F-002", "FR-001", "OP-003",
	"OP-005", "QR-001", "QR-003", "QR-005", "RT-001", "RT-002", "RT-003",
}

// ExpectedDecision and ExpectedProfile mirror the gate's expected values.
const (
	ExpectedDecision = "DEC-001-single-node-alpha-20260905"
	ExpectedProfile  = "single-node-alpha"
)

// ExpectedLanes mirrors the gate's expected_lanes selection.
var ExpectedLanes = map[string]string{
	"artifact":    "validate",
	"container":   "not_selected",
	"corpus":      "run",
	"external_v1": "not_selected",
	"hermetic":    "run",
	"owned_db":    "run",
	"performance": "not_selected",
	"recovery":    "artifact",
	"soak":        "artifact",
}

// RecoveryOutcomeIDs is the exact sorted outcome set the gate requires.
var RecoveryOutcomeIDs = []string{
	"crash-after-commit",
	"crash-before-commit",
	"crash-during-publication",
	"drain-idle",
	"drain-moderate",
	"drain-saturated",
	"postgres-restart",
}

// HostIdentity mirrors the gate's record host object.
type HostIdentity struct {
	ID      string `json:"id"`
	OS      string `json:"os"`
	Kernel  string `json:"kernel"`
	Arch    string `json:"arch"`
	Runtime string `json:"runtime"`
}

// LocalHostIdentity derives host identity from the current runtime.
// Kernel comes from uname when available; callers on the qualification host
// may override via flags and the value is recorded verbatim.
func LocalHostIdentity(id string) HostIdentity {
	return HostIdentity{
		ID:      id,
		OS:      runtime.GOOS,
		Kernel:  unameRelease(),
		Arch:    runtime.GOARCH,
		Runtime: runtime.Version(),
	}
}

// ArtifactRef is one gate-schema artifact entry.
type ArtifactRef struct {
	Path      string `json:"path"`
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size_bytes"`
}

// HashFileArtifact builds an ArtifactRef for a file under evidenceRoot,
// storing rel (slash-separated, relative) as the artifact path.
func HashFileArtifact(evidenceRoot, rel string) (ArtifactRef, error) {
	if err := checkRelativePath(rel); err != nil {
		return ArtifactRef{}, err
	}
	full := filepath.Join(evidenceRoot, filepath.FromSlash(rel))
	fi, err := os.Lstat(full)
	if err != nil {
		return ArtifactRef{}, fmt.Errorf("artifact %q: %w", rel, err)
	}
	if !fi.Mode().IsRegular() {
		return ArtifactRef{}, fmt.Errorf("artifact %q is not a regular file", rel)
	}
	if fi.Size() <= 0 {
		return ArtifactRef{}, fmt.Errorf("artifact %q is empty", rel)
	}
	data, err := os.ReadFile(full)
	if err != nil {
		return ArtifactRef{}, fmt.Errorf("artifact %q: %w", rel, err)
	}
	sum := sha256.Sum256(data)
	return ArtifactRef{
		Path:      rel,
		SHA256:    hex.EncodeToString(sum[:]),
		SizeBytes: int64(len(data)),
	}, nil
}

// checkRelativePath enforces the gate's safe_relative_file policy:
// non-empty, relative, no "..", no control characters, no empty/dot segments.
func checkRelativePath(rel string) error {
	if rel == "" || strings.HasPrefix(rel, "/") || strings.Contains(rel, "..") {
		return fmt.Errorf("unsafe artifact path %q", rel)
	}
	for _, r := range rel {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("unsafe artifact path %q", rel)
		}
	}
	for _, part := range strings.Split(rel, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("unsafe artifact path %q", rel)
		}
	}
	return nil
}

// ceilSeconds returns ceil(d) in whole seconds for non-negative durations.
func ceilSeconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Second - 1) / time.Second)
}

// parseArtifactSpec parses path:size:sha256 (path may not contain ':').
func parseArtifactSpec(spec string) (ArtifactRef, error) {
	parts := strings.Split(spec, ":")
	if len(parts) != 3 {
		return ArtifactRef{}, fmt.Errorf("artifact spec %q must be path:size:sha256", spec)
	}
	var size int64
	if _, err := fmt.Sscanf(parts[1], "%d", &size); err != nil || size <= 0 {
		return ArtifactRef{}, fmt.Errorf("artifact spec %q has invalid size", spec)
	}
	if err := checkRelativePath(parts[0]); err != nil {
		return ArtifactRef{}, err
	}
	if !isHex64(parts[2]) {
		return ArtifactRef{}, fmt.Errorf("artifact spec %q has invalid sha256", spec)
	}
	return ArtifactRef{Path: parts[0], SizeBytes: size, SHA256: parts[2]}, nil
}

func isHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func isHex40(s string) bool {
	if len(s) != 40 {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

// writeJSONFile writes indented JSON atomically (create-only, no overwrite).
func writeJSONFile(path string, v any) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if _, err := f.Write(raw); err != nil {
		_ = os.Remove(path)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = os.Remove(path)
		return err
	}
	return f.Close()
}
