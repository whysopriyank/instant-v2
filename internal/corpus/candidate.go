// Package corpus implements scenario replay and explicit wire comparisons.
// Only a successful live differential run establishes agreement with v1.
package corpus

// Candidate-bound provenance for managed local recording (CF-002).
//
// The historical `record` path stores caller-supplied endpoint/source/fixture
// IDs as assertions. A managed local lifecycle instead derives identity from
// the locally built binary, the launched process, the bound loopback
// endpoint, the revision control state, and the owned fixture, and re-verifies
// that identity before, during, and after capture. Secrets are never part of
// any digest or manifest: callers record only presence bits for secrets.

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
)

// CandidateIdentity is the proven (not caller-asserted) identity of one
// managed local capture. Every field is derived locally: git state from the
// repository, binary digest from the built file, PID/executable from the
// launched process, endpoint from the bound loopback listener, config digest
// from non-secret configuration, fixture IDs from the owned database.
type CandidateIdentity struct {
	GitSHA       string `json:"gitSha"`
	GitDirty     bool   `json:"gitDirty"`
	GoVersion    string `json:"goVersion"`
	GOOS         string `json:"goos"`
	GOARCH       string `json:"goarch"`
	BinaryPath   string `json:"binaryPath"`
	BinarySHA256 string `json:"binarySha256"`
	PID          int    `json:"pid"`
	Executable   string `json:"executable"`
	Endpoint     string `json:"endpoint"`
	ConfigDigest string `json:"configDigest"`
	FixtureDB    string `json:"fixtureDb"`
	FixtureApp   string `json:"fixtureApp,omitempty"`
}

// GitIdentity returns the full HEAD SHA and dirty state of the repository at
// dir. Any output of `git status --porcelain` (including untracked files)
// counts as dirty.
func GitIdentity(repoDir string) (sha string, dirty bool, err error) {
	shaOut, err := exec.Command("git", "-C", repoDir, "rev-parse", "HEAD").Output()
	if err != nil {
		return "", false, fmt.Errorf("read candidate revision: %w", err)
	}
	sha = strings.TrimSpace(string(shaOut))
	if !isCommit(sha) {
		return "", false, fmt.Errorf("candidate revision %q is not a full commit SHA", sha)
	}
	statusOut, err := exec.Command("git", "-C", repoDir, "status", "--porcelain").Output()
	if err != nil {
		return "", false, fmt.Errorf("read candidate dirty state: %w", err)
	}
	return sha, len(statusOut) > 0, nil
}

// HashFile returns the hex SHA-256 of the file at path.
func HashFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// SHA256Hex returns the hex SHA-256 of b.
func SHA256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// ConfigDigest hashes non-secret configuration over sorted key/value pairs.
// Callers must exclude every secret value and record only presence bits
// (for example "storage-secret-is-set=true"). The digest never contains
// secret material because it never receives it.
func ConfigDigest(fields map[string]string) string {
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write([]byte(fields[k]))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// RequireLoopbackEndpoint rejects any endpoint that is not a local loopback
// listener. It accepts absolute URLs and bare host:port forms. Hostnames
// other than "localhost" and non-loopback IPs fail closed so a managed
// capture can never be bound to a remote or wildcard listener.
func RequireLoopbackEndpoint(endpoint string) error {
	host := strings.TrimSpace(endpoint)
	if host == "" {
		return errors.New("endpoint identity is required")
	}
	if strings.Contains(host, "://") {
		u, err := url.Parse(host)
		if err != nil || u.Hostname() == "" {
			return fmt.Errorf("invalid endpoint URL %q", endpoint)
		}
		host = u.Hostname()
	} else if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("endpoint %q is not a loopback address", endpoint)
	}
	if !ip.IsLoopback() {
		return fmt.Errorf("endpoint %q is not a loopback address", endpoint)
	}
	return nil
}

// VerifyProcessAlive fails closed when pid names no live process. A stale or
// reused PID held without a process handle is rejected: callers that launched
// the daemon must keep its process handle open (which pins the PID against
// reuse) and call this before, during, and after capture.
func VerifyProcessAlive(pid int) error {
	if pid <= 0 {
		return fmt.Errorf("invalid pid %d", pid)
	}
	if runtime.GOOS == "windows" {
		return errors.New("process identity is unsupported on windows")
	}
	if err := syscall.Kill(pid, 0); err == nil {
		return nil
	} else if errors.Is(err, syscall.EPERM) {
		return nil
	} else if errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("process %d is not running", pid)
	} else {
		return fmt.Errorf("probe process %d: %w", pid, err)
	}
}

// VerifyBinaryDigest fails when the file at path no longer hashes to want.
// Managed captures call this before and after every scenario so a replaced
// binary cannot be mistaken for the recorded candidate.
func VerifyBinaryDigest(path, want string) error {
	got, err := HashFile(path)
	if err != nil {
		return err
	}
	if got != strings.ToLower(strings.TrimSpace(want)) {
		return fmt.Errorf("binary %q changed during capture", path)
	}
	return nil
}

// VerifyGitIdentity fails when the repository revision or dirty state no
// longer matches the recorded candidate.
func VerifyGitIdentity(repoDir, wantSHA string, wantDirty bool) error {
	sha, dirty, err := GitIdentity(repoDir)
	if err != nil {
		return err
	}
	if sha != wantSHA || dirty != wantDirty {
		return fmt.Errorf("candidate revision changed during capture (was %s dirty=%v)", wantSHA, wantDirty)
	}
	return nil
}

// Validate checks every required identity field and format without trusting
// any caller assertion about what the values prove.
func (c CandidateIdentity) Validate() error {
	if !isCommit(c.GitSHA) {
		return fmt.Errorf("invalid git SHA %q", c.GitSHA)
	}
	if strings.TrimSpace(c.GoVersion) == "" || !strings.HasPrefix(c.GoVersion, "go") {
		return fmt.Errorf("invalid go version %q", c.GoVersion)
	}
	if strings.TrimSpace(c.GOOS) == "" || strings.TrimSpace(c.GOARCH) == "" {
		return fmt.Errorf("invalid goos/goarch %q/%q", c.GOOS, c.GOARCH)
	}
	if strings.TrimSpace(c.BinaryPath) == "" {
		return errors.New("binary path is required")
	}
	if len(c.BinarySHA256) != 64 {
		return fmt.Errorf("invalid binary SHA-256 %q", c.BinarySHA256)
	}
	if _, err := hex.DecodeString(c.BinarySHA256); err != nil {
		return fmt.Errorf("invalid binary SHA-256 %q", c.BinarySHA256)
	}
	if c.PID <= 0 {
		return fmt.Errorf("invalid pid %d", c.PID)
	}
	if strings.TrimSpace(c.Executable) == "" {
		return errors.New("executable identity is required")
	}
	if err := RequireLoopbackEndpoint(c.Endpoint); err != nil {
		return err
	}
	if len(c.ConfigDigest) != 64 {
		return fmt.Errorf("invalid config digest %q", c.ConfigDigest)
	}
	if _, err := hex.DecodeString(c.ConfigDigest); err != nil {
		return fmt.Errorf("invalid config digest %q", c.ConfigDigest)
	}
	if !strings.HasPrefix(c.FixtureDB, "instant_test_") {
		return fmt.Errorf("fixture database %q is not an owned instant_test_* fixture", c.FixtureDB)
	}
	if c.FixtureApp != "" && !isUUIDish(c.FixtureApp) {
		return fmt.Errorf("fixture app %q is not a UUID", c.FixtureApp)
	}
	return nil
}

// VerifyLive re-proves the launch-time identity against current state: the
// repository revision/dirty bit must be unchanged, the binary file must still
// hash identically, and the recorded PID must still be alive. Endpoint
// liveness is proven separately by a successful request to the recorded
// loopback URL; fixture identity is proven by the owned database handle.
func (c CandidateIdentity) VerifyLive(repoDir string) error {
	if err := c.Validate(); err != nil {
		return err
	}
	if err := VerifyGitIdentity(repoDir, c.GitSHA, c.GitDirty); err != nil {
		return err
	}
	if err := VerifyBinaryDigest(c.BinaryPath, c.BinarySHA256); err != nil {
		return err
	}
	if err := VerifyProcessAlive(c.PID); err != nil {
		return err
	}
	return nil
}

// ArtifactEntry binds one evidence file to its digest.
type ArtifactEntry struct {
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

// CaptureManifest binds proven candidate identity, exact fixture
// pre/post-conditions, and per-artifact checksums into one verifiable unit.
// Precondition and FinalState are exact (for example sorted triple dumps or
// counts); equality across scenarios after reset proves reset equivalence.
type CaptureManifest struct {
	Candidate    CandidateIdentity `json:"candidate"`
	Precondition string            `json:"precondition"`
	FinalState   string            `json:"finalState"`
	Artifacts    []ArtifactEntry   `json:"artifacts"`
}

// ManifestArtifacts digests already-published evidence files in dir.
func ManifestArtifacts(dir string, names []string) ([]ArtifactEntry, error) {
	entries := make([]ArtifactEntry, 0, len(names))
	for _, name := range names {
		if strings.ContainsAny(name, `/\`) || name == "" || name == "." || name == ".." {
			return nil, fmt.Errorf("invalid artifact name %q", name)
		}
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		entries = append(entries, ArtifactEntry{Name: name, SHA256: SHA256Hex(b), Size: int64(len(b))})
	}
	return entries, nil
}

// VerifyCaptureManifest recomputes every artifact digest in dir and rejects
// any mismatch, missing file, or invalid candidate identity. A checksum
// mismatch proves the artifact is not the recorded evidence.
func VerifyCaptureManifest(dir string, m CaptureManifest) error {
	if err := m.Candidate.Validate(); err != nil {
		return err
	}
	if len(m.Artifacts) == 0 {
		return errors.New("manifest has no artifacts")
	}
	for _, a := range m.Artifacts {
		if strings.ContainsAny(a.Name, `/\`) || a.Name == "" {
			return fmt.Errorf("invalid artifact name %q", a.Name)
		}
		b, err := os.ReadFile(filepath.Join(dir, a.Name))
		if err != nil {
			return err
		}
		if int64(len(b)) != a.Size || SHA256Hex(b) != strings.ToLower(a.SHA256) {
			return fmt.Errorf("artifact %q checksum mismatch", a.Name)
		}
	}
	return nil
}
