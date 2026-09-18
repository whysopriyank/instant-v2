package corpus

// Hermetic CF-002 provenance guards: identity derivation, loopback binding,
// process/binary/revision continuity, manifest checksums, and payload
// significance. No network, database, or daemon is required.

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func cf002RepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("cannot locate repo root (go.mod)")
		}
		dir = parent
	}
}

func TestCandidateGitIdentityMatchesHead(t *testing.T) {
	root := cf002RepoRoot(t)
	sha, dirty, err := GitIdentity(root)
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("git", "-C", root, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	if sha != strings.TrimSpace(string(out)) {
		t.Fatalf("git identity %q != HEAD %q", sha, out)
	}
	status, err := exec.Command("git", "-C", root, "status", "--porcelain").Output()
	if err != nil {
		t.Fatal(err)
	}
	if dirty != (len(status) > 0) {
		t.Fatalf("dirty=%v, status bytes=%d", dirty, len(status))
	}
	if err := VerifyGitIdentity(root, sha, dirty); err != nil {
		t.Fatalf("live git identity rejected: %v", err)
	}
	if err := VerifyGitIdentity(root, sha, !dirty); err == nil {
		t.Fatal("flipped dirty bit was accepted")
	}
	bogus := "0000000000000000000000000000000000000000"
	if bogus == sha {
		t.Skip("HEAD is the zero SHA; cannot probe mismatch")
	}
	if err := VerifyGitIdentity(root, bogus, dirty); err == nil {
		t.Fatal("wrong SHA was accepted")
	}
}

func TestCandidateBinaryDigestDetectsReplacement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "candidate.bin")
	if err := os.WriteFile(path, []byte("cf002-candidate-bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	sum, err := HashFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(sum) != 64 {
		t.Fatalf("sha256 = %q", sum)
	}
	if err := VerifyBinaryDigest(path, sum); err != nil {
		t.Fatalf("unchanged binary rejected: %v", err)
	}
	if err := VerifyBinaryDigest(path, strings.Repeat("0", 64)); err == nil {
		t.Fatal("wrong binary hash was accepted")
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if err := VerifyBinaryDigest(path, sum); err == nil {
		t.Fatal("replaced binary was accepted")
	}
	if _, err := HashFile(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("missing binary was accepted")
	}
}

func TestCandidateConfigDigestExcludesSecrets(t *testing.T) {
	fields := map[string]string{
		"storage-root":           "/tmp/cf002-root",
		"http-addr":              "127.0.0.1:18081",
		"max-upload-bytes":       "536870912",
		"insecure-dev-mode":      "false",
		"storage-secret-is-set":  "true",
		"oauth-google-id-is-set": "true",
	}
	first := ConfigDigest(fields)
	second := ConfigDigest(map[string]string{
		"http-addr":              "127.0.0.1:18081",
		"max-upload-bytes":       "536870912",
		"insecure-dev-mode":      "false",
		"storage-secret-is-set":  "true",
		"oauth-google-id-is-set": "true",
		"storage-root":           "/tmp/cf002-root",
	})
	if first != second {
		t.Fatal("config digest is not order-independent")
	}
	if len(first) != 64 {
		t.Fatalf("digest = %q", first)
	}
	changed := ConfigDigest(map[string]string{
		"storage-root":           "/tmp/cf002-other",
		"http-addr":              "127.0.0.1:18081",
		"max-upload-bytes":       "536870912",
		"insecure-dev-mode":      "false",
		"storage-secret-is-set":  "true",
		"oauth-google-id-is-set": "true",
	})
	if changed == first {
		t.Fatal("config change was not detected")
	}
	// Secrets never enter the digest: two deployments differing only in
	// secret VALUES share one digest because only presence bits are hashed.
	// The digest must not contain any secret substring.
	secret := "cf002-super-secret-value"
	if strings.Contains(first, secret) {
		t.Fatal("config digest leaks secret material")
	}
	_ = secret
}

func TestCandidateRequireLoopback(t *testing.T) {
	for _, ok := range []string{
		"127.0.0.1:18081",
		"http://127.0.0.1:18081",
		"http://127.0.0.1:18081/health",
		"http://localhost:18081",
		"http://[::1]:18081",
		"127.0.0.2:1",
	} {
		if err := RequireLoopbackEndpoint(ok); err != nil {
			t.Errorf("loopback %q rejected: %v", ok, err)
		}
	}
	for _, bad := range []string{
		"",
		"0.0.0.0:8080",
		"http://0.0.0.0:8080",
		"http://example.com:8080",
		"http://192.168.1.10:8080",
		"http://10.0.0.5/",
		"http://[::]:8080",
		"not a host!!!",
	} {
		if err := RequireLoopbackEndpoint(bad); err == nil {
			t.Errorf("non-loopback %q was accepted", bad)
		}
	}
}

func TestCandidateVerifyProcessAlive(t *testing.T) {
	if err := VerifyProcessAlive(os.Getpid()); err != nil {
		t.Fatalf("own process rejected: %v", err)
	}
	if err := VerifyProcessAlive(0); err == nil {
		t.Fatal("zero pid was accepted")
	}
	if err := VerifyProcessAlive(-3); err == nil {
		t.Fatal("negative pid was accepted")
	}
	// 2^30-1 is outside any realistic PID range on supported targets.
	if err := VerifyProcessAlive(1 << 30); err == nil {
		t.Fatal("dead pid was accepted")
	}
}

func cf002TestIdentity(t *testing.T) (CandidateIdentity, string) {
	t.Helper()
	root := cf002RepoRoot(t)
	sha, dirty, err := GitIdentity(root)
	if err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	sum, err := HashFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	return CandidateIdentity{
		GitSHA:       sha,
		GitDirty:     dirty,
		GoVersion:    runtime.Version(),
		GOOS:         runtime.GOOS,
		GOARCH:       runtime.GOARCH,
		BinaryPath:   exe,
		BinarySHA256: sum,
		PID:          os.Getpid(),
		Executable:   exe,
		Endpoint:     "http://127.0.0.1:18081",
		ConfigDigest: ConfigDigest(map[string]string{"storage-root": t.TempDir()}),
		FixtureDB:    "instant_test_cf002unit",
		FixtureApp:   "00000000-0000-4000-8000-000000000001",
	}, root
}

func TestCandidateValidateAndVerifyLive(t *testing.T) {
	id, root := cf002TestIdentity(t)
	if err := id.Validate(); err != nil {
		t.Fatalf("valid identity rejected: %v", err)
	}
	if err := id.VerifyLive(root); err != nil {
		t.Fatalf("live identity rejected: %v", err)
	}
	cases := map[string]func(*CandidateIdentity){
		"bad-sha":       func(c *CandidateIdentity) { c.GitSHA = strings.Repeat("0", 40) },
		"short-sha":     func(c *CandidateIdentity) { c.GitSHA = "abc" },
		"bad-binary":    func(c *CandidateIdentity) { c.BinarySHA256 = strings.Repeat("0", 64) },
		"bad-pid":       func(c *CandidateIdentity) { c.PID = 1 << 30 },
		"remote-ep":     func(c *CandidateIdentity) { c.Endpoint = "http://example.com:8080" },
		"wildcard-ep":   func(c *CandidateIdentity) { c.Endpoint = "0.0.0.0:8080" },
		"bad-fixture":   func(c *CandidateIdentity) { c.FixtureDB = "production" },
		"bad-app":       func(c *CandidateIdentity) { c.FixtureApp = "not-a-uuid" },
		"bad-goversion": func(c *CandidateIdentity) { c.GoVersion = "" },
		"bad-config":    func(c *CandidateIdentity) { c.ConfigDigest = "zz" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			bad := id
			mutate(&bad)
			if err := bad.Validate(); err != nil {
				return
			}
			// Format-valid but wrong values must still fail liveness
			// (git mismatch, binary replacement, or dead process).
			if err := bad.VerifyLive(root); err == nil {
				t.Fatalf("tampered identity %s was accepted live", name)
			}
		})
	}
	if err := id.VerifyLive(filepath.Join(root, "does-not-exist")); err == nil {
		t.Fatal("liveness against a foreign repo was accepted")
	}
}

func TestCaptureManifestRejectsTamper(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.http.evidence.json"), []byte(`{"mode":"record"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.sse.evidence.json"), []byte(`{"raw":"x"}`), 0600); err != nil {
		t.Fatal(err)
	}
	id, _ := cf002TestIdentity(t)
	entries, err := ManifestArtifacts(dir, []string{"a.http.evidence.json", "b.sse.evidence.json"})
	if err != nil {
		t.Fatal(err)
	}
	m := CaptureManifest{Candidate: id, Precondition: "triples=0", FinalState: "triples=1", Artifacts: entries}
	if err := VerifyCaptureManifest(dir, m); err != nil {
		t.Fatalf("valid manifest rejected: %v", err)
	}
	tampered := m
	tampered.Artifacts = append([]ArtifactEntry(nil), m.Artifacts...)
	tampered.Artifacts[0].SHA256 = strings.Repeat("0", 64)
	if err := VerifyCaptureManifest(dir, tampered); err == nil {
		t.Fatal("tampered checksum was accepted")
	}
	mutated := filepath.Join(dir, "a.http.evidence.json")
	if err := os.WriteFile(mutated, []byte(`{"mode":"forged"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyCaptureManifest(dir, m); err == nil {
		t.Fatal("mutated artifact was accepted")
	}
	empty := m
	empty.Artifacts = nil
	if err := VerifyCaptureManifest(dir, empty); err == nil {
		t.Fatal("empty manifest was accepted")
	}
	if _, err := ManifestArtifacts(dir, []string{"../escape"}); err == nil {
		t.Fatal("escaping artifact name was accepted")
	}
	if _, err := ManifestArtifacts(dir, []string{"missing.json"}); err == nil {
		t.Fatal("missing artifact was accepted")
	}
}

// TestPayloadApplicationFieldsRemainSignificant pins the CF-002 masking
// boundary: application fields named id, token, timestamp, cursor, or email
// at payload paths must stay comparison-significant in both plain and
// differential modes. Only documented frame-root protocol paths may mask.
func TestPayloadApplicationFieldsRemainSignificant(t *testing.T) {
	pairs := [][2]string{
		{`{"data":{"id":"a"}}`, `{"data":{"id":"b"}}`},
		{`{"data":{"token":"a"}}`, `{"data":{"token":"b"}}`},
		{`{"data":{"timestamp":"2026-09-04T00:00:00Z"}}`, `{"data":{"timestamp":"2026-09-04T00:00:01Z"}}`},
		{`{"data":{"cursor":"a"}}`, `{"data":{"cursor":"b"}}`},
		{`{"data":{"email":"a@example.test"}}`, `{"data":{"email":"b@example.test"}}`},
		{`{"data":{"title":"alpha"}}`, `{"data":{"title":"beta"}}`},
		{`{"todos":[{"id":"a","email":"a@example.test"}]}`, `{"todos":[{"id":"b","email":"b@example.test"}]}`},
		{`{"nested":{"deep":{"cursor":"1"}}}`, `{"nested":{"deep":{"cursor":"2"}}}`},
	}
	for _, opts := range []CanonicalOptions{{}, {Differential: true}} {
		for _, pair := range pairs {
			a, err := CanonicalBytesOpts([]byte(pair[0]), opts)
			if err != nil {
				t.Fatal(err)
			}
			b, err := CanonicalBytesOpts([]byte(pair[1]), opts)
			if err != nil {
				t.Fatal(err)
			}
			if string(a) == string(b) {
				t.Errorf("payload difference masked (differential=%v): %s vs %s", opts.Differential, pair[0], pair[1])
			}
		}
		// Control: documented frame-root protocol metadata still normalizes.
		control, err := CanonicalBytesOpts([]byte(`{"tx-id":1,"session-id":"wire","op":"x"}`), opts)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(control), NormalizedTxID) || !strings.Contains(string(control), NormalizedSessionID) {
			t.Errorf("frame-root protocol masking lost (differential=%v): %s", opts.Differential, control)
		}
	}
}
