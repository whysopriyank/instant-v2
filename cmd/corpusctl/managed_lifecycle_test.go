package main

// CF-002 managed-record end-to-end and probe tests.
//
// TestCF002ManagedRecordEndToEnd drives the supported production entry point
// (`corpusctl --mode managed-record`), not a duplicate test-only
// implementation, and verifies the persistent candidate-bound evidence it
// leaves behind. Probe tests prove fail-closed behavior with zero residue.

import (
	"bytes"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/corpus"
)

func cf002E2ERepoRoot(t *testing.T) string {
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

func cf002E2EHead(t *testing.T, root string) string {
	t.Helper()
	out, err := exec.Command("git", "-C", root, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(out))
}

func TestCF002ManagedRecordEndToEnd(t *testing.T) {
	if os.Getenv("INSTANT_TEST_INTEGRATION") != "1" || os.Getenv("DATABASE_URL") == "" {
		t.Skip("integration test: set INSTANT_TEST_INTEGRATION=1 and DATABASE_URL")
	}
	root := cf002E2ERepoRoot(t)
	if _, dirty, err := corpus.GitIdentity(root); err != nil {
		t.Fatal(err)
	} else if dirty {
		t.Skip("managed-record acceptance requires a clean worktree")
	}
	head := cf002E2EHead(t, root)

	outDir := filepath.Join(t.TempDir(), "run-1")
	var out, stderr bytes.Buffer
	if code := run([]string{"--mode", "managed-record", "--output-dir", outDir, "--repo", root}, &out, &stderr); code != 0 {
		t.Fatalf("managed-record failed code=%d stderr=%s", code, &stderr)
	}
	if !strings.Contains(out.String(), "PASS managed-record "+head) {
		t.Fatalf("missing pass line for %s: %s (stderr=%s)", head, &out, &stderr)
	}

	dirFI, err := os.Stat(outDir)
	if err != nil || dirFI.Mode().Perm() != 0700 {
		t.Fatalf("output dir not 0700: %v %v", dirFI, err)
	}
	manifestBytes, err := os.ReadFile(filepath.Join(outDir, "manifest.json"))
	if err != nil {
		t.Fatalf("manifest missing: %v", err)
	}
	var manifest corpus.CaptureManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatalf("manifest invalid: %v", err)
	}
	if manifest.Candidate.GitSHA != head {
		t.Fatalf("manifest SHA %q != HEAD %q", manifest.Candidate.GitSHA, head)
	}
	if manifest.Candidate.GitDirty {
		t.Fatalf("manifest records dirty=true on a clean candidate")
	}
	if len(manifest.Candidate.BinarySHA256) != 64 {
		t.Fatalf("binary hash missing: %q", manifest.Candidate.BinarySHA256)
	}
	if manifest.Candidate.PID <= 0 {
		t.Fatalf("PID missing: %d", manifest.Candidate.PID)
	}
	if err := corpus.RequireLoopbackEndpoint(manifest.Candidate.Endpoint); err != nil {
		t.Fatalf("endpoint not loopback: %v", err)
	}
	if len(manifest.Candidate.ConfigDigest) != 64 {
		t.Fatalf("config digest missing: %q", manifest.Candidate.ConfigDigest)
	}
	if !strings.HasPrefix(manifest.Candidate.FixtureDB, "instant_test_") {
		t.Fatalf("fixture DB not owned: %q", manifest.Candidate.FixtureDB)
	}
	wantPre := `apps=1 title="cf002-managed" attrs=0 triples=0 idents=0 rows=[]`
	if manifest.Precondition != wantPre {
		t.Fatalf("precondition = %s; want %s", manifest.Precondition, wantPre)
	}
	if !strings.HasPrefix(manifest.FinalState, `apps=1 title="cf002-managed" attrs=2 triples=2 idents=2 rows=[todos/id="`) ||
		!strings.HasSuffix(manifest.FinalState, `todos/title="cf002-managed-todo"]`) {
		t.Fatalf("final state not exact: %s", manifest.FinalState)
	}
	if len(manifest.Artifacts) != 4 {
		t.Fatalf("artifacts = %d, want 4 (http+sse x s1/s2)", len(manifest.Artifacts))
	}
	for _, name := range []string{
		"cf002-s1.http.evidence.json", "cf002-s1.sse.evidence.json",
		"cf002-s2.http.evidence.json", "cf002-s2.sse.evidence.json",
	} {
		b, err := os.ReadFile(filepath.Join(outDir, name))
		if err != nil {
			t.Fatalf("artifact %s missing: %v", name, err)
		}
		if !bytes.Contains(b, []byte("managed-record")) {
			t.Fatalf("artifact %s missing managed-record marker", name)
		}
		if fi, err := os.Stat(filepath.Join(outDir, name)); err != nil || fi.Mode().Perm() != 0600 {
			t.Fatalf("artifact %s not 0600: %v %v", name, fi, err)
		}
	}
	httpRaw, err := os.ReadFile(filepath.Join(outDir, "cf002-s1.http.evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	// []byte fields marshal as base64; decode through the corpus types and
	// assert on the exact raw bytes, not the envelope encoding.
	var httpEnv struct {
		Raw        corpus.HTTPExchange `json:"raw"`
		Normalized string              `json:"normalized"`
	}
	if err := json.Unmarshal(httpRaw, &httpEnv); err != nil {
		t.Fatalf("http evidence invalid: %v", err)
	}
	if !bytes.Contains(httpEnv.Raw.Response.Body, []byte(`"ok":true`)) {
		t.Fatalf("http artifact missing raw health body: %s", httpEnv.Raw.Response.Body)
	}
	var httpNorm map[string]any
	if err := json.Unmarshal([]byte(httpEnv.Normalized), &httpNorm); err != nil || httpNorm["ok"] != true {
		t.Fatalf("http artifact missing canonical body: %q", httpEnv.Normalized)
	}
	sseRaw, err := os.ReadFile(filepath.Join(outDir, "cf002-s1.sse.evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	var sseEnv struct {
		RawRecords []corpus.SSERecord `json:"rawRecords"`
		Normalized []corpus.SSERecord `json:"normalized"`
	}
	if err := json.Unmarshal(sseRaw, &sseEnv); err != nil {
		t.Fatalf("sse evidence invalid: %v", err)
	}
	if len(sseEnv.RawRecords) != 1 || len(sseEnv.Normalized) != 1 {
		t.Fatalf("sse artifact record counts = %d/%d", len(sseEnv.RawRecords), len(sseEnv.Normalized))
	}
	if !bytes.Contains(sseEnv.RawRecords[0].Raw, []byte("session-id")) ||
		!bytes.Contains(sseEnv.RawRecords[0].Data, []byte(`"op":"init-ok"`)) {
		t.Fatalf("sse artifact missing raw hello framing")
	}
	if !bytes.Contains(sseEnv.Normalized[0].Normalized, []byte(corpus.NormalizedSessionID)) {
		t.Fatalf("sse artifact missing canonical session mask")
	}
	if err := corpus.VerifyCaptureManifest(outDir, manifest); err != nil {
		t.Fatalf("manifest verification: %v", err)
	}
	// No secret values in the manifest or evidence headers: the storage
	// secret, the admin token header value, and the database URL must not
	// appear. (Raw SSE hello retains its short-lived session token inside
	// the private directory, matching the established private-evidence
	// posture.)
	for _, f := range []string{"manifest.json", "cf002-s1.http.evidence.json"} {
		b, _ := os.ReadFile(filepath.Join(outDir, f))
		for _, secret := range []string{"cf002-managed-record-secret", os.Getenv("DATABASE_URL")} {
			if secret != "" && bytes.Contains(b, []byte(secret)) {
				t.Fatalf("%s leaks secret material", f)
			}
		}
	}

	// The daemon recorded in the manifest must be stopped: its endpoint
	// refuses connections after corpusctl exits.
	endpoint := strings.TrimPrefix(manifest.Candidate.Endpoint, "http://")
	if conn, err := net.DialTimeout("tcp", endpoint, 500*time.Millisecond); err == nil {
		_ = conn.Close()
		t.Fatalf("managed daemon still listening on %s after exit", endpoint)
	}

	// Output reuse fails closed without changing the first evidence.
	before, err := os.ReadFile(filepath.Join(outDir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var out2, stderr2 bytes.Buffer
	if code := run([]string{"--mode", "managed-record", "--output-dir", outDir, "--repo", root}, &out2, &stderr2); code == 0 {
		t.Fatal("output reuse reported success")
	} else if !strings.Contains(stderr2.String(), "already exists") {
		t.Fatalf("reuse error unstable: %s", stderr2.String())
	}
	after, err := os.ReadFile(filepath.Join(outDir, "manifest.json"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("output reuse changed the first evidence")
	}
}

// TestCF002ManagedRecordRequiresCleanTree proves dirty-worktree fail-closed
// without touching a database: a scratch git repo with an untracked file is
// rejected before any build, daemon, or fixture work, and no output appears.
func TestCF002ManagedRecordRequiresCleanTree(t *testing.T) {
	repo := t.TempDir()
	for _, args := range [][]string{
		{"init", "-q", repo},
		{"-C", repo, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "--allow-empty", "-m", "init"},
	} {
		if out, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	if err := os.WriteFile(filepath.Join(repo, "dirty.txt"), []byte("untracked"), 0600); err != nil {
		t.Fatal(err)
	}
	outDir := filepath.Join(t.TempDir(), "dirty-out")
	var out, stderr bytes.Buffer
	if code := run([]string{"--mode", "managed-record", "--output-dir", outDir, "--repo", repo}, &out, &stderr); code == 0 {
		t.Fatal("dirty worktree reported success")
	} else if !strings.Contains(stderr.String(), "clean Git worktree") {
		t.Fatalf("dirty-tree error unstable: %s", stderr.String())
	}
	if _, err := os.Stat(outDir); !os.IsNotExist(err) {
		t.Fatalf("dirty worktree run produced output at %s", outDir)
	}
}

// TestCF002ManagedRecordRejectsSymlinkedOutput proves symlink fail-closed
// before any database or daemon work: nothing is created under the link.
func TestCF002ManagedRecordRejectsSymlinkedOutput(t *testing.T) {
	base := t.TempDir()
	target := filepath.Join(base, "real")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link-out")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	var out, stderr bytes.Buffer
	if code := run([]string{"--mode", "managed-record", "--output-dir", link, "--repo", cf002E2ERepoRoot(t)}, &out, &stderr); code == 0 {
		t.Fatal("symlinked output reported success")
	} else if !strings.Contains(stderr.String(), "symlink") {
		t.Fatalf("symlink error unstable: %s", stderr.String())
	}
	entries, err := os.ReadDir(target)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("symlinked output run wrote %d entries under the target", len(entries))
	}
}

// TestCF002ManagedRecordSkipResetFails proves an omitted fixture reset goes
// red through the production entry point: the second scenario's exact
// precondition assertion fails and no eligible manifest is published.
func TestCF002ManagedRecordSkipResetFails(t *testing.T) {
	if os.Getenv("INSTANT_TEST_INTEGRATION") != "1" || os.Getenv("DATABASE_URL") == "" {
		t.Skip("integration test: set INSTANT_TEST_INTEGRATION=1 and DATABASE_URL")
	}
	root := cf002E2ERepoRoot(t)
	if _, dirty, err := corpus.GitIdentity(root); err != nil {
		t.Fatal(err)
	} else if dirty {
		t.Skip("managed-record acceptance requires a clean worktree")
	}
	managedProbeBypassReset = true
	defer func() { managedProbeBypassReset = false }()
	outDir := filepath.Join(t.TempDir(), "noreset-out")
	var out, stderr bytes.Buffer
	if code := run([]string{"--mode", "managed-record", "--output-dir", outDir, "--repo", root}, &out, &stderr); code == 0 {
		t.Fatal("omitted reset reported success")
	} else if !strings.Contains(stderr.String(), "precondition") {
		t.Fatalf("reset-omission error unstable: %s", stderr.String())
	}
	if _, err := os.Stat(filepath.Join(outDir, "manifest.json")); !os.IsNotExist(err) {
		t.Fatal("omitted-reset run published an eligible manifest")
	}
}

// TestCF002WSRecordCreatesNoArtifact proves the WS recording exclusion is
// enforced: direct `record --transport ws` fails with the stable documented
// error and creates no output directory or eligible artifact.
func TestCF002WSRecordCreatesNoArtifact(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ws-out")
	var out, stderr bytes.Buffer
	code := run([]string{
		"--mode", "record", "--transport", "ws",
		"--corpus", "../../corpus/00-smoke.ndjson",
		"--target", "http://127.0.0.1:1",
		"--output-dir", dir,
		"--endpoint-id", "ep", "--source-id", "src", "--fixture-id", "fix",
	}, &out, &stderr)
	if code == 0 {
		t.Fatal("WS record reported success")
	}
	if !strings.Contains(stderr.String(), "record mode is unsupported for ws") {
		t.Fatalf("unstable WS exclusion error: %s", stderr.String())
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("WS record produced output at %s", dir)
	}
}
