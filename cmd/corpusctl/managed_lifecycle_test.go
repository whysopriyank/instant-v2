package main

// CF-002 managed-record end-to-end and probe tests.
//
// TestCF002ManagedRecordEndToEnd drives the supported production entry point
// (`corpusctl --mode managed-record`), not a duplicate test-only
// implementation, and verifies the persistent candidate-bound evidence it
// leaves behind. Probe tests prove fail-closed behavior with zero residue.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
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

// cf002ShimSource is a healthy-but-different compatible binary: it serves
// /health with ok:true db:true and /runtime/sse with an init-ok hello. It is
// deliberately NOT built from the recorded candidate, so its SHA-256 differs
// from any candidate build. A /bin/echo stand-in would only prove health
// checking; this shim stays healthy yet must still be rejected because no
// override path exists.
const cf002ShimSource = `package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
)

func main() {
	addr := os.Getenv("CF002_SHIM_ADDR")
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, ` + "`" + `{"ok":true,"db":true}` + "`" + `)
	})
	mux.HandleFunc("/runtime/sse", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = fmt.Fprint(w, "data: {\"op\":\"init-ok\",\"session-id\":\"wire-shim-123\"}\n\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	})
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	_ = http.Serve(ln, mux)
}
`

func cf002BuildShim(t *testing.T, dir string) string {
	t.Helper()
	src := filepath.Join(dir, "shim.go")
	if err := os.WriteFile(src, []byte(cf002ShimSource), 0600); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(dir, "shim-instantd")
	cmd := exec.Command("go", "build", "-o", bin, src)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build healthy shim: %v\n%s", err, out)
	}
	if _, err := os.Stat(bin); err != nil {
		t.Fatalf("shim binary missing: %v", err)
	}
	return bin
}

func cf002FreeLoopbackAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// TestCF002ManagedRecordRejectsInstantdBinaryOverride proves candidate
// binding by removal: --instantd-binary no longer exists, so even a healthy
// (healthy-but-different) compatible binary is rejected before any build,
// capture, or output reservation. It first proves the shim itself is healthy,
// then proves the recorder refuses the override with no output directory or
// eligible manifest and a diagnostic free of binary contents, credentials,
// or database URLs.
func TestCF002ManagedRecordRejectsInstantdBinaryOverride(t *testing.T) {
	base := t.TempDir()
	shimBin := cf002BuildShim(t, base)
	shimSHA, err := corpus.HashFile(shimBin)
	if err != nil || len(shimSHA) != 64 {
		t.Fatalf("shim hash invalid: %v %q", err, shimSHA)
	}

	// Prove the shim is healthy (not a /bin/echo stand-in): it must serve
	// /health with ok:true db:true and /runtime/sse with an init-ok hello.
	shimAddr := cf002FreeLoopbackAddr(t)
	shimCmd := exec.Command(shimBin)
	shimCmd.Env = append(os.Environ(), "CF002_SHIM_ADDR="+shimAddr)
	shimCmd.Stdout = io.Discard
	shimCmd.Stderr = io.Discard
	if err := shimCmd.Start(); err != nil {
		t.Fatalf("start healthy shim: %v", err)
	}
	defer func() {
		_ = shimCmd.Process.Kill()
		_, _ = shimCmd.Process.Wait()
	}()
	shimBase := "http://" + shimAddr
	client := &http.Client{Timeout: 5 * time.Second}
	healthy := false
	for i := 0; i < 50; i++ {
		resp, err := client.Get(shimBase + "/health")
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK && bytes.Contains(body, []byte(`"ok":true`)) && bytes.Contains(body, []byte(`"db":true`)) {
				healthy = true
				break
			}
		} else {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !healthy {
		t.Fatal("healthy shim never served /health with ok:true db:true")
	}
	sseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sseReq, _ := http.NewRequestWithContext(sseCtx, http.MethodGet, shimBase+"/runtime/sse?app_id=x", nil)
	sseResp, err := client.Do(sseReq)
	if err != nil {
		t.Fatalf("shim SSE unreachable (not healthy): %v", err)
	}
	sseBody, _ := io.ReadAll(io.LimitReader(sseResp.Body, 1<<20))
	_ = sseResp.Body.Close()
	if !bytes.Contains(sseBody, []byte(`"op":"init-ok"`)) || !bytes.Contains(sseBody, []byte("session-id")) {
		t.Fatalf("shim SSE hello missing init-ok/session-id: %q", sseBody)
	}

	// The override must be rejected before capture with no output/manifest.
	root := cf002E2ERepoRoot(t)
	outDir := filepath.Join(base, "override-out")
	var out, stderr bytes.Buffer
	code := run([]string{"--mode", "managed-record", "--output-dir", outDir, "--repo", root, "--instantd-binary", shimBin}, &out, &stderr)
	if code == 0 {
		t.Fatal("instantd-binary override reported success (candidate binding removed)")
	}
	diag := stderr.String()
	if !strings.Contains(diag, "instantd-binary") {
		t.Fatalf("override rejection does not name the removed flag: %q", diag)
	}
	if _, err := os.Stat(outDir); !os.IsNotExist(err) {
		t.Fatalf("override run produced output at %s", outDir)
	}
	if _, err := os.Stat(filepath.Join(outDir, "manifest.json")); !os.IsNotExist(err) {
		t.Fatal("override run published an eligible manifest")
	}
	if strings.Contains(out.String(), "PASS managed-record") {
		t.Fatal("override run printed a pass line")
	}
	// Diagnostic must not leak binary contents, credentials, or database URLs.
	if dbURL := os.Getenv("DATABASE_URL"); dbURL != "" && strings.Contains(diag, dbURL) {
		t.Fatal("override diagnostic leaks DATABASE_URL")
	}
	for _, leak := range []string{"postgres://", "postgresql://", "cf002-managed-record-secret", "cf002-google-secret", "cf002-github-secret"} {
		if strings.Contains(diag, leak) {
			t.Fatalf("override diagnostic leaks %q", leak)
		}
	}
	if shimBytes, err := os.ReadFile(shimBin); err == nil && len(shimBytes) > 0 {
		// Binary blobs are non-UTF8; assert the diagnostic carries no long
		// printable slice of the binary (proof it was not dumped).
		printable := make([]byte, 0, 64)
		for _, b := range shimBytes {
			if b >= 32 && b < 127 {
				printable = append(printable, b)
				if len(printable) >= 64 {
					break
				}
			} else if len(printable) >= 32 {
				break
			}
		}
		if len(printable) >= 32 && strings.Contains(diag, string(printable)) {
			t.Fatal("override diagnostic contains binary contents")
		}
	}
}

// TestCF002ManagedRecordDefaultBuildReachesDatabaseStage proves the default
// candidate build is accepted: with no override flag, a clean candidate gets
// past the build stage and fails only at the owned-database stage when no
// database URL is configured. A build failure would report "build instantd
// from candidate" instead.
func TestCF002ManagedRecordDefaultBuildReachesDatabaseStage(t *testing.T) {
	root := cf002E2ERepoRoot(t)
	if _, dirty, err := corpus.GitIdentity(root); err != nil {
		t.Fatal(err)
	} else if dirty {
		t.Skip("managed-record acceptance requires a clean worktree")
	}
	oldURL, hadURL := os.LookupEnv("DATABASE_URL")
	_ = os.Unsetenv("DATABASE_URL")
	defer func() {
		if hadURL {
			_ = os.Setenv("DATABASE_URL", oldURL)
		}
	}()
	outDir := filepath.Join(t.TempDir(), "default-build-out")
	var out, stderr bytes.Buffer
	code := run([]string{"--mode", "managed-record", "--output-dir", outDir, "--repo", root}, &out, &stderr)
	if code == 0 {
		t.Fatal("record without DATABASE_URL reported success")
	}
	diag := stderr.String()
	if !strings.Contains(diag, "requires DATABASE_URL") {
		t.Fatalf("default build did not reach database stage (build not accepted?): %q", diag)
	}
	if strings.Contains(diag, "build instantd from candidate") {
		t.Fatalf("default candidate build was rejected: %q", diag)
	}
	if _, err := os.Stat(outDir); !os.IsNotExist(err) {
		t.Fatalf("pre-reservation failure produced output at %s", outDir)
	}
}

// TestCF002ManagedRecordFailurePreservesVictimUnderReplacement is the
// deterministic replacement-race regression for the safe failure policy:
// reserve, pause on an injected publication failure, replace the visible
// pathname with a victim directory holding a sentinel, release the failure,
// then prove the victim survives byte-identical, nothing outside the pinned
// directory is removed, no eligible manifest exists, the incomplete residue
// is clearly ineligible, and the command exits nonzero.
func TestCF002ManagedRecordFailurePreservesVictimUnderReplacement(t *testing.T) {
	if os.Getenv("INSTANT_TEST_INTEGRATION") != "1" || os.Getenv("DATABASE_URL") == "" {
		t.Skip("integration test: set INSTANT_TEST_INTEGRATION=1 and DATABASE_URL")
	}
	root := cf002E2ERepoRoot(t)
	if _, dirty, err := corpus.GitIdentity(root); err != nil {
		t.Fatal(err)
	} else if dirty {
		t.Skip("managed-record acceptance requires a clean worktree")
	}
	base := t.TempDir()
	outDir := filepath.Join(base, "out")
	victimDir := filepath.Join(base, "victim")
	if err := os.Mkdir(victimDir, 0700); err != nil {
		t.Fatal(err)
	}
	sentinel := []byte("victim-sentinel-cf002-race")
	if err := os.WriteFile(filepath.Join(victimDir, "sentinel.txt"), sentinel, 0600); err != nil {
		t.Fatal(err)
	}
	stash := filepath.Join(base, "stashed-reserved")

	reservedCh := make(chan struct{}, 1)
	releaseCh := make(chan struct{})
	oldHook := managedTestHookAfterReserve
	managedTestHookAfterReserve = func(reserved *corpus.ReservedDir) error {
		reservedCh <- struct{}{}
		<-releaseCh
		return errors.New("injected publication failure")
	}
	defer func() { managedTestHookAfterReserve = oldHook }()

	type runResult struct {
		code   int
		stdout string
		stderr string
	}
	resultCh := make(chan runResult, 1)
	go func() {
		var out, stderr bytes.Buffer
		code := run([]string{"--mode", "managed-record", "--output-dir", outDir, "--repo", root}, &out, &stderr)
		resultCh <- runResult{code, out.String(), stderr.String()}
	}()

	select {
	case <-reservedCh:
	case <-time.After(3 * time.Minute):
		t.Fatal("timed out waiting for output reservation")
	}
	// Swap the visible pathname: move the pinned reservation aside, then
	// move the victim into its place. A path-based cleanup of outDir would
	// now destroy the victim; the safe policy must leave it untouched.
	if err := os.Rename(outDir, stash); err != nil {
		t.Fatalf("stash reserved dir: %v", err)
	}
	if err := os.Rename(victimDir, outDir); err != nil {
		t.Fatalf("replace pathname with victim: %v", err)
	}
	close(releaseCh)

	var res runResult
	select {
	case res = <-resultCh:
	case <-time.After(3 * time.Minute):
		t.Fatal("timed out waiting for managed-record failure")
	}
	if res.code == 0 {
		t.Fatal("injected failure reported success")
	}
	if !strings.Contains(res.stderr, "injected publication failure") {
		t.Fatalf("failure diagnostic unstable: %q", res.stderr)
	}
	if strings.Contains(res.stdout, "PASS managed-record") {
		t.Fatal("failed run printed a pass line")
	}

	// Victim directory and sentinel survive byte-identical at the visible path.
	got, err := os.ReadFile(filepath.Join(outDir, "sentinel.txt"))
	if err != nil {
		t.Fatalf("victim sentinel missing after failure (deleted?): %v", err)
	}
	if !bytes.Equal(got, sentinel) {
		t.Fatalf("victim sentinel altered: got %q want %q", got, sentinel)
	}
	if fi, err := os.Stat(outDir); err != nil || !fi.IsDir() {
		t.Fatalf("victim directory missing after failure: %v %v", fi, err)
	}
	// No eligible manifest exists at the visible (victim) path.
	if _, err := os.Stat(filepath.Join(outDir, "manifest.json")); !os.IsNotExist(err) {
		t.Fatal("victim path gained an eligible manifest")
	}
	// Incomplete residue (stashed pinned dir) is left in place and clearly
	// ineligible: it holds no manifest.json.
	if fi, err := os.Stat(stash); err != nil || !fi.IsDir() {
		t.Fatalf("pinned residue missing (must be left in place): %v", err)
	}
	if _, err := os.Stat(filepath.Join(stash, "manifest.json")); !os.IsNotExist(err) {
		t.Fatal("incomplete residue holds an eligible manifest")
	}
	// Diagnostic carries no credentials or database URL.
	if dbURL := os.Getenv("DATABASE_URL"); dbURL != "" && strings.Contains(res.stderr, dbURL) {
		t.Fatal("failure diagnostic leaks DATABASE_URL")
	}
	for _, leak := range []string{"postgres://", "postgresql://", "cf002-managed-record-secret"} {
		if strings.Contains(res.stderr, leak) {
			t.Fatalf("failure diagnostic leaks %q", leak)
		}
	}
}
