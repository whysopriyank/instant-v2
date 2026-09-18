package main

// CF-002 managed local recording lifecycle.
//
// Unlike `--mode record` (caller-supplied endpoint/source/fixture assertions),
// this test proves candidate binding: it builds the local `instantd` binary
// from the exact starting candidate, launches a loopback-only daemon against
// an owned `instant_test_*` fixture, derives every identity field locally,
// resets the fixture before each mutating scenario, captures HTTP and SSE
// through the corpus recorders, and publishes a checksummed manifest. Any
// change of process, binary, endpoint, revision, configuration, or fixture
// identity during capture fails closed.
//
// Set CF002_SKIP_RESET=1 to omit the inter-scenario reset; the exact
// precondition assertion then fails (mutation-probe red path). The variable
// is unset in all acceptance runs.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/corpus"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/testkit"
)

const cf002StorageSecret = "cf002-managed-test-secret-not-logged"

var cf002HTTP = &http.Client{Timeout: 10 * time.Second}

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

func cf002BuildInstantd(t *testing.T) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "instantd-cf002")
	cmd := exec.Command("go", "build", "-o", out, "./cmd/instantd")
	cmd.Dir = cf002RepoRoot(t)
	if combined, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build instantd: %v\n%s", err, combined)
	}
	return out
}

func cf002FreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

func cf002Start(t *testing.T, bin, dsn, root, addr, logPath string) *exec.Cmd {
	t.Helper()
	logF, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin)
	cmd.Stdout = logF
	cmd.Stderr = logF
	env := os.Environ()
	strip := func(prefix string) {
		out := env[:0]
		for _, kv := range env {
			if !strings.HasPrefix(kv, prefix) {
				out = append(out, kv)
			}
		}
		env = out
	}
	for _, p := range []string{
		"DATABASE_URL=", "INSTANT_V2_STORAGE_ROOT=", "INSTANT_V2_STORAGE_SECRET=",
		"INSTANT_V2_HTTP_ADDR=", "INSTANT_V2_HTTP_PORT=", "INSTANT_V2_METRICS_ADDR=",
		"INSTANT_V2_INSECURE_DEV_SECRETS=", "INSTANT_V2_READ_URL=",
		"INSTANT_OAUTH_GOOGLE_CLIENT_ID=", "INSTANT_OAUTH_GOOGLE_CLIENT_SECRET=",
		"INSTANT_OAUTH_GITHUB_CLIENT_ID=", "INSTANT_OAUTH_GITHUB_CLIENT_SECRET=",
	} {
		strip(p)
	}
	env = append(env,
		"DATABASE_URL="+dsn,
		"INSTANT_V2_STORAGE_ROOT="+root,
		"INSTANT_V2_STORAGE_SECRET="+cf002StorageSecret,
		"INSTANT_V2_HTTP_ADDR="+addr,
		"INSTANT_V2_METRICS_ADDR=",
		"INSTANT_OAUTH_GOOGLE_CLIENT_ID=cf002-google-id",
		"INSTANT_OAUTH_GOOGLE_CLIENT_SECRET=cf002-google-secret",
		"INSTANT_OAUTH_GITHUB_CLIENT_ID=cf002-github-id",
		"INSTANT_OAUTH_GITHUB_CLIENT_SECRET=cf002-github-secret",
	)
	cmd.Env = env
	if err := cmd.Start(); err != nil {
		_ = logF.Close()
		t.Fatalf("start instantd %s: %v", addr, err)
	}
	_ = logF.Close()
	t.Cleanup(func() {
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			return
		}
		_ = cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _, _ = cmd.Process.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
		}
	})
	return cmd
}

func cf002WaitHealth(t *testing.T, baseURL string) {
	t.Helper()
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := cf002HTTP.Get(baseURL + "/health")
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK && bytes.Contains(body, []byte(`"ok":true`)) {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("managed daemon %s never became healthy", baseURL)
}

func cf002Stop(t *testing.T, cmd *exec.Cmd, addr string) {
	t.Helper()
	if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
		t.Fatalf("managed daemon %s already exited before bounded stop", addr)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM %s: %v", addr, err)
	}
	done := make(chan error, 1)
	go func() { _, err := cmd.Process.Wait(); done <- err }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		t.Fatalf("managed daemon %s did not exit within bound", addr)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
		if err != nil {
			return
		}
		_ = c.Close()
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("managed daemon still listening on %s after exit", addr)
}

func cf002NewUUID(t *testing.T) [16]byte {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return b
}

func cf002UUIDStr(b [16]byte) string {
	s := hex.EncodeToString(b[:])
	return s[0:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:32]
}

// cf002FixtureState returns the exact observable fixture state for appID:
// app row presence/title plus the complete sorted (etype, label, value) triple
// set. Equality of this string across scenarios after reset proves reset
// equivalence; inequality after a failed reset proves the reset mattered.
func cf002FixtureState(t *testing.T, ctx context.Context, fixture *testkit.Postgres, appID [16]byte) string {
	t.Helper()
	var title string
	var apps int
	if err := fixture.Pool.QueryRow(ctx, `SELECT count(*), coalesce(max(title),'') FROM apps WHERE id=$1`, appID).Scan(&apps, &title); err != nil {
		t.Fatalf("fixture state apps: %v", err)
	}
	var attrs, triples, idents int
	if err := fixture.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM attrs WHERE app_id=$1), (SELECT count(*) FROM triples WHERE app_id=$1), (SELECT count(*) FROM idents WHERE app_id=$1)`, appID).Scan(&attrs, &triples, &idents); err != nil {
		t.Fatalf("fixture state counts: %v", err)
	}
	rows, err := fixture.Pool.Query(ctx, `
		SELECT a.etype, a.label, t.value::text
		FROM triples t JOIN attrs a ON a.id = t.attr_id
		WHERE t.app_id=$1 ORDER BY 1,2,3`, appID)
	if err != nil {
		t.Fatalf("fixture state triples: %v", err)
	}
	var parts []string
	for rows.Next() {
		var etype, label, value string
		if err := rows.Scan(&etype, &label, &value); err != nil {
			rows.Close()
			t.Fatalf("fixture state scan: %v", err)
		}
		parts = append(parts, etype+"/"+label+"="+value)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("fixture state rows: %v", err)
	}
	return fmt.Sprintf("apps=%d title=%q attrs=%d triples=%d idents=%d rows=[%s]", apps, title, attrs, triples, idents, strings.Join(parts, " "))
}

// cf002ResetFixture deletes the app row (cascading to attrs/triples/idents)
// and recreates it with the same identity and admin token: an exact,
// observable reset to the empty precondition.
func cf002ResetFixture(t *testing.T, ctx context.Context, fixture *testkit.Postgres, appID, creatorID, adminID [16]byte, title string) {
	t.Helper()
	tx, err := fixture.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `DELETE FROM apps WHERE id=$1`, appID); err != nil {
		t.Fatalf("reset delete app: %v", err)
	}
	if err := platform.CreateApp(ctx, tx, creatorID, appID, title); err != nil {
		t.Fatalf("reset create app: %v", err)
	}
	if err := platform.SetAdminToken(ctx, tx, appID, adminID); err != nil {
		t.Fatalf("reset admin token: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("reset commit: %v", err)
	}
}

func cf002TransactTitle(t *testing.T, baseURL, appStr, adminStr, entityStr, title string) int64 {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"app-id": appStr,
		"steps":  []any{[]any{"update", "todos", entityStr, map[string]any{"title": title}}},
	})
	req, err := http.NewRequest(http.MethodPost, baseURL+"/admin/transact", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-admin-token", adminStr)
	resp, err := cf002HTTP.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("admin transact: %d %s", resp.StatusCode, raw)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("transact envelope: %v (%s)", err, raw)
	}
	var txID int64
	if rawTx, ok := envelope["tx-id"]; !ok || json.Unmarshal(rawTx, &txID) != nil || txID <= 0 {
		t.Fatalf("transact tx-id missing: %s", raw)
	}
	return txID
}

func TestCF002ManagedLocalLifecycle(t *testing.T) {
	fixture := testkit.NewPostgres(t, testkit.PostgresOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	root := cf002RepoRoot(t)
	gitSHA, gitDirty, err := corpus.GitIdentity(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("candidate %s dirty=%v %s %s/%s", gitSHA, gitDirty, runtime.Version(), runtime.GOOS, runtime.GOARCH)

	bin := cf002BuildInstantd(t)
	binBytes, err := os.ReadFile(bin)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(binBytes)
	binSHA := hex.EncodeToString(sum[:])

	storageRoot, err := os.MkdirTemp("", "cf002-managed-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(storageRoot) })
	if !filepath.IsAbs(storageRoot) {
		t.Fatalf("storage root must be absolute: %q", storageRoot)
	}

	addr := fmt.Sprintf("127.0.0.1:%d", cf002FreePort(t))
	baseURL := "http://" + addr
	if err := corpus.RequireLoopbackEndpoint(baseURL); err != nil {
		t.Fatalf("managed endpoint is not loopback: %v", err)
	}
	logPath := filepath.Join(t.TempDir(), "managed.log")
	daemon := cf002Start(t, bin, fixture.DSN, storageRoot, addr, logPath)
	pid := daemon.Process.Pid
	cf002WaitHealth(t, baseURL)
	t.Logf("managed daemon pid=%d endpoint=%s fixture=%s binary=%s", pid, baseURL, fixture.Name, binSHA[:16])

	if err := corpus.VerifyProcessAlive(pid); err != nil {
		t.Fatalf("managed process not alive: %v", err)
	}
	if err := corpus.VerifyBinaryDigest(bin, binSHA); err != nil {
		t.Fatal(err)
	}

	// Configuration digest covers non-secret fields only; the secret value
	// never enters the digest, logs, or manifest (presence bit only).
	configDigest := corpus.ConfigDigest(map[string]string{
		"storage-root":           storageRoot,
		"http-addr":              addr,
		"max-upload-bytes":       "536870912",
		"insecure-dev-mode":      "false",
		"storage-secret-is-set":  "true",
		"oauth-google-id-is-set": "true",
		"oauth-github-id-is-set": "true",
		"invalidation-bus":       "none",
	})

	appID := cf002NewUUID(t)
	creatorID := cf002NewUUID(t)
	adminID := cf002NewUUID(t)
	appStr := cf002UUIDStr(appID)
	adminStr := cf002UUIDStr(adminID)
	const appTitle = "cf002-managed"
	if _, err := fixture.Pool.Exec(ctx, `INSERT INTO instant_users (id,email) VALUES ($1,$2)`, creatorID, "cf002@example.test"); err != nil {
		t.Fatalf("seed creator: %v", err)
	}
	cf002ResetFixture(t, ctx, fixture, appID, creatorID, adminID, appTitle)

	identity := corpus.CandidateIdentity{
		GitSHA:       gitSHA,
		GitDirty:     gitDirty,
		GoVersion:    runtime.Version(),
		GOOS:         runtime.GOOS,
		GOARCH:       runtime.GOARCH,
		BinaryPath:   bin,
		BinarySHA256: binSHA,
		PID:          pid,
		Executable:   bin,
		Endpoint:     baseURL,
		ConfigDigest: configDigest,
		FixtureDB:    fixture.Name,
		FixtureApp:   appStr,
	}
	if err := identity.Validate(); err != nil {
		t.Fatalf("candidate identity invalid: %v", err)
	}
	if err := identity.VerifyLive(root); err != nil {
		t.Fatalf("candidate liveness failed before capture: %v", err)
	}

	// Both mutating scenarios use the same entity and title so reset
	// equivalence is an exact equality: equal starts, equal ends.
	entityID := cf002NewUUID(t)
	entityStr := cf002UUIDStr(entityID)
	const entityTitle = "cf002-managed-todo"
	skipReset := os.Getenv("CF002_SKIP_RESET") == "1"

	outDir := filepath.Join(t.TempDir(), "cf002-evidence")
	reserved, err := corpus.ReserveOutputDir(outDir)
	if err != nil {
		t.Fatalf("reserve output: %v", err)
	}
	defer func() { _ = reserved.Close() }()
	if fi, err := os.Stat(outDir); err != nil || fi.Mode().Perm() != 0700 {
		t.Fatalf("output dir not 0700: %v %v", fi, err)
	}

	type scenarioResult struct {
		pre, post string
		files     []string
	}
	var results []scenarioResult
	for _, name := range []string{"s1", "s2"} {
		if !skipReset {
			cf002ResetFixture(t, ctx, fixture, appID, creatorID, adminID, appTitle)
		}
		pre := cf002FixtureState(t, ctx, fixture, appID)
		wantPre := fmt.Sprintf("apps=1 title=%q attrs=0 triples=0 idents=0 rows=[]", appTitle)
		if pre != wantPre {
			t.Fatalf("scenario %s precondition = %s; want %s (reset %s)", name, pre, wantPre, map[bool]string{true: "omitted", false: "applied"}[skipReset])
		}
		if err := identity.VerifyLive(root); err != nil {
			t.Fatalf("scenario %s identity drift before capture: %v", name, err)
		}

		httpExchange, err := corpus.CaptureHTTP(ctx, cf002HTTP, baseURL, corpus.HTTPRequest{Method: http.MethodGet, Target: "/health"}, corpus.CaptureOptions{})
		if err != nil {
			t.Fatalf("scenario %s HTTP capture: %v", name, err)
		}
		if httpExchange.Response.Status != http.StatusOK {
			t.Fatalf("scenario %s health status = %d", name, httpExchange.Response.Status)
		}
		var healthBody map[string]any
		if err := json.Unmarshal(httpExchange.Response.Body, &healthBody); err != nil || healthBody["ok"] != true || healthBody["db"] != true {
			t.Fatalf("scenario %s health body = %s", name, httpExchange.Response.Body)
		}
		httpCanonical, err := corpus.CanonicalBytes(httpExchange.Response.Body)
		if err != nil {
			t.Fatalf("scenario %s health canonical: %v", name, err)
		}
		again, err := corpus.CanonicalBytes(httpExchange.Response.Body)
		if err != nil || !bytes.Equal(httpCanonical, again) {
			t.Fatalf("scenario %s canonical not deterministic", name)
		}

		sseResult := corpus.CaptureSSEWithOptions(ctx, cf002HTTP, baseURL,
			corpus.HTTPRequest{Method: http.MethodGet, Target: "/runtime/sse?app_id=" + appStr},
			nil, 1, 15*time.Second, corpus.SSECaptureOptions{})
		if sseResult.Err != nil {
			t.Fatalf("scenario %s SSE capture: %v", sseResult.Err, name)
		}
		if len(sseResult.Records) != 1 || sseResult.Records[0].Kind != "data" {
			t.Fatalf("scenario %s SSE records = %#v", name, sseResult.Records)
		}
		rec := sseResult.Records[0]
		if len(rec.Raw) == 0 || !bytes.Contains(rec.Raw, []byte("session-id")) {
			t.Fatalf("scenario %s SSE raw framing missing: %q", name, rec.Raw)
		}
		if !bytes.Contains(rec.Data, []byte(`"op":"init-ok"`)) {
			t.Fatalf("scenario %s SSE hello = %s", name, rec.Data)
		}
		if !bytes.Contains(rec.Normalized, []byte(corpus.NormalizedSessionID)) {
			t.Fatalf("scenario %s SSE canonical session unset: %s", name, rec.Normalized)
		}

		if err := identity.VerifyLive(root); err != nil {
			t.Fatalf("scenario %s identity drift during capture: %v", name, err)
		}
		// The capture must have hit the recorded endpoint, not another
		// listener: health plus SSE hello both came from baseURL, and the
		// daemon handle is still the recorded PID.
		if daemon.Process.Pid != pid {
			t.Fatalf("scenario %s daemon handle changed", name)
		}

		txID := cf002TransactTitle(t, baseURL, appStr, adminStr, entityStr, entityTitle)
		if txID <= 0 {
			t.Fatalf("scenario %s bad tx id %d", name, txID)
		}
		post := cf002FixtureState(t, ctx, fixture, appID)
		wantPost := "apps=1 title=\"cf002-managed\" attrs=2 triples=2 idents=2 rows=[todos/id=\"" + entityStr + "\" todos/title=\"cf002-managed-todo\"]"
		if post != wantPost {
			t.Fatalf("scenario %s final state = %s; want %s", name, post, wantPost)
		}
		var tripleCount int
		if err := fixture.Pool.QueryRow(ctx, `SELECT count(*) FROM triples WHERE app_id=$1`, appID).Scan(&tripleCount); err != nil || tripleCount == 0 {
			t.Fatalf("scenario %s triples = %d (%v)", name, tripleCount, err)
		}

		httpFile := "cf002-" + name + ".http.evidence.json"
		sseFile := "cf002-" + name + ".sse.evidence.json"
		if err := reserved.WriteEvidence(httpFile, map[string]any{
			"mode": "managed-record", "transport": "http", "scenario": name,
			"status": "recorded", "candidate": identity, "targetUrl": baseURL,
			"precondition": pre, "txId": txID,
			"raw":        httpExchange,
			"normalized": string(httpCanonical),
		}); err != nil {
			t.Fatalf("scenario %s publish http evidence: %v", name, err)
		}
		if err := reserved.WriteEvidence(sseFile, map[string]any{
			"mode": "managed-record", "transport": "sse", "scenario": name,
			"status": "recorded", "candidate": identity, "targetUrl": baseURL,
			"precondition": pre, "txId": txID,
			"rawRecords": sseResult.Records,
			"normalized": corpus.NormalizeSSERecords(sseResult.Records),
		}); err != nil {
			t.Fatalf("scenario %s publish sse evidence: %v", name, err)
		}
		for _, f := range []string{httpFile, sseFile} {
			if fi, err := os.Stat(filepath.Join(outDir, f)); err != nil || fi.Mode().Perm() != 0600 {
				t.Fatalf("scenario %s evidence %s not 0600: %v %v", name, f, fi, err)
			}
		}
		results = append(results, scenarioResult{pre: pre, post: post, files: []string{httpFile, sseFile}})
	}

	// Reset equivalence: equal starts and equal ends across the reset.
	if results[0].pre != results[1].pre {
		t.Fatalf("starting states diverged across reset:\n%s\n%s", results[0].pre, results[1].pre)
	}
	if results[0].post != results[1].post {
		t.Fatalf("final states diverged across reset:\n%s\n%s", results[0].post, results[1].post)
	}
	t.Logf("reset equivalence: pre=%s", results[0].pre)
	t.Logf("reset equivalence: post=%s", results[0].post)

	var allFiles []string
	for _, r := range results {
		allFiles = append(allFiles, r.files...)
	}
	entries, err := corpus.ManifestArtifacts(outDir, allFiles)
	if err != nil {
		t.Fatal(err)
	}
	manifest := corpus.CaptureManifest{
		Candidate:    identity,
		Precondition: results[0].pre,
		FinalState:   results[0].post,
		Artifacts:    entries,
	}
	if err := reserved.WriteEvidence("manifest.json", manifest); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	if err := corpus.VerifyCaptureManifest(outDir, manifest); err != nil {
		t.Fatalf("manifest verification: %v", err)
	}
	if fi, err := os.Stat(filepath.Join(outDir, "manifest.json")); err != nil || fi.Mode().Perm() != 0600 {
		t.Fatalf("manifest not 0600: %v %v", fi, err)
	}

	// Publication is write-once: republishing any artifact must fail.
	if err := reserved.WriteEvidence(results[0].files[0], map[string]any{"x": 1}); err == nil {
		t.Fatal("evidence overwrite was accepted")
	}

	cf002Stop(t, daemon, addr)
	if err := corpus.VerifyBinaryDigest(bin, binSHA); err != nil {
		t.Fatalf("binary changed across lifecycle: %v", err)
	}
	if err := corpus.VerifyGitIdentity(root, gitSHA, gitDirty); err != nil {
		t.Fatalf("revision changed across lifecycle: %v", err)
	}
	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(logBytes, []byte(storageRoot)) {
		t.Fatal("daemon log does not name the configured root")
	}
	if bytes.Contains(logBytes, []byte(cf002StorageSecret)) {
		t.Fatal("daemon log exposes the storage secret")
	}
	// Only the owned fixture database and test-owned directories are removed
	// (testkit drops instant_test_*; t.Cleanup removes storage/output/log).
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
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
