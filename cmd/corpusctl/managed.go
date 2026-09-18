// Managed local recording entry point (CF-002).
//
// `corpusctl --mode managed-record` turns the previously test-only managed
// lifecycle into a supported runnable recorder. It always builds the local
// instantd binary from the exact recorded candidate, starts it loopback-only
// against a newly owned instant_test_* database, derives every identity
// field locally, resets the owned fixture before each mutating scenario,
// captures HTTP and SSE evidence with raw+canonical forms, and publishes a
// checksummed manifest into the caller's --output-dir. The evidence persists
// after exit; only owned temporary runtime resources are removed.
//
// Acceptance requires a clean Git worktree. Any identity, fixture, capture,
// or publication failure fails closed without an eligible manifest. The
// output directory is reserved descriptor-relatively; on failure the pinned
// reservation is only closed and any private incomplete residue is left in
// place as untrusted and ineligible. No path-based deletion is performed
// after reservation, so a concurrently renamed or replaced pathname can
// never cause deletion outside the pinned directory.
//
// Database errors use static messages: connection strings may embed
// credentials and are never printed, logged, or recorded.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/instant-v2/instant-v2/internal/corpus"
	"github.com/instant-v2/instant-v2/internal/platform"
)

// managedProbeBypassReset is a test-only probe hook proving that an omitted
// fixture reset fails the exact precondition assertion. It defaults to false
// and is set only by in-package tests; the CLI path never enables it, and no
// flag or environment variable can enable it.
var managedProbeBypassReset = false

// managedTestHookAfterReserve is a test-only hook run immediately after the
// output directory is reserved and before any evidence is published. It is
// nil in production; tests use it to deterministically simulate a pathname
// replacement race paired with an injected publication failure.
var managedTestHookAfterReserve func(reserved *corpus.ReservedDir) error

const managedStorageSecret = "cf002-managed-record-secret-not-logged"

var managedHTTP = &http.Client{Timeout: 15 * time.Second}

type managedResources struct {
	bin         string
	buildTmp    string
	storageRoot string
	logPath     string
	addr        string
	baseURL     string
	daemon      *exec.Cmd
	pid         int
	ownedDB     string
	admin       *pgx.Conn
	pool        *pgxpool.Pool
	appID       [16]byte
	creatorID   [16]byte
	adminID     [16]byte
	appStr      string
	adminStr    string
}

func runManagedRecord(o options, out, diagnostic io.Writer) int {
	fail := func(err error) int { _, _ = fmt.Fprintf(diagnostic, "corpusctl: %v\n", err); return 1 }
	timeout := o.timeout
	if timeout < time.Minute {
		// Build plus daemon boot need a floor; a shorter --timeout is
		// raised, never silently applied to the whole lifecycle.
		timeout = 3 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if strings.TrimSpace(o.outputDir) == "" {
		return fail(fmt.Errorf("--output-dir is required for managed-record mode"))
	}
	repoDir := strings.TrimSpace(o.repo)
	if repoDir == "" {
		repoDir = "."
	}
	absRepo, err := filepath.Abs(repoDir)
	if err != nil {
		return fail(fmt.Errorf("resolve repository path: %v", err))
	}
	absOut, err := filepath.Abs(o.outputDir)
	if err != nil {
		return fail(fmt.Errorf("resolve output directory: %v", err))
	}
	if absOut == absRepo || strings.HasPrefix(absOut, absRepo+string(filepath.Separator)) {
		return fail(fmt.Errorf("output directory must be outside the repository (it would dirty the candidate mid-run)"))
	}
	// Fail fast on obviously non-fresh output paths; the descriptor-relative
	// reservation below remains the authoritative freshness check.
	if err := corpus.CheckFreshOutputDir(absOut); err != nil {
		return fail(err)
	}

	gitSHA, gitDirty, err := corpus.GitIdentity(absRepo)
	if err != nil {
		return fail(err)
	}
	if gitDirty {
		return fail(fmt.Errorf("managed-record requires a clean Git worktree for acceptance"))
	}

	var r managedResources
	// The binary is always built from the clean recorded candidate (--repo).
	// No override flag exists: accepting a caller-supplied binary would break
	// the cryptographic binding between the recorded Git SHA and the code
	// that is actually executed and recorded in the manifest.
	// Build-artifact cleanup runs last: the binary hash is verified
	// throughout the lifecycle, so the file must survive until the end.
	tmp, err := os.MkdirTemp("", "cf002-managed-bin-*")
	if err != nil {
		return fail(err)
	}
	r.buildTmp = tmp
	r.bin = filepath.Join(tmp, "instantd-managed")
	cmd := exec.CommandContext(ctx, "go", "build", "-o", r.bin, "./cmd/instantd")
	cmd.Dir = absRepo
	if combined, err := cmd.CombinedOutput(); err != nil {
		_ = os.RemoveAll(tmp)
		r.buildTmp = ""
		return fail(fmt.Errorf("build instantd from candidate: %v\n%s", err, combined))
	}
	defer func() {
		if r.buildTmp != "" {
			_ = os.RemoveAll(r.buildTmp)
		}
	}()
	binSHA, err := corpus.HashFile(r.bin)
	if err != nil {
		return fail(fmt.Errorf("hash instantd binary: %v", err))
	}

	adminDSN := strings.TrimSpace(o.databaseURL)
	if adminDSN == "" {
		adminDSN = strings.TrimSpace(os.Getenv("DATABASE_URL"))
	}
	if adminDSN == "" {
		return fail(fmt.Errorf("managed-record requires DATABASE_URL or --database-url for an owned PostgreSQL role with CREATEDB privilege"))
	}
	// Static errors below: connection strings may embed credentials.
	if _, err := pgxpool.ParseConfig(adminDSN); err != nil {
		return fail(fmt.Errorf("managed-record database URL is not a valid PostgreSQL connection string"))
	}
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		return fail(fmt.Errorf("managed-record PostgreSQL connection failed; check DATABASE_URL and server readiness"))
	}
	r.admin = admin
	defer func() { _ = admin.Close(context.Background()) }()
	r.ownedDB = "instant_test_" + managedRandHex(12)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{r.ownedDB}.Sanitize()+" TEMPLATE template0"); err != nil {
		return fail(fmt.Errorf("create owned integration database (role requires CREATEDB)"))
	}
	defer func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(dropCtx, "DROP DATABASE "+pgx.Identifier{r.ownedDB}.Sanitize()+" WITH (FORCE)"); err != nil {
			_, _ = fmt.Fprintf(diagnostic, "corpusctl: warning: drop owned database %s failed\n", r.ownedDB)
		}
	}()
	ownedDSN, err := managedDatabaseDSN(adminDSN, r.ownedDB)
	if err != nil {
		return fail(err)
	}

	storageRoot, err := os.MkdirTemp("", "cf002-managed-root-*")
	if err != nil {
		return fail(err)
	}
	r.storageRoot = storageRoot
	defer func() { _ = os.RemoveAll(storageRoot) }()
	if !filepath.IsAbs(storageRoot) {
		return fail(fmt.Errorf("storage root must be absolute"))
	}
	r.addr = "127.0.0.1:" + fmt.Sprint(managedFreePort())
	r.baseURL = "http://" + r.addr
	if err := corpus.RequireLoopbackEndpoint(r.baseURL); err != nil {
		return fail(err)
	}
	logDir, err := os.MkdirTemp("", "cf002-managed-log-*")
	if err != nil {
		return fail(err)
	}
	defer func() { _ = os.RemoveAll(logDir) }()
	r.logPath = filepath.Join(logDir, "managed.log")
	if err := managedStartDaemon(ctx, &r, ownedDSN); err != nil {
		return fail(err)
	}
	defer managedStopDaemon(&r)
	r.pid = r.daemon.Process.Pid
	if err := managedWaitHealth(ctx, r.baseURL); err != nil {
		return fail(err)
	}
	if err := corpus.VerifyProcessAlive(r.pid); err != nil {
		return fail(fmt.Errorf("managed daemon process not alive: %v", err))
	}
	if err := corpus.VerifyBinaryDigest(r.bin, binSHA); err != nil {
		return fail(err)
	}

	configDigest := corpus.ConfigDigest(map[string]string{
		"storage-root":           storageRoot,
		"http-addr":              r.addr,
		"max-upload-bytes":       "536870912",
		"insecure-dev-mode":      "false",
		"storage-secret-is-set":  "true",
		"oauth-google-id-is-set": "true",
		"oauth-github-id-is-set": "true",
		"invalidation-bus":       "none",
	})

	poolCfg, err := pgxpool.ParseConfig(ownedDSN)
	if err != nil {
		return fail(fmt.Errorf("managed-record owned database URL is invalid"))
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fail(fmt.Errorf("open owned integration database pool"))
	}
	r.pool = pool
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fail(fmt.Errorf("ping owned integration database failed"))
	}

	managedNewUUID(r.appID[:])
	managedNewUUID(r.creatorID[:])
	managedNewUUID(r.adminID[:])
	r.appStr = managedUUIDStr(r.appID)
	r.adminStr = managedUUIDStr(r.adminID)
	const appTitle = "cf002-managed"
	if _, err := pool.Exec(ctx, `INSERT INTO instant_users (id,email) VALUES ($1,$2)`, r.creatorID, "cf002@example.test"); err != nil {
		return fail(fmt.Errorf("seed managed creator"))
	}
	if err := managedResetFixture(ctx, &r, appTitle); err != nil {
		return fail(err)
	}

	exe := r.bin
	identity := corpus.CandidateIdentity{
		GitSHA:       gitSHA,
		GitDirty:     gitDirty,
		GoVersion:    runtime.Version(),
		GOOS:         runtime.GOOS,
		GOARCH:       runtime.GOARCH,
		BinaryPath:   r.bin,
		BinarySHA256: binSHA,
		PID:          r.pid,
		Executable:   exe,
		Endpoint:     r.baseURL,
		ConfigDigest: configDigest,
		FixtureDB:    r.ownedDB,
		FixtureApp:   r.appStr,
	}
	if err := identity.Validate(); err != nil {
		return fail(fmt.Errorf("candidate identity invalid: %v", err))
	}
	if err := identity.VerifyLive(absRepo); err != nil {
		return fail(fmt.Errorf("candidate liveness failed before capture: %v", err))
	}

	entity := managedFreshUUID()
	entityStr := managedUUIDStr(entity)
	const entityTitle = "cf002-managed-todo"
	type scenarioResult struct {
		pre, post string
		files     []string
	}
	var results []scenarioResult
	// Evidence payloads accumulate in memory; the output directory is
	// reserved only after every capture and mutation succeeds, so a failed
	// run leaves no eligible manifest behind.
	type pendingEvidence struct {
		name  string
		value any
	}
	var pending []pendingEvidence
	for _, name := range []string{"s1", "s2"} {
		if !managedProbeBypassReset {
			if err := managedResetFixture(ctx, &r, appTitle); err != nil {
				return fail(err)
			}
		}
		pre, err := managedFixtureState(ctx, &r)
		if err != nil {
			return fail(err)
		}
		wantPre := fmt.Sprintf("apps=1 title=%q attrs=0 triples=0 idents=0 rows=[]", appTitle)
		if pre != wantPre {
			return fail(fmt.Errorf("scenario %s precondition = %s; want %s", name, pre, wantPre))
		}
		if err := identity.VerifyLive(absRepo); err != nil {
			return fail(fmt.Errorf("scenario %s identity drift before capture: %v", name, err))
		}

		httpExchange, err := corpus.CaptureHTTP(ctx, managedHTTP, r.baseURL, corpus.HTTPRequest{Method: http.MethodGet, Target: "/health"}, corpus.CaptureOptions{})
		if err != nil {
			return fail(fmt.Errorf("scenario %s HTTP capture: %v", name, err))
		}
		if httpExchange.Response.Status != http.StatusOK {
			return fail(fmt.Errorf("scenario %s health status = %d", name, httpExchange.Response.Status))
		}
		var healthBody map[string]any
		if err := json.Unmarshal(httpExchange.Response.Body, &healthBody); err != nil || healthBody["ok"] != true || healthBody["db"] != true {
			return fail(fmt.Errorf("scenario %s unexpected health body", name))
		}
		httpCanonical, err := corpus.CanonicalBytes(httpExchange.Response.Body)
		if err != nil {
			return fail(fmt.Errorf("scenario %s health canonical: %v", name, err))
		}

		sseResult := corpus.CaptureSSEWithOptions(ctx, managedHTTP, r.baseURL,
			corpus.HTTPRequest{Method: http.MethodGet, Target: "/runtime/sse?app_id=" + r.appStr},
			nil, 1, 15*time.Second, corpus.SSECaptureOptions{})
		if sseResult.Err != nil {
			return fail(fmt.Errorf("scenario %s SSE capture: %v", name, sseResult.Err))
		}
		if len(sseResult.Records) != 1 || sseResult.Records[0].Kind != "data" {
			return fail(fmt.Errorf("scenario %s unexpected SSE records", name))
		}
		rec := sseResult.Records[0]
		if len(rec.Raw) == 0 || !bytes.Contains(rec.Raw, []byte("session-id")) {
			return fail(fmt.Errorf("scenario %s SSE raw framing missing", name))
		}
		if !bytes.Contains(rec.Data, []byte(`"op":"init-ok"`)) {
			return fail(fmt.Errorf("scenario %s SSE hello missing", name))
		}
		if !bytes.Contains(rec.Normalized, []byte(corpus.NormalizedSessionID)) {
			return fail(fmt.Errorf("scenario %s SSE canonical session unset", name))
		}

		if err := identity.VerifyLive(absRepo); err != nil {
			return fail(fmt.Errorf("scenario %s identity drift during capture: %v", name, err))
		}
		if r.daemon.Process.Pid != r.pid {
			return fail(fmt.Errorf("scenario %s daemon handle changed", name))
		}

		txID, err := managedTransactTitle(ctx, &r, entityStr, entityTitle)
		if err != nil {
			return fail(err)
		}
		post, err := managedFixtureState(ctx, &r)
		if err != nil {
			return fail(err)
		}
		wantPost := "apps=1 title=\"cf002-managed\" attrs=2 triples=2 idents=2 rows=[todos/id=\"" + entityStr + "\" todos/title=\"cf002-managed-todo\"]"
		if post != wantPost {
			return fail(fmt.Errorf("scenario %s final state = %s; want %s", name, post, wantPost))
		}

		httpFile := "cf002-" + name + ".http.evidence.json"
		sseFile := "cf002-" + name + ".sse.evidence.json"
		pending = append(pending,
			pendingEvidence{name: httpFile, value: map[string]any{
				"mode": "managed-record", "transport": "http", "scenario": name,
				"status": "recorded", "candidate": identity, "targetUrl": r.baseURL,
				"precondition": pre, "txId": txID,
				"raw":        httpExchange,
				"normalized": string(httpCanonical),
			}},
			pendingEvidence{name: sseFile, value: map[string]any{
				"mode": "managed-record", "transport": "sse", "scenario": name,
				"status": "recorded", "candidate": identity, "targetUrl": r.baseURL,
				"precondition": pre, "txId": txID,
				"rawRecords": sseResult.Records,
				"normalized": corpus.NormalizeSSERecords(sseResult.Records),
			}},
		)
		results = append(results, scenarioResult{pre: pre, post: post, files: []string{httpFile, sseFile}})
	}
	if results[0].pre != results[1].pre {
		return fail(fmt.Errorf("starting states diverged across reset"))
	}
	if results[0].post != results[1].post {
		return fail(fmt.Errorf("final states diverged across reset"))
	}

	reserved, err := corpus.ReserveOutputDir(absOut)
	if err != nil {
		return fail(err)
	}
	// Safe failure policy: the reservation is pinned by descriptor and
	// (dev,ino). On failure only close the descriptors and leave any private
	// incomplete residue in place as untrusted and ineligible. Never delete
	// through the output pathname after reservation: it may have been
	// renamed or replaced concurrently, and path-based deletion could
	// destroy unrelated data. No manifest.json is published on failure.
	defer func() {
		_ = reserved.Close()
	}()
	if managedTestHookAfterReserve != nil {
		if err := managedTestHookAfterReserve(reserved); err != nil {
			return fail(err)
		}
	}
	if fi, err := os.Stat(absOut); err != nil || fi.Mode().Perm() != 0700 {
		return fail(fmt.Errorf("reserved output directory is not mode 0700"))
	}
	for _, p := range pending {
		if err := reserved.WriteEvidence(p.name, p.value); err != nil {
			return fail(fmt.Errorf("publish %s: %v", p.name, err))
		}
	}
	var allFiles []string
	for _, res := range results {
		allFiles = append(allFiles, res.files...)
	}
	entries, err := corpus.ManifestArtifacts(absOut, allFiles)
	if err != nil {
		return fail(err)
	}
	manifest := corpus.CaptureManifest{
		Candidate:    identity,
		Precondition: results[0].pre,
		FinalState:   results[0].post,
		Artifacts:    entries,
	}
	if err := reserved.WriteEvidence("manifest.json", manifest); err != nil {
		return fail(fmt.Errorf("publish manifest: %v", err))
	}
	if err := corpus.VerifyCaptureManifest(absOut, manifest); err != nil {
		return fail(fmt.Errorf("manifest verification: %v", err))
	}
	// Write-once is enforced by the no-replace publication above and proven
	// at the CLI layer by re-running into the same directory (refused as
	// already-exists with the first evidence untouched). No self-check write
	// is performed here: a failed republish would leave temp residue inside
	// the evidence directory.
	for _, f := range append(allFiles, "manifest.json") {
		if fi, err := os.Stat(filepath.Join(absOut, f)); err != nil || fi.Mode().Perm() != 0600 {
			return fail(fmt.Errorf("evidence %s is not mode 0600", f))
		}
	}

	if err := corpus.VerifyBinaryDigest(r.bin, binSHA); err != nil {
		return fail(fmt.Errorf("binary changed across lifecycle: %v", err))
	}
	if err := corpus.VerifyGitIdentity(absRepo, gitSHA, gitDirty); err != nil {
		return fail(fmt.Errorf("revision changed across lifecycle: %v", err))
	}
	logBytes, err := os.ReadFile(r.logPath)
	if err != nil {
		return fail(fmt.Errorf("read daemon log"))
	}
	if !bytes.Contains(logBytes, []byte(storageRoot)) {
		return fail(fmt.Errorf("daemon log does not name the configured root"))
	}
	if bytes.Contains(logBytes, []byte(managedStorageSecret)) {
		return fail(fmt.Errorf("daemon log exposes the storage secret"))
	}

	_, _ = fmt.Fprintf(out, "PASS managed-record %s pid=%d fixture=%s artifacts=%d\n", gitSHA, r.pid, r.ownedDB, len(allFiles)+1)
	_, _ = fmt.Fprintf(out, "precondition: %s\nfinal: %s\n", results[0].pre, results[0].post)
	return 0
}

func managedRandHex(n int) string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func managedDatabaseDSN(dsn, name string) (string, error) {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", fmt.Errorf("managed-record database URL is not a valid PostgreSQL connection string")
		}
		u.Path = "/" + name
		u.RawPath = ""
		q := u.Query()
		q.Del("dbname")
		q.Del("database")
		u.RawQuery = q.Encode()
		return u.String(), nil
	}
	return "", fmt.Errorf("managed-record database URL must be a postgres:// URL")
}

func managedFreePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

func managedStartDaemon(ctx context.Context, r *managedResources, dsn string) error {
	logF, err := os.Create(r.logPath)
	if err != nil {
		return err
	}
	cmd := exec.Command(r.bin)
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
		"INSTANT_V2_STORAGE_ROOT="+r.storageRoot,
		"INSTANT_V2_STORAGE_SECRET="+managedStorageSecret,
		"INSTANT_V2_HTTP_ADDR="+r.addr,
		"INSTANT_V2_METRICS_ADDR=",
		"INSTANT_OAUTH_GOOGLE_CLIENT_ID=cf002-google-id",
		"INSTANT_OAUTH_GOOGLE_CLIENT_SECRET=cf002-google-secret",
		"INSTANT_OAUTH_GITHUB_CLIENT_ID=cf002-github-id",
		"INSTANT_OAUTH_GITHUB_CLIENT_SECRET=cf002-github-secret",
	)
	cmd.Env = env
	if err := cmd.Start(); err != nil {
		_ = logF.Close()
		return fmt.Errorf("start managed instantd: %v", err)
	}
	_ = logF.Close()
	r.daemon = cmd
	return nil
}

func managedStopDaemon(r *managedResources) {
	if r.daemon == nil || r.daemon.ProcessState != nil && r.daemon.ProcessState.Exited() {
		return
	}
	_ = r.daemon.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() { _, _ = r.daemon.Process.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		_ = r.daemon.Process.Kill()
		_, _ = r.daemon.Process.Wait()
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", r.addr, 300*time.Millisecond)
		if err != nil {
			return
		}
		_ = c.Close()
		time.Sleep(100 * time.Millisecond)
	}
}

func managedWaitHealth(ctx context.Context, baseURL string) error {
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return fmt.Errorf("managed daemon never became healthy: %v", ctx.Err())
		default:
		}
		resp, err := managedHTTP.Get(baseURL + "/health")
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK && bytes.Contains(body, []byte(`"ok":true`)) {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("managed daemon never became healthy: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	return fmt.Errorf("managed daemon never became healthy")
}

func managedNewUUID(b []byte) {
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
}

func managedFreshUUID() [16]byte {
	var b [16]byte
	managedNewUUID(b[:])
	return b
}

func managedUUIDStr(b [16]byte) string {
	s := hex.EncodeToString(b[:])
	return s[0:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:32]
}

func managedResetFixture(ctx context.Context, r *managedResources, title string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("reset fixture begin")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `DELETE FROM apps WHERE id=$1`, r.appID); err != nil {
		return fmt.Errorf("reset fixture delete app")
	}
	if err := platform.CreateApp(ctx, tx, r.creatorID, r.appID, title); err != nil {
		return fmt.Errorf("reset fixture create app")
	}
	if err := platform.SetAdminToken(ctx, tx, r.appID, r.adminID); err != nil {
		return fmt.Errorf("reset fixture admin token")
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("reset fixture commit")
	}
	return nil
}

func managedFixtureState(ctx context.Context, r *managedResources) (string, error) {
	var title string
	var apps int
	if err := r.pool.QueryRow(ctx, `SELECT count(*), coalesce(max(title),'') FROM apps WHERE id=$1`, r.appID).Scan(&apps, &title); err != nil {
		return "", fmt.Errorf("fixture state apps")
	}
	var attrs, triples, idents int
	if err := r.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM attrs WHERE app_id=$1), (SELECT count(*) FROM triples WHERE app_id=$1), (SELECT count(*) FROM idents WHERE app_id=$1)`, r.appID).Scan(&attrs, &triples, &idents); err != nil {
		return "", fmt.Errorf("fixture state counts")
	}
	rows, err := r.pool.Query(ctx, `
		SELECT a.etype, a.label, t.value::text
		FROM triples t JOIN attrs a ON a.id = t.attr_id
		WHERE t.app_id=$1 ORDER BY 1,2,3`, r.appID)
	if err != nil {
		return "", fmt.Errorf("fixture state triples")
	}
	var parts []string
	for rows.Next() {
		var etype, label, value string
		if err := rows.Scan(&etype, &label, &value); err != nil {
			rows.Close()
			return "", fmt.Errorf("fixture state scan")
		}
		parts = append(parts, etype+"/"+label+"="+value)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("fixture state rows")
	}
	return fmt.Sprintf("apps=%d title=%q attrs=%d triples=%d idents=%d rows=[%s]", apps, title, attrs, triples, idents, strings.Join(parts, " ")), nil
}

func managedTransactTitle(ctx context.Context, r *managedResources, entityStr, title string) (int64, error) {
	body, _ := json.Marshal(map[string]any{
		"app-id": r.appStr,
		"steps":  []any{[]any{"update", "todos", entityStr, map[string]any{"title": title}}},
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.baseURL+"/admin/transact", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("transact request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-admin-token", r.adminStr)
	resp, err := managedHTTP.Do(req)
	if err != nil {
		return 0, fmt.Errorf("transact request failed")
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("admin transact rejected")
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return 0, fmt.Errorf("transact envelope invalid")
	}
	var txID int64
	if rawTx, ok := envelope["tx-id"]; !ok || json.Unmarshal(rawTx, &txID) != nil || txID <= 0 {
		return 0, fmt.Errorf("transact tx-id missing")
	}
	return txID, nil
}
