// Managed multi-client recording entry point (FU-01, packet CF-003-2B1).
//
// `corpusctl --mode managed-record-multiclient` extends the CF-002 managed
// lifecycle (clean-tree gate, build from --repo, owned isolated DB, loopback,
// identity, reset) with two concurrent SSE topologies over the
// production-mounted GET /runtime/sse + POST /runtime/sse path:
//
//   - shared-query: subscribers A and B race add-query through a start
//     barrier, converge to one identical ordered snapshot, converge again to
//     identical refresh frames at one exact trigger tx, then late joiner C
//     converges to the identical final snapshot at the same tx;
//   - shared-room: A joins solo, B late-joins to the exact two-member
//     snapshot, B resyncs explicitly, A updates presence for both peers, A
//     broadcasts peer-only with the exact sender session, B leaves and A
//     converges to the exact one-member snapshot.
//
// Every post-handshake data frame is retained verbatim (decoded deep copy) in
// arrival order keyed (subscriber, seq). The transport hello is excluded from
// retention; session/token distinctness is asserted at open instead. Any
// cross-subscriber divergence fails the capture; there is no quorum vote.
// After the final expected frame per subscriber a bounded quiet window
// (mcQuietWindow, 250ms) proves quiescence: any frame inside fails.
//
// Output discipline mirrors runManagedRecord: all evidence accumulates in
// memory and the output directory is reserved only after every capture and
// oracle succeeds (Policy A); on failure after reservation only the
// descriptor-pinned reservation is closed and private incomplete residue is
// left in place as untrusted and ineligible (Policy B). Per-subscriber
// retention persists as genuine NDJSON (one {"subscriber","seq","frame"}
// object per line) plus a facts JSON envelope and a checksummed manifest.
//
// Secrecy scoping: sse-token values (transport hello) are never retained and
// never persisted; POST envelopes carrying tokens are never persisted; the
// database DSN and admin token live only in memory; stdout/stderr and the
// manifest/facts files never carry session IDs, tokens, or DSNs. Verbatim
// retained frames inside the private 0600 NDJSON necessarily carry the
// session-keyed presence oracle; that is private evidence, not disclosure.
//
// Contract: docs/plans/finish-up/fu01-multiclient-capture-contract.md.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/instant-v2/instant-v2/internal/corpus"
)

// mcQuietWindow is the bounded quiescence bound recorded as a capture fact.
// Any frame arriving on a converged stream inside this window fails the
// capture. A finite window distinguishes quiescence from absence of evidence;
// it never claims no later event can occur.
const mcQuietWindow = 250 * time.Millisecond

// mcReadTimeout bounds each expected-frame read during capture.
const mcReadTimeout = 10 * time.Second

// mcRoomID is the capture-fact room all room-leg peers share. The daemon is
// fresh per run so the room starts empty; the ID is recorded in the facts
// file as part of the replay oracle.
const mcRoomID = "fu01-room"

// mcAppTitle is the owned fixture app title for this recorder.
const mcAppTitle = "fu01-managed"

// mcSeedTitles are the query-leg seed titles for entities A, B, C.
var mcSeedTitles = []string{"fu01-alpha", "fu01-beta", "fu01-gamma"}

// mcTriggerTitle is the query-leg trigger title applied to entity B.
const mcTriggerTitle = "fu01-beta-2"

// mcTestHookAfterReserve is a test-only hook run immediately after the output
// directory is reserved and before any evidence is published. Nil in
// production; mirrors managedTestHookAfterReserve for this recorder path.
var mcTestHookAfterReserve func(reserved *corpus.ReservedDir) error

// mcFrame is one retained post-handshake data frame.
type mcFrame struct {
	Subscriber string         `json:"subscriber"`
	Seq        int            `json:"seq"`
	Frame      map[string]any `json:"frame"`
}

// mcSub is one open SSE stream. Token is held in memory only: asserted
// distinct at open, used for POSTs, never retained or persisted.
type mcSub struct {
	name      string
	resp      *http.Response
	scanner   *bufio.Scanner
	sessionID string
	token     string
	app       string
	baseURL   string
}

// mcStreamClient has no overall timeout: streams stay open across the whole
// leg and the context governs the deadline. (managedHTTP's 15s timeout would
// kill a converged stream mid-leg.)
var mcStreamClient = &http.Client{}

func runManagedMulticlient(o options, out, diagnostic io.Writer) int {
	fail := func(err error) int { _, _ = fmt.Fprintf(diagnostic, "corpusctl: %v\n", err); return 1 }
	timeout := o.timeout
	if timeout < time.Minute {
		timeout = 3 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if strings.TrimSpace(o.outputDir) == "" {
		return fail(fmt.Errorf("--output-dir is required for managed-record-multiclient mode"))
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
	if err := corpus.CheckFreshOutputDir(absOut); err != nil {
		return fail(err)
	}

	gitSHA, gitDirty, err := corpus.GitIdentity(absRepo)
	if err != nil {
		return fail(err)
	}
	if gitDirty {
		return fail(fmt.Errorf("managed-record-multiclient requires a clean Git worktree for acceptance"))
	}

	var r managedResources
	tmp, err := os.MkdirTemp("", "fu01-managed-bin-*")
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
		return fail(fmt.Errorf("managed-record-multiclient requires DATABASE_URL or --database-url for an owned PostgreSQL role with CREATEDB privilege"))
	}
	// Static errors below: connection strings may embed credentials.
	if _, err := pgxpool.ParseConfig(adminDSN); err != nil {
		return fail(fmt.Errorf("managed-record-multiclient database URL is not a valid PostgreSQL connection string"))
	}
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		return fail(fmt.Errorf("managed-record-multiclient PostgreSQL connection failed; check DATABASE_URL and server readiness"))
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

	storageRoot, err := os.MkdirTemp("", "fu01-managed-root-*")
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
	logDir, err := os.MkdirTemp("", "fu01-managed-log-*")
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
		return fail(fmt.Errorf("managed-record-multiclient owned database URL is invalid"))
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
	if _, err := pool.Exec(ctx, `INSERT INTO instant_users (id,email) VALUES ($1,$2)`, r.creatorID, "fu01@example.test"); err != nil {
		return fail(fmt.Errorf("seed managed creator"))
	}
	if err := managedResetFixture(ctx, &r, mcAppTitle); err != nil {
		return fail(err)
	}

	identity := corpus.CandidateIdentity{
		GitSHA:       gitSHA,
		GitDirty:     gitDirty,
		GoVersion:    runtime.Version(),
		GOOS:         runtime.GOOS,
		GOARCH:       runtime.GOARCH,
		BinaryPath:   r.bin,
		BinarySHA256: binSHA,
		PID:          r.pid,
		Executable:   r.bin,
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

	pre, err := managedFixtureState(ctx, &r)
	if err != nil {
		return fail(err)
	}
	wantPre := fmt.Sprintf("apps=1 title=%q attrs=0 triples=0 idents=0 rows=[]", mcAppTitle)
	if pre != wantPre {
		return fail(fmt.Errorf("query leg precondition = %s; want %s", pre, wantPre))
	}

	// Room leg first: it needs no DB state and its init oracle pins empty
	// attrs, which holds only on the freshly reset fixture. The query leg
	// seeds afterwards; the recorded final state is the query-final state.
	rlogs, rfacts, err := mcCaptureRoomLeg(ctx, &r)
	if err != nil {
		mcCloseAll(rfacts.subs)
		return fail(err)
	}
	mcCloseAll(rfacts.subs)
	if err := identity.VerifyLive(absRepo); err != nil {
		return fail(fmt.Errorf("identity drift between legs: %v", err))
	}
	if r.daemon.Process.Pid != r.pid {
		return fail(fmt.Errorf("daemon handle changed between legs"))
	}

	qlogs, qfacts, err := mcCaptureQueryLeg(ctx, &r)
	if err != nil {
		mcCloseAll(qlogsToSubs(qfacts))
		return fail(err)
	}
	mcCloseAll(qfacts.subs)
	if err := identity.VerifyLive(absRepo); err != nil {
		return fail(fmt.Errorf("identity drift after query leg: %v", err))
	}

	final, err := managedFixtureState(ctx, &r)
	if err != nil {
		return fail(err)
	}
	if err := mcAssertQueryFinalState(final, qfacts); err != nil {
		return fail(err)
	}

	// All captures and oracles succeeded in memory; only now reserve output.
	reserved, err := corpus.ReserveOutputDir(absOut)
	if err != nil {
		return fail(err)
	}
	// Policy B: on failure only close the pinned reservation. Never delete
	// through the output pathname after reservation.
	defer func() {
		_ = reserved.Close()
	}()
	// Pin the same-file identity before the test hook: a post-reservation
	// rename-away plus replacement plant must fail every later publication
	// with the victim untouched.
	pin, err := mcPinOutputDir(reserved.Path())
	if err != nil {
		return fail(err)
	}
	if mcTestHookAfterReserve != nil {
		if err := mcTestHookAfterReserve(reserved); err != nil {
			return fail(err)
		}
	}
	if err := mcVerifyOutputPin(pin); err != nil {
		return fail(err)
	}

	ndjsonFiles := []struct {
		name string
		log  []mcFrame
	}{
		{"fu01-query-a.ndjson", qlogs["query-A"]},
		{"fu01-query-b.ndjson", qlogs["query-B"]},
		{"fu01-query-c.ndjson", qlogs["query-C"]},
		{"fu01-room-a.ndjson", rlogs["room-A"]},
		{"fu01-room-b.ndjson", rlogs["room-B"]},
	}
	var artifactNames []string
	for _, f := range ndjsonFiles {
		payload, err := mcRenderNDJSON(f.log)
		if err != nil {
			return fail(fmt.Errorf("render %s: %v", f.name, err))
		}
		base := strings.TrimSuffix(f.name, ".ndjson")
		if err := mcPublishNDJSON(reserved, pin, base, ".ndjson", payload); err != nil {
			return fail(fmt.Errorf("publish %s: %v", f.name, err))
		}
		artifactNames = append(artifactNames, f.name)
	}
	facts := mcBuildFacts(pre, final, qfacts)
	if err := reserved.WriteEvidence("fu01-capture-facts.json", facts); err != nil {
		return fail(fmt.Errorf("publish fu01-capture-facts.json: %v", err))
	}
	artifactNames = append(artifactNames, "fu01-capture-facts.json")

	entries, err := corpus.ManifestArtifacts(absOut, artifactNames)
	if err != nil {
		return fail(err)
	}
	manifest := corpus.CaptureManifest{
		Candidate:    identity,
		Precondition: pre,
		FinalState:   final,
		Artifacts:    entries,
	}
	if err := reserved.WriteEvidence("manifest.json", manifest); err != nil {
		return fail(fmt.Errorf("publish manifest: %v", err))
	}
	if err := corpus.VerifyCaptureManifest(absOut, manifest); err != nil {
		return fail(fmt.Errorf("manifest verification: %v", err))
	}
	for _, f := range append(append([]string(nil), artifactNames...), "manifest.json") {
		if fi, err := os.Stat(filepath.Join(absOut, f)); err != nil || fi.Mode().Perm() != 0600 {
			return fail(fmt.Errorf("evidence %s is not mode 0600", f))
		}
	}

	// The oracle is re-run from disk artifacts alone before PASS: checksums,
	// facts, every retained frame, ordering, payloads, and convergence.
	if err := mcVerifyDiskCapture(absOut); err != nil {
		return fail(err)
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

	_, _ = fmt.Fprintf(out, "PASS managed-record-multiclient %s pid=%d fixture=%s artifacts=%d\n", gitSHA, r.pid, r.ownedDB, len(artifactNames)+1)
	_, _ = fmt.Fprintf(out, "precondition: %s\nfinal: %s\n", pre, final)
	return 0
}

// mcCaptureFacts is the persisted capture-facts envelope. It carries trigger
// facts needed for replay (entity IDs, titles, tx IDs, attr IDs, room ID,
// quiet bound) and never carries session IDs, tokens, or DSNs: replay derives
// live sessions from the retained NDJSON init frames instead.
type mcCaptureFacts struct {
	Mode            string   `json:"mode"`
	Contract        string   `json:"contract"`
	RoomID          string   `json:"roomId"`
	QuietWindowMS   int64    `json:"quietWindowMs"`
	RedactedHeaders []string `json:"redactedHeaders"`
	Precondition    string   `json:"precondition"`
	FinalState      string   `json:"finalState"`
	Query           struct {
		Entities    []string          `json:"entities"`
		SeedTitles  []string          `json:"seedTitles"`
		FinalTitles map[string]string `json:"finalTitles"`
		TriggerTxID int64             `json:"triggerTxId"`
		IDAttr      string            `json:"idAttr"`
		TitleAttr   string            `json:"titleAttr"`
		WantInitial []any             `json:"wantInitial"`
		WantLate    []any             `json:"wantLate"`
	} `json:"query"`
	Room struct {
		Peers    []string `json:"peers"`
		EventIDs []string `json:"eventIds"`
	} `json:"room"`
}

// mcBuildFacts assembles the persisted capture-facts envelope from the
// in-memory query run. Used by the recorder and its tests identically.
func mcBuildFacts(pre, final string, q mcQueryRun) mcCaptureFacts {
	var facts mcCaptureFacts
	facts.Mode = "managed-record-multiclient"
	facts.Contract = "docs/plans/finish-up/fu01-multiclient-capture-contract.md"
	facts.RoomID = mcRoomID
	facts.QuietWindowMS = int64(mcQuietWindow / time.Millisecond)
	facts.RedactedHeaders = append([]string(nil), defaultRedactionHeaders...)
	facts.Precondition = pre
	facts.FinalState = final
	facts.Query.Entities = q.entities
	facts.Query.SeedTitles = append([]string(nil), mcSeedTitles...)
	facts.Query.FinalTitles = q.wantFinal
	facts.Query.TriggerTxID = q.triggerTx
	facts.Query.IDAttr = q.idAttr
	facts.Query.TitleAttr = q.titleAttr
	facts.Query.WantInitial = q.wantInitial
	// WantLate is the late joiner's first answer: the same ID-sorted rows
	// with final (post-trigger) titles. C attaches after the trigger, so
	// seed titles would be the wrong oracle here.
	facts.Query.WantLate = make([]any, 0, len(q.sortedEntities))
	for _, eid := range q.sortedEntities {
		facts.Query.WantLate = append(facts.Query.WantLate, map[string]any{"id": eid, "title": q.wantFinal[eid]})
	}
	facts.Room.Peers = []string{"peer-a", "peer-b"}
	facts.Room.EventIDs = []string{"join-a", "join-b", "presence-a", "broadcast-a", "leave-b"}
	return facts
}

// Sessions/tokens here never leave memory except inside verbatim NDJSON
// frames published to the private directory.
type mcQueryRun struct {
	subs           []*mcSub
	entities       []string
	sortedEntities []string
	triggerTx      int64
	idAttr         string
	titleAttr      string
	wantInitial    []any
	wantFinal      map[string]string
}

func qlogsToSubs(q mcQueryRun) []*mcSub { return q.subs }

// mcRoomRun carries in-memory room-leg state.
type mcRoomRun struct {
	subs []*mcSub
}

func mcCloseAll(subs []*mcSub) {
	for _, s := range subs {
		if s != nil && s.resp != nil && s.resp.Body != nil {
			_ = s.resp.Body.Close()
		}
	}
}

// mcAssertDistinctCredentials proves pairwise session/token uniqueness across
// every subscriber: no two subscribers share a session ID or a token. Either
// reuse fails the capture; tokens never leave memory.
func mcAssertDistinctCredentials(subs []*mcSub) error {
	if len(subs) == 0 {
		return fmt.Errorf("credential distinctness requires at least one subscriber")
	}
	for _, s := range subs {
		if s == nil || s.sessionID == "" || s.token == "" {
			return fmt.Errorf("subscriber credentials incomplete; aborting capture")
		}
	}
	for i := 0; i < len(subs); i++ {
		for j := i + 1; j < len(subs); j++ {
			if subs[i].sessionID == subs[j].sessionID {
				return fmt.Errorf("subscribers reused session; aborting capture")
			}
			if subs[i].token == subs[j].token {
				return fmt.Errorf("subscribers reused token; aborting capture")
			}
		}
	}
	return nil
}

// mcOpenSSE opens one SSE stream and consumes the transport hello without
// retaining it. Session/token distinctness against prior subs is asserted by
// the caller; this function validates the hello shape and extracts identity.
func mcOpenSSE(ctx context.Context, baseURL, app, name string) (*mcSub, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/runtime/sse?app_id="+app, nil)
	if err != nil {
		return nil, fmt.Errorf("subscriber %s: sse open request", name)
	}
	resp, err := mcStreamClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("subscriber %s: sse open failed", name)
	}
	if resp.StatusCode != http.StatusOK || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("subscriber %s: sse connect rejected", name)
	}
	sub := &mcSub{name: name, resp: resp, scanner: bufio.NewScanner(resp.Body), app: app, baseURL: baseURL}
	sub.scanner.Buffer(make([]byte, 64*1024), 1<<20)
	hello, err := mcReadRaw(sub, 10*time.Second)
	if err != nil {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("subscriber %s: sse handshake missing", name)
	}
	sessionID, _ := hello["session-id"].(string)
	token, _ := hello["sse-token"].(string)
	machineID, _ := hello["machine-id"].(string)
	if len(hello) != 4 || hello["op"] != "init-ok" || machineID == "" || sessionID == "" || token == "" {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("subscriber %s: sse handshake malformed", name)
	}
	sub.sessionID = sessionID
	sub.token = token
	return sub, nil
}

// mcReadRaw reads the next data frame without retaining it (used for the
// transport hello only).
func mcReadRaw(sub *mcSub, timeout time.Duration) (map[string]any, error) {
	type outcome struct {
		frame map[string]any
		err   error
	}
	ch := make(chan outcome, 1)
	go func() {
		for sub.scanner.Scan() {
			line := sub.scanner.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var frame map[string]any
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &frame); err != nil {
				ch <- outcome{err: err}
				return
			}
			ch <- outcome{frame: frame}
			return
		}
		if err := sub.scanner.Err(); err != nil {
			ch <- outcome{err: err}
		} else {
			ch <- outcome{err: io.EOF}
		}
	}()
	select {
	case got := <-ch:
		return got.frame, got.err
	case <-time.After(timeout):
		return nil, fmt.Errorf("subscriber %s: frame timeout", sub.name)
	}
}

// mcRead reads the next post-handshake data frame and retains a verbatim deep
// copy keyed (subscriber, seq). At most one outstanding read per subscriber
// may exist; after a timeout the capture fails so no further reads follow.
func mcRead(sub *mcSub, log *[]mcFrame) (map[string]any, error) {
	frame, err := mcReadRaw(sub, mcReadTimeout)
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("subscriber %s: stream ended", sub.name)
		}
		return nil, err
	}
	raw, err := json.Marshal(frame)
	if err != nil {
		return nil, fmt.Errorf("subscriber %s: retain frame", sub.name)
	}
	var cp map[string]any
	if err := json.Unmarshal(raw, &cp); err != nil {
		return nil, fmt.Errorf("subscriber %s: retain frame", sub.name)
	}
	*log = append(*log, mcFrame{Subscriber: sub.name, Seq: len(*log), Frame: cp})
	return frame, nil
}

// mcAssertQuiet proves quiescence by the bounded window: any data frame (or a
// stream end) inside the window fails the capture. The timed-out reader stays
// blocked only until stream close, and no further reads follow on that
// scanner after a quiet success.
func mcAssertQuiet(sub *mcSub) error {
	type outcome struct {
		frame map[string]any
		err   error
		ended bool
	}
	ch := make(chan outcome, 1)
	go func() {
		for sub.scanner.Scan() {
			line := sub.scanner.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var frame map[string]any
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &frame); err != nil {
				ch <- outcome{err: err}
				return
			}
			ch <- outcome{frame: frame}
			return
		}
		ch <- outcome{ended: true}
	}()
	select {
	case got := <-ch:
		if got.err != nil {
			return fmt.Errorf("subscriber %s: quiet window decode error", sub.name)
		}
		if got.ended {
			return fmt.Errorf("subscriber %s: stream ended inside quiet window", sub.name)
		}
		return fmt.Errorf("subscriber %s: unexpected frame inside quiet window", sub.name)
	case <-time.After(mcQuietWindow):
		return nil
	}
}

// mcPostRaw posts messages for one session and returns the raw result. It
// never includes session or token material in errors.
func mcPostRaw(ctx context.Context, baseURL, app, sessionID, token string, name string, messages ...map[string]any) (int, string, string, error) {
	payload, err := json.Marshal(map[string]any{
		"machine_id": "fu01-multiclient", "app_id": app,
		"session_id": sessionID, "sse_token": token,
		"messages": messages,
	})
	if err != nil {
		return 0, "", "", fmt.Errorf("subscriber %s: encode post", name)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/runtime/sse", bytes.NewReader(payload))
	if err != nil {
		return 0, "", "", fmt.Errorf("subscriber %s: post request", name)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := managedHTTP.Do(req)
	if err != nil {
		return 0, "", "", fmt.Errorf("subscriber %s: post failed", name)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, "", "", fmt.Errorf("subscriber %s: post body unreadable", name)
	}
	return resp.StatusCode, resp.Header.Get("Content-Type"), string(raw), nil
}

// mcPost asserts the exact POST acknowledgement contract.
func mcPost(ctx context.Context, sub *mcSub, messages ...map[string]any) error {
	status, contentType, body, err := mcPostRaw(ctx, sub.baseURL, sub.app, sub.sessionID, sub.token, sub.name, messages...)
	if err != nil {
		return err
	}
	if status != http.StatusOK || contentType != "application/json" || body != "{}\n" {
		return fmt.Errorf("subscriber %s: post acknowledgement rejected", sub.name)
	}
	return nil
}

// mcTransactSteps commits admin steps against the owned fixture and returns
// the exact triggering transaction ID. Only the tx ID is retained; response
// bodies and the admin token never enter evidence or diagnostics.
func mcTransactSteps(ctx context.Context, baseURL, appStr, adminStr string, steps []any) (int64, error) {
	body, err := json.Marshal(map[string]any{"app-id": appStr, "steps": steps})
	if err != nil {
		return 0, fmt.Errorf("transact encode failed")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/admin/transact", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("transact request failed")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-admin-token", adminStr)
	resp, err := managedHTTP.Do(req)
	if err != nil {
		return 0, fmt.Errorf("transact request failed")
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("admin transact rejected")
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil || len(envelope) != 1 {
		return 0, fmt.Errorf("transact envelope invalid")
	}
	var txID int64
	if txRaw, ok := envelope["tx-id"]; !ok || json.Unmarshal(txRaw, &txID) != nil || txID <= 0 {
		return 0, fmt.Errorf("transact tx-id missing")
	}
	return txID, nil
}

// mcDescribeFrame summarizes a frame for diagnostics without session, token,
// or payload values: op plus sorted top-level keys (and member count for
// presence data). Full frames stay in private NDJSON only.
func mcDescribeFrame(frame map[string]any) string {
	op, _ := frame["op"].(string)
	keys := make([]string, 0, len(frame))
	for k := range frame {
		keys = append(keys, k)
	}
	// Deterministic key order via sorted insertion.
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	extra := ""
	if data, ok := frame["data"].(map[string]any); ok && op == "refresh-presence" {
		extra = fmt.Sprintf(" members=%d", len(data))
	}
	return fmt.Sprintf("op=%q keys=[%s]%s", op, strings.Join(keys, ","), extra)
}

// mcAssertRetentionSeq proves a log carries a dense 0-based sequence with the
// exact subscriber tag.
func mcAssertRetentionSeq(log []mcFrame, name string, wantLen int) error {
	if len(log) != wantLen {
		return fmt.Errorf("retention %s length = %d; want exactly %d", name, len(log), wantLen)
	}
	for i, entry := range log {
		if entry.Subscriber != name || entry.Seq != i {
			return fmt.Errorf("retention %s entry %d miskeyed; want (%q, %d)", name, i, name, i)
		}
		if len(entry.Frame) == 0 {
			return fmt.Errorf("retention %s entry %d is empty", name, i)
		}
	}
	return nil
}

// mcOps returns the op sequence of a retained log.
func mcOps(log []mcFrame) []string {
	ops := make([]string, 0, len(log))
	for _, entry := range log {
		op, _ := entry.Frame["op"].(string)
		ops = append(ops, op)
	}
	return ops
}

// mcAssertInit asserts the query-leg protocol init shape and returns attr IDs.
func mcAssertInit(frame map[string]any, appID, name string) (idAttr, titleAttr string, err error) {
	fail := func() (string, string, error) {
		return "", "", fmt.Errorf("subscriber %s: init shape invalid (%s)", name, mcDescribeFrame(frame))
	}
	if len(frame) != 5 || frame["op"] != "init-ok" {
		return fail()
	}
	if sessionID, _ := frame["session-id"].(string); sessionID == "" {
		return fail()
	}
	if want := map[string]any{"status": "active"}; !reflect.DeepEqual(frame["app-status"], want) {
		return fail()
	}
	if want := map[string]any{"admin?": false, "app": map[string]any{"id": appID}, "user": nil}; !reflect.DeepEqual(frame["auth"], want) {
		return fail()
	}
	attrs, ok := frame["attrs"].([]any)
	if !ok || len(attrs) != 2 {
		return fail()
	}
	for _, raw := range attrs {
		attr, ok := raw.(map[string]any)
		if !ok {
			return fail()
		}
		identity, ok := attr["forward-identity"].([]any)
		if !ok || len(identity) != 3 || identity[0] != appID || identity[1] != "todos" {
			return fail()
		}
		attrID, _ := attr["id"].(string)
		if attrID == "" {
			return fail()
		}
		base := map[string]any{
			"cardinality": "one", "checked-data-type": nil, "forward-identity": identity,
			"id": attrID, "index?": false, "required?": false, "unique?": false, "value-type": "blob",
		}
		switch identity[2] {
		case "id":
			base["index?"] = true
			base["primary?"] = true
			base["required?"] = true
			base["unique?"] = true
			idAttr = attrID
		case "title":
			titleAttr = attrID
		default:
			return fail()
		}
		if !reflect.DeepEqual(attr, base) {
			return fail()
		}
	}
	if idAttr == "" || titleAttr == "" {
		return fail()
	}
	return idAttr, titleAttr, nil
}

// mcAssertAddQuery asserts the exact add-query acknowledgement.
func mcAssertAddQuery(frame map[string]any, eventID, name string) error {
	want := map[string]any{"client-event-id": eventID, "op": "add-query-ok"}
	if len(frame) != len(want) || !reflect.DeepEqual(frame, want) {
		return fmt.Errorf("subscriber %s: add-query ack invalid (%s)", name, mcDescribeFrame(frame))
	}
	return nil
}

// mcAssertOrderedSnapshot asserts a peer's initial whole-result snapshot: tx
// exactly 0 with the exact ordered object-tree rows (lengths before elements).
func mcAssertOrderedSnapshot(frame map[string]any, want []any, name string) error {
	if len(want) == 0 {
		return fmt.Errorf("subscriber %s: snapshot oracle must be non-empty", name)
	}
	txID, ok := frame["processed-tx-id"].(float64)
	if !ok || txID != 0 {
		return fmt.Errorf("subscriber %s: snapshot watermark invalid", name)
	}
	if len(frame) != 3 {
		return fmt.Errorf("subscriber %s: snapshot envelope invalid (%s)", name, mcDescribeFrame(frame))
	}
	wantFrame := map[string]any{
		"op": "refresh-ok", "processed-tx-id": txID,
		"computations": []any{map[string]any{
			"instaql-query":  map[string]any{"todos": map[string]any{}},
			"instaql-result": map[string]any{"todos": want},
		}},
	}
	if !reflect.DeepEqual(frame, wantFrame) {
		return fmt.Errorf("subscriber %s: snapshot diverged (%s)", name, mcDescribeFrame(frame))
	}
	return nil
}

// mcOrderedRefreshTitles reduces a transactional node-list result to
// id→title with exact per-triple shape checks.
func mcOrderedRefreshTitles(raw any, idAttr, titleAttr, name string) (map[string]string, error) {
	nodes, _ := raw.([]any)
	if len(nodes) == 0 {
		return nil, fmt.Errorf("subscriber %s: refresh result has no nodes", name)
	}
	titles := map[string]string{}
	idSeen := map[string]bool{}
	for _, rawNode := range nodes {
		node, _ := rawNode.(map[string]any)
		if len(node) != 2 {
			return nil, fmt.Errorf("subscriber %s: refresh node malformed", name)
		}
		if children, _ := node["child-nodes"].([]any); len(children) != 0 {
			return nil, fmt.Errorf("subscriber %s: refresh node has child nodes", name)
		}
		data, _ := node["data"].(map[string]any)
		datalog, _ := data["datalog-result"].(map[string]any)
		rows, _ := datalog["join-rows"].([]any)
		if len(rows) == 0 {
			return nil, fmt.Errorf("subscriber %s: refresh join rows empty", name)
		}
		for _, rawRow := range rows {
			triples, _ := rawRow.([]any)
			if len(triples) == 0 {
				return nil, fmt.Errorf("subscriber %s: refresh row empty", name)
			}
			for _, rawTriple := range triples {
				triple, _ := rawTriple.([]any)
				if len(triple) != 3 {
					return nil, fmt.Errorf("subscriber %s: refresh triple malformed", name)
				}
				eid, _ := triple[0].(string)
				attr, _ := triple[1].(string)
				value, _ := triple[2].(string)
				if eid == "" || value == "" {
					return nil, fmt.Errorf("subscriber %s: refresh triple incomplete", name)
				}
				switch attr {
				case idAttr:
					if eid != value {
						return nil, fmt.Errorf("subscriber %s: refresh id triple mismatched", name)
					}
					if idSeen[eid] {
						return nil, fmt.Errorf("subscriber %s: refresh duplicate id triple", name)
					}
					idSeen[eid] = true
				case titleAttr:
					if _, dup := titles[eid]; dup {
						return nil, fmt.Errorf("subscriber %s: refresh duplicate title triple", name)
					}
					titles[eid] = value
				default:
					return nil, fmt.Errorf("subscriber %s: refresh triple has unexpected attr", name)
				}
			}
		}
	}
	if len(idSeen) != len(titles) {
		return nil, fmt.Errorf("subscriber %s: refresh id/title coverage incomplete", name)
	}
	for eid, title := range titles {
		if !idSeen[eid] || title == "" {
			return nil, fmt.Errorf("subscriber %s: refresh entity incomplete", name)
		}
	}
	return titles, nil
}

// mcAssertOrderedRefresh asserts a peer refresh carries the exact triggering
// tx as a float64 watermark, no delta key (full-snapshot convergence), and
// the exact final titles.
func mcAssertOrderedRefresh(frame map[string]any, idAttr, titleAttr string, txID int64, want map[string]string, name string) error {
	if len(want) == 0 {
		return fmt.Errorf("subscriber %s: refresh oracle must be non-empty", name)
	}
	gotTx, ok := frame["processed-tx-id"].(float64)
	if !ok || gotTx != float64(txID) {
		return fmt.Errorf("subscriber %s: refresh watermark invalid for tx %d", name, txID)
	}
	if len(frame) != 3 || frame["op"] != "refresh-ok" {
		return fmt.Errorf("subscriber %s: refresh envelope invalid (%s)", name, mcDescribeFrame(frame))
	}
	computations, _ := frame["computations"].([]any)
	if len(computations) != 1 {
		return fmt.Errorf("subscriber %s: refresh computations invalid (%s)", name, mcDescribeFrame(frame))
	}
	entry, _ := computations[0].(map[string]any)
	if len(entry) != 2 || !reflect.DeepEqual(entry["instaql-query"], map[string]any{"todos": map[string]any{}}) {
		return fmt.Errorf("subscriber %s: refresh computation invalid (%s)", name, mcDescribeFrame(frame))
	}
	if _, hasDelta := entry["delta"]; hasDelta {
		return fmt.Errorf("subscriber %s: refresh carried delta", name)
	}
	titles, err := mcOrderedRefreshTitles(entry["instaql-result"], idAttr, titleAttr, name)
	if err != nil {
		return err
	}
	if len(titles) != len(want) {
		return fmt.Errorf("subscriber %s: refresh title count = %d; want exactly %d", name, len(titles), len(want))
	}
	if !reflect.DeepEqual(titles, want) {
		return fmt.Errorf("subscriber %s: refresh titles diverged", name)
	}
	return nil
}

// mcAssertLateSnapshot asserts a fresh subscriber's first answer: the
// init-query object tree at the exact current tx with the exact ordered rows.
// This envelope differs from peers' transactional node-list refreshes; both
// carry exact oracles and neither substitutes for the other.
func mcAssertLateSnapshot(frame map[string]any, txID int64, want []any, name string) error {
	if len(want) == 0 {
		return fmt.Errorf("subscriber %s: late snapshot oracle must be non-empty", name)
	}
	gotTx, ok := frame["processed-tx-id"].(float64)
	if !ok || gotTx != float64(txID) {
		return fmt.Errorf("subscriber %s: late snapshot watermark invalid for tx %d", name, txID)
	}
	if len(frame) != 3 || frame["op"] != "refresh-ok" {
		return fmt.Errorf("subscriber %s: late snapshot envelope invalid (%s)", name, mcDescribeFrame(frame))
	}
	computations, _ := frame["computations"].([]any)
	if len(computations) != 1 {
		return fmt.Errorf("subscriber %s: late snapshot computations invalid", name)
	}
	entry, _ := computations[0].(map[string]any)
	if len(entry) != 2 || !reflect.DeepEqual(entry["instaql-query"], map[string]any{"todos": map[string]any{}}) {
		return fmt.Errorf("subscriber %s: late snapshot computation invalid", name)
	}
	if _, hasDelta := entry["delta"]; hasDelta {
		return fmt.Errorf("subscriber %s: late snapshot carried delta", name)
	}
	result, _ := entry["instaql-result"].(map[string]any)
	if len(result) != 1 {
		return fmt.Errorf("subscriber %s: late snapshot result keys invalid", name)
	}
	rows, _ := result["todos"].([]any)
	if len(rows) != len(want) {
		return fmt.Errorf("subscriber %s: late snapshot rows = %d; want exactly %d", name, len(rows), len(want))
	}
	if !reflect.DeepEqual(rows, want) {
		return fmt.Errorf("subscriber %s: late snapshot rows diverged", name)
	}
	return nil
}

// mcLateTreeTitles reduces an init-tree snapshot to id→title.
func mcLateTreeTitles(frame map[string]any, name string) (map[string]string, error) {
	computations, _ := frame["computations"].([]any)
	if len(computations) != 1 {
		return nil, fmt.Errorf("subscriber %s: late snapshot computations invalid", name)
	}
	entry, _ := computations[0].(map[string]any)
	result, _ := entry["instaql-result"].(map[string]any)
	rows, _ := result["todos"].([]any)
	titles := map[string]string{}
	for _, rawRow := range rows {
		row, _ := rawRow.(map[string]any)
		if len(row) != 2 {
			return nil, fmt.Errorf("subscriber %s: late snapshot row malformed", name)
		}
		id, _ := row["id"].(string)
		title, _ := row["title"].(string)
		if id == "" || title == "" {
			return nil, fmt.Errorf("subscriber %s: late snapshot row incomplete", name)
		}
		if _, dup := titles[id]; dup {
			return nil, fmt.Errorf("subscriber %s: late snapshot duplicate id", name)
		}
		titles[id] = title
	}
	return titles, nil
}

// mcAssertRoomInit asserts the room-leg protocol init shape and returns the
// mounted session ID (kept in memory; replay derives it from retention).
func mcAssertRoomInit(frame map[string]any, appID, name string) (string, error) {
	fail := func() (string, error) {
		return "", fmt.Errorf("subscriber %s: room init shape invalid (%s)", name, mcDescribeFrame(frame))
	}
	if len(frame) != 5 || frame["op"] != "init-ok" {
		return fail()
	}
	sessionID, _ := frame["session-id"].(string)
	if sessionID == "" {
		return fail()
	}
	if attrs, ok := frame["attrs"].([]any); !ok || len(attrs) != 0 {
		return fail()
	}
	if want := map[string]any{"status": "active"}; !reflect.DeepEqual(frame["app-status"], want) {
		return fail()
	}
	if want := map[string]any{"admin?": false, "app": map[string]any{"id": appID}, "user": nil}; !reflect.DeepEqual(frame["auth"], want) {
		return fail()
	}
	return sessionID, nil
}

// mcExactFrame asserts exact key-count plus DeepEqual, with nested data
// key-counts checked before elements for broadcast payloads.
func mcExactFrame(got, want map[string]any, name, what string) error {
	if len(got) != len(want) {
		return fmt.Errorf("subscriber %s: %s key count = %d; want exactly %d", name, what, len(got), len(want))
	}
	if !reflect.DeepEqual(got, want) {
		return fmt.Errorf("subscriber %s: %s diverged (%s)", name, what, mcDescribeFrame(got))
	}
	if gotData, ok := got["data"]; ok {
		if wantData, ok := want["data"].(map[string]any); ok {
			gotMap, ok := gotData.(map[string]any)
			if !ok || len(gotMap) != len(wantData) {
				return fmt.Errorf("subscriber %s: %s data keys invalid", name, what)
			}
		}
	}
	return nil
}

// mcAssertRoomPresence asserts the exact presence snapshot for roomID with
// lengths checked before elements at the member level.
func mcAssertRoomPresence(frame map[string]any, roomID string, want map[string]any, name string) error {
	if len(want) == 0 {
		return fmt.Errorf("subscriber %s: presence oracle must be non-empty", name)
	}
	if len(frame) != 3 || frame["op"] != "refresh-presence" || frame["room-id"] != roomID {
		return fmt.Errorf("subscriber %s: presence envelope invalid (%s)", name, mcDescribeFrame(frame))
	}
	data, ok := frame["data"].(map[string]any)
	if !ok || len(data) != len(want) {
		return fmt.Errorf("subscriber %s: presence member count invalid (%s)", name, mcDescribeFrame(frame))
	}
	for sid, rawWant := range want {
		wantMember, _ := rawWant.(map[string]any)
		if wantMember == nil {
			return fmt.Errorf("subscriber %s: presence oracle member malformed", name)
		}
		gotMember, ok := data[sid].(map[string]any)
		if !ok || len(gotMember) != len(wantMember) {
			return fmt.Errorf("subscriber %s: presence member shape invalid (%s)", name, mcDescribeFrame(frame))
		}
	}
	if !reflect.DeepEqual(data, want) {
		return fmt.Errorf("subscriber %s: presence diverged (%s)", name, mcDescribeFrame(frame))
	}
	return nil
}

// mcAwaitAckAndPresence reads the actor's ack and its caused presence in
// either order as exactly the set {ack, presence} with no other interleave.
func mcAwaitAckAndPresence(sub *mcSub, log *[]mcFrame, ackOp string, wantAck, wantPresence map[string]any, roomID string) error {
	ackSeen, presenceSeen := false, false
	for i := 0; i < 2; i++ {
		frame, err := mcRead(sub, log)
		if err != nil {
			return err
		}
		switch frame["op"] {
		case ackOp:
			if ackSeen {
				return fmt.Errorf("subscriber %s: duplicate ack (%s)", sub.name, mcDescribeFrame(frame))
			}
			if err := mcExactFrame(frame, wantAck, sub.name, "ack"); err != nil {
				return err
			}
			ackSeen = true
		case "refresh-presence":
			if presenceSeen {
				return fmt.Errorf("subscriber %s: duplicate presence (%s)", sub.name, mcDescribeFrame(frame))
			}
			if err := mcAssertRoomPresence(frame, roomID, wantPresence, sub.name); err != nil {
				return err
			}
			presenceSeen = true
		default:
			return fmt.Errorf("subscriber %s: unexpected frame during %s (%s)", sub.name, ackOp, mcDescribeFrame(frame))
		}
	}
	if !ackSeen || !presenceSeen {
		return fmt.Errorf("subscriber %s: ack/presence incomplete", sub.name)
	}
	return nil
}

// mcAssertRoomOps asserts the retained op sequence step by step, where each
// step is the exact set of ops (order-free within the step, exact across
// steps). This normalizes the allowed ack/presence either-order while keeping
// every step exact.
func mcAssertRoomOps(log []mcFrame, name string, wantSteps [][]string) error {
	got := mcOps(log)
	total := 0
	for _, step := range wantSteps {
		total += len(step)
	}
	if len(got) != total {
		return fmt.Errorf("retention %s ops = %d frames; want exactly %d in %d steps", name, len(got), total, len(wantSteps))
	}
	offset := 0
	for s, step := range wantSteps {
		window := got[offset : offset+len(step)]
		counts := map[string]int{}
		for _, op := range window {
			counts[op]++
		}
		wantCounts := map[string]int{}
		for _, op := range step {
			wantCounts[op]++
		}
		if !reflect.DeepEqual(counts, wantCounts) {
			return fmt.Errorf("retention %s step %d ops invalid", name, s)
		}
		offset += len(step)
	}
	return nil
}

// mcAssertQueryFinalState sanity-checks the post-trigger fixture state. The
// exact string is recorded in the manifest; this gate only proves the three
// seeded entities with final titles survived, fail-closed on anything else.
func mcAssertQueryFinalState(final string, q mcQueryRun) error {
	if !strings.HasPrefix(final, fmt.Sprintf("apps=1 title=%q ", mcAppTitle)) {
		return fmt.Errorf("query leg final state prefix invalid")
	}
	for _, eid := range q.entities {
		if !strings.Contains(final, eid) {
			return fmt.Errorf("query leg final state missing entity")
		}
	}
	for _, title := range q.wantFinal {
		if !strings.Contains(final, title) {
			return fmt.Errorf("query leg final state missing title")
		}
	}
	return nil
}

// mcCaptureQueryLeg runs the shared-query topology: seed three entities, open
// A and B with distinct sessions/tokens, init both, race add-query through a
// start barrier, assert one identical ordered snapshot, commit one trigger
// change and assert identical refresh frames at its exact tx, then admit late
// joiner C to the identical final snapshot at the same tx. All retention is
// in-memory; the caller publishes after reservation.
func mcCaptureQueryLeg(ctx context.Context, r *managedResources) (map[string][]mcFrame, mcQueryRun, error) {
	var q mcQueryRun
	logs := map[string][]mcFrame{}
	fail := func(err error) (map[string][]mcFrame, mcQueryRun, error) {
		return nil, q, err
	}

	for i := 0; i < 3; i++ {
		q.entities = append(q.entities, managedUUIDStr(managedFreshUUID()))
	}
	// The server returns init-tree rows sorted by entity ID (observed: random
	// creation order converges to lexicographic ID order on the wire). The
	// exact ordered oracle is therefore built in ID-sorted order, which is
	// deterministic given the minted IDs.
	sortedEntities := append([]string(nil), q.entities...)
	for i := 1; i < len(sortedEntities); i++ {
		for j := i; j > 0 && sortedEntities[j] < sortedEntities[j-1]; j-- {
			sortedEntities[j], sortedEntities[j-1] = sortedEntities[j-1], sortedEntities[j]
		}
	}
	q.sortedEntities = sortedEntities
	var seedSteps []any
	for i, eid := range q.entities {
		seedSteps = append(seedSteps, []any{"update", "todos", eid, map[string]any{"title": mcSeedTitles[i]}})
	}
	if _, err := mcTransactSteps(ctx, r.baseURL, r.appStr, r.adminStr, seedSteps); err != nil {
		return fail(fmt.Errorf("query leg seed: %v", err))
	}

	a, err := mcOpenSSE(ctx, r.baseURL, r.appStr, "query-A")
	if err != nil {
		return fail(err)
	}
	b, err := mcOpenSSE(ctx, r.baseURL, r.appStr, "query-B")
	if err != nil {
		_ = a.resp.Body.Close()
		return fail(err)
	}
	q.subs = []*mcSub{a, b}
	if err := mcAssertDistinctCredentials(q.subs); err != nil {
		return fail(fmt.Errorf("query leg %v", err))
	}

	var logA, logB, logC []mcFrame
	if err := mcPost(ctx, a, map[string]any{"op": "init", "app-id": r.appStr}); err != nil {
		return fail(err)
	}
	frame, err := mcRead(a, &logA)
	if err != nil {
		return fail(err)
	}
	idAttrA, titleAttrA, err := mcAssertInit(frame, r.appStr, "query-A")
	if err != nil {
		return fail(err)
	}
	if err := mcPost(ctx, b, map[string]any{"op": "init", "app-id": r.appStr}); err != nil {
		return fail(err)
	}
	frame, err = mcRead(b, &logB)
	if err != nil {
		return fail(err)
	}
	idAttrB, titleAttrB, err := mcAssertInit(frame, r.appStr, "query-B")
	if err != nil {
		return fail(err)
	}
	if idAttrA != idAttrB || titleAttrA != titleAttrB {
		return fail(fmt.Errorf("query leg attr identity drift; aborting capture"))
	}
	q.idAttr, q.titleAttr = idAttrA, titleAttrA

	// Concurrent subscription through a start barrier. The barrier encourages
	// but does not force DB-level overlap; convergence is proven by the
	// DeepEqual oracle below, never by schedule assumption.
	clients := []*mcSub{a, b}
	eventIDs := []string{"fu01-query-a", "fu01-query-b"}
	type postResult struct {
		status      int
		contentType string
		body        string
		err         error
	}
	results := make([]postResult, len(clients))
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, client := range clients {
		wg.Add(1)
		go func(i int, client *mcSub) {
			defer wg.Done()
			<-start
			status, contentType, body, err := mcPostRaw(ctx, client.baseURL, client.app, client.sessionID, client.token, client.name, map[string]any{
				"op": "add-query", "q": map[string]any{"todos": map[string]any{}},
				"client-event-id": eventIDs[i],
			})
			results[i] = postResult{status: status, contentType: contentType, body: body, err: err}
		}(i, client)
	}
	close(start)
	wg.Wait()
	names := []string{"query-A", "query-B"}
	for i, result := range results {
		if result.err != nil {
			return fail(fmt.Errorf("query leg concurrent add-query failed on %s", names[i]))
		}
		if result.status != http.StatusOK || result.contentType != "application/json" || result.body != "{}\n" {
			return fail(fmt.Errorf("query leg concurrent add-query rejected on %s", names[i]))
		}
	}

	seedByEntity := map[string]string{}
	for i, eid := range q.entities {
		seedByEntity[eid] = mcSeedTitles[i]
	}
	q.wantInitial = make([]any, 0, 3)
	for _, eid := range q.sortedEntities {
		q.wantInitial = append(q.wantInitial, map[string]any{"id": eid, "title": seedByEntity[eid]})
	}
	snapshots := make([]map[string]any, len(clients))
	logPtrs := []*[]mcFrame{&logA, &logB}
	for i, client := range clients {
		frame, err := mcRead(client, logPtrs[i])
		if err != nil {
			return fail(err)
		}
		if err := mcAssertAddQuery(frame, eventIDs[i], names[i]); err != nil {
			return fail(err)
		}
		frame, err = mcRead(client, logPtrs[i])
		if err != nil {
			return fail(err)
		}
		if err := mcAssertOrderedSnapshot(frame, q.wantInitial, names[i]); err != nil {
			return fail(err)
		}
		snapshots[i] = frame
	}
	if !reflect.DeepEqual(snapshots[0], snapshots[1]) {
		return fail(fmt.Errorf("query leg concurrent snapshots diverged; aborting capture"))
	}

	txID, err := mcTransactSteps(ctx, r.baseURL, r.appStr, r.adminStr,
		[]any{[]any{"update", "todos", q.entities[1], map[string]any{"title": mcTriggerTitle}}})
	if err != nil {
		return fail(fmt.Errorf("query leg trigger: %v", err))
	}
	q.triggerTx = txID
	q.wantFinal = map[string]string{
		q.entities[0]: mcSeedTitles[0], q.entities[1]: mcTriggerTitle, q.entities[2]: mcSeedTitles[2],
	}
	refreshes := make([]map[string]any, len(clients))
	for i, client := range clients {
		frame, err := mcRead(client, logPtrs[i])
		if err != nil {
			return fail(err)
		}
		if err := mcAssertOrderedRefresh(frame, q.idAttr, q.titleAttr, txID, q.wantFinal, names[i]); err != nil {
			return fail(err)
		}
		refreshes[i] = frame
	}
	if !reflect.DeepEqual(refreshes[0], refreshes[1]) {
		return fail(fmt.Errorf("query leg concurrent refreshes diverged at tx %d; aborting capture", txID))
	}

	for i, want := range [][]string{
		{"init-ok", "add-query-ok", "refresh-ok", "refresh-ok"},
		{"init-ok", "add-query-ok", "refresh-ok", "refresh-ok"},
	} {
		if got := mcOps(*logPtrs[i]); !reflect.DeepEqual(got, want) {
			return fail(fmt.Errorf("query leg retention %s ops invalid", names[i]))
		}
		if err := mcAssertRetentionSeq(*logPtrs[i], names[i], 4); err != nil {
			return fail(err)
		}
	}
	// Replay from retention alone: canonical snapshots/refreshes re-assert
	// without touching the live streams.
	replaySnaps := []map[string]any{logA[2].Frame, logB[2].Frame}
	for _, snap := range replaySnaps {
		if err := mcAssertOrderedSnapshot(snap, q.wantInitial, "replay"); err != nil {
			return fail(fmt.Errorf("query leg replay snapshot: %v", err))
		}
	}
	if !reflect.DeepEqual(replaySnaps[0], replaySnaps[1]) {
		return fail(fmt.Errorf("query leg replayed snapshots diverged; aborting capture"))
	}
	replayRefresh := []map[string]any{logA[3].Frame, logB[3].Frame}
	for _, frame := range replayRefresh {
		if err := mcAssertOrderedRefresh(frame, q.idAttr, q.titleAttr, txID, q.wantFinal, "replay"); err != nil {
			return fail(fmt.Errorf("query leg replay refresh: %v", err))
		}
	}
	if !reflect.DeepEqual(replayRefresh[0], replayRefresh[1]) {
		return fail(fmt.Errorf("query leg replayed refreshes diverged; aborting capture"))
	}

	if err := mcAssertQuiet(a); err != nil {
		return fail(err)
	}
	if err := mcAssertQuiet(b); err != nil {
		return fail(err)
	}

	// Late joiner C attaches the same query after the trigger and must
	// converge to the identical final snapshot at the exact trigger tx via
	// the init-tree envelope.
	c, err := mcOpenSSE(ctx, r.baseURL, r.appStr, "query-C")
	if err != nil {
		return fail(err)
	}
	q.subs = append(q.subs, c)
	if err := mcAssertDistinctCredentials(q.subs); err != nil {
		return fail(fmt.Errorf("query leg late joiner %v", err))
	}
	if err := mcPost(ctx, c, map[string]any{"op": "init", "app-id": r.appStr}); err != nil {
		return fail(err)
	}
	lateInit, err := mcRead(c, &logC)
	if err != nil {
		return fail(err)
	}
	if attrs, ok := lateInit["attrs"].([]any); !ok || len(attrs) != 2 {
		return fail(fmt.Errorf("query leg late joiner attrs invalid"))
	}
	if err := mcPost(ctx, c, map[string]any{
		"op": "add-query", "q": map[string]any{"todos": map[string]any{}},
		"client-event-id": "fu01-query-c",
	}); err != nil {
		return fail(err)
	}
	frame, err = mcRead(c, &logC)
	if err != nil {
		return fail(err)
	}
	if err := mcAssertAddQuery(frame, "fu01-query-c", "query-C"); err != nil {
		return fail(err)
	}
	lateSnap, err := mcRead(c, &logC)
	if err != nil {
		return fail(err)
	}
	wantLate := make([]any, 0, 3)
	for _, eid := range q.sortedEntities {
		title := seedByEntity[eid]
		if eid == q.entities[1] {
			title = mcTriggerTitle
		}
		wantLate = append(wantLate, map[string]any{"id": eid, "title": title})
	}
	if err := mcAssertLateSnapshot(lateSnap, txID, wantLate, "query-C"); err != nil {
		return fail(err)
	}
	titles, err := mcLateTreeTitles(lateSnap, "query-C")
	if err != nil {
		return fail(err)
	}
	if !reflect.DeepEqual(titles, q.wantFinal) {
		return fail(fmt.Errorf("query leg late joiner titles diverged; aborting capture"))
	}
	if err := mcAssertRetentionSeq(logC, "query-C", 3); err != nil {
		return fail(err)
	}
	replayLate := logC[2].Frame
	if err := mcAssertLateSnapshot(replayLate, txID, wantLate, "replay-C"); err != nil {
		return fail(fmt.Errorf("query leg replay late snapshot: %v", err))
	}
	if replayTitles, err := mcLateTreeTitles(replayLate, "replay-C"); err != nil || !reflect.DeepEqual(replayTitles, q.wantFinal) {
		return fail(fmt.Errorf("query leg replayed late titles diverged; aborting capture"))
	}
	if err := mcAssertQuiet(c); err != nil {
		return fail(err)
	}

	logs["query-A"] = logA
	logs["query-B"] = logB
	logs["query-C"] = logC
	return logs, q, nil
}

// mcCaptureRoomLeg runs the shared-room topology: A joins solo, B late-joins
// to the exact two-member snapshot, B resyncs explicitly, A updates presence
// for both peers, A broadcasts peer-only with the exact sender session, B
// leaves and A converges to the exact one-member snapshot. Sender isolation
// is proven by ordering: A's frame after its broadcast ack must be the
// leave-driven presence, never an echo.
func mcCaptureRoomLeg(ctx context.Context, r *managedResources) (map[string][]mcFrame, mcRoomRun, error) {
	var rm mcRoomRun
	logs := map[string][]mcFrame{}
	fail := func(err error) (map[string][]mcFrame, mcRoomRun, error) {
		return nil, rm, err
	}

	a, err := mcOpenSSE(ctx, r.baseURL, r.appStr, "room-A")
	if err != nil {
		return fail(err)
	}
	b, err := mcOpenSSE(ctx, r.baseURL, r.appStr, "room-B")
	if err != nil {
		_ = a.resp.Body.Close()
		return fail(err)
	}
	rm.subs = []*mcSub{a, b}
	if err := mcAssertDistinctCredentials(rm.subs); err != nil {
		return fail(fmt.Errorf("room leg %v", err))
	}

	var logA, logB []mcFrame
	if err := mcPost(ctx, a, map[string]any{"op": "init", "app-id": r.appStr}); err != nil {
		return fail(err)
	}
	frame, err := mcRead(a, &logA)
	if err != nil {
		return fail(err)
	}
	aSess, err := mcAssertRoomInit(frame, r.appStr, "room-A")
	if err != nil {
		return fail(err)
	}
	if err := mcPost(ctx, b, map[string]any{"op": "init", "app-id": r.appStr}); err != nil {
		return fail(err)
	}
	frame, err = mcRead(b, &logB)
	if err != nil {
		return fail(err)
	}
	bSess, err := mcAssertRoomInit(frame, r.appStr, "room-B")
	if err != nil {
		return fail(err)
	}
	if aSess == "" || bSess == "" || aSess == bSess {
		return fail(fmt.Errorf("room leg sessions not distinct; aborting capture"))
	}

	if err := mcPost(ctx, a, map[string]any{
		"op": "join-room", "room-id": mcRoomID, "peer-id": "peer-a",
		"data": map[string]any{"mood": "ok"}, "client-event-id": "join-a",
	}); err != nil {
		return fail(err)
	}
	wantSolo := map[string]any{
		aSess: map[string]any{"peer": "peer-a", "data": map[string]any{"mood": "ok"}},
	}
	if err := mcAwaitAckAndPresence(a, &logA, "join-room-ok", map[string]any{
		"op": "join-room-ok", "room-id": mcRoomID, "client-event-id": "join-a",
	}, wantSolo, mcRoomID); err != nil {
		return fail(err)
	}

	wantBoth := map[string]any{
		aSess: map[string]any{"peer": "peer-a", "data": map[string]any{"mood": "ok"}},
		bSess: map[string]any{"peer": "peer-b", "data": map[string]any{}},
	}
	if err := mcPost(ctx, b, map[string]any{
		"op": "join-room", "room-id": mcRoomID, "peer-id": "peer-b",
		"data": map[string]any{}, "client-event-id": "join-b",
	}); err != nil {
		return fail(err)
	}
	if err := mcAwaitAckAndPresence(b, &logB, "join-room-ok", map[string]any{
		"op": "join-room-ok", "room-id": mcRoomID, "client-event-id": "join-b",
	}, wantBoth, mcRoomID); err != nil {
		return fail(err)
	}
	frame, err = mcRead(a, &logA)
	if err != nil {
		return fail(err)
	}
	if err := mcAssertRoomPresence(frame, mcRoomID, wantBoth, "room-A"); err != nil {
		return fail(err)
	}

	if err := mcPost(ctx, b, map[string]any{"op": "refresh-presence", "room-id": mcRoomID}); err != nil {
		return fail(err)
	}
	frame, err = mcRead(b, &logB)
	if err != nil {
		return fail(err)
	}
	if err := mcAssertRoomPresence(frame, mcRoomID, wantBoth, "room-B"); err != nil {
		return fail(err)
	}

	if err := mcPost(ctx, a, map[string]any{
		"op": "set-presence", "room-id": mcRoomID,
		"data": map[string]any{"mood": "great"}, "client-event-id": "presence-a",
	}); err != nil {
		return fail(err)
	}
	wantUpdated := map[string]any{
		aSess: map[string]any{"peer": "peer-a", "data": map[string]any{"mood": "great"}},
		bSess: map[string]any{"peer": "peer-b", "data": map[string]any{}},
	}
	if err := mcAwaitAckAndPresence(a, &logA, "set-presence-ok", map[string]any{
		"op": "set-presence-ok", "room-id": mcRoomID, "client-event-id": "presence-a",
	}, wantUpdated, mcRoomID); err != nil {
		return fail(err)
	}
	frame, err = mcRead(b, &logB)
	if err != nil {
		return fail(err)
	}
	if err := mcAssertRoomPresence(frame, mcRoomID, wantUpdated, "room-B"); err != nil {
		return fail(err)
	}

	if err := mcPost(ctx, a, map[string]any{
		"op": "client-broadcast", "room-id": mcRoomID, "topic": "chat",
		"data": map[string]any{"message": "hello"}, "client-event-id": "broadcast-a",
	}); err != nil {
		return fail(err)
	}
	frame, err = mcRead(a, &logA)
	if err != nil {
		return fail(err)
	}
	if err := mcExactFrame(frame, map[string]any{
		"op": "client-broadcast-ok", "client-event-id": "broadcast-a",
	}, "room-A", "broadcast ack"); err != nil {
		return fail(err)
	}
	frame, err = mcRead(b, &logB)
	if err != nil {
		return fail(err)
	}
	wantBroadcast := map[string]any{
		"op": "server-broadcast", "room-id": mcRoomID, "topic": "chat",
		"session-id": aSess,
		"data":       map[string]any{"peer": "peer-a", "data": map[string]any{"message": "hello"}},
	}
	if err := mcExactFrame(frame, wantBroadcast, "room-B", "broadcast"); err != nil {
		return fail(err)
	}

	if err := mcPost(ctx, b, map[string]any{
		"op": "leave-room", "room-id": mcRoomID, "client-event-id": "leave-b",
	}); err != nil {
		return fail(err)
	}
	frame, err = mcRead(b, &logB)
	if err != nil {
		return fail(err)
	}
	if err := mcExactFrame(frame, map[string]any{
		"op": "leave-room-ok", "room-id": mcRoomID, "client-event-id": "leave-b",
	}, "room-B", "leave ack"); err != nil {
		return fail(err)
	}
	wantA := map[string]any{
		aSess: map[string]any{"peer": "peer-a", "data": map[string]any{"mood": "great"}},
	}
	frame, err = mcRead(a, &logA)
	if err != nil {
		return fail(err)
	}
	if err := mcAssertRoomPresence(frame, mcRoomID, wantA, "room-A"); err != nil {
		return fail(err)
	}

	if err := mcAssertRoomOps(logA, "room-A", [][]string{
		{"init-ok"},
		{"join-room-ok", "refresh-presence"},
		{"refresh-presence"},
		{"set-presence-ok", "refresh-presence"},
		{"client-broadcast-ok"},
		{"refresh-presence"},
	}); err != nil {
		return fail(err)
	}
	if err := mcAssertRoomOps(logB, "room-B", [][]string{
		{"init-ok"},
		{"join-room-ok", "refresh-presence"},
		{"refresh-presence"},
		{"refresh-presence"},
		{"server-broadcast"},
		{"leave-room-ok"},
	}); err != nil {
		return fail(err)
	}
	if err := mcAssertRetentionSeq(logA, "room-A", 8); err != nil {
		return fail(err)
	}
	if err := mcAssertRetentionSeq(logB, "room-B", 7); err != nil {
		return fail(err)
	}
	// Replay from retention alone: every presence frame re-asserts against
	// the recorded snapshots and the broadcast re-asserts the exact sender.
	if err := mcReplayRoomPresence(logA, "room-A", []map[string]any{wantSolo, wantBoth, wantUpdated, wantA}); err != nil {
		return fail(err)
	}
	if err := mcReplayRoomPresence(logB, "room-B", []map[string]any{wantBoth, wantBoth, wantUpdated}); err != nil {
		return fail(err)
	}
	if found := mcFindOp(logB, "server-broadcast"); found == nil {
		return fail(fmt.Errorf("room leg replay: server-broadcast missing"))
	} else if err := mcExactFrame(found, wantBroadcast, "replay-B", "broadcast"); err != nil {
		return fail(fmt.Errorf("room leg replay broadcast: %v", err))
	}

	if err := mcQuiesceRoomLeg(a, b); err != nil {
		return fail(err)
	}

	logs["room-A"] = logA
	logs["room-B"] = logB
	return logs, rm, nil
}

// mcQuiesceRoomLeg proves quiescence on both room streams after the final
// leave convergence: any frame (or stream end) inside the bounded window on
// either stream fails the capture. B's check is mandatory: B's final
// leave-room-ok is not proof of silence.
func mcQuiesceRoomLeg(a, b *mcSub) error {
	if err := mcAssertQuiet(a); err != nil {
		return err
	}
	return mcAssertQuiet(b)
}

// mcReplayRoomPresence re-asserts every retained refresh-presence frame
// against the expected snapshot sequence in order.
func mcReplayRoomPresence(log []mcFrame, name string, want []map[string]any) error {
	var presence []map[string]any
	for _, entry := range log {
		if entry.Frame["op"] == "refresh-presence" {
			presence = append(presence, entry.Frame)
		}
	}
	if len(presence) != len(want) {
		return fmt.Errorf("replay %s presence frames = %d; want exactly %d", name, len(presence), len(want))
	}
	for i := range want {
		if err := mcAssertRoomPresence(presence[i], mcRoomID, want[i], name); err != nil {
			return fmt.Errorf("replay %s presence %d: %v", name, i, err)
		}
	}
	return nil
}

// mcFindOp returns the retained frame with op, or nil unless exactly one.
func mcFindOp(log []mcFrame, op string) map[string]any {
	var found map[string]any
	count := 0
	for _, entry := range log {
		if entry.Frame["op"] == op {
			found = entry.Frame
			count++
		}
	}
	if count != 1 {
		return nil
	}
	return found
}

// mcReadNDJSONFile reads one per-subscriber NDJSON artifact back into
// retention, enforcing newline-delimited single-object lines.
func mcReadNDJSONFile(dir, name string) ([]mcFrame, error) {
	raw, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return nil, fmt.Errorf("read %s: %v", name, err)
	}
	if len(raw) == 0 || raw[len(raw)-1] != '\n' {
		return nil, fmt.Errorf("%s is not newline-delimited", name)
	}
	var log []mcFrame
	for i, line := range bytes.Split(bytes.TrimSuffix(raw, []byte("\n")), []byte("\n")) {
		var entry mcFrame
		if err := json.Unmarshal(line, &entry); err != nil {
			return nil, fmt.Errorf("%s line %d invalid: %v", name, i, err)
		}
		log = append(log, entry)
	}
	return log, nil
}

// mcDiskAppID extracts the auth app ID from a retained init frame.
func mcDiskAppID(frame map[string]any) (string, error) {
	auth, _ := frame["auth"].(map[string]any)
	app, _ := auth["app"].(map[string]any)
	id, _ := app["id"].(string)
	if id == "" {
		return "", fmt.Errorf("init frame carries no auth app id")
	}
	return id, nil
}

// mcVerifyDiskCapture re-runs the entire capture oracle from disk artifacts
// alone: manifest checksums first, then facts plus all five NDJSON logs with
// every retained frame, exact op-step ordering on both room streams, init and
// ack payloads (including query-B/C acks and the room-B leave event ID),
// session relationships, snapshots, watermarks, no-delta, and no-quorum
// DeepEqual convergence. Any mismatch returns an error; PASS is emitted only
// on nil.
func mcVerifyDiskCapture(outDir string) error {
	manifestBytes, err := os.ReadFile(filepath.Join(outDir, "manifest.json"))
	if err != nil {
		return fmt.Errorf("disk replay: manifest missing: %v", err)
	}
	var manifest corpus.CaptureManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return fmt.Errorf("disk replay: manifest invalid: %v", err)
	}
	if err := corpus.VerifyCaptureManifest(outDir, manifest); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	if len(manifest.Artifacts) != 6 {
		return fmt.Errorf("disk replay: artifacts = %d; want 6 (5 NDJSON + facts)", len(manifest.Artifacts))
	}
	for _, want := range []string{
		"fu01-query-a.ndjson", "fu01-query-b.ndjson", "fu01-query-c.ndjson",
		"fu01-room-a.ndjson", "fu01-room-b.ndjson", "fu01-capture-facts.json",
	} {
		found := false
		for _, a := range manifest.Artifacts {
			if a.Name == want {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("disk replay: manifest omits %s", want)
		}
	}
	factsBytes, err := os.ReadFile(filepath.Join(outDir, "fu01-capture-facts.json"))
	if err != nil {
		return fmt.Errorf("disk replay: facts missing: %v", err)
	}
	var facts mcCaptureFacts
	if err := json.Unmarshal(factsBytes, &facts); err != nil {
		return fmt.Errorf("disk replay: facts invalid: %v", err)
	}
	if facts.QuietWindowMS != int64(mcQuietWindow/time.Millisecond) || facts.RoomID != mcRoomID {
		return fmt.Errorf("disk replay: facts capture identity incomplete")
	}
	if facts.Query.TriggerTxID <= 0 || facts.Query.IDAttr == "" || facts.Query.TitleAttr == "" {
		return fmt.Errorf("disk replay: facts query oracle incomplete")
	}
	if len(facts.Query.Entities) != 3 || len(facts.Query.WantInitial) != 3 || len(facts.Query.WantLate) != 3 || len(facts.Query.FinalTitles) != 3 {
		return fmt.Errorf("disk replay: facts query entities incomplete")
	}
	if !reflect.DeepEqual(facts.Room.EventIDs, []string{"join-a", "join-b", "presence-a", "broadcast-a", "leave-b"}) {
		return fmt.Errorf("disk replay: facts room event IDs incomplete")
	}
	if facts.Precondition != manifest.Precondition || facts.FinalState != manifest.FinalState {
		return fmt.Errorf("disk replay: facts fixture mismatch")
	}

	logA, err := mcReadNDJSONFile(outDir, "fu01-query-a.ndjson")
	if err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	logB, err := mcReadNDJSONFile(outDir, "fu01-query-b.ndjson")
	if err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	logC, err := mcReadNDJSONFile(outDir, "fu01-query-c.ndjson")
	if err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	roomA, err := mcReadNDJSONFile(outDir, "fu01-room-a.ndjson")
	if err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	roomB, err := mcReadNDJSONFile(outDir, "fu01-room-b.ndjson")
	if err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}

	if err := mcAssertRetentionSeq(logA, "query-A", 4); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	if err := mcAssertRetentionSeq(logB, "query-B", 4); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	if err := mcAssertRetentionSeq(logC, "query-C", 3); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	if err := mcAssertRetentionSeq(roomA, "room-A", 8); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	if err := mcAssertRetentionSeq(roomB, "room-B", 7); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}

	// Query init envelopes pin the app and attr IDs; every init must agree
	// and match the facts oracle.
	appID, err := mcDiskAppID(logA[0].Frame)
	if err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	if manifest.Candidate.FixtureApp != "" && manifest.Candidate.FixtureApp != appID {
		return fmt.Errorf("disk replay: init app diverges from manifest fixture app")
	}
	for _, tc := range []struct {
		name string
		log  []mcFrame
	}{
		{"query-A", logA}, {"query-B", logB}, {"query-C", logC},
	} {
		other, err := mcDiskAppID(tc.log[0].Frame)
		if err != nil {
			return fmt.Errorf("disk replay: %v", err)
		}
		if other != appID {
			return fmt.Errorf("disk replay: %s init app diverged", tc.name)
		}
	}
	idAttrA, titleAttrA, err := mcAssertInit(logA[0].Frame, appID, "disk-A")
	if err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	idAttrB, titleAttrB, err := mcAssertInit(logB[0].Frame, appID, "disk-B")
	if err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	idAttrC, titleAttrC, err := mcAssertInit(logC[0].Frame, appID, "disk-C")
	if err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	if idAttrA != facts.Query.IDAttr || titleAttrA != facts.Query.TitleAttr ||
		idAttrB != facts.Query.IDAttr || titleAttrB != facts.Query.TitleAttr ||
		idAttrC != facts.Query.IDAttr || titleAttrC != facts.Query.TitleAttr {
		return fmt.Errorf("disk replay: init attr IDs diverged from facts")
	}

	// Every add-query ack carries its exact client event ID.
	if err := mcAssertAddQuery(logA[1].Frame, "fu01-query-a", "disk-A"); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	if err := mcAssertAddQuery(logB[1].Frame, "fu01-query-b", "disk-B"); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	if err := mcAssertAddQuery(logC[1].Frame, "fu01-query-c", "disk-C"); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}

	// Exact op ordering on all three query streams.
	if got := mcOps(logA); !reflect.DeepEqual(got, []string{"init-ok", "add-query-ok", "refresh-ok", "refresh-ok"}) {
		return fmt.Errorf("disk replay: query-A ops = %#v", got)
	}
	if got := mcOps(logB); !reflect.DeepEqual(got, []string{"init-ok", "add-query-ok", "refresh-ok", "refresh-ok"}) {
		return fmt.Errorf("disk replay: query-B ops = %#v", got)
	}
	if got := mcOps(logC); !reflect.DeepEqual(got, []string{"init-ok", "add-query-ok", "refresh-ok"}) {
		return fmt.Errorf("disk replay: query-C ops = %#v", got)
	}

	// Snapshots and refreshes: exact oracle plus no-quorum DeepEqual, exact
	// watermarks, no delta.
	if !reflect.DeepEqual(logA[2].Frame, logB[2].Frame) {
		return fmt.Errorf("disk replay: snapshots diverged")
	}
	if err := mcAssertOrderedSnapshot(logA[2].Frame, facts.Query.WantInitial, "disk-A"); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	if err := mcAssertOrderedSnapshot(logB[2].Frame, facts.Query.WantInitial, "disk-B"); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	if !reflect.DeepEqual(logA[3].Frame, logB[3].Frame) {
		return fmt.Errorf("disk replay: refreshes diverged")
	}
	if err := mcAssertOrderedRefresh(logA[3].Frame, facts.Query.IDAttr, facts.Query.TitleAttr, facts.Query.TriggerTxID, facts.Query.FinalTitles, "disk-A"); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	if err := mcAssertOrderedRefresh(logB[3].Frame, facts.Query.IDAttr, facts.Query.TitleAttr, facts.Query.TriggerTxID, facts.Query.FinalTitles, "disk-B"); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	if err := mcAssertLateSnapshot(logC[2].Frame, facts.Query.TriggerTxID, facts.Query.WantLate, "disk-C"); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	if titles, err := mcLateTreeTitles(logC[2].Frame, "disk-C"); err != nil || !reflect.DeepEqual(titles, facts.Query.FinalTitles) {
		return fmt.Errorf("disk replay: late titles diverged (%v)", err)
	}

	// Query sessions recorded in the init frames must be pairwise distinct.
	sessQA, _ := logA[0].Frame["session-id"].(string)
	sessQB, _ := logB[0].Frame["session-id"].(string)
	sessQC, _ := logC[0].Frame["session-id"].(string)
	if sessQA == "" || sessQB == "" || sessQC == "" || sessQA == sessQB || sessQA == sessQC || sessQB == sessQC {
		return fmt.Errorf("disk replay: query sessions not distinct")
	}

	// Room init envelopes pin empty attrs and the app; sessions must be
	// distinct and every later presence key must be one of them.
	if _, err := mcAssertRoomInit(roomA[0].Frame, appID, "disk-room-A"); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	if _, err := mcAssertRoomInit(roomB[0].Frame, appID, "disk-room-B"); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	aSess, _ := roomA[0].Frame["session-id"].(string)
	bSess, _ := roomB[0].Frame["session-id"].(string)
	if aSess == "" || bSess == "" || aSess == bSess {
		return fmt.Errorf("disk replay: room sessions not distinct")
	}
	memberA := func(mood string) map[string]any {
		return map[string]any{"peer": "peer-a", "data": map[string]any{"mood": mood}}
	}
	wantSolo := map[string]any{aSess: memberA("ok")}
	wantBoth := map[string]any{aSess: memberA("ok"), bSess: map[string]any{"peer": "peer-b", "data": map[string]any{}}}
	wantUpdated := map[string]any{aSess: memberA("great"), bSess: map[string]any{"peer": "peer-b", "data": map[string]any{}}}
	wantOne := map[string]any{aSess: memberA("great")}
	if err := mcReplayRoomPresence(roomA, "disk-room-A", []map[string]any{wantSolo, wantBoth, wantUpdated, wantOne}); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	if err := mcReplayRoomPresence(roomB, "disk-room-B", []map[string]any{wantBoth, wantBoth, wantUpdated}); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}

	// Every room ack carries its exact client event ID; the broadcast pins
	// the exact sender session.
	countOp := func(log []mcFrame, op string) []map[string]any {
		var out []map[string]any
		for _, e := range log {
			if e.Frame["op"] == op {
				out = append(out, e.Frame)
			}
		}
		return out
	}
	joinA := countOp(roomA, "join-room-ok")
	if len(joinA) != 1 {
		return fmt.Errorf("disk replay: room-A join acks = %d; want exactly 1", len(joinA))
	}
	if err := mcExactFrame(joinA[0], map[string]any{
		"op": "join-room-ok", "room-id": mcRoomID, "client-event-id": "join-a",
	}, "disk-room-A", "join ack"); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	joinB := countOp(roomB, "join-room-ok")
	if len(joinB) != 1 {
		return fmt.Errorf("disk replay: room-B join acks = %d; want exactly 1", len(joinB))
	}
	if err := mcExactFrame(joinB[0], map[string]any{
		"op": "join-room-ok", "room-id": mcRoomID, "client-event-id": "join-b",
	}, "disk-room-B", "join ack"); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	setP := countOp(roomA, "set-presence-ok")
	if len(setP) != 1 {
		return fmt.Errorf("disk replay: room-A presence acks = %d; want exactly 1", len(setP))
	}
	if err := mcExactFrame(setP[0], map[string]any{
		"op": "set-presence-ok", "room-id": mcRoomID, "client-event-id": "presence-a",
	}, "disk-room-A", "presence ack"); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	bcA := countOp(roomA, "client-broadcast-ok")
	if len(bcA) != 1 {
		return fmt.Errorf("disk replay: room-A broadcast acks = %d; want exactly 1", len(bcA))
	}
	if err := mcExactFrame(bcA[0], map[string]any{
		"op": "client-broadcast-ok", "client-event-id": "broadcast-a",
	}, "disk-room-A", "broadcast ack"); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	leaveB := countOp(roomB, "leave-room-ok")
	if len(leaveB) != 1 {
		return fmt.Errorf("disk replay: room-B leave acks = %d; want exactly 1", len(leaveB))
	}
	if err := mcExactFrame(leaveB[0], map[string]any{
		"op": "leave-room-ok", "room-id": mcRoomID, "client-event-id": "leave-b",
	}, "disk-room-B", "leave ack"); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	broadcast := mcFindOp(roomB, "server-broadcast")
	if broadcast == nil {
		return fmt.Errorf("disk replay: broadcast missing")
	}
	if err := mcExactFrame(broadcast, map[string]any{
		"op": "server-broadcast", "room-id": mcRoomID, "topic": "chat",
		"session-id": aSess,
		"data":       map[string]any{"peer": "peer-a", "data": map[string]any{"message": "hello"}},
	}, "disk-room-B", "broadcast"); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}

	// Exact op-step ordering on both room streams.
	if err := mcAssertRoomOps(roomA, "room-A", [][]string{
		{"init-ok"},
		{"join-room-ok", "refresh-presence"},
		{"refresh-presence"},
		{"set-presence-ok", "refresh-presence"},
		{"client-broadcast-ok"},
		{"refresh-presence"},
	}); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	if err := mcAssertRoomOps(roomB, "room-B", [][]string{
		{"init-ok"},
		{"join-room-ok", "refresh-presence"},
		{"refresh-presence"},
		{"refresh-presence"},
		{"server-broadcast"},
		{"leave-room-ok"},
	}); err != nil {
		return fmt.Errorf("disk replay: %v", err)
	}
	return nil
}

// mcRenderNDJSON renders retained frames as genuine NDJSON: one
// {"subscriber","seq","frame"} object per line with deterministic key order.
func mcRenderNDJSON(log []mcFrame) ([]byte, error) {
	var buf bytes.Buffer
	for _, entry := range log {
		line, err := json.Marshal(entry)
		if err != nil {
			return nil, err
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return buf.Bytes(), nil
}

// mcOutputPin is a same-file identity pin for the reserved output directory,
// captured immediately after reservation (before the test hook) and verified
// before every publication. os.SameFile compares the pinned baseline against
// the live pathname, so a post-reservation rename-away plus replacement
// plant fails closed before any byte is written.
type mcOutputPin struct {
	path string
	info os.FileInfo
}

// mcTestHookPublishBeforeWrite forces a write failure inside NDJSON
// publication (nil in production). The temp file may remain as untrusted
// residue; the final artifact must never appear.
var mcTestHookPublishBeforeWrite func() error

// mcTestHookPublishBeforeSync forces a sync failure inside NDJSON
// publication (nil in production). No final artifact may appear.
var mcTestHookPublishBeforeSync func() error

// mcTestHookPublishBeforeDirSync forces a directory-sync failure inside NDJSON
// publication (nil in production). The final artifact is untrusted and the
// operation must report failure.
var mcTestHookPublishBeforeDirSync func() error

// mcPinOutputDir captures the same-file identity baseline for a reserved
// output directory. Call immediately after ReserveOutputDir, before any hook
// that may swap the visible pathname.
func mcPinOutputDir(path string) (*mcOutputPin, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat pinned output directory: %v", err)
	}
	if !fi.IsDir() || fi.Mode().Perm() != 0700 {
		return nil, fmt.Errorf("reserved output directory is not mode 0700")
	}
	return &mcOutputPin{path: path, info: fi}, nil
}

// mcVerifyOutputPin fails closed when the live pathname no longer refers to
// the pinned directory (rename-away, replacement plant, symlink swap, or
// mode change). It writes nothing.
func mcVerifyOutputPin(pin *mcOutputPin) error {
	if pin == nil || pin.path == "" || pin.info == nil {
		return fmt.Errorf("output pin is missing; aborting publication")
	}
	fi, err := os.Stat(pin.path)
	if err != nil {
		return fmt.Errorf("reserved output directory was replaced or swapped")
	}
	if !os.SameFile(pin.info, fi) {
		return fmt.Errorf("reserved output directory was replaced or swapped")
	}
	if !fi.IsDir() || fi.Mode().Perm() != 0700 {
		return fmt.Errorf("reserved output directory is not mode 0700")
	}
	return nil
}

// mcPublishNDJSON publishes one raw NDJSON payload into the reserved
// directory with write-once semantics through the descriptor-relative
// ReservedDir publisher: the pin is verified before publication (a
// post-reservation rename-away plus replacement plant fails here with the
// victim untouched), and temp creation, writes, sync, and no-replace
// publication all run against the pinned reservation FD via
// ReservedDir.WriteRawEvidence -- no output pathname is ever opened, linked,
// or synced. A swap of the visible directory after the pin check therefore
// cannot land payload or a final artifact in the replacement: the
// descriptor-relative publisher fails closed on identity mismatch with the
// victim untouched. The destination must not exist (write-once); any write,
// sync, or directory-sync failure returns an error with no final artifact
// published except the directory-sync case, which reports the final artifact
// as untrusted and incomplete. The manifest checksum published afterwards
// binds the exact bytes; no manifest is published unless every file lands.
func mcPublishNDJSON(reserved *corpus.ReservedDir, pin *mcOutputPin, scenarioID, suffix string, payload []byte) error {
	if runtime.GOOS == "windows" {
		return fmt.Errorf("multiclient NDJSON publication is unsupported on windows")
	}
	if reserved == nil {
		return fmt.Errorf("reserved output directory is missing; aborting publication")
	}
	if err := mcVerifyOutputPin(pin); err != nil {
		return err
	}
	path, err := reserved.EvidencePath(scenarioID, suffix)
	if err != nil {
		return err
	}
	if len(payload) == 0 {
		return fmt.Errorf("evidence payload for %q is empty", scenarioID+suffix)
	}
	if mcTestHookPublishBeforeWrite != nil {
		if err := mcTestHookPublishBeforeWrite(); err != nil {
			return fmt.Errorf("write evidence file %q: %v", scenarioID+suffix, err)
		}
	}
	if mcTestHookPublishBeforeSync != nil {
		if err := mcTestHookPublishBeforeSync(); err != nil {
			return fmt.Errorf("write evidence file %q: %v", scenarioID+suffix, err)
		}
	}
	if err := reserved.WriteRawEvidence(filepath.Base(path), payload); err != nil {
		return err
	}
	if mcTestHookPublishBeforeDirSync != nil {
		if err := mcTestHookPublishBeforeDirSync(); err != nil {
			return fmt.Errorf("sync evidence directory: %v; final artifact is untrusted and incomplete", err)
		}
	}
	return nil
}
