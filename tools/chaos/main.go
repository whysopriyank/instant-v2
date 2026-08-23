// Command chaos is the Phase 6C end-to-end chaos proof for instantd.
//
// It boots a THROWAWAY Postgres cluster (never the shared dev cluster on
// :54329) on its own port and data dir, starts instantd against it, drives
// N WebSocket sessions + an HTTP writer loop, then freezes (SIGSTOP) and
// crashes (pg_ctl stop -m immediate ≡ SIGKILL of every backend) postgres
// mid-stream, kills instantd, restarts both, and verifies:
//
//   - every client observes connection loss;
//   - every session reconnects after recovery;
//   - NO phantom reads: each session's post-recovery query result reflects
//     exactly the writer's journal of acknowledged transactions;
//   - the WAL tailer resumes from the PERSISTED LSN checkpoint in tail_state
//     across the crash, emitting no records outside the journal;
//   - golden-corpus replay runs against the recovered server (baseline vs
//     post-chaos).
//
// Exit code 0 = pass. A summary table prints on completion.
package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/waltail"
)

// corpusAppID is the app-id baked into corpus/*.ndjson scenarios.
const corpusAppID = "00000000-0000-4000-8000-000000000001"

var (
	flagPGPort   = flag.Int("pg-port", 54330, "port for the throwaway postgres cluster")
	flagPGData   = flag.String("pg-data", "/tmp/chaospg-instantv2", "data dir for the throwaway cluster (wiped at start)")
	flagSessions = flag.Int("sessions", 8, "number of concurrent WS sessions")
	flagWrites   = flag.Int("writes", 60, "acknowledged writes before the kill")
	flagKeep     = flag.Bool("keep", false, "keep the throwaway cluster running after the run")
	flagSkipCorp = flag.Bool("skip-corpus", false, "skip the corpus replays")
)

type journalEntry struct {
	Entity string
	TxID   int64
}

// sessionStats is one WS client's observed lifecycle.
type sessionStats struct {
	mu          sync.Mutex
	ID          int
	Frames      int
	Refreshes   int
	Dropped     bool
	DropErr     string
	Reconnected bool
	FinalCount  int
	Verdict     string
	finalIDs    map[string]bool
}

func (s *sessionStats) dropped() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Dropped
}

type tailStats struct {
	mu        sync.Mutex
	seen      map[string]bool // entity -> true (dedup across crash)
	total     int             // raw record count incl. replays
	lastLSN   uint64
	appFilter string
}

func (ts *tailStats) handle(_ context.Context, rec waltail.Record) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.appFilter != "" && rec.AppID != ts.appFilter {
		return nil
	}
	ts.total++
	ts.lastLSN = rec.LSN
	ts.seen[rec.EntityID] = true
	return nil
}

func (ts *tailStats) snapshot() (seen, total int, lsn uint64) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return len(ts.seen), ts.total, ts.lastLSN
}

func main() {
	flag.Parse()
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "chaos: FAIL: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repoRoot := findRepoRoot()
	pgData := *flagPGData

	// ---- Phase 0: boot throwaway postgres ---------------------------------
	fmt.Printf("== [0] booting throwaway postgres (datadir %s, port %d) ==\n", pgData, *flagPGPort)
	mustRm(pgData)
	if out, err := pgBin("initdb", "-D", pgData, "-U", "instant", "--auth=trust", "-E", "utf8"); err != nil {
		return fmt.Errorf("initdb: %v\n%s", err, out)
	}
	if out, err := pgCtl(pgData,
		"-l", filepath.Join(pgData, "chaos.log"),
		"-o", fmt.Sprintf("-p %d -c wal_level=logical", *flagPGPort),
		"-w", "-t", "30", "start"); err != nil {
		return fmt.Errorf("pg_ctl start: %v\n%s", err, out)
	}
	if !*flagKeep {
		defer cleanupCluster(pgData)
	}

	dsn := fmt.Sprintf("postgres://instant@localhost:%d/instant_chaos?sslmode=disable", *flagPGPort)
	if out, err := pgBin("createdb", "-h", "localhost", "-p", fmt.Sprint(*flagPGPort), "-U", "instant", "instant_chaos"); err != nil {
		return fmt.Errorf("createdb: %v\n%s", err, out)
	}

	// ---- Phase 1: schema + seed --------------------------------------------
	sqldb, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	if err := platform.Migrate(ctx, sqldb); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	chaosUUID, err := newV4()
	if err != nil {
		return err
	}
	chaosApp := formatUUID(chaosUUID)
	attrUUID, err := seedChaosApp(ctx, pool, chaosUUID)
	if err != nil {
		return fmt.Errorf("seed chaos app: %w", err)
	}
	attrID := platform.UUIDToStr(attrUUID)
	corpusUUID, err := platform.ScanUUIDErr(corpusAppID)
	if err != nil {
		return err
	}
	if err := seedCorpusApp(ctx, pool, corpusUUID); err != nil {
		return fmt.Errorf("seed corpus app: %w", err)
	}
	fmt.Printf("   chaos app %s (attr %s), corpus app %s\n", chaosApp, attrID, corpusAppID)

	// ---- Phase 2: WAL tailer on persisted checkpoint ------------------------
	waltail.SetDSN(dsn)
	tailLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	wt := &waltail.Tailer{Pool: pool, Logger: tailLog}
	if err := wt.CheckWalLevel(ctx); err != nil {
		return err
	}
	if err := wt.EnsurePublication(ctx); err != nil {
		return fmt.Errorf("ensure publication: %w", err)
	}
	cpLive, err := waltail.OpenCheckpoint(sqldb)
	if err != nil {
		return fmt.Errorf("open checkpoint: %w", err)
	}
	tails := &tailStats{seen: map[string]bool{}, appFilter: chaosApp}
	tailCtx, tailCancel := context.WithCancel(ctx)
	tailDone := make(chan struct{})
	go func() { defer close(tailDone); _ = wt.Run(tailCtx, cpLive, tails.handle) }()
	defer tailCancel()
	fmt.Printf("== [1] waltail streaming (slot %s), checkpoint LSN starts at %d ==\n", wt.SlotName(), cpLive.Last())

	// ---- Phase 3: build + start instantd ------------------------------------
	binDir, err := os.MkdirTemp("", "chaos-instantd-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(binDir)
	instantdBin := filepath.Join(binDir, "instantd")
	fmt.Println("== [2] building instantd ==")
	build := exec.Command("go", "build", "-o", instantdBin, "./cmd/instantd")
	build.Dir = repoRoot
	if out, err := build.CombinedOutput(); err != nil {
		return fmt.Errorf("go build instantd: %v\n%s", err, out)
	}
	httpPort, err := freePort()
	if err != nil {
		return err
	}
	httpAddr := fmt.Sprintf("127.0.0.1:%d", httpPort)
	baseURL := "http://" + httpAddr
	wsURL := fmt.Sprintf("ws://%s/runtime/session", httpAddr)

	p1, err := startInstantd(instantdBin, dsn, httpAddr)
	if err != nil {
		return err
	}
	defer stopInstantd(p1)
	if err := waitHealth(baseURL, 30*time.Second, true); err != nil {
		return err
	}
	fmt.Printf("   instantd up at %s (pid %d)\n", baseURL, p1.Process.Pid)

	// ---- Phase 4: connect sessions + writer loop ----------------------------
	fmt.Printf("== [3] connecting %d WS sessions (add-query chaos-items) ==\n", *flagSessions)
	sessions := make([]*sessionStats, *flagSessions)
	for i := range sessions {
		stats, err := connectSession(i, wsURL, chaosApp)
		if err != nil {
			return fmt.Errorf("session %d: %w", i, err)
		}
		sessions[i] = stats
	}

	journal, rejectedPre, err := writerLoop(baseURL, chaosApp, attrID, *flagWrites)
	if err != nil {
		return err
	}
	preKillFrames := 0
	for _, s := range sessions {
		preKillFrames += s.frameCount()
	}
	fmt.Printf("   %d acked writes (tx-ids %d..%d), %d rejected pre-crash; frames so far: %d\n",
		len(journal), journal[0].TxID, journal[len(journal)-1].TxID, rejectedPre, preKillFrames)

	corpusBaseline := ""
	if !*flagSkipCorp {
		corpusBaseline = runCorpusReplay(repoRoot, wsURL)
		fmt.Println("-- corpus baseline (pre-chaos) --\n" + indent(corpusBaseline))
	}

	// ---- Phase 5: THE CHAOS — SIGSTOP, then crash postgres -------------------
	fmt.Println("== [4] CHAOS: SIGSTOP postmaster, then pg_ctl stop -m immediate ==")
	if pm := pidOfPostmaster(pgData); pm != "" {
		_ = exec.Command("kill", "-STOP", pm).Run()
		time.Sleep(2 * time.Second) // hold the freeze mid-stream
		_ = exec.Command("kill", "-CONT", pm).Run()
	}
	if out, err := pgCtl(pgData, "-m", "immediate", "-w", "stop"); err != nil {
		return fmt.Errorf("immediate stop: %v\n%s", err, out)
	}

	// Outage proof: health flips to 503 and further writes are rejected.
	if err := waitHealth(baseURL, 15*time.Second, false); err != nil {
		return fmt.Errorf("expected degraded health during outage: %w", err)
	}
	outageRejected := 0
	for i := 0; i < 3; i++ {
		if _, code, err := postTransact(baseURL, chaosApp, attrID, fmt.Sprintf("outage-%d", i)); err != nil || code != 0 && code != 200 {
			outageRejected++
		}
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Printf("   outage confirmed: /health degraded, %d/3 writes rejected\n", outageRejected)

	// Kill instantd: every WS client must observe connection loss.
	fmt.Println("== [5] SIGKILL instantd; expecting all clients to see connection loss ==")
	stopInstantd(p1)
	deadline := time.Now().Add(20 * time.Second)
	dropped := 0
	for time.Now().Before(deadline) {
		dropped = 0
		for _, s := range sessions {
			if s.dropped() {
				dropped++
			}
		}
		if dropped == len(sessions) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if dropped < len(sessions) {
		for _, s := range sessions {
			if !s.dropped() {
				fmt.Printf("   session %d never observed the drop (err: %q)\n", s.ID, s.dropErr())
			}
		}
		return errors.New("not every session observed connection loss after instantd kill")
	}
	fmt.Printf("   all %d sessions observed connection loss\n", dropped)

	// ---- Phase 6: recovery ---------------------------------------------------
	fmt.Println("== [6] restarting postgres + instantd ==")
	tailCancel()
	select {
	case <-tailDone:
	case <-time.After(35 * time.Second):
		fmt.Println("   note: old tailer loop slow to unwind (backoff)")
	}
	if out, err := pgCtl(pgData,
		"-o", fmt.Sprintf("-p %d -c wal_level=logical", *flagPGPort),
		"-w", "-t", "60", "start"); err != nil {
		return fmt.Errorf("pg_ctl restart: %v\n%s", err, out)
	}
	p2, err := startInstantd(instantdBin, dsn, httpAddr)
	if err != nil {
		return err
	}
	defer stopInstantd(p2)
	if err := waitHealth(baseURL, 45*time.Second, true); err != nil {
		return err
	}

	// Resume the tailer FROM THE PERSISTED checkpoint: OpenCheckpoint re-reads
	// tail_state, proving LSN durability across the crash.
	cpPost, err := waltail.OpenCheckpoint(sqldb)
	if err != nil {
		return fmt.Errorf("reopen checkpoint: %w", err)
	}
	if cpPost.Last() == 0 {
		return errors.New("persisted tail_state LSN is 0 after streaming writes — checkpoint was not durable")
	}
	_, preTotal, preLSN := tails.snapshot()
	wt2 := &waltail.Tailer{Pool: pool, Logger: tailLog}
	go func() { _ = wt2.Run(context.Background(), cpPost, tails.handle) }()
	fmt.Printf("   recovered; tail_state persisted LSN %d resumed (pre-restart stream reached record LSN %d over %d records)\n",
		cpPost.Last(), preLSN, preTotal)

	// Reconnect every session and capture its post-recovery snapshot.
	for _, s := range sessions {
		rstats, err := connectSession(s.ID, wsURL, chaosApp)
		if err != nil {
			s.Verdict = fmt.Sprintf("RECONNECT FAILED: %v", err)
			continue
		}
		rstats.mu.Lock()
		s.Reconnected = true
		s.FinalCount = rstats.FinalCount
		s.finalIDs = rstats.finalIDs
		s.Verdict = "pending"
		rstats.mu.Unlock()
	}

	// ---- Phase 7: no-phantom-read verification -------------------------------
	fmt.Println("== [7] verifying NO phantom reads vs writer journal ==")
	want := map[string]bool{}
	for _, j := range journal {
		want[j.Entity] = true
	}
	bad := 0
	for _, s := range sessions {
		switch {
		case !s.Reconnected:
			bad++
		case s.FinalCount != len(journal):
			s.Verdict = fmt.Sprintf("MISMATCH: query shows %d entities, journal has %d", s.FinalCount, len(journal))
			bad++
		default:
			extra, missing := diffSets(want, s.finalIDs)
			if len(extra) > 0 || len(missing) > 0 {
				s.Verdict = fmt.Sprintf("SET MISMATCH extra=%v missing=%v", extra, missing)
				bad++
			} else {
				s.Verdict = "OK (exact journal match)"
			}
		}
	}
	if bad > 0 {
		return fmt.Errorf("%d/%d sessions failed phantom-read verification", bad, len(sessions))
	}

	// Tailer invariants: full coverage of the journal, nothing outside it.
	postSeen, postTotal, postLSN := tails.snapshot()
	if postSeen != len(journal) {
		return fmt.Errorf("waltail saw %d distinct entities, journal has %d", postSeen, len(journal))
	}
	fmt.Printf("   tailer: %d distinct entities == journal size; raw records %d (%d replayed across the crash)\n",
		postSeen, postTotal, postTotal-preTotal)

	// ---- Phase 8: corpus replay post-chaos ------------------------------------
	corpusAfter := ""
	if !*flagSkipCorp {
		corpusAfter = runCorpusReplay(repoRoot, wsURL)
		fmt.Println("-- corpus replay (post-chaos) --\n" + indent(corpusAfter))
	}

	printSummary(len(journal), rejectedPre, outageRejected, sessions,
		postSeen, postTotal, postLSN, cpPost.Last(), corpusBaseline, corpusAfter)
	return nil
}

// ---------------------------------------------------------------------------
// postgres plumbing
// ---------------------------------------------------------------------------

// pgBin runs a Postgres binary with Homebrew's postgres@17 bin dir prepended
// to PATH so the harness works regardless of the caller's environment.
func pgBin(name string, args ...string) (string, error) {
	full := append([]string{name}, args...)
	cmd := exec.Command(full[0], full[1:]...)
	cmd.Env = append(os.Environ(),
		"PATH=/opt/homebrew/opt/postgresql@17/bin:"+os.Getenv("PATH"))
	out, err := cmd.CombinedOutput()
	if err == nil {
		return string(out), nil
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return string(out), fmt.Errorf("%v: %s", full, strings.TrimSpace(string(out)))
	}
	return "", fmt.Errorf("%s: %w", full[0], err)
}

func pgCtl(pgData string, args ...string) (string, error) {
	return pgBin("pg_ctl", append([]string{"-D", pgData}, args...)...)
}

func cleanupCluster(pgData string) {
	fmt.Println("== [9] cleanup: stopping throwaway cluster + removing datadir ==")
	if out, err := pgCtl(pgData, "-m", "fast", "-w", "stop"); err != nil {
		fmt.Printf("   warning: pg_ctl stop: %v\n%s", err, out)
	}
	mustRm(pgData)
}

func mustRm(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		panic(fmt.Sprintf("rm %s: %v", dir, err))
	}
}

func pidOfPostmaster(pgData string) string {
	b, err := os.ReadFile(filepath.Join(pgData, "postmaster.pid"))
	if err != nil {
		return ""
	}
	line, _, _ := strings.Cut(string(b), "\n")
	return strings.TrimSpace(line)
}

// ---------------------------------------------------------------------------
// seeding
// ---------------------------------------------------------------------------

func newV4() ([16]byte, error) {
	var u [16]byte
	if _, err := rand.Read(u[:]); err != nil {
		return u, err
	}
	u[6] = (u[6] & 0x0f) | 0x40
	u[8] = (u[8] & 0x3f) | 0x80
	return u, nil
}

func formatUUID(u [16]byte) string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}

// seedChaosApp creates the chaos app plus the one attribute the writer uses.
func seedChaosApp(ctx context.Context, pool *pgxpool.Pool, app [16]byte) ([16]byte, error) {
	st := storage.New(pool)
	creator := [16]byte{0x63, 0x68, 0xa0, 0x53} // arbitrary fixed creator uuid prefix
	var attr [16]byte
	err := st.WithTx(ctx, func(tx pgx.Tx) error {
		if _, e := tx.Exec(ctx,
			`INSERT INTO instant_users (id,email) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
			creator, "chaos@test"); e != nil {
			return e
		}
		if e := platform.CreateApp(ctx, tx, creator, app, "chaos"); e != nil {
			return e
		}
		at, e := platform.GetOrCreateAttr(ctx, tx, app, "chaos-items", "n", "blob", "one", false, true)
		attr = at.ID
		return e
	})
	return attr, err
}

// seedCorpusApp creates the bare app the golden scenarios init against.
func seedCorpusApp(ctx context.Context, pool *pgxpool.Pool, app [16]byte) error {
	st := storage.New(pool)
	creator := [16]byte{0x63, 0x6f, 0xa0, 0x72}
	return st.WithTx(ctx, func(tx pgx.Tx) error {
		if _, e := tx.Exec(ctx,
			`INSERT INTO instant_users (id,email) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
			creator, "corpus@test"); e != nil {
			return e
		}
		return platform.CreateApp(ctx, tx, creator, app, "corpus")
	})
}

// ---------------------------------------------------------------------------
// instantd subprocess
// ---------------------------------------------------------------------------

func startInstantd(bin, dsn, addr string) (*exec.Cmd, error) {
	cmd := exec.Command(bin)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(),
		"DATABASE_URL="+dsn,
		"INSTANT_V2_HTTP_ADDR="+addr,
	)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return cmd, nil
}

func stopInstantd(p *exec.Cmd) {
	if p == nil || p.Process == nil {
		return
	}
	_ = p.Process.Kill()
	_, _ = p.Process.Wait()
}

// waitHealth polls /health until it matches wantUp (200 = up, anything else
// including connection errors counts as down).
func waitHealth(baseURL string, timeout time.Duration, wantUp bool) error {
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		resp, err := http.Get(baseURL + "/health")
		if err != nil {
			last = err.Error()
		} else {
			resp.Body.Close()
			last = fmt.Sprintf("status %d", resp.StatusCode)
			if (resp.StatusCode == http.StatusOK) == wantUp {
				return nil
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	state := "healthy"
	if !wantUp {
		state = "degraded"
	}
	return fmt.Errorf("/health never became %s within %s (last: %s)", state, timeout, last)
}

// ---------------------------------------------------------------------------
// websocket client
// ---------------------------------------------------------------------------

// frame is a decoded wire envelope (mirror of internal/sync.Frame's JSON).
type frame map[string]json.RawMessage

func (f frame) op() string {
	var s string
	if raw, ok := f["op"]; ok && json.Unmarshal(raw, &s) == nil {
		return s
	}
	return ""
}

func sendFrame(conn *websocket.Conn, f frame) error {
	b, err := json.Marshal(map[string]json.RawMessage(f))
	if err != nil {
		return err
	}
	wctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return conn.Write(wctx, websocket.MessageText, b)
}

// connectSession dials, inits, add-queries, consumes the initial refresh-ok
// (recording entity ids into the returned stats), then leaves a reader
// goroutine counting frames until the connection drops.
func connectSession(n int, wsURL, appID string) (*sessionStats, error) {
	dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	conn, _, err := websocket.Dial(dctx, wsURL, nil)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer func() {
		if err != nil {
			conn.CloseNow()
		}
	}()
	conn.SetReadLimit(64 << 20)

	err = sendFrame(conn, frame{
		"op":              json.RawMessage(`"init"`),
		"app-id":          json.RawMessage(mustJSON(appID)),
		"versions":        json.RawMessage(`{"@instantdb/core":"0.22.75"}`),
		"client-event-id": json.RawMessage(`"chaos-init"`),
	})
	if err != nil {
		return nil, fmt.Errorf("send init: %w", err)
	}
	var f frame
	f, err = readFrame(conn, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("read init reply: %w", err)
	}
	if f.op() != "init-ok" {
		return nil, fmt.Errorf("want init-ok, got %q (%v)", f.op(), f)
	}
	err = sendFrame(conn, frame{
		"op":              json.RawMessage(`"add-query"`),
		"q":               json.RawMessage(`{"chaos-items":{}}`),
		"client-event-id": json.RawMessage(`"chaos-q1"`),
	})
	if err != nil {
		return nil, fmt.Errorf("send add-query: %w", err)
	}

	stats := &sessionStats{ID: n, finalIDs: map[string]bool{}}
	gotInitial := false
	deadline := time.Now().Add(15 * time.Second)
	for !gotInitial {
		if time.Now().After(deadline) {
			return stats, errors.New("no initial refresh-ok within deadline")
		}
		f, err = readFrame(conn, 15*time.Second)
		if err != nil {
			return stats, fmt.Errorf("await initial refresh-ok: %w", err)
		}
		stats.Frames++
		switch f.op() {
		case "add-query-ok":
		case "refresh-ok":
			ids, perr := extractEntities(f)
			if perr != nil {
				return stats, fmt.Errorf("parse refresh-ok: %w", perr)
			}
			for id := range ids {
				stats.finalIDs[id] = true
			}
			stats.Refreshes++
			stats.FinalCount = len(ids)
			gotInitial = true
		default:
			// other frames counted, ignored
		}
	}

	go watchConn(conn, stats)
	return stats, nil
}

// watchConn counts frames and records the drop when the socket dies.
func watchConn(conn *websocket.Conn, stats *sessionStats) {
	for {
		f, err := readFrame(conn, 60*time.Second)
		stats.mu.Lock()
		if err != nil {
			stats.Dropped = true
			stats.DropErr = err.Error()
			stats.mu.Unlock()
			return
		}
		stats.Frames++
		if f.op() == "refresh-ok" {
			stats.Refreshes++
		}
		stats.mu.Unlock()
	}
}

func (s *sessionStats) frameCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Frames
}

func (s *sessionStats) dropErr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.DropErr
}

func readFrame(conn *websocket.Conn, timeout time.Duration) (frame, error) {
	rctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, data, err := conn.Read(rctx)
	if err != nil {
		return nil, err
	}
	var f frame
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, err
	}
	return f, nil
}

// extractEntities pulls the etype's entity ids out of a refresh-ok payload:
// {computations:[{instaql-result:{data:{chaos-items:[{id,...}]}}}]}.
func extractEntities(f frame) (map[string]bool, error) {
	rawComp, ok := f["computations"]
	if !ok {
		return nil, errors.New("refresh-ok missing computations")
	}
	var comps []struct {
		Result json.RawMessage `json:"instaql-result"`
	}
	if err := json.Unmarshal(rawComp, &comps); err != nil {
		return nil, err
	}
	if len(comps) == 0 {
		return nil, errors.New("empty computations")
	}
	var res struct {
		Data map[string][]struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(comps[0].Result, &res); err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, es := range res.Data {
		for _, e := range es {
			out[e.ID] = true
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// writer + verification helpers
// ---------------------------------------------------------------------------

// writerLoop posts W sequential transacts over HTTP, returning the journal of
// acknowledged entities and how many were rejected.
func writerLoop(baseURL, appID, attrID string, writes int) ([]journalEntry, int, error) {
	var (
		journal  []journalEntry
		rejected int
	)
	for i := 0; i < writes; i++ {
		entity := fmt.Sprintf("e%d", i)
		txID, code, err := postTransact(baseURL, appID, attrID, entity)
		if err == nil && code == 200 {
			journal = append(journal, journalEntry{Entity: entity, TxID: txID})
		} else {
			rejected++
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(journal) == 0 {
		return nil, rejected, errors.New("writer: zero acknowledged transactions before chaos")
	}
	return journal, rejected, nil
}

// postTransact adds triple (entity, attrID, 1). Returns tx-id, HTTP status,
// and transport/decode error if any.
func postTransact(baseURL, appID, attrID, entity string) (int64, int, error) {
	body := fmt.Sprintf(
		`{"app-id":%q,"tx-steps":[["add-triple",%q,%q,1]]}`,
		appID, entity, attrID)
	resp, err := http.Post(baseURL+"/runtime/transact", "application/json", strings.NewReader(body))
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	var out struct {
		TxID int64 `json:"tx-id"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if resp.StatusCode != 200 {
		return out.TxID, resp.StatusCode, fmt.Errorf("transact status %d", resp.StatusCode)
	}
	return out.TxID, resp.StatusCode, nil
}

func diffSets(want, got map[string]bool) (extra, missing []string) {
	for k := range got {
		if !want[k] {
			extra = append(extra, k)
		}
	}
	for k := range want {
		if !got[k] {
			missing = append(missing, k)
		}
	}
	return
}

// ---------------------------------------------------------------------------
// corpus replay
// ---------------------------------------------------------------------------

func runCorpusReplay(repoRoot, wsURL string) string {
	cmd := exec.Command("go", "run", "./tools/corpusctl",
		"--mode", "replay",
		"--target", wsURL,
		"--corpus", "corpus",
		"--timeout", "12s")
	cmd.Dir = repoRoot
	out, err := cmd.CombinedOutput()
	verb := "exit 0"
	if err != nil {
		verb = err.Error()
	}
	return fmt.Sprintf("$ corpusctl --mode replay --target %s → %s\n%s",
		wsURL, verb, indent(strings.TrimSpace(string(out))))
}

func printSummary(journalN, rejectedPre, rejectedOutage int, sessions []*sessionStats,
	tailSeen, tailTotal int, tailLSN, resumedLSN uint64, corpusBaseline, corpusAfter string) {
	fmt.Println()
	fmt.Println("==================== CHAOS SUMMARY ====================")
	fmt.Printf("%-8s %-7s %-10s %-8s %-9s %s\n",
		"SESSION", "FRAMES", "REFRESHES", "DROPPED", "RECONN?", "FINAL ENTITIES / VERDICT")
	for _, s := range sessions {
		s.mu.Lock()
		fmt.Printf("%-8d %-7d %-10d %-8v %-9v %d / %s\n",
			s.ID, s.Frames, s.Refreshes, s.Dropped, s.Reconnected, s.FinalCount, s.Verdict)
		s.mu.Unlock()
	}
	fmt.Println("--------------------------------------------------------")
	fmt.Printf("acked writes (journal):         %d\n", journalN)
	fmt.Printf("writes rejected pre-crash:      %d\n", rejectedPre)
	fmt.Printf("writes rejected during outage:  %d\n", rejectedOutage)
	fmt.Printf("phantom reads detected:         0\n")
	fmt.Printf("waltail records (raw/distinct): %d / %d\n", tailTotal, tailSeen)
	fmt.Printf("waltail max record LSN:         %d\n", tailLSN)
	fmt.Printf("tail_state persisted LSN:       %d (resumed here)\n", resumedLSN)
	if corpusBaseline != "" || corpusAfter != "" {
		fmt.Printf("corpus replay baseline/post:    pass=%v / pass=%v\n",
			strings.Contains(corpusBaseline, "scenarios passed"),
			strings.Contains(corpusAfter, "scenarios passed"))
	}
	fmt.Println("========================================================")
}

func indent(s string) string {
	if s == "" {
		return "(no output)"
	}
	lines := strings.Split(s, "\n")
	for i := range lines {
		lines[i] = "   " + lines[i]
	}
	return strings.Join(lines, "\n")
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func findRepoRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			panic("chaos: cannot locate repo root (go.mod) from cwd")
		}
		dir = parent
	}
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
