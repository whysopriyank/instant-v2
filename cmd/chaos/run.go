package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/waltail"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

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
	// Preferred: build the live working tree (tests what will actually ship).
	// Fallback: if that fails — e.g. a sibling workstream holds half-finished
	// uncommitted edits — retry from a pristine `git archive HEAD` export so
	// the chaos proof stays runnable.
	fmt.Println("== [2] building instantd ==")
	binDir, err := os.MkdirTemp("", "chaos-instantd-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(binDir) }()
	instantdBin := filepath.Join(binDir, "instantd")

	buildAt := func(dir string) ([]byte, error) {
		b := exec.Command("go", "build", "-o", instantdBin, "./cmd/instantd")
		b.Dir = dir
		return b.CombinedOutput()
	}
	// The working tree is shared with concurrently-running workstreams; a
	// build may transiently fail while a sibling saves mid-edit. Retry a
	// few times before falling back to the pristine HEAD export.
	buildSrc := repoRoot
	var lastOut []byte
	var lastErr error
	for attempt := 1; attempt <= 8; attempt++ {
		lastOut, lastErr = buildAt(repoRoot)
		if lastErr == nil {
			break
		}
		fmt.Printf("   working-tree build attempt %d failed; retrying in 15s (%v)\n", attempt, lastErr)
		time.Sleep(15 * time.Second)
	}
	if lastErr != nil {
		fmt.Println("   falling back to HEAD snapshot")
		srcDir, xerr := exportHead(repoRoot)
		if xerr != nil {
			return fmt.Errorf("export HEAD: %w", xerr)
		}
		defer func() { _ = os.RemoveAll(srcDir) }()
		out2, err2 := buildAt(srcDir)
		if err2 != nil {
			return fmt.Errorf("go build instantd (working tree):\n%s\ngo build instantd (HEAD):\n%s", lastOut, out2)
		}
		buildSrc = srcDir // corpusctl + corpus replay against the same snapshot
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
		corpusBaseline = runCorpusReplay(buildSrc, wsURL)
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
		if _, code, _, err := postTransact(baseURL, chaosApp, attrID, fmt.Sprintf("outage-%d", i)); err != nil || code != 0 && code != 200 {
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
	fmt.Println("   stopping old tailer...")
	tailCancel()
	select {
	case <-tailDone:
	case <-time.After(35 * time.Second):
		fmt.Println("   note: old tailer loop slow to unwind (backoff)")
	}
	if out, err := pgCtl(pgData,
		"-l", filepath.Join(pgData, "chaos.log"),
		"-o", fmt.Sprintf("-p %d -c wal_level=logical", *flagPGPort),
		"-w", "-t", "60", "start"); err != nil {
		return fmt.Errorf("pg_ctl restart: %v\n%s", err, out)
	}
	fmt.Printf("   starting instantd at %s...\n", httpAddr)
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
		for _, s := range sessions {
			s.mu.Lock()
			fmt.Printf("   session %d verdict: %s\n      raw: %s\n", s.ID, s.Verdict, truncate(s.rawResult, 600))
			s.mu.Unlock()
		}
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
		corpusAfter = runCorpusReplay(buildSrc, wsURL)
		fmt.Println("-- corpus replay (post-chaos) --\n" + indent(corpusAfter))
	}

	printSummary(len(journal), rejectedPre, outageRejected, sessions,
		postSeen, postTotal, postLSN, cpPost.Last(), corpusBaseline, corpusAfter)
	return nil
}
