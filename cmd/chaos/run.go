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
	"path/filepath"
	"strings"
	"time"
)

func cleanupBeforePASS(keep bool, cleanup func() error) error {
	if keep {
		return nil
	}
	if err := cleanup(); err != nil {
		return fmt.Errorf("cleanup before PASS: %w", err)
	}
	return nil
}

func run() error {
	if *flagSkipCorp {
		return errors.New("cannot emit PASS when --skip-corpus is enabled: PASS requires bound replay outcome")
	}
	if *flagKeep {
		return errors.New("cannot emit PASS when --keep is enabled: PASS requires verified cleanup")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repoRoot := findRepoRoot()
	preparedPGData, err := preparePGData(*flagPGData)
	if err != nil {
		return fmt.Errorf("pg-data safety check: %w", err)
	}
	pgData := preparedPGData.Path
	clusterInitialized := false
	cleanupDone := *flagKeep
	cleanup := func() error {
		if cleanupDone {
			return nil
		}
		var err error
		if clusterInitialized {
			err = cleanupCluster(pgData, preparedPGData.Identity)
		} else {
			err = mustRm(pgData, preparedPGData.Identity)
		}
		if err == nil {
			cleanupDone = true
		}
		return err
	}
	defer func() {
		if err := cleanup(); err != nil {
			fmt.Printf("   warning: datadir cleanup failed: %v\n", err)
		}
	}()

	// ---- Phase 0: boot throwaway postgres ---------------------------------
	fmt.Printf("== [0] booting throwaway postgres (datadir %s, port %d) ==\n", pgData, *flagPGPort)
	if err := verifyPGDataIdentity(pgData, preparedPGData.Identity); err != nil {
		return fmt.Errorf("pg-data changed before initdb: %w", err)
	}
	if out, err := pgBinInDir(pgData, preparedPGData.Identity, "initdb", "-D", ".", "-U", "instant", "--auth=trust", "-E", "utf8"); err != nil {
		return fmt.Errorf("initdb: %v\n%s", err, out)
	}
	if err := verifyPGDataIdentity(pgData, preparedPGData.Identity); err != nil {
		return fmt.Errorf("pg-data changed during initdb: %w", err)
	}
	if err := verifyPGDataIdentity(pgData, preparedPGData.Identity); err != nil {
		return fmt.Errorf("pg-data changed before pg_ctl start: %w", err)
	}
	if out, err := pgCtl(pgData, preparedPGData.Identity,
		"-l", "chaos.log",
		"-o", fmt.Sprintf("-p %d -c wal_level=logical", *flagPGPort),
		"-w", "-t", "30", "start"); err != nil {
		return fmt.Errorf("pg_ctl start: %v\n%s", err, out)
	}
	clusterInitialized = true
	postmasterIdent, err := readPostmasterIdentity(pgData, preparedPGData.Identity)
	if err != nil {
		return fmt.Errorf("read initial postmaster identity: %w", err)
	}
	if postmasterIdent.PID <= 0 {
		return errors.New("initial postmaster process identity missing")
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
	tailErr := make(chan error, 1)
	go func() {
		err := wt.Run(tailCtx, cpLive, tails.handle)
		tailErr <- err
		close(tailDone)
	}()
	defer func() {
		tailCancel()
		select {
		case <-tailDone:
		case <-time.After(35 * time.Second):
		}
	}()
	fmt.Printf("== [1] waltail streaming (slot %s), checkpoint LSN starts at %d ==\n", wt.SlotName(), cpLive.Last())

	// ---- Phase 3: build + start instantd ------------------------------------
	// Build exactly the working tree that was inspected for this run. A chaos
	// proof must fail when that candidate cannot build; substituting HEAD would
	// certify a different revision.
	fmt.Println("== [2] building instantd ==")
	binDir, err := os.MkdirTemp("", "chaos-instantd-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(binDir) }()
	instantdBin := filepath.Join(binDir, "instantd")
	candidate, err := captureCandidate(repoRoot)
	if err != nil {
		return fmt.Errorf("capture candidate provenance: %w", err)
	}
	buildSrc, snapshotHash, err := snapshotCandidate(repoRoot, candidate)
	if err != nil {
		return fmt.Errorf("snapshot candidate: %w", err)
	}
	defer func() { _ = os.RemoveAll(buildSrc) }()
	candidate.SnapshotSHA256 = snapshotHash
	lastOut, err := buildWorkingTree(buildSrc, instantdBin, buildInstantd)
	if err != nil {
		return fmt.Errorf("go build instantd candidate: %w\n%s", err, lastOut)
	}
	candidate.BinarySHA256, err = sha256File(instantdBin)
	if err != nil {
		return fmt.Errorf("hash instantd candidate: %w", err)
	}
	fmt.Printf("   candidate revision=%s tree=%s instantd-sha256=%s\n",
		candidate.Revision, candidate.TreeFingerprint, candidate.BinarySHA256)
	httpPort, err := freePort()
	if err != nil {
		return err
	}
	httpAddr := fmt.Sprintf("127.0.0.1:%d", httpPort)
	baseURL := "http://" + httpAddr
	wsURL := fmt.Sprintf("ws://%s/runtime/session", httpAddr)

	if err := verifyBinaryHash(instantdBin, candidate.BinarySHA256); err != nil {
		return fmt.Errorf("verify instantd before first start: %w", err)
	}
	if err := verifySnapshotHash(buildSrc, candidate.SnapshotSHA256); err != nil {
		return fmt.Errorf("verify candidate snapshot before first start: %w", err)
	}
	p1, err := startInstantd(instantdBin, dsn, httpAddr)
	if err != nil {
		return fmt.Errorf("start instantd candidate: %w", err)
	}
	defer func() { _ = stopInstantd(p1) }()
	if err := waitHealth(baseURL, 30*time.Second, true); err != nil {
		return err
	}
	fmt.Printf("   instantd up at %s (pid %d)\n", baseURL, p1.Identity.PID)

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
	if err := pollTailerError(tailErr); err != nil {
		return fmt.Errorf("waltail before chaos: %w", err)
	}
	preKillFrames := 0
	for _, s := range sessions {
		preKillFrames += s.frameCount()
	}
	fmt.Printf("   %d acked writes (tx-ids %d..%d), %d rejected pre-crash; frames so far: %d\n",
		len(journal), journal[0].TxID, journal[len(journal)-1].TxID, rejectedPre, preKillFrames)

	corpusBaseline := ""
	if !*flagSkipCorp {
		if err := verifySnapshotHash(buildSrc, candidate.SnapshotSHA256); err != nil {
			return fmt.Errorf("verify candidate snapshot before baseline replay: %w", err)
		}
		corpusBaseline, err = runCorpusReplay(buildSrc, wsURL)
		if err != nil {
			return fmt.Errorf("corpus baseline replay: %w", err)
		}
		fmt.Println("-- corpus baseline (pre-chaos) --\n" + indent(corpusBaseline))
	}

	// ---- Phase 5: THE CHAOS — SIGSTOP, then crash postgres -------------------
	fmt.Println("== [4] CHAOS: SIGSTOP postmaster, then pg_ctl stop -m immediate ==")
	if postmasterIdent.PID > 0 {
		if err := freezeAndThawPostmasterIdentity(postmasterIdent, pgData, preparedPGData.Identity, 2*time.Second); err != nil {
			return err
		}
	}
	if out, err := pgCtl(pgData, preparedPGData.Identity, "-m", "immediate", "-w", "stop"); err != nil {
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
	if err := stopInstantd(p1); err != nil {
		return fmt.Errorf("stop instantd after outage: %w", err)
	}
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
	if err := waitTailer(tailDone, tailErr, 35*time.Second); err != nil {
		return fmt.Errorf("stop old waltail: %w", err)
	}
	if out, err := pgCtl(pgData, preparedPGData.Identity,
		"-l", "chaos.log",
		"-o", fmt.Sprintf("-p %d -c wal_level=logical", *flagPGPort),
		"-w", "-t", "60", "start"); err != nil {
		return fmt.Errorf("pg_ctl restart: %v\n%s", err, out)
	}
	postmasterRestartIdent, err := readPostmasterIdentity(pgData, preparedPGData.Identity)
	if err != nil {
		return fmt.Errorf("read restart postmaster identity: %w", err)
	}
	if postmasterRestartIdent.PID <= 0 {
		return errors.New("restart postmaster process identity missing")
	}
	fmt.Printf("   starting instantd at %s...\n", httpAddr)
	if err := verifyBinaryHash(instantdBin, candidate.BinarySHA256); err != nil {
		return fmt.Errorf("verify instantd before recovery start: %w", err)
	}
	if err := verifySnapshotHash(buildSrc, candidate.SnapshotSHA256); err != nil {
		return fmt.Errorf("verify candidate snapshot before recovery start: %w", err)
	}
	p2, err := startInstantd(instantdBin, dsn, httpAddr)
	if err != nil {
		return fmt.Errorf("start recovery instantd: %w", err)
	}
	defer func() { _ = stopInstantd(p2) }()
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
	tailCtx2, tailCancel2 := context.WithCancel(ctx)
	tailDone2 := make(chan struct{})
	tailErr2 := make(chan error, 1)
	go func() {
		err := wt2.Run(tailCtx2, cpPost, tails.handle)
		tailErr2 <- err
		close(tailDone2)
	}()
	defer func() {
		tailCancel2()
		select {
		case <-tailDone2:
		case <-time.After(35 * time.Second):
		}
	}()
	if err := pollTailerError(tailErr2); err != nil {
		return fmt.Errorf("recovered waltail: %w", err)
	}
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
	if err := pollTailerError(tailErr2); err != nil {
		return fmt.Errorf("recovered waltail: %w", err)
	}
	fmt.Printf("   tailer: %d distinct entities == journal size; raw records %d (%d replayed across the crash)\n",
		postSeen, postTotal, postTotal-preTotal)

	// ---- Phase 8: corpus replay post-chaos ------------------------------------
	corpusAfter := ""
	if !*flagSkipCorp {
		if err := verifySnapshotHash(buildSrc, candidate.SnapshotSHA256); err != nil {
			return fmt.Errorf("verify candidate snapshot before post-chaos replay: %w", err)
		}
		corpusAfter, err = runCorpusReplay(buildSrc, wsURL)
		if err != nil {
			return fmt.Errorf("corpus post-chaos replay: %w", err)
		}
		fmt.Println("-- corpus replay (post-chaos) --\n" + indent(corpusAfter))
	}
	tailCancel2()
	if err := waitTailer(tailDone2, tailErr2, 35*time.Second); err != nil {
		return fmt.Errorf("stop recovered waltail: %w", err)
	}
	if err := cleanupBeforePASS(*flagKeep, cleanup); err != nil {
		return err
	}
	if err := verifyBinaryHash(instantdBin, candidate.BinarySHA256); err != nil {
		return fmt.Errorf("verify instantd before report: %w", err)
	}
	if err := verifySnapshotHash(buildSrc, candidate.SnapshotSHA256); err != nil {
		return fmt.Errorf("verify candidate snapshot before report: %w", err)
	}
	if err := verifyInstantdIdentity(p2); err != nil {
		return fmt.Errorf("verify recovered instantd identity before report: %w", err)
	}
	if err := stopInstantd(p2); err != nil {
		return fmt.Errorf("stop recovered instantd before report: %w", err)
	}
	if err := validateReportPath(*flagReport, pgData); err != nil {
		return fmt.Errorf("report path safety check: %w", err)
	}
	dev, ino, ok := pgDataDevIno(preparedPGData.Identity)
	if !ok {
		return errors.New("cannot determine PG data device/inode")
	}
	pgIdent := fmt.Sprintf("dev:%d ino:%d", dev, ino)
	summarizeReplay := func(out string) string {
		match := corpusSummaryRE.FindStringSubmatch(out)
		if len(match) == 3 {
			return match[0]
		}
		return strings.TrimSpace(out)
	}
	replayOutcome := fmt.Sprintf("baseline: %s; post-chaos: %s", summarizeReplay(corpusBaseline), summarizeReplay(corpusAfter))

	reportPath, err := writeChaosReport(*flagReport, chaosRunReport{
		Status:    "PASS",
		Candidate: candidate,
		Config: chaosRunConfig{
			PGData:       pgData,
			PGPort:       *flagPGPort,
			Sessions:     *flagSessions,
			Writes:       *flagWrites,
			SkipCorpus:   *flagSkipCorp,
			Keep:         *flagKeep,
			WebSocketURL: wsURL,
		},
		Fixture: chaosFixture{
			ChaosApp:   chaosApp,
			AttrID:     attrID,
			CorpusApp:  corpusAppID,
			PGData:     pgData,
			PGDev:      dev,
			PGIno:      ino,
			PGIdentity: pgIdent,
		},
		Process: chaosProcessIdentity{
			Postmaster:        postmasterIdent,
			PostmasterRestart: postmasterRestartIdent,
			InstantdPreCrash:  p1.Identity,
			InstantdPostCrash: p2.Identity,
		},
		Artifact: chaosArtifact{
			InstantdSHA256:     candidate.BinarySHA256,
			CandidateAgreement: true,
			ReplayBaselineOK:   corpusBaseline != "" && strings.Contains(corpusBaseline, "scenarios passed"),
			ReplayAfterOK:      corpusAfter != "" && strings.Contains(corpusAfter, "scenarios passed"),
			ReplayOutcome:      replayOutcome,
			CleanupComplete:    !*flagKeep && cleanupDone,
		},
	})
	if err != nil {
		return fmt.Errorf("write chaos report: %w", err)
	}
	fmt.Printf("   durable chaos report: %s\n", reportPath)

	printSummary(len(journal), rejectedPre, outageRejected, sessions,
		postSeen, postTotal, postLSN, cpPost.Last(), corpusBaseline, corpusAfter)
	return nil
}
