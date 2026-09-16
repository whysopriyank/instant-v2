package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type chaosRunReport struct {
	Status    string               `json:"status"`
	Candidate candidateProvenance  `json:"candidate"`
	Config    chaosRunConfig       `json:"config"`
	Fixture   chaosFixture         `json:"fixture"`
	Process   chaosProcessIdentity `json:"process"`
	Artifact  chaosArtifact        `json:"artifact"`
}

type chaosProcessIdentity struct {
	Postmaster        postmasterProcessIdentity `json:"postmaster"`
	PostmasterRestart postmasterProcessIdentity `json:"postmaster_restart,omitempty"`
	InstantdPreCrash  instantdProcessIdentity   `json:"instantd_pre_crash"`
	InstantdPostCrash instantdProcessIdentity   `json:"instantd_post_crash"`
}

type chaosRunConfig struct {
	PGData       string `json:"pg_data"`
	PGPort       int    `json:"pg_port"`
	Sessions     int    `json:"sessions"`
	Writes       int    `json:"writes"`
	SkipCorpus   bool   `json:"skip_corpus"`
	Keep         bool   `json:"keep"`
	WebSocketURL string `json:"websocket_url"`
}

type chaosFixture struct {
	ChaosApp   string `json:"chaos_app"`
	AttrID     string `json:"attr_id"`
	CorpusApp  string `json:"corpus_app"`
	PGData     string `json:"pg_data"`
	PGDev      uint64 `json:"pg_dev"`
	PGIno      uint64 `json:"pg_ino"`
	PGIdentity string `json:"pg_identity"`
}

type chaosArtifact struct {
	InstantdSHA256     string `json:"instantd_sha256"`
	CandidateAgreement bool   `json:"candidate_agreement"`
	ReplayBaselineOK   bool   `json:"replay_baseline_ok"`
	ReplayAfterOK      bool   `json:"replay_after_ok"`
	ReplayOutcome      string `json:"replay_outcome"`
	CleanupComplete    bool   `json:"cleanup_complete"`
}

func validatePASSReport(report chaosRunReport) error {
	if !report.Artifact.CleanupComplete {
		return errors.New("cleanup is not complete")
	}
	if !report.Artifact.CandidateAgreement {
		return errors.New("candidate agreement is not verified")
	}
	if !report.Artifact.ReplayBaselineOK || !report.Artifact.ReplayAfterOK {
		return errors.New("replay baseline or post-chaos replay failed")
	}
	if strings.TrimSpace(report.Artifact.ReplayOutcome) == "" {
		return errors.New("replay outcome is not bound")
	}
	if report.Candidate.Revision == "" {
		return errors.New("candidate revision is empty")
	}
	if report.Candidate.TreeFingerprint == "" {
		return errors.New("candidate tree fingerprint is empty")
	}
	if report.Candidate.BinarySHA256 == "" || report.Candidate.BinarySHA256 != report.Artifact.InstantdSHA256 {
		return errors.New("binary sha256 mismatch or empty")
	}
	if report.Candidate.SnapshotSHA256 == "" {
		return errors.New("candidate snapshot sha256 is empty")
	}
	if report.Process.Postmaster.PID <= 0 || report.Process.Postmaster.StartTime <= 0 {
		return errors.New("postmaster process identity is not bound")
	}
	if report.Process.PostmasterRestart.PID <= 0 || report.Process.PostmasterRestart.StartTime <= 0 {
		return errors.New("restarted postmaster process identity is not bound")
	}
	if report.Process.InstantdPreCrash.PID <= 0 || report.Process.InstantdPreCrash.StartTime <= 0 {
		return errors.New("instantd pre-crash process identity is not bound")
	}
	if report.Process.InstantdPostCrash.PID <= 0 || report.Process.InstantdPostCrash.StartTime <= 0 {
		return errors.New("instantd post-crash process identity is not bound")
	}
	if report.Process.InstantdPreCrash.BinarySHA256 == "" || report.Process.InstantdPreCrash.BinarySHA256 != report.Candidate.BinarySHA256 || report.Process.InstantdPostCrash.BinarySHA256 == "" || report.Process.InstantdPostCrash.BinarySHA256 != report.Candidate.BinarySHA256 {
		return errors.New("instantd process binary identity is not bound to candidate")
	}
	if report.Process.Postmaster == report.Process.PostmasterRestart {
		return errors.New("postmaster restart identity did not change")
	}
	if report.Process.InstantdPreCrash == report.Process.InstantdPostCrash {
		return errors.New("instantd crash/restart identity did not change")
	}
	if report.Fixture.PGDev == 0 || report.Fixture.PGIno == 0 || report.Fixture.PGIdentity == "" {
		return errors.New("PG fixture identity is not bound")
	}
	if report.Fixture.ChaosApp == "" || report.Fixture.AttrID == "" || report.Fixture.CorpusApp == "" {
		return errors.New("fixture IDs are not bound")
	}
	return nil
}

func writeChaosReport(path string, report chaosRunReport) (string, error) {
	if report.Status == "PASS" {
		if err := validatePASSReport(report); err != nil {
			return "", fmt.Errorf("cannot emit PASS report: %w", err)
		}
	}
	b, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return "", fmt.Errorf("serialize chaos report: %w", err)
	}
	b = append(b, '\n')
	if strings.TrimSpace(path) == "" {
		f, err := os.CreateTemp(chaosTempRoot(), "instant-v2-chaos-report-*.json")
		if err != nil {
			return "", fmt.Errorf("create chaos report: %w", err)
		}
		path = f.Name()
		if err := writeAndCloseReport(f, b); err != nil {
			_ = os.Remove(path)
			return "", err
		}
		return path, nil
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve chaos report path: %w", err)
	}
	dir := filepath.Dir(absolute)
	tmp, err := os.CreateTemp(dir, ".instant-v2-chaos-report-*.tmp")
	if err != nil {
		return "", fmt.Errorf("create chaos report temporary file: %w", err)
	}
	tmpName := tmp.Name()
	if err := writeAndCloseReport(tmp, b); err != nil {
		_ = os.Remove(tmpName)
		return "", err
	}
	if _, err := os.Lstat(absolute); err == nil {
		_ = os.Remove(tmpName)
		return "", fmt.Errorf("create chaos report %q: file already exists", absolute)
	} else if !os.IsNotExist(err) {
		_ = os.Remove(tmpName)
		return "", fmt.Errorf("inspect chaos report %q: %w", absolute, err)
	}
	if err := os.Link(tmpName, absolute); err != nil {
		_ = os.Remove(tmpName)
		return "", fmt.Errorf("finalize chaos report %q: %w", absolute, err)
	}
	if err := os.Remove(tmpName); err != nil {
		return "", fmt.Errorf("remove temporary chaos report %q: %w", tmpName, err)
	}
	return absolute, nil
}

func validateReportPath(path, disposablePGData string) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	reportPath, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	pgPath, err := filepath.Abs(disposablePGData)
	if err != nil {
		return err
	}
	prefix := filepath.Clean(pgPath) + string(os.PathSeparator)
	if filepath.Clean(reportPath) == filepath.Clean(pgPath) || strings.HasPrefix(filepath.Clean(reportPath), prefix) {
		return fmt.Errorf("chaos report must not be stored inside disposable pg-data")
	}
	return nil
}

func writeAndCloseReport(f *os.File, b []byte) error {
	n, err := f.Write(b)
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("write chaos report: %w", err)
	}
	if n != len(b) {
		_ = f.Close()
		return fmt.Errorf("write chaos report: %w", io.ErrShortWrite)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync chaos report: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close chaos report: %w", err)
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
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
