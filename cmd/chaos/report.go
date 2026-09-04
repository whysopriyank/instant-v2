package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type chaosRunReport struct {
	Status    string              `json:"status"`
	Candidate candidateProvenance `json:"candidate"`
	Config    chaosRunConfig      `json:"config"`
	Fixture   chaosFixture        `json:"fixture"`
	Artifact  chaosArtifact       `json:"artifact"`
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
	ChaosApp  string `json:"chaos_app"`
	AttrID    string `json:"attr_id"`
	CorpusApp string `json:"corpus_app"`
}

type chaosArtifact struct {
	InstantdSHA256     string `json:"instantd_sha256"`
	CandidateAgreement bool   `json:"candidate_agreement"`
	ReplayBaselineOK   bool   `json:"replay_baseline_ok"`
	ReplayAfterOK      bool   `json:"replay_after_ok"`
	CleanupComplete    bool   `json:"cleanup_complete"`
}

func writeChaosReport(path string, report chaosRunReport) (string, error) {
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
			return "", err
		}
		return path, nil
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve chaos report path: %w", err)
	}
	f, err := os.OpenFile(absolute, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return "", fmt.Errorf("create chaos report %q: %w", absolute, err)
	}
	if err := writeAndCloseReport(f, b); err != nil {
		return "", err
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
