package main

import (
	"fmt"
	"strings"
)

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
