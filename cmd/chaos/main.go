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
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

var (
	flagPGPort   = flag.Int("pg-port", 54330, "port for the throwaway postgres cluster")
	flagPGData   = flag.String("pg-data", filepath.Join(os.TempDir(), "chaospg-instantv2"), "marker-owned direct child of the system temp directory for the throwaway cluster")
	flagSessions = flag.Int("sessions", 8, "number of concurrent WS sessions")
	flagWrites   = flag.Int("writes", 60, "acknowledged writes before the kill")
	flagKeep     = flag.Bool("keep", false, "keep the throwaway cluster running after the run")
	flagSkipCorp = flag.Bool("skip-corpus", false, "skip the corpus replays")
	flagReport   = flag.String("report", "", "write a durable JSON provenance report (defaults to a unique system-temp file)")
)

func main() {
	if handled, err := maybeRunPGHelper(); handled {
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	flag.Parse()
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "chaos: FAIL: %v\n", err)
		os.Exit(1)
	}
}
