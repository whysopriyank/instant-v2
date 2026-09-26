package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Declared QR-001 alpha soak budgets. Ramp/settle/tx-rate are explicit
// campaign parameters chosen here (not tuned per-run); the coordinator
// records them in the campaign guide before the run.
const (
	soakSessions     = 500
	soakActive       = 900 * time.Second
	soakRamp         = 60 * time.Second
	soakSettle       = 60 * time.Second
	soakGlobalTxRate = 8.0
	soakMaxP99Lag    = "10s"
	// soakSDKVersion negotiates delta-refresh (>= 0.23.0) so the whole-table
	// subscription is served as structural patches, the current v2 wire path;
	// full envelopes of an unboundedly growing result would make per-tx
	// fan-out bytes grow without bound for the whole run.
	soakSDKVersion = "0.23.0"
)

// The structs below mirror cmd/soak's real output with EXACT json tags:
// CompletenessManifest / SummaryManifest / WorkloadManifest / ArtifactEntry
// from cmd/soak/evidence.go and LedgerEntry + EntryState constants from
// cmd/soak/ledger.go. TestSoakOutputTagsPinned reads those sources and fails
// if any tag, state name, or emitted event name changes, so tag drift can
// never silently pass. Decoding is strict (DisallowUnknownFields): unknown
// field/state/event names FAIL the mapping instead of being guessed.

// soakManifest mirrors cmd/soak CompletenessManifest.
type soakManifest struct {
	SchemaVersion    int            `json:"schema_version"`
	Status           string         `json:"status"`
	Completed        bool           `json:"completed"`
	RunID            string         `json:"run_id"`
	StartedAt        time.Time      `json:"started_at"`
	FinishedAt       time.Time      `json:"finished_at"`
	DurationSeconds  float64        `json:"duration_seconds"`
	TargetURL        string         `json:"target_url"`
	AppID            string         `json:"app_id"`
	Workload         soakWorkload   `json:"workload"`
	Summary          soakSummary    `json:"summary"`
	Artifacts        []soakArtifact `json:"artifacts"`
	PprofEndpoint    string         `json:"pprof_endpoint,omitempty"`
	CompletionMarker string         `json:"completion_marker,omitempty"`
}

// soakWorkload mirrors cmd/soak WorkloadManifest.
type soakWorkload struct {
	Sessions     int     `json:"sessions"`
	Duration     string  `json:"duration"`
	GlobalTxRate float64 `json:"global_tx_rate"`
	TxInterval   string  `json:"tx_interval"`
	RampUp       string  `json:"ramp_up"`
	Settle       string  `json:"settle"`
	Quiescence   string  `json:"quiescence"`
	MaxP99Lag    string  `json:"max_p99_lag,omitempty"`
	SDKVersion   string  `json:"sdk_version,omitempty"`
}

// soakSummary mirrors cmd/soak SummaryManifest.
type soakSummary struct {
	Connects     int64  `json:"connects"`
	Transacts    int64  `json:"transacts"`
	Refreshes    int64  `json:"refreshes"`
	Dropped      int    `json:"dropped"`
	Unresolved   int    `json:"unresolved"`
	LagSamples   int    `json:"lag_samples"`
	LagP50       string `json:"lag_p50,omitempty"`
	LagP99       string `json:"lag_p99,omitempty"`
	LagMax       string `json:"lag_max,omitempty"`
	Success      bool   `json:"success"`
	FailureError string `json:"failure_error,omitempty"`
}

// soakArtifact mirrors cmd/soak ArtifactEntry.
type soakArtifact struct {
	Name      string `json:"name"`
	Path      string `json:"path"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
	Required  bool   `json:"required"`
}

// soakLedgerEntry mirrors cmd/soak LedgerEntry (cmd/soak/ledger.go).
type soakLedgerEntry struct {
	ClientEventID    string        `json:"client_event_id"`
	ServerTxID       string        `json:"server_tx_id,omitempty"`
	SessionID        int           `json:"session_id"`
	SubmittedAt      time.Time     `json:"submitted_at"`
	AckAt            time.Time     `json:"ack_at,omitempty"`
	RefreshAt        time.Time     `json:"refresh_at,omitempty"`
	Lag              time.Duration `json:"lag,omitempty"`
	RefreshBeforeAck bool          `json:"refresh_before_ack,omitempty"`
	State            string        `json:"state"`
	TerminalReason   string        `json:"terminal_reason,omitempty"`
}

// Ledger states with an acknowledged server tx, listed explicitly from
// cmd/soak/ledger.go: RecordAck moves submitted → acknowledged, and any
// refresh correlation afterwards moves → resolved (directly, through the
// buffered-refresh path, or through the session watermark path). "refreshed"
// is a defined EntryState that this ledger version never assigns; it is
// accepted as ack-implying for forward compatibility but documented as such.
var soakAckImplyingStates = map[string]bool{
	"acknowledged": true,
	"resolved":     true,
	"refreshed":    true,
}

// soakKnownLedgerStates is the exact EntryState set from cmd/soak/ledger.go.
var soakKnownLedgerStates = map[string]bool{
	"submitted": true, "acknowledged": true, "refreshed": true,
	"resolved": true, "terminal": true,
}

// soakAllowedEventFields is the exact payload field set per event name, taken
// from the Emit call sites in cmd/soak/main.go (plus the EventWriter
// envelope "event"/"at" from internal/benchharness/observe.go). Any other
// field FAILs the mapping instead of being guessed.
var soakAllowedEventFields = map[string]map[string]bool{
	"run_started": {
		"url": true, "sessions": true, "duration": true, "global_tx_rate": true,
		"ramp": true, "settle": true, "workload": true, "quiescence": true,
	},
	"scheduler_error":    {"error": true},
	"scheduler_finished": {"scheduled": true, "slip_samples": true, "error": true},
	"progress": {
		"sessions": true, "refreshes": true, "transacts": true,
		"dropped": true, "unresolved": true,
	},
	"run_finished": {
		"status": true, "dropped": true, "refreshes": true, "transacts": true,
		"unresolved": true, "reason": true, "sessions": true, "lag_samples": true,
		"p99": true, "budget": true,
	},
	"lag_diagnostic": {
		"metric_kind": true, "semantic_claim": true, "samples": true,
		"p50": true, "p99": true, "max": true,
	},
}

// soakRunEvent is one run-level evidence event from the soak events file.
type soakRunEvent struct {
	Name      string
	Status    string
	Sessions  *int64
	Refreshes *int64
	Transacts *int64
}

// soakEvidence is the seven gate details for QR-001.
type soakEvidence struct {
	Sessions                 int
	ActiveSeconds            int64
	CommittedTransactions    int64
	AcknowledgedTransactions int64
	RefreshedTransactions    int64
	DroppedTransactions      int64
	UnresolvedTransactions   int64
}

// decodeStrict decodes JSON with unknown fields rejected.
func decodeStrict(data []byte, v any) error {
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if dec.More() {
		return fmt.Errorf("trailing JSON content")
	}
	return nil
}

// mapSoakManifest derives the seven gate details from the soak's own
// completeness manifest plus its events/ledger artifact, binding strictly to
// cmd/soak's real field and state names:
//
//   - status must be "complete", completed true, summary.success true;
//   - sessions = workload.sessions; active_seconds =
//     trunc(duration_seconds - workload.ramp_up - workload.settle): cmd/soak's
//     duration spans ramp and settle, and writes only run after both;
//   - with per-tx ledger entries present: committed = distinct non-empty
//     server_tx_id (an id exists only after RecordAck); acknowledged =
//     entries in state acknowledged/resolved (ack-implying, listed above);
//     refreshed = entries whose own refresh was observed (refresh_at set);
//     dropped = summary.dropped cross-checked against terminal-state entries
//     (mismatch → FAIL); unresolved = summary.unresolved cross-checked
//     against non-resolved entries (mismatch → FAIL);
//   - without ledger entries: summary-certified mapping. success=true is set
//     by cmd/soak only after ledger.QuiesceAll (every submitted tx resolved,
//     hence acked+refreshed) with zero session drops, so committed =
//     acknowledged = refreshed = summary.transacts is the soak's certified
//     invariant, not a guess; the certification is recorded in derivedFrom;
//   - the events file's final run_finished counters must equal the summary
//     counters (mismatch → FAIL); unknown event/field/state names → FAIL.
func mapSoakManifest(m soakManifest, ledger []soakLedgerEntry, events []soakRunEvent) (soakEvidence, []string, error) {
	var ev soakEvidence
	var derived []string
	if m.Status != "complete" {
		return ev, nil, fmt.Errorf("soak manifest status %q (want complete)", m.Status)
	}
	if !m.Completed {
		return ev, nil, fmt.Errorf("soak manifest completed=false")
	}
	if !m.Summary.Success {
		return ev, nil, fmt.Errorf("soak manifest success=false: %s", m.Summary.FailureError)
	}
	if m.Workload.Sessions <= 0 {
		return ev, nil, fmt.Errorf("soak manifest workload.sessions=%d (want > 0)", m.Workload.Sessions)
	}
	ev.Sessions = m.Workload.Sessions
	if m.DurationSeconds <= 0 {
		return ev, nil, fmt.Errorf("soak manifest duration_seconds=%v (want > 0)", m.DurationSeconds)
	}
	ramp, rerr := time.ParseDuration(m.Workload.RampUp)
	settle, serr := time.ParseDuration(m.Workload.Settle)
	if rerr != nil || serr != nil || ramp < 0 || settle < 0 {
		return ev, nil, fmt.Errorf("soak manifest ramp_up=%q settle=%q are not valid durations", m.Workload.RampUp, m.Workload.Settle)
	}
	active := m.DurationSeconds - ramp.Seconds() - settle.Seconds()
	if active <= 0 {
		return ev, nil, fmt.Errorf("soak manifest has no active window (duration_seconds=%v, ramp_up=%s, settle=%s)", m.DurationSeconds, ramp, settle)
	}
	ev.ActiveSeconds = int64(active) // truncation toward zero, documented
	if len(ledger) > 0 {
		committed := map[string]bool{}
		var acked, refreshed int64
		var terminal, nonResolved int64
		for _, e := range ledger {
			if !soakKnownLedgerStates[e.State] {
				return ev, nil, fmt.Errorf("unknown ledger state %q (no fallback guessing)", e.State)
			}
			if e.ServerTxID != "" {
				committed[e.ServerTxID] = true
			}
			if soakAckImplyingStates[e.State] {
				acked++
			}
			if !e.RefreshAt.IsZero() {
				refreshed++
			}
			if e.State == "terminal" {
				terminal++
			}
			if e.State != "resolved" {
				nonResolved++
			}
		}
		if int64(m.Summary.Dropped) != terminal {
			return ev, nil, fmt.Errorf("dropped mismatch: summary=%d ledger-terminal=%d", m.Summary.Dropped, terminal)
		}
		if int64(m.Summary.Unresolved) != nonResolved {
			return ev, nil, fmt.Errorf("unresolved mismatch: summary=%d ledger-non-resolved=%d", m.Summary.Unresolved, nonResolved)
		}
		ev.CommittedTransactions = int64(len(committed))
		ev.AcknowledgedTransactions = acked
		ev.RefreshedTransactions = refreshed
		ev.DroppedTransactions = int64(m.Summary.Dropped)
		ev.UnresolvedTransactions = int64(m.Summary.Unresolved)
		derived = append(derived, "ledger(states=submitted/acknowledged/refreshed/resolved/terminal)")
	} else {
		ev.CommittedTransactions = m.Summary.Transacts
		ev.AcknowledgedTransactions = m.Summary.Transacts
		ev.RefreshedTransactions = m.Summary.Transacts
		ev.DroppedTransactions = int64(m.Summary.Dropped)
		ev.UnresolvedTransactions = int64(m.Summary.Unresolved)
		derived = append(derived, "summary-certified(success=true ⇒ quiescence: all transacts acked+resolved, zero drops)")
	}
	if err := checkRunFinishedEvent(m.Summary, events); err != nil {
		return ev, nil, err
	}
	return ev, derived, nil
}

// checkRunFinishedEvent requires the events artifact's final run_finished
// counters to equal the manifest summary counters.
func checkRunFinishedEvent(summary soakSummary, events []soakRunEvent) error {
	var last *soakRunEvent
	for i := range events {
		if events[i].Name == "run_finished" {
			last = &events[i]
		}
	}
	if last == nil {
		return fmt.Errorf("events artifact has no run_finished event")
	}
	if last.Status != "" && last.Status != "passed" {
		return fmt.Errorf("final run_finished status %q (want passed)", last.Status)
	}
	if last.Transacts != nil && *last.Transacts != summary.Transacts {
		return fmt.Errorf("transacts mismatch: summary=%d run_finished=%d", summary.Transacts, *last.Transacts)
	}
	if last.Refreshes != nil && *last.Refreshes != summary.Refreshes {
		return fmt.Errorf("refreshes mismatch: summary=%d run_finished=%d", summary.Refreshes, *last.Refreshes)
	}
	if last.Sessions != nil && *last.Sessions != summary.Connects {
		return fmt.Errorf("connects mismatch: summary=%d run_finished=%d", summary.Connects, *last.Sessions)
	}
	return nil
}

// parseSoakArtifactFile reads the manifest's events/ledger artifact.
// Every non-blank line must be either a known run-level event (by "event",
// with exactly the payload fields cmd/soak/main.go emits for that name) or
// a per-tx ledger entry (by "client_event_id" with exact LedgerEntry tags);
// anything else FAILs.
func parseSoakArtifactFile(path string) ([]soakRunEvent, []soakLedgerEntry, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	var events []soakRunEvent
	var ledger []soakLedgerEntry
	for n, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var shape map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &shape); err != nil {
			return nil, nil, fmt.Errorf("line %d: not JSON", n+1)
		}
		if _, isLedger := shape["client_event_id"]; isLedger {
			var e soakLedgerEntry
			if err := decodeStrict([]byte(line), &e); err != nil {
				return nil, nil, fmt.Errorf("line %d: ledger entry: %w", n+1, err)
			}
			if e.ClientEventID == "" {
				return nil, nil, fmt.Errorf("line %d: ledger entry without client_event_id", n+1)
			}
			if !soakKnownLedgerStates[e.State] {
				return nil, nil, fmt.Errorf("line %d: unknown ledger state %q", n+1, e.State)
			}
			ledger = append(ledger, e)
			continue
		}
		if _, isEvent := shape["event"]; isEvent {
			e, err := parseSoakRunEvent(line)
			if err != nil {
				return nil, nil, fmt.Errorf("line %d: %w", n+1, err)
			}
			events = append(events, e)
			continue
		}
		return nil, nil, fmt.Errorf("line %d: unknown shape (want client_event_id or event)", n+1)
	}
	return events, ledger, nil
}

// parseSoakRunEvent validates one run-level event line against the exact
// Emit call sites: known name, envelope plus exactly the payload fields
// that name emits.
func parseSoakRunEvent(line string) (soakRunEvent, error) {
	var e soakRunEvent
	var shape map[string]json.RawMessage
	if err := json.Unmarshal([]byte(line), &shape); err != nil {
		return e, fmt.Errorf("event: not JSON")
	}
	var name string
	if err := json.Unmarshal(shape["event"], &name); err != nil || name == "" {
		return e, fmt.Errorf("event without a string event name")
	}
	allowed, ok := soakAllowedEventFields[name]
	if !ok {
		return e, fmt.Errorf("unknown event %q", name)
	}
	e.Name = name
	num := func(key string) (*int64, error) {
		raw, present := shape[key]
		if !present {
			return nil, nil
		}
		var f float64
		if err := json.Unmarshal(raw, &f); err != nil {
			return nil, fmt.Errorf("event %q field %q is not a number", name, key)
		}
		n := int64(f)
		return &n, nil
	}
	for k, raw := range shape {
		if k == "event" || k == "at" {
			continue
		}
		if !allowed[k] {
			return e, fmt.Errorf("event %q has unknown field %q", name, k)
		}
		switch k {
		case "status":
			if err := json.Unmarshal(raw, &e.Status); err != nil {
				return e, fmt.Errorf("event %q field status is not a string", name)
			}
		case "sessions":
			v, err := num(k)
			if err != nil {
				return e, err
			}
			e.Sessions = v
		case "refreshes":
			v, err := num(k)
			if err != nil {
				return e, err
			}
			e.Refreshes = v
		case "transacts":
			v, err := num(k)
			if err != nil {
				return e, err
			}
			e.Transacts = v
		}
	}
	return e, nil
}

// soakManifestPath mirrors cmd/soak DeriveManifestPath: trim the events
// file extension, append .manifest.json.
func soakManifestPath(eventsPath string) string {
	if ext := filepath.Ext(eventsPath); ext != "" {
		return strings.TrimSuffix(eventsPath, ext) + ".manifest.json"
	}
	return eventsPath + ".manifest.json"
}

func runSoak(args []string) int {
	fs := flag.NewFlagSet("soak", flag.ContinueOnError)
	instantd := fs.String("instantd", "", "candidate instantd binary PATH (required for live run)")
	databaseURL := fs.String("database-url", os.Getenv("DATABASE_URL"), "owned PostgreSQL DSN (never printed)")
	soaksetup := fs.String("soaksetup", "", "cmd/soaksetup binary for seeding (required for live run)")
	soakBin := fs.String("soak-bin", "", "cmd/soak binary (required for live run)")
	addr := fs.String("addr", "127.0.0.1:18081", "loopback addr for the candidate under test")
	out := fs.String("out", "", "lane result JSON output path (required)")
	evidence := fs.String("soak-evidence", "", "soak completeness manifest to map (alternative to live run)")
	ledger := fs.String("soak-ledger", "", "events/ledger artifact override for --soak-evidence (default: manifest artifacts[0])")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *out == "" {
		fmt.Fprintln(os.Stderr, "soak: --out is required")
		return 2
	}
	started := time.Now().UTC()
	if *evidence != "" {
		return mapSoakEvidenceFile(*evidence, *ledger, *out, started)
	}
	if *instantd == "" || *soaksetup == "" || *soakBin == "" {
		return failLane(*out, "QR-001", started, map[string]any{
			"error": "live run requires --instantd, --soaksetup, --soak-bin (or --soak-evidence for mapping)",
		}, nil)
	}
	return runSoakLive(runSoakConfig{
		instantd: *instantd, databaseURL: *databaseURL, soaksetup: *soaksetup,
		soakBin: *soakBin, addr: *addr, out: *out, started: started,
	})
}

// mapSoakEvidenceFile maps a real cmd/soak completeness manifest (plus the
// events/ledger artifact its artifacts list names, or --soak-ledger) into
// the QR-001 lane verdict. Hermetic; used by tests and by the orchestrator
// after the soak binary completes inside the qualification image.
func mapSoakEvidenceFile(manifestPath, ledgerPath, out string, started time.Time) int {
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "soak:", err)
		return 2
	}
	var m soakManifest
	if err := decodeStrict(raw, &m); err != nil {
		return failLane(out, "QR-001", started, map[string]any{"error": fmt.Sprintf("manifest is not a strict completeness manifest: %v", err)}, nil)
	}
	artifactPath := ledgerPath
	if artifactPath == "" {
		if len(m.Artifacts) == 0 {
			return failLane(out, "QR-001", started, map[string]any{"error": "manifest lists no artifacts and no --soak-ledger given"}, nil)
		}
		artifactPath = filepath.Join(filepath.Dir(manifestPath), filepath.FromSlash(m.Artifacts[0].Path))
	}
	events, ledger, err := parseSoakArtifactFile(artifactPath)
	if err != nil {
		return failLane(out, "QR-001", started, map[string]any{"error": err.Error()}, nil)
	}
	ev, derived, err := mapSoakManifest(m, ledger, events)
	if err != nil {
		return failLane(out, "QR-001", started, map[string]any{"error": err.Error()}, nil)
	}
	return writeSoakResult(out, started, ev, derived)
}

// writeSoakResult applies QR-001 budgets to mapped evidence.
func writeSoakResult(out string, started time.Time, ev soakEvidence, derived []string) int {
	result := "PASS"
	reasons := []string{}
	if ev.Sessions < soakSessions {
		result = "FAIL"
		reasons = append(reasons, fmt.Sprintf("sessions %d < %d", ev.Sessions, soakSessions))
	}
	if ev.ActiveSeconds < int64((soakActive / time.Second)) {
		result = "FAIL"
		reasons = append(reasons, fmt.Sprintf("active_seconds %d < %d", ev.ActiveSeconds, int64((soakActive/time.Second))))
	}
	if ev.CommittedTransactions <= 0 {
		result = "FAIL"
		reasons = append(reasons, "committed_transactions must be > 0")
	}
	if ev.AcknowledgedTransactions != ev.CommittedTransactions {
		result = "FAIL"
		reasons = append(reasons, "acknowledged_transactions != committed_transactions")
	}
	if ev.RefreshedTransactions != ev.CommittedTransactions {
		result = "FAIL"
		reasons = append(reasons, "refreshed_transactions != committed_transactions")
	}
	if ev.DroppedTransactions != 0 {
		result = "FAIL"
		reasons = append(reasons, "dropped_transactions != 0")
	}
	if ev.UnresolvedTransactions != 0 {
		result = "FAIL"
		reasons = append(reasons, "unresolved_transactions != 0")
	}
	details := map[string]any{
		"sessions":                  ev.Sessions,
		"active_seconds":            ev.ActiveSeconds,
		"committed_transactions":    ev.CommittedTransactions,
		"acknowledged_transactions": ev.AcknowledgedTransactions,
		"refreshed_transactions":    ev.RefreshedTransactions,
		"dropped_transactions":      ev.DroppedTransactions,
		"unresolved_transactions":   ev.UnresolvedTransactions,
	}
	if len(derived) > 0 {
		details["derived_from_ledger"] = derived
	}
	if result == "FAIL" {
		details["reasons"] = reasons
	}
	lr := laneResult{
		Packet: "QR-001", SelectedCount: int(ev.CommittedTransactions), SkippedCount: 0,
		Result: result, Details: details, Artifacts: []ArtifactRef{},
		StartedAt: started.Format(time.RFC3339), FinishedAt: time.Now().UTC().Format(time.RFC3339),
	}
	if err := writeJSONFile(out, lr); err != nil {
		fmt.Fprintln(os.Stderr, "soak:", err)
		return 1
	}
	if result == "FAIL" {
		fmt.Fprintln(os.Stderr, "soak: FAIL:", strings.Join(reasons, "; "))
		return 1
	}
	fmt.Fprintf(os.Stderr, "soak: PASS sessions=%d committed=%d\n", ev.Sessions, ev.CommittedTransactions)
	return 0
}

type runSoakConfig struct {
	instantd    string
	databaseURL string
	soaksetup   string
	soakBin     string
	addr        string
	out         string
	started     time.Time
}

// runSoakLive seeds via the existing cmd/soaksetup flow, runs the existing
// cmd/soak binary against the candidate instantd with the declared alpha
// budgets, samples process identity pre/post (like scripts/quality-soak.sh),
// and samples RSS/fds/PG connections every 10s to a time-series artifact.
func runSoakLive(cfg runSoakConfig) int {
	if err := checkLoopbackAddr(cfg.addr); err != nil {
		return failLane(cfg.out, "QR-001", cfg.started, map[string]any{"error": err.Error()}, nil)
	}
	workdir := filepath.Dir(cfg.out)
	tsPath := filepath.Join(workdir, "soak-timeseries.jsonl")
	eventsPath := filepath.Join(workdir, "soak-events.jsonl")
	soakLog := filepath.Join(workdir, "soak.log")

	// Seed via soaksetup.
	seedCtx, cancel := contextTimeout(2 * time.Minute)
	defer cancel()
	seedCmd := exec.CommandContext(seedCtx, cfg.soaksetup, "-database-url", cfg.databaseURL)
	seedCmd.Env = append(os.Environ(), "BENCHMARK_MARKER=qualify-soak")
	seedOut, err := seedCmd.Output()
	if err != nil {
		return failLane(cfg.out, "QR-001", cfg.started, map[string]any{"error": fmt.Sprintf("soaksetup: %v", err)}, nil)
	}
	var appID, attrID string
	for _, line := range strings.Split(string(seedOut), "\n") {
		if v, ok := strings.CutPrefix(line, "APP="); ok {
			appID = strings.TrimSpace(v)
		}
		if v, ok := strings.CutPrefix(line, "ATTR="); ok {
			attrID = strings.TrimSpace(v)
		}
	}
	if appID == "" || attrID == "" {
		return failLane(cfg.out, "QR-001", cfg.started, map[string]any{"error": "soaksetup did not emit APP/ATTR"}, nil)
	}

	// Start the candidate.
	drv := &recoveryDriver{instantd: cfg.instantd, databaseURL: cfg.databaseURL, addr: cfg.addr, log: os.Stderr}
	procCtx, stopProc := contextCancel()
	defer stopProc()
	proc, err := drv.startInstantd(procCtx, cfg.databaseURL)
	if err != nil {
		return failLane(cfg.out, "QR-001", cfg.started, map[string]any{"error": err.Error()}, nil)
	}
	defer func() {
		_ = proc.Process.Kill()
		_, _ = proc.Process.Wait()
	}()
	preIdentity := sampleProcessIdentity(proc)

	// Time-series sampler: RSS, fds, PG connections every 10s.
	samplerDone := make(chan struct{})
	go sampleTimeSeries(samplerDone, tsPath, proc.Process.Pid, cfg.databaseURL)

	// Run the existing soak binary with declared alpha budgets.
	soakCmd := exec.Command(cfg.soakBin,
		"-url", "ws://"+cfg.addr+"/runtime/session",
		"-app", appID, "-attr", attrID,
		"-sessions", fmt.Sprint(soakSessions),
		"-duration", (soakRamp + soakSettle + soakActive).String(),
		"-ramp", soakRamp.String(), "-settle", soakSettle.String(),
		"-global-tx-rate", fmt.Sprint(soakGlobalTxRate),
		"-max-p99-lag", soakMaxP99Lag,
		"-sdk-version", soakSDKVersion,
		"-events", eventsPath,
	)
	logF, err := os.Create(soakLog)
	if err != nil {
		return failLane(cfg.out, "QR-001", cfg.started, map[string]any{"error": err.Error()}, nil)
	}
	soakCmd.Stdout = logF
	soakCmd.Stderr = logF
	runErr := soakCmd.Run()
	_ = logF.Close()
	close(samplerDone)

	postIdentity := sampleProcessIdentity(proc)
	if preIdentity != "" && postIdentity != "" && preIdentity != postIdentity {
		return failLane(cfg.out, "QR-001", cfg.started, map[string]any{
			"error": fmt.Sprintf("process identity changed across soak (replacement): %q -> %q", preIdentity, postIdentity),
		}, nil)
	}
	if runErr != nil {
		return failLane(cfg.out, "QR-001", cfg.started, map[string]any{"error": fmt.Sprintf("soak binary: %v", runErr)}, nil)
	}
	// Map the soak's own evidence: the completeness manifest at the
	// DeriveManifestPath location plus the events artifact it lists.
	manifestPath := soakManifestPath(eventsPath)
	if _, err := os.Stat(manifestPath); err != nil {
		return failLane(cfg.out, "QR-001", cfg.started, map[string]any{"error": fmt.Sprintf("soak manifest missing at %s: %v", manifestPath, err)}, nil)
	}
	return mapSoakEvidenceFile(manifestPath, eventsPath, cfg.out, cfg.started)
}
