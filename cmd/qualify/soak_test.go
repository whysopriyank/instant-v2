package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// realManifestFixture returns a completeness manifest in cmd/soak's exact
// wire format: field names are the literal json tags from
// cmd/soak/evidence.go (verified by TestSoakOutputTagsPinned), so any tag
// rename upstream breaks that pin test and forces this fixture to follow.
func realManifestFixture(t *testing.T, dir string, sessions int, durationSecs float64, transacts, refreshes, connects int64, dropped, unresolved int, success bool) string {
	t.Helper()
	m := map[string]any{
		"schema_version":   1,
		"status":           "complete",
		"completed":        true,
		"run_id":           "soak-test",
		"started_at":       "2026-09-20T00:00:00Z",
		"finished_at":      "2026-09-20T00:15:30Z",
		"duration_seconds": durationSecs,
		"target_url":       "ws://127.0.0.1:18081/runtime/session",
		"app_id":           "00000000-0000-4000-8000-000000000001",
		"workload": map[string]any{
			"sessions":       sessions,
			"duration":       "15m0s",
			"global_tx_rate": 8.0,
			"tx_interval":    "2s",
			"ramp_up":        "1m0s",
			"settle":         "1m0s",
			"quiescence":     "10s",
			"max_p99_lag":    "10s",
		},
		"summary": map[string]any{
			"connects":    connects,
			"transacts":   transacts,
			"refreshes":   refreshes,
			"dropped":     dropped,
			"unresolved":  unresolved,
			"lag_samples": transacts,
			"lag_p50":     "12ms",
			"lag_p99":     "200ms",
			"lag_max":     "400ms",
			"success":     success,
		},
		"artifacts": []any{
			map[string]any{"name": "events.jsonl", "path": "events.jsonl", "size_bytes": 10, "sha256": strings.Repeat("a", 64), "required": true},
		},
	}
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(p, append(raw, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// realEventsFixture writes run-level events exactly as cmd/soak/main.go
// emits them (names pinned by TestSoakOutputTagsPinned).
func realEventsFixture(t *testing.T, dir string, transacts, refreshes, connects int64) string {
	t.Helper()
	lines := []string{
		`{"at":"2026-09-20T00:00:00Z","event":"run_started","url":"ws://127.0.0.1:18081/runtime/session","sessions":500}`,
		`{"at":"2026-09-20T00:01:00Z","event":"progress","sessions":500,"refreshes":100,"transacts":100,"dropped":0,"unresolved":0}`,
		`{"at":"2026-09-20T00:15:00Z","event":"lag_diagnostic","metric_kind":"transport_diagnostic","semantic_claim":false,"samples":100,"p50":"12ms","p99":"200ms","max":"400ms"}`,
	}
	fin, _ := json.Marshal(map[string]any{
		"at": "2026-09-20T00:15:30Z", "event": "run_finished", "status": "passed",
		"sessions": connects, "refreshes": refreshes, "transacts": transacts, "lag_samples": 100,
	})
	lines = append(lines, string(fin))
	p := filepath.Join(dir, "events.jsonl")
	if err := os.WriteFile(p, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func readManifest(t *testing.T, p string) soakManifest {
	t.Helper()
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	var m soakManifest
	if err := decodeStrict(raw, &m); err != nil {
		t.Fatalf("real-format fixture must strict-decode: %v", err)
	}
	return m
}

func TestMapSoakManifestSummaryCertified(t *testing.T) {
	dir := t.TempDir()
	mp := realManifestFixture(t, dir, 500, 930.4, 1200, 1200, 500, 0, 0, true)
	ep := realEventsFixture(t, dir, 1200, 1200, 500)
	m := readManifest(t, mp)
	events, ledger, err := parseSoakArtifactFile(ep)
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger) != 0 {
		t.Fatalf("run-level events file must yield zero ledger entries, got %d", len(ledger))
	}
	ev, derived, err := mapSoakManifest(m, ledger, events)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Sessions != 500 || ev.ActiveSeconds != 930 {
		t.Fatalf("sessions/active_seconds wrong: %+v", ev)
	}
	if ev.CommittedTransactions != 1200 || ev.AcknowledgedTransactions != 1200 || ev.RefreshedTransactions != 1200 {
		t.Fatalf("summary-certified counts wrong: %+v", ev)
	}
	if ev.DroppedTransactions != 0 || ev.UnresolvedTransactions != 0 {
		t.Fatalf("dropped/unresolved wrong: %+v", ev)
	}
	if len(derived) == 0 {
		t.Fatal("certification basis must be documented")
	}
	if code := writeSoakResult(filepath.Join(dir, "lane.json"), time.Now().UTC(), ev, derived); code != 0 {
		t.Fatal("in-budget certified evidence must PASS")
	}
}

func TestMapSoakManifestLedgerDerived(t *testing.T) {
	dir := t.TempDir()
	mp := realManifestFixture(t, dir, 500, 905.0, 4, 4, 500, 0, 0, true)
	// Per-tx ledger entries in cmd/soak LedgerEntry wire format (tags from
	// cmd/soak/ledger.go, pinned below): 4 resolved (acked + own refresh
	// observed), one with refresh_before_ack set.
	ledgerLines := []string{
		`{"client_event_id":"t-0-1","server_tx_id":"101","session_id":0,"submitted_at":"2026-09-20T00:00:01Z","ack_at":"2026-09-20T00:00:02Z","refresh_at":"2026-09-20T00:00:03Z","state":"resolved"}`,
		`{"client_event_id":"t-0-2","server_tx_id":"102","session_id":0,"submitted_at":"2026-09-20T00:00:04Z","ack_at":"2026-09-20T00:00:05Z","refresh_at":"2026-09-20T00:00:06Z","state":"resolved"}`,
		`{"client_event_id":"t-1-1","server_tx_id":"103","session_id":1,"submitted_at":"2026-09-20T00:00:07Z","ack_at":"2026-09-20T00:00:08Z","refresh_at":"2026-09-20T00:00:09Z","refresh_before_ack":true,"state":"resolved"}`,
		`{"client_event_id":"t-1-2","server_tx_id":"104","session_id":1,"submitted_at":"2026-09-20T00:00:10Z","ack_at":"2026-09-20T00:00:11Z","refresh_at":"2026-09-20T00:00:12Z","state":"resolved"}`,
		`{"at":"2026-09-20T00:15:30Z","event":"run_finished","status":"passed","sessions":500,"refreshes":4,"transacts":4,"lag_samples":4}`,
	}
	ep := filepath.Join(dir, "events.jsonl")
	if err := os.WriteFile(ep, []byte(strings.Join(ledgerLines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	m := readManifest(t, mp)
	events, ledger, err := parseSoakArtifactFile(ep)
	if err != nil {
		t.Fatal(err)
	}
	ev, derived, err := mapSoakManifest(m, ledger, events)
	if err != nil {
		t.Fatal(err)
	}
	if ev.CommittedTransactions != 4 { // distinct server tx ids acknowledged
		t.Fatalf("committed = %d, want 4", ev.CommittedTransactions)
	}
	if ev.AcknowledgedTransactions != 4 { // acknowledged + resolved states
		t.Fatalf("acknowledged = %d, want 4", ev.AcknowledgedTransactions)
	}
	if ev.RefreshedTransactions != 4 { // own refresh observed (refresh_at set)
		t.Fatalf("refreshed = %d, want 4", ev.RefreshedTransactions)
	}
	if len(derived) == 0 {
		t.Fatal("ledger derivation must be documented")
	}
	if code := writeSoakResult(filepath.Join(dir, "lane.json"), time.Now().UTC(), ev, derived); code != 0 {
		t.Fatal("derived in-budget evidence must PASS")
	}
}

func TestMapSoakManifestAckImplyingStates(t *testing.T) {
	dir := t.TempDir()
	// 1 resolved + 1 acknowledged (acked, refresh not yet observed):
	// acknowledged counts as acked but stays unresolved (non-resolved).
	mp := realManifestFixture(t, dir, 500, 905.0, 2, 2, 500, 0, 1, true)
	lines := []string{
		`{"client_event_id":"t-0-1","server_tx_id":"101","session_id":0,"submitted_at":"2026-09-20T00:00:01Z","ack_at":"2026-09-20T00:00:02Z","refresh_at":"2026-09-20T00:00:03Z","state":"resolved"}`,
		`{"client_event_id":"t-0-2","server_tx_id":"102","session_id":0,"submitted_at":"2026-09-20T00:00:04Z","ack_at":"2026-09-20T00:00:05Z","state":"acknowledged"}`,
		`{"at":"2026-09-20T00:15:30Z","event":"run_finished","status":"passed","sessions":500,"refreshes":2,"transacts":2,"lag_samples":1}`,
	}
	ep := filepath.Join(dir, "events.jsonl")
	if err := os.WriteFile(ep, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	m := readManifest(t, mp)
	events, ledger, err := parseSoakArtifactFile(ep)
	if err != nil {
		t.Fatal(err)
	}
	ev, _, err := mapSoakManifest(m, ledger, events)
	if err != nil {
		t.Fatal(err)
	}
	if ev.CommittedTransactions != 2 || ev.AcknowledgedTransactions != 2 {
		t.Fatalf("acked states must count: %+v", ev)
	}
	if ev.RefreshedTransactions != 1 {
		t.Fatalf("only refresh_at entries count as refreshed: %+v", ev)
	}
	if ev.UnresolvedTransactions != 1 {
		t.Fatalf("acknowledged-but-unrefreshed stays unresolved: %+v", ev)
	}
}

func TestMapSoakManifestLedgerMismatchFails(t *testing.T) {
	dir := t.TempDir()
	// Summary claims dropped=0 but the ledger holds a terminal entry.
	mp := realManifestFixture(t, dir, 500, 905.0, 2, 2, 500, 0, 0, true)
	lines := []string{
		`{"client_event_id":"t-0-1","server_tx_id":"101","session_id":0,"submitted_at":"2026-09-20T00:00:01Z","ack_at":"2026-09-20T00:00:02Z","refresh_at":"2026-09-20T00:00:03Z","state":"resolved"}`,
		`{"client_event_id":"t-0-2","session_id":0,"submitted_at":"2026-09-20T00:00:04Z","state":"terminal","terminal_reason":"quiescence_timeout_missing_ack"}`,
		`{"at":"2026-09-20T00:15:30Z","event":"run_finished","status":"passed","sessions":500,"refreshes":2,"transacts":2,"lag_samples":1}`,
	}
	ep := filepath.Join(dir, "events.jsonl")
	if err := os.WriteFile(ep, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	m := readManifest(t, mp)
	events, ledger, err := parseSoakArtifactFile(ep)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := mapSoakManifest(m, ledger, events); err == nil {
		t.Fatal("summary/ledger dropped mismatch must FAIL")
	}
}

func TestMapSoakManifestUnknownStateFails(t *testing.T) {
	dir := t.TempDir()
	mp := realManifestFixture(t, dir, 500, 905.0, 1, 1, 500, 0, 0, true)
	lines := []string{
		`{"client_event_id":"t-0-1","server_tx_id":"101","session_id":0,"submitted_at":"2026-09-20T00:00:01Z","state":"refreshed"}`,
		`{"at":"2026-09-20T00:15:30Z","event":"run_finished","status":"passed","sessions":500,"refreshes":1,"transacts":1,"lag_samples":1}`,
	}
	// "refreshed" is a known ledger state (accepted); an invented one fails.
	lines = append(lines, `{"client_event_id":"t-0-2","session_id":0,"submitted_at":"2026-09-20T00:00:02Z","state":"done"}`)
	ep := filepath.Join(dir, "events.jsonl")
	if err := os.WriteFile(ep, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := parseSoakArtifactFile(ep); err == nil {
		t.Fatal("unknown ledger state must FAIL (no fallback guessing)")
	}
	_ = mp
}

func TestMapSoakManifestUnknownEventFails(t *testing.T) {
	dir := t.TempDir()
	ep := filepath.Join(dir, "events.jsonl")
	if err := os.WriteFile(ep, []byte("{\"at\":\"2026-09-20T00:00:00Z\",\"event\":\"tx_committed\",\"tx\":1}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := parseSoakArtifactFile(ep); err == nil {
		t.Fatal("unknown event name must FAIL")
	}
}

func TestMapSoakManifestRequiresCompleteSuccess(t *testing.T) {
	dir := t.TempDir()
	mp := realManifestFixture(t, dir, 500, 905.0, 0, 0, 500, 1, 0, false)
	ep := realEventsFixture(t, dir, 0, 0, 500)
	m := readManifest(t, mp)
	events, ledger, err := parseSoakArtifactFile(ep)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := mapSoakManifest(m, ledger, events); err == nil {
		t.Fatal("success=false manifest must FAIL")
	}
	raw, _ := os.ReadFile(mp)
	var asMap map[string]any
	if err := json.Unmarshal(raw, &asMap); err != nil {
		t.Fatal(err)
	}
	asMap["status"] = "partial"
	raw2, _ := json.Marshal(asMap)
	var m2 soakManifest
	if err := decodeStrict(raw2, &m2); err != nil {
		t.Fatal(err)
	}
	if _, _, err := mapSoakManifest(m2, nil, events); err == nil {
		t.Fatal("status != complete must FAIL")
	}
}

func TestMapSoakManifestRunFinishedMismatchFails(t *testing.T) {
	dir := t.TempDir()
	mp := realManifestFixture(t, dir, 500, 905.0, 1200, 1200, 500, 0, 0, true)
	ep := realEventsFixture(t, dir, 999, 1200, 500) // transacts disagree
	m := readManifest(t, mp)
	events, ledger, err := parseSoakArtifactFile(ep)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := mapSoakManifest(m, ledger, events); err == nil {
		t.Fatal("summary/run_finished counter mismatch must FAIL")
	}
}

func TestWriteSoakResultBudgets(t *testing.T) {
	good := soakEvidence{Sessions: 500, ActiveSeconds: 900, CommittedTransactions: 5, AcknowledgedTransactions: 5, RefreshedTransactions: 5}
	if code := writeSoakResult(filepath.Join(t.TempDir(), "a.json"), time.Now().UTC(), good, nil); code != 0 {
		t.Fatal("boundary budgets (500/900) must PASS")
	}
	bad := good
	bad.DroppedTransactions = 1
	if code := writeSoakResult(filepath.Join(t.TempDir(), "b.json"), time.Now().UTC(), bad, nil); code == 0 {
		t.Fatal("dropped != 0 must FAIL")
	}
	bad2 := good
	bad2.AcknowledgedTransactions = 4
	if code := writeSoakResult(filepath.Join(t.TempDir(), "c.json"), time.Now().UTC(), bad2, nil); code == 0 {
		t.Fatal("ack != committed must FAIL")
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "lane.json")
	if code := writeSoakResult(p, time.Now().UTC(), good, []string{"summary-certified(success=true ⇒ quiescence: all transacts acked+resolved, zero drops)"}); code != 0 {
		t.Fatal(code)
	}
	raw, _ := os.ReadFile(p)
	var lr laneResult
	if err := json.Unmarshal(raw, &lr); err != nil {
		t.Fatal(err)
	}
	if _, ok := lr.Details["derived_from_ledger"]; !ok {
		t.Fatal("lane result must document ledger derivation")
	}
	projected, err := projectDetails("QR-001", lr.Details)
	if err != nil {
		t.Fatal(err)
	}
	if len(projected) != 7 {
		t.Fatalf("projected soak details must have exactly 7 keys: %v", projected)
	}
}

// TestSoakOutputTagsPinned fails if cmd/soak's evidence/ledger field tags,
// ledger state names, or emitted event names change. The strict mirror
// structs above must then be updated in lockstep; a silent drift would
// otherwise corrupt the QR-001 mapping.
func TestSoakOutputTagsPinned(t *testing.T) {
	evidenceSrc, err := os.ReadFile("../../cmd/soak/evidence.go")
	if err != nil {
		t.Fatal(err)
	}
	ledgerSrc, err := os.ReadFile("../../cmd/soak/ledger.go")
	if err != nil {
		t.Fatal(err)
	}
	mainSrc, err := os.ReadFile("../../cmd/soak/main.go")
	if err != nil {
		t.Fatal(err)
	}
	// Tag presence is checked by leading-quote name match ("`+`"name" matches
	// both `json:"name"` and `json:"name,omitempty"`), so an omitempty rename
	// still trips the pin.
	for _, tag := range []string{
		`"schema_version`, `"status`, `"completed`, `"run_id`, `"started_at`,
		`"finished_at`, `"duration_seconds`, `"target_url`, `"app_id`,
		`"workload`, `"summary`, `"artifacts`, `"pprof_endpoint`,
		`"completion_marker`, `"sessions`, `"duration`, `"global_tx_rate`,
		`"tx_interval`, `"ramp_up`, `"settle`, `"quiescence`, `"max_p99_lag`,
		`"connects`, `"transacts`, `"refreshes`, `"dropped`, `"unresolved`,
		`"lag_samples`, `"lag_p50`, `"lag_p99`, `"lag_max`, `"success`,
		`"failure_error`, `"name`, `"path`, `"size_bytes`, `"sha256`, `"required`,
	} {
		if !strings.Contains(string(evidenceSrc), tag) {
			t.Fatalf("evidence.go no longer contains tag %s: update soakMirror structs", tag)
		}
	}
	for _, tag := range []string{
		`"client_event_id`, `"server_tx_id`, `"session_id`, `"submitted_at`,
		`"ack_at`, `"refresh_at`, `"lag`, `"refresh_before_ack`, `"state`,
		`"terminal_reason`,
	} {
		if !strings.Contains(string(ledgerSrc), tag) {
			t.Fatalf("ledger.go no longer contains tag %s: update soakLedgerEntry", tag)
		}
	}
	for _, state := range []string{
		`StateSubmitted    EntryState = "submitted"`,
		`StateAcknowledged EntryState = "acknowledged"`,
		`StateRefreshed    EntryState = "refreshed"`,
		`StateResolved     EntryState = "resolved"`,
		`StateTerminal     EntryState = "terminal"`,
	} {
		if !strings.Contains(string(ledgerSrc), state) {
			t.Fatalf("ledger.go state declaration changed %q: update soakKnownLedgerStates", state)
		}
	}
	for _, emit := range []string{
		`Emit("run_started"`, `Emit("scheduler_error"`, `Emit("scheduler_finished"`,
		`Emit("progress"`, `Emit("run_finished"`, `Emit("lag_diagnostic"`,
	} {
		if !strings.Contains(string(mainSrc), emit) {
			t.Fatalf("main.go no longer emits %s: update soakAllowedEventFields", emit)
		}
	}
	// The event envelope (event/at keys) lives in benchharness/observe.go;
	// the per-name payload sets above must match the Emit call sites.
	observeSrc, err := os.ReadFile("../../internal/benchharness/observe.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(observeSrc), `"event": name, "at"`) {
		t.Fatalf("observe.go Emit envelope changed: update parseSoakRunEvent")
	}
	for _, payload := range []string{
		`"status": "passed"`, `"transacts": transacts.Load()`, `"lag_samples": len(sorted)`,
	} {
		if !strings.Contains(string(mainSrc), payload) {
			t.Fatalf("main.go run_finished payload changed %q: update checkRunFinishedEvent", payload)
		}
	}
}
