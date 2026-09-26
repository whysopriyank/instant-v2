package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// Declared load levels for the SIGTERM drain outcomes (transactions/sec of
// sustained write load at the moment SIGTERM is delivered).
const (
	drainModerateTxRate   = 8.0
	drainSaturatedTxRate  = 64.0
	drainSettleWindow     = 20 * time.Second
	drainExitBudget       = 30 * time.Second
	postgresRTOBudget     = 3600 * time.Second
	saturatedMinInFlight  = 8 // ledger max-outstanding required at SIGTERM
	crashPreconditionWait = 30 * time.Second
	convergeWait          = 90 * time.Second
	restartWait           = 5 * time.Minute
)

// txTriple is one add-triple step: entity UUID, attr UUID, blob string value.
type txTriple struct {
	Entity string `json:"entity"`
	Attr   string `json:"attr"`
	Value  string `json:"value"`
}

// txDisposition is the observed end state of one submitted tx.
type txDisposition string

const (
	txAcked   txDisposition = "acked"
	txError   txDisposition = "error"
	txUnknown txDisposition = "unknown-at-crash"
)

// txRecord is one per-tx ledger entry: submitted → acked(server tx id) |
// error(explicit reason) | unknown-at-crash. No verdict field is ever
// assigned: verdicts are computed from these records plus the DB oracle.
type txRecord struct {
	ClientEventID string        `json:"client_event_id"`
	Triples       []txTriple    `json:"triples"`
	ServerTxID    string        `json:"server_tx_id,omitempty"`
	Disposition   txDisposition `json:"disposition"`
	FailReason    string        `json:"fail_reason,omitempty"`
	SubmittedAt   time.Time     `json:"submitted_at"`
	AckAt         time.Time     `json:"ack_at,omitempty"`
}

// txLedger is the per-outcome client-side ledger shared by pipelined writer
// sessions. submit/ack/fail may run from reader goroutines; all verdict
// inputs below are derived from snapshot().
type txLedger struct {
	mu             sync.Mutex
	order          []string
	byID           map[string]*txRecord
	outstanding    int
	maxOutstanding int
}

func newTxLedger() *txLedger { return &txLedger{byID: map[string]*txRecord{}} }

func (l *txLedger) submit(rec *txRecord) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.order = append(l.order, rec.ClientEventID)
	l.byID[rec.ClientEventID] = rec
	l.outstanding++
	if l.outstanding > l.maxOutstanding {
		l.maxOutstanding = l.outstanding
	}
}

func (l *txLedger) settle(id, serverTxID, failReason string, at time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	rec, ok := l.byID[id]
	if !ok || rec.Disposition != txUnknown {
		return
	}
	if failReason != "" {
		rec.Disposition = txError
		rec.FailReason = failReason
	} else {
		rec.Disposition = txAcked
		rec.ServerTxID = serverTxID
		rec.AckAt = at
	}
	l.outstanding--
}

func (l *txLedger) snapshot() []*txRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]*txRecord, 0, len(l.order))
	for _, id := range l.order {
		cp := *l.byID[id]
		out = append(out, &cp)
	}
	return out
}

func (l *txLedger) pendingCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.outstanding
}

func (l *txLedger) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.order)
}

func (l *txLedger) maxOutstandingCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.maxOutstanding
}

func (l *txLedger) ackedRecords() []*txRecord {
	var out []*txRecord
	for _, r := range l.snapshot() {
		if r.Disposition == txAcked {
			out = append(out, r)
		}
	}
	return out
}

// subObserved is the measured client-side evidence for one subscriber
// session: add-query admission, every add-query-ok/refresh-ok result, and
// the real websocket close status when the socket broke.
type subObserved struct {
	Session      int      `json:"session"`
	AddQueryOK   bool     `json:"add_query_ok"`
	RefreshOKs   int      `json:"refresh_oks"`
	MaxTx        int64    `json:"max_processed_tx"`
	HasMaxTx     bool     `json:"has_max_processed_tx"`
	ValueLeaves  []string `json:"value_leaves_truncated,omitempty"`
	LeavesCapped bool     `json:"leaves_capped"`
	ErrorFrames  []string `json:"error_frames,omitempty"`
	CloseCode    string   `json:"close_code,omitempty"`
}

// outcomeEvidence is the fully-measured evidence for one recovery outcome.
// Every verdict input is computed from process/DB/client observation by the
// driver (never assigned a literal); verdictOutcome only compares.
type outcomeEvidence struct {
	ID               string
	Ran              bool
	HarnessError     string
	Precondition     string
	PreconditionMet  bool
	AckedMissing     []string // acked tx ids with ≥1 triple absent from the oracle
	PartialTxIDs     []string // submitted tx ids with 0 < present < len(triples)
	ExpectedSubs     int
	ObservedSubs     int // subscribers whose add-query was admitted
	DivergedSubs     []string
	Converged        bool // computed by compareSubscriberSets, never assigned
	RTO              time.Duration
	ExitSeconds      int64
	ExitRecorded     bool
	ExitStatus       string
	PortReleased     bool
	InFlightResolved bool // computed for drain: every submitted tx accounted
	UnresolvedTxIDs  []string
	CloseCodes       []string // observed real websocket close statuses
	PIDBefore        int
	PIDAfter         int
	MaxOutstanding   int
	// Artifact detail (informational, not verdict inputs):
	Ledger       []*txRecord         `json:"-"`
	OracleValues []string            `json:"-"`
	SubValues    map[string][]string `json:"-"`
	SubSnapshots []subObserved       `json:"-"`
	Universe     []string            `json:"-"`
	Timings      map[string]string   `json:"-"`
}

// verdictOutcome evaluates one measured observation to PASS/FAIL.
// Budgets: RTO ≤ 3600 s (recovery), drain exit ≤ 30 s. Every check compares
// measured fields; a missing measurement fails closed.
func verdictOutcome(o outcomeEvidence) (pass bool, reason string) {
	if !o.Ran {
		return false, "outcome did not run"
	}
	if o.HarnessError != "" {
		return false, o.HarnessError
	}
	if !o.PreconditionMet {
		return false, "precondition not reached: " + o.Precondition
	}
	if len(o.AckedMissing) > 0 {
		return false, fmt.Sprintf("acked tx missing from durable state: %v", o.AckedMissing)
	}
	if len(o.PartialTxIDs) > 0 {
		return false, fmt.Sprintf("partial (non-atomic) tx observed: %v", o.PartialTxIDs)
	}
	if !o.Converged {
		return false, fmt.Sprintf("client final state diverged from DB oracle: %v", o.DivergedSubs)
	}
	switch o.ID {
	case "drain-idle", "drain-moderate", "drain-saturated":
		if !o.ExitRecorded {
			return false, "exit status not recorded"
		}
		if o.ExitSeconds > 30 {
			return false, fmt.Sprintf("drain exit %ds exceeds 30s budget", o.ExitSeconds)
		}
		if !o.PortReleased {
			return false, "listen port still bound after exit"
		}
		if !o.InFlightResolved {
			return false, fmt.Sprintf("in-flight tx not resolved: %v", o.UnresolvedTxIDs)
		}
	case "postgres-restart":
		if o.PIDBefore != o.PIDAfter {
			return false, fmt.Sprintf("instantd restarted during postgres-restart (pid %d -> %d)", o.PIDBefore, o.PIDAfter)
		}
		if ceilSeconds(o.RTO) > 3600 {
			return false, fmt.Sprintf("RTO %ds exceeds 3600s budget", ceilSeconds(o.RTO))
		}
	}
	return true, ""
}

// verdictRecovery folds all seven observations into the OP-005 details object.
// Any unknown ID, missing outcome, or budget miss fails that outcome; the
// record result is FAIL unless every outcome passes.
func verdictRecovery(obs []outcomeEvidence) (details map[string]any, allPass bool) {
	byID := map[string]outcomeEvidence{}
	for _, o := range obs {
		byID[o.ID] = o
	}
	outcomes := make([]map[string]any, 0, len(RecoveryOutcomeIDs))
	var maxRTO, maxDrain int64
	allPass = len(obs) > 0
	for _, id := range RecoveryOutcomeIDs {
		o, ok := byID[id]
		if !ok {
			o = outcomeEvidence{ID: id}
		}
		o.ID = id
		pass, reason := verdictOutcome(o)
		status := "PASS"
		if !pass {
			status = "FAIL"
			allPass = false
		}
		entry := map[string]any{"id": id, "result": status}
		if !pass {
			entry["reason"] = reason
		}
		// NOTE: entries stay exactly {id,result} (+lane-local reason on
		// FAIL): the gate requires outcomes[] keys == ["id","result"], so
		// close codes and all other evidence live in the per-outcome
		// artifact files (recorded via record --artifact), never here.
		outcomes = append(outcomes, entry)
		if id == "postgres-restart" && o.Ran && pass {
			if rto := ceilSeconds(o.RTO); rto > maxRTO {
				maxRTO = rto
			}
		}
		if strings.HasPrefix(id, "drain-") && o.Ran && pass {
			if o.ExitSeconds > maxDrain {
				maxDrain = o.ExitSeconds
			}
		}
	}
	return map[string]any{
		"max_rto_seconds":   maxRTO,
		"max_drain_seconds": maxDrain,
		"outcomes":          outcomes,
	}, allPass
}

func runRecovery(args []string) int {
	fs := flag.NewFlagSet("recovery", flag.ContinueOnError)
	instantd := fs.String("instantd", "", "candidate instantd binary PATH (required)")
	databaseURL := fs.String("database-url", os.Getenv("DATABASE_URL"), "owned PostgreSQL DSN (never printed)")
	soaksetup := fs.String("soaksetup", "", "cmd/soaksetup binary for fresh app/fixture seeding (required for live run)")
	pgRestart := fs.String("pg-restart-cmd", "", "host command that restarts the owned DB container (required for live run)")
	addr := fs.String("addr", "127.0.0.1:18080", "loopback addr for the candidate under test")
	out := fs.String("out", "", "lane result JSON output path (required)")
	eventsDir := fs.String("events-dir", "", "directory for per-outcome evidence (default: alongside --out)")
	evidenceRoot := fs.String("evidence-root", "", "evidence root for artifact paths (default: parent of --events-dir)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *out == "" {
		fmt.Fprintln(os.Stderr, "recovery: --out is required")
		return 2
	}
	started := time.Now().UTC()
	if *instantd == "" || *soaksetup == "" || *pgRestart == "" {
		return failLane(*out, "OP-005", started, map[string]any{
			"error": "live run requires --instantd, --soaksetup, and --pg-restart-cmd",
		}, nil)
	}
	if strings.TrimSpace(*databaseURL) == "" {
		return failLane(*out, "OP-005", started, map[string]any{"error": "owned DATABASE_URL is required"}, nil)
	}
	dir := *eventsDir
	if dir == "" {
		dir = filepath.Dir(*out)
	}
	root := *evidenceRoot
	if root == "" {
		root = filepath.Dir(dir)
	}
	drv := &recoveryDriver{
		instantd:     *instantd,
		databaseURL:  *databaseURL,
		soaksetup:    *soaksetup,
		pgRestart:    *pgRestart,
		addr:         *addr,
		eventsDir:    dir,
		evidenceRoot: root,
		log:          os.Stderr,
	}
	obs := drv.runAll(context.Background())
	details, allPass := verdictRecovery(obs)
	selected := len(RecoveryOutcomeIDs)
	lr := laneResult{
		Packet: "OP-005", SelectedCount: selected, SkippedCount: 0,
		Result:  map[bool]string{true: "PASS", false: "FAIL"}[allPass],
		Details: details, Artifacts: drv.artifacts,
		StartedAt: started.Format(time.RFC3339), FinishedAt: time.Now().UTC().Format(time.RFC3339),
	}
	if err := writeJSONFile(*out, lr); err != nil {
		fmt.Fprintln(os.Stderr, "recovery:", err)
		return 1
	}
	if !allPass {
		fmt.Fprintln(os.Stderr, "recovery: FAIL (see lane result details)")
		return 1
	}
	fmt.Fprintln(os.Stderr, "recovery: PASS 7/7 outcomes")
	return 0
}

// failLane writes a FAIL lane result with an error detail and returns 1
// (2 when the invocation itself was invalid is handled by callers).
func failLane(out, packet string, started time.Time, details map[string]any, artifacts []ArtifactRef) int {
	lr := laneResult{
		Packet: packet, SelectedCount: 0, SkippedCount: 0, Result: "FAIL",
		Details: details, Artifacts: artifacts,
		StartedAt: started.Format(time.RFC3339), FinishedAt: time.Now().UTC().Format(time.RFC3339),
	}
	if err := writeJSONFile(out, lr); err != nil {
		fmt.Fprintln(os.Stderr, "write lane result:", err)
	}
	return 1
}

// ---- live driver (candidate binary + owned DB + WS clients) ----

type recoveryDriver struct {
	instantd     string
	databaseURL  string
	soaksetup    string
	pgRestart    string
	addr         string
	eventsDir    string
	evidenceRoot string
	log          *os.File
	artifacts    []ArtifactRef
}

// outcomeDatabase creates a fresh owned database for one outcome and
// returns its DSN. cmd/soaksetup mints a fixed service identity per database
// (plus one app per call), so re-seeding the same database collides; a fresh
// database per outcome gives each outcome an isolated fixture with no
// cross-outcome state. Names stay under the disposable instant_bench_
// prefix; the cluster owner (superuser in the campaign) must allow CREATE
// DATABASE, else the outcome fails closed here.
func (d *recoveryDriver) outcomeDatabase(ctx context.Context, tag string) (string, error) {
	base, err := url.Parse(d.databaseURL)
	if err != nil || base.Path == "" {
		return "", fmt.Errorf("owned DATABASE_URL is not a URL with a database path")
	}
	name := strings.TrimPrefix(base.Path, "/") + "_qh_" + tag
	if !strings.HasPrefix(name, "instant_bench_") {
		return "", fmt.Errorf("derived database %q is not disposable instant_bench_", name)
	}
	admin, err := sql.Open("pgx", d.databaseURL)
	if err != nil {
		return "", err
	}
	defer func() { _ = admin.Close() }()
	qctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	quoted := `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	if _, err := admin.ExecContext(qctx, `CREATE DATABASE `+quoted); err != nil {
		return "", fmt.Errorf("create outcome database: %w", err)
	}
	base.Path = "/" + name
	return base.String(), nil
}

// seedFixture runs soaksetup against one outcome database to create a fresh
// app+attr and returns their IDs. It mirrors the existing cmd/soaksetup flow
// instead of reimplementing schema writes, so fixture semantics cannot drift
// from the soak lane.
func (d *recoveryDriver) seedFixture(ctx context.Context, dbURL string) (appID, attrID string, err error) {
	cmd := exec.CommandContext(ctx, d.soaksetup, "-database-url", dbURL)
	// soaksetup requires a benchmark marker; qualify owns its fixtures.
	cmd.Env = append(os.Environ(), "BENCHMARK_MARKER=qualify-recovery")
	out, err := cmd.Output()
	if err != nil {
		return "", "", commandError("soaksetup", err)
	}
	for _, line := range strings.Split(string(out), "\n") {
		if v, ok := strings.CutPrefix(line, "APP="); ok {
			appID = strings.TrimSpace(v)
		}
		if v, ok := strings.CutPrefix(line, "ATTR="); ok {
			attrID = strings.TrimSpace(v)
		}
	}
	if appID == "" || attrID == "" {
		return "", "", fmt.Errorf("soaksetup did not emit APP/ATTR")
	}
	return appID, attrID, nil
}

// requiredRuntimeEnv mirrors internal/config non-dev startup requirements:
// secrets travel via the process environment (set per campaign by the host
// orchestrator), never via files or evidence. The insecure dev fallback is
// refused outright.
func requiredRuntimeEnv() error {
	for _, k := range []string{"INSTANT_V2_STORAGE_SECRET", "INSTANT_V2_STORAGE_ROOT"} {
		if strings.TrimSpace(os.Getenv(k)) == "" {
			return fmt.Errorf("non-dev startup requires %s in the environment (refusing insecure fallback)", k)
		}
	}
	if strings.TrimSpace(os.Getenv("DATABASE_URL")) != "" {
		for _, k := range []string{
			"INSTANT_OAUTH_GOOGLE_CLIENT_ID", "INSTANT_OAUTH_GOOGLE_CLIENT_SECRET",
			"INSTANT_OAUTH_GITHUB_CLIENT_ID", "INSTANT_OAUTH_GITHUB_CLIENT_SECRET",
		} {
			if strings.TrimSpace(os.Getenv(k)) == "" {
				return fmt.Errorf("non-dev startup with DATABASE_URL requires %s in the environment", k)
			}
		}
	}
	if v := strings.TrimSpace(os.Getenv("INSTANT_V2_INSECURE_DEV_SECRETS")); v == "1" || strings.EqualFold(v, "true") {
		return fmt.Errorf("refusing INSTANT_V2_INSECURE_DEV_SECRETS in qualification")
	}
	return nil
}

func (d *recoveryDriver) startInstantd(ctx context.Context, dbURL string, extraEnv ...string) (*exec.Cmd, error) {
	if err := checkLoopbackAddr(d.addr); err != nil {
		return nil, err
	}
	if err := requiredRuntimeEnv(); err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, d.instantd)
	cmd.Stdout = d.log
	cmd.Stderr = d.log
	cmd.Env = append(os.Environ(),
		"DATABASE_URL="+dbURL,
		"INSTANT_V2_HTTP_ADDR="+d.addr,
	)
	cmd.Env = append(cmd.Env, extraEnv...)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	if err := waitHealth("http://"+d.addr, 30*time.Second, true); err != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		return nil, err
	}
	return cmd, nil
}

func checkLoopbackAddr(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid --addr %q: %w", addr, err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("refusing non-loopback --addr %q", addr)
	}
	return nil
}

func waitHealth(baseURL string, timeout time.Duration, wantUp bool) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 5 * time.Second}
	var last string
	for time.Now().Before(deadline) {
		resp, err := client.Get(baseURL + "/health")
		if err != nil {
			last = err.Error()
		} else {
			_ = resp.Body.Close()
			last = fmt.Sprintf("status %d", resp.StatusCode)
			if (resp.StatusCode == http.StatusOK) == wantUp {
				return nil
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("/health never reached wantUp=%v within %s (last: %s)", wantUp, timeout, last)
}

// dbOracle reads the exact triple set for an app from PostgreSQL: keys are
// entity\x00attr\x00value where value is the raw jsonb text (e.g. `"v"` for
// a blob string), so submitted triples normalize through the same encoding.
func (d *recoveryDriver) dbOracle(dbURL, appID string) (map[string]struct{}, error) {
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query(`SELECT entity_id::text, attr_id::text, Value::text FROM triples WHERE app_id = $1`, appID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	got := map[string]struct{}{}
	for rows.Next() {
		var e, a, v string
		if err := rows.Scan(&e, &a, &v); err != nil {
			return nil, err
		}
		got[e+"\x00"+a+"\x00"+v] = struct{}{}
	}
	return got, rows.Err()
}

// oracleKey builds the oracle key for one submitted triple through the same
// encoding the DB returns (jsonb text of the value).
func oracleKey(t txTriple) string {
	raw, err := json.Marshal(t.Value)
	if err != nil {
		raw = []byte(strconv.Quote(t.Value))
	}
	return t.Entity + "\x00" + t.Attr + "\x00" + string(raw)
}

// decodeOracleValue extracts the blob string from a jsonb-text value for the
// oracle value set; non-string values fall back to their raw text.
func decodeOracleValue(vtext string) string {
	var s string
	if err := json.Unmarshal([]byte(vtext), &s); err == nil {
		return s
	}
	return vtext
}

// oracleValueSet returns the sorted set of blob-string values present for
// the outcome's fixture attr(s).
func oracleValueSet(oracle map[string]struct{}, attrs map[string]struct{}) []string {
	set := map[string]struct{}{}
	for k := range oracle {
		parts := strings.Split(k, "\x00")
		if len(parts) != 3 {
			continue
		}
		if _, ok := attrs[parts[1]]; !ok {
			continue
		}
		set[decodeOracleValue(parts[2])] = struct{}{}
	}
	return sortedKeys(set)
}

// txPresence counts how many of tx's triples are present in the oracle.
func txPresence(oracle map[string]struct{}, triples []txTriple) int {
	n := 0
	for _, t := range triples {
		if _, ok := oracle[oracleKey(t)]; ok {
			n++
		}
	}
	return n
}

// compareValueSets compares one subscriber's observed value set to the DB
// oracle value set. Equal iff every oracle value was reported and nothing
// outside the oracle was reported. Pure function over measured inputs.
func compareValueSets(observed, oracle []string) (missing, extra []string) {
	want := map[string]struct{}{}
	for _, v := range oracle {
		want[v] = struct{}{}
	}
	seen := map[string]struct{}{}
	for _, v := range observed {
		if _, dup := seen[v]; dup {
			continue
		}
		seen[v] = struct{}{}
		if _, ok := want[v]; !ok {
			extra = append(extra, v)
		}
	}
	have := map[string]struct{}{}
	for _, v := range observed {
		have[v] = struct{}{}
	}
	for _, v := range oracle {
		if _, ok := have[v]; !ok {
			missing = append(missing, v)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	return missing, extra
}

func sortedKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// runAll executes the seven outcomes in gate-sorted order, each on a fresh
// app/fixture and (except postgres-restart) a fresh instantd process.
func (d *recoveryDriver) runAll(ctx context.Context) []outcomeEvidence {
	obs := make([]outcomeEvidence, 0, 7)
	obs = append(obs, d.outcomeCrash(ctx, "crash-before-commit", killBeforeAck))
	obs = append(obs, d.outcomeCrash(ctx, "crash-after-commit", killAfterAck))
	obs = append(obs, d.outcomeCrash(ctx, "crash-during-publication", killDuringPublication))
	obs = append(obs, d.outcomePostgresRestart(ctx))
	for _, lv := range []struct {
		id   string
		rate float64
	}{
		{"drain-idle", 0},
		{"drain-moderate", drainModerateTxRate},
		{"drain-saturated", drainSaturatedTxRate},
	} {
		obs = append(obs, d.outcomeDrain(ctx, lv.id, lv.rate))
	}
	return obs
}

// finalizeOutcome attaches the measured ledger, value universe, and
// subscriber snapshots to the evidence (when they exist) and writes the
// per-outcome artifact file. Nil arguments are kept minimal for
// harness-error paths that never got that far.
func (d *recoveryDriver) finalizeOutcome(o *outcomeEvidence, ledger *txLedger, universe map[string]struct{}, subs []*liveSubscriber) {
	if ledger != nil {
		if o.Ledger == nil {
			o.Ledger = ledger.snapshot()
		}
		if universe != nil {
			o.Universe = sortedKeys(universe)
		}
	}
	if subs != nil {
		o.SubSnapshots = snapSubscribers(subs)
	}
	// Missing outcome evidence fails the outcome closed rather than PASSing
	// without its artifact.
	if err := d.recordOutcomeArtifact(*o); err != nil && o.HarnessError == "" {
		o.HarnessError = "outcome artifact: " + err.Error()
	}
}

// recordOutcomeArtifact writes the per-outcome evidence file (ledger with
// submitted/acked/errored ids and server tx ids, subscriber final sets,
// oracle set, partial count, precondition evidence, timings, exit status,
// close codes) and registers it in the lane's artifact list.
func (d *recoveryDriver) recordOutcomeArtifact(ev outcomeEvidence) error {
	ev.Ledger = append([]*txRecord{}, ev.Ledger...)
	// Normalize nils to empty arrays/objects so artifacts never carry JSON
	// nulls where the schema documents a list.
	strList := func(in []string) []string {
		if in == nil {
			return []string{}
		}
		return in
	}
	if ev.SubValues == nil {
		ev.SubValues = map[string][]string{}
	}
	if ev.SubSnapshots == nil {
		ev.SubSnapshots = []subObserved{}
	}
	if ev.Timings == nil {
		ev.Timings = map[string]string{}
	}
	doc := map[string]any{
		"outcome":           ev.ID,
		"ran":               ev.Ran,
		"harness_error":     ev.HarnessError,
		"precondition":      ev.Precondition,
		"precondition_met":  ev.PreconditionMet,
		"acked_missing":     strList(ev.AckedMissing),
		"partial_txs":       strList(ev.PartialTxIDs),
		"expected_subs":     ev.ExpectedSubs,
		"observed_subs":     ev.ObservedSubs,
		"diverged_subs":     strList(ev.DivergedSubs),
		"converged":         ev.Converged,
		"rto_seconds":       ceilSeconds(ev.RTO),
		"exit_seconds":      ev.ExitSeconds,
		"exit_recorded":     ev.ExitRecorded,
		"exit_status":       ev.ExitStatus,
		"port_released":     ev.PortReleased,
		"inflight_resolved": ev.InFlightResolved,
		"unresolved_txs":    strList(ev.UnresolvedTxIDs),
		"close_codes":       strList(ev.CloseCodes),
		"pid_before":        ev.PIDBefore,
		"pid_after":         ev.PIDAfter,
		"max_outstanding":   ev.MaxOutstanding,
		"oracle_values":     strList(ev.OracleValues),
		"subscriber_values": ev.SubValues,
		"subscribers":       ev.SubSnapshots,
		"universe_values":   strList(ev.Universe),
		"timings":           ev.Timings,
		"ledger":            ev.Ledger,
	}
	path := filepath.Join(d.eventsDir, "recovery-"+ev.ID+".json")
	if err := writeJSONFile(path, doc); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	rel, err := filepath.Rel(d.evidenceRoot, path)
	if err != nil {
		return fmt.Errorf("rel: %w", err)
	}
	ref, err := HashFileArtifact(d.evidenceRoot, filepath.ToSlash(rel))
	if err != nil {
		return fmt.Errorf("hash: %w", err)
	}
	d.artifacts = append(d.artifacts, ref)
	return nil
}

// dbAlive reports whether the given DSN accepts a connection right now.
func (d *recoveryDriver) dbAlive(dbURL string) bool {
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		return false
	}
	defer func() { _ = db.Close() }()
	db.SetConnMaxLifetime(5 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return db.PingContext(ctx) == nil
}
