// Command benchsmoke runs diagnostic, non-claimable live attempts through the
// production target executor. It exists to prove protocol/provisioning seams
// before committing hours to the balanced seven-block benchmark.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/instant-v2/instant-v2/internal/benchrun"
)

type smokeResult struct {
	TargetID           string                `json:"target_id"`
	TargetKind         string                `json:"target_kind"`
	TargetRevision     string                `json:"target_revision"`
	Passed             bool                  `json:"passed"`
	PrimaryClass       benchrun.FailureClass `json:"primary_class"`
	QualificationOK    bool                  `json:"qualification_passed"`
	ExpectedLedgerRows int                   `json:"expected_ledger_rows"`
	LedgerRows         int                   `json:"ledger_rows"`
	ProtocolErrors     int                   `json:"protocol_errors"`
	BehaviorErrors     int                   `json:"behavior_errors"`
	MeasuredWindowOK   bool                  `json:"measured_window_valid"`
	MeasuredMillis     int64                 `json:"measured_millis"`
	Failure            string                `json:"failure,omitempty"`
}

type smokeReport struct {
	SchemaVersion  string        `json:"schema_version"`
	DiagnosticOnly bool          `json:"diagnostic_only"`
	Family         string        `json:"family"`
	Scale          int           `json:"scale"`
	Seed           int64         `json:"seed"`
	StartedAt      time.Time     `json:"started_at"`
	FinishedAt     time.Time     `json:"finished_at"`
	Passed         bool          `json:"passed"`
	Results        []smokeResult `json:"results"`
}

func main() {
	configPath := flag.String("config", "", "authorized live configuration JSON")
	targetSet := flag.String("targets", "v1", "target selection: v1 or all")
	output := flag.String("output", "-", "diagnostic JSON path, or - for stdout")
	measureSeconds := flag.Int("measure-seconds", 1, "diagnostic measured window, 1-10 seconds")
	perTargetTimeout := flag.Duration("per-target-timeout", 45*time.Minute, "hard timeout per selected target")
	validateNamespace := flag.Bool("validate-namespace", false, "prove the current Linux namespace is the signed loopback-only benchmark namespace")
	validateProgressPath := flag.String("validate-progress", "", "validate an in-progress H300 triad bundle and emit progress JSON")
	allowPartialProgress := flag.Bool("allow-partial-progress", false, "treat malformed live artifacts as pending evidence while the bundle is running")
	redactStream := flag.Bool("redact-stream", false, "redact and cap stdin into -output")
	snapshotSource := flag.String("snapshot-source", "", "copy one source file through a stable descriptor into -output")
	snapshotMode := flag.String("snapshot-mode", "0600", "octal mode for -snapshot-source output")
	lockOutput := flag.String("lock-output", "", "acquire a Linux output lock safely, then exec arguments after --")
	flag.Parse()
	if *redactStream {
		if *output == "" || *output == "-" {
			fail("-redact-stream requires a regular -output path")
		}
		if err := redactStreamTo(*output); err != nil {
			fail(benchrun.Redact(err.Error()))
		}
		return
	}
	if *snapshotSource != "" {
		if *output == "" || *output == "-" {
			fail("-snapshot-source requires a regular -output path")
		}
		modeValue, err := strconv.ParseUint(*snapshotMode, 8, 32)
		if err != nil {
			fail("-snapshot-mode must be an octal file mode")
		}
		digest, err := snapshotStable(*snapshotSource, *output, os.FileMode(modeValue))
		if err != nil {
			fail(benchrun.Redact(err.Error()))
		}
		fmt.Fprintln(os.Stdout, digest)
		return
	}
	if *lockOutput != "" {
		if runtime.GOOS != "linux" {
			fail("-lock-output requires Linux dirfd/O_NOFOLLOW support")
		}
		if err := runLockedCommand(*lockOutput, flag.Args()); err != nil {
			fail(benchrun.Redact(err.Error()))
		}
		return
	}

	if *configPath == "" {
		if *validateProgressPath != "" {
			fail("-config is required with -validate-progress")
		}
		if *validateNamespace {
			fail("-config is required with -validate-namespace")
		}
		fail("-config is required")
	}
	if *validateNamespace {
		if err := validateDedicatedNamespace(*configPath); err != nil {
			fail(benchrun.Redact(err.Error()))
		}
		return
	}
	if *validateProgressPath != "" {
		if runtime.GOOS != "linux" {
			fail("-validate-progress requires Linux descriptor-safe artifact reads")
		}
		progress, err := validateProgress(*validateProgressPath, *configPath, *allowPartialProgress)
		if err != nil {
			progress.Error = benchrun.Redact(err.Error())
		}
		progress.Warning = benchrun.Redact(progress.Warning)
		if encodeErr := json.NewEncoder(os.Stdout).Encode(progress); encodeErr != nil {
			fail(benchrun.Redact(encodeErr.Error()))
		}
		if err != nil {
			os.Exit(1)
		}
		return
	}
	if *measureSeconds < 1 || *measureSeconds > 10 {
		fail("-measure-seconds must be between 1 and 10")
	}
	if *perTargetTimeout <= 0 || *perTargetTimeout > 2*time.Hour {
		fail("-per-target-timeout must be positive and at most 2h")
	}
	cfg, err := benchrun.LoadLiveConfig(*configPath)
	if err != nil {
		fail(benchrun.Redact(err.Error()))
	}
	if cfg.Family != "H-append" || cfg.Scale != 300 {
		fail("diagnostic smoke requires the canonical H-append/300 contract")
	}
	selected, err := selectSmokeTargets(cfg, *targetSet)
	if err != nil {
		fail(err.Error())
	}
	executor, err := cfg.Executor()
	if err != nil {
		fail(benchrun.Redact(err.Error()))
	}

	started := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(len(selected))*(*perTargetTimeout))
	defer cancel()
	results, runErr := runSmoke(ctx, cfg, executor, selected, smokePlan(cfg, *measureSeconds))
	report := smokeReport{
		SchemaVersion: benchrun.SchemaVersion, DiagnosticOnly: true,
		Family: cfg.Family, Scale: cfg.Scale, Seed: cfg.Seed,
		StartedAt: started, FinishedAt: time.Now().UTC(),
		Passed: runErr == nil && len(results) == len(selected), Results: results,
	}
	if err := writeSmokeReport(*output, report); err != nil {
		fail(benchrun.Redact(err.Error()))
	}
	if runErr != nil {
		fail(benchrun.Redact(runErr.Error()))
	}
}

func smokePlan(cfg benchrun.LiveConfig, measureSeconds int) benchrun.Plan {
	return benchrun.Plan{
		SchemaVersion: benchrun.SchemaVersion,
		Seed:          cfg.Seed, Families: []string{cfg.Family}, Scales: []int{cfg.Scale}, Pairs: 1,
		RampSeconds: 1, SettleSeconds: 1, WarmupSeconds: 1,
		MeasureSeconds: measureSeconds, GraceSeconds: 5,
	}
}

func selectSmokeTargets(cfg benchrun.LiveConfig, selection string) ([]benchrun.LiveTargetConfig, error) {
	selection = strings.TrimSpace(selection)
	if selection != "v1" && selection != "all" {
		return nil, errors.New("-targets must be v1 or all")
	}
	selected := make([]benchrun.LiveTargetConfig, 0, len(cfg.Targets))
	for _, target := range cfg.Targets {
		if selection == "all" || target.ID == "v1" {
			selected = append(selected, target)
		}
	}
	if selection == "v1" && (len(selected) != 1 || selected[0].ID != "v1") {
		return nil, errors.New("authorized config does not contain exactly one v1 target")
	}
	if selection == "all" && len(selected) != 3 {
		return nil, errors.New("all-target smoke requires v1, v2_reference, and v2_current")
	}
	if selection == "all" {
		required := map[string]bool{"v1": false, "v2_reference": false, "v2_current": false}
		for _, target := range selected {
			if _, exists := required[target.ID]; !exists || required[target.ID] {
				return nil, errors.New("all-target smoke requires exactly one each of v1, v2_reference, and v2_current")
			}
			required[target.ID] = true
		}
		for _, found := range required {
			if !found {
				return nil, errors.New("all-target smoke requires exactly one each of v1, v2_reference, and v2_current")
			}
		}
	}
	return selected, nil
}

func runSmoke(ctx context.Context, cfg benchrun.LiveConfig, executor benchrun.TargetExecutor, selected []benchrun.LiveTargetConfig, plan benchrun.Plan) ([]smokeResult, error) {
	results := make([]smokeResult, 0, len(selected))
	for index, spec := range selected {
		target := benchrun.Target{
			SchemaVersion: benchrun.SchemaVersion,
			ID:            spec.ID, Role: spec.Role, Kind: spec.Kind, Revision: spec.Revision,
			Protocol: spec.Transport, DatabaseName: spec.DatabaseName,
		}
		result := smokeResult{TargetID: target.ID, TargetKind: target.Kind, TargetRevision: target.Revision}
		qualification, err := executor.Qualify(ctx, target)
		result.QualificationOK = err == nil && qualification.Passed && qualificationChecksPass(qualification.Checks)
		if !result.QualificationOK {
			result.PrimaryClass = benchrun.SetupInvalid
			result.Failure = benchrun.Redact(qualification.Failure)
			if result.Failure == "" && err != nil {
				result.Failure = benchrun.Redact(err.Error())
			}
			results = append(results, result)
			return results, fmt.Errorf("target %s smoke qualification failed: %s", target.ID, result.Failure)
		}

		runID := fmt.Sprintf("%s-smoke-%s", cfg.PairID, target.ID)
		run := benchrun.Run{
			SchemaVersion: benchrun.SchemaVersion, ID: runID, PairID: cfg.PairID + "-smoke",
			TargetID: target.ID, TargetRevision: target.Revision,
			Family: cfg.Family, Scale: cfg.Scale, Seed: cfg.Seed,
			StartedAt: time.Now().UTC(), PrimaryClass: benchrun.Pass,
		}
		execution, err := executor.Execute(ctx, benchrun.RunSpec{
			Run: run, Target: target, Plan: plan, Attempt: index + 1, Order: target.ID,
		})
		result.PrimaryClass = execution.Run.PrimaryClass
		result.ExpectedLedgerRows = execution.Run.ExpectedLedgerRows
		result.LedgerRows = len(execution.Ledger)
		result.ProtocolErrors = len(execution.Run.ProtocolErrors)
		result.BehaviorErrors = len(execution.Run.BehaviorErrors)
		result.MeasuredWindowOK = !execution.Run.MeasuredStartedAt.IsZero() &&
			!execution.Run.MeasuredFinishedAt.IsZero() &&
			!execution.Run.MeasuredFinishedAt.Before(execution.Run.MeasuredStartedAt)
		if result.MeasuredWindowOK {
			result.MeasuredMillis = execution.Run.MeasuredFinishedAt.Sub(execution.Run.MeasuredStartedAt).Milliseconds()
		}
		if err != nil {
			result.Failure = benchrun.Redact(err.Error())
		} else {
			result.Failure = benchrun.Redact(execution.Run.Failure)
		}
		if err == nil {
			if validationErr := validateSmokeExecution(run, target, execution, plan); validationErr != nil {
				result.Failure = benchrun.Redact(validationErr.Error())
			} else {
				result.Passed = true
			}
		}
		results = append(results, result)
		if !result.Passed {
			return results, fmt.Errorf("target %s diagnostic smoke failed: class=%s ledger=%d/%d protocol=%d behavior=%d failure=%s",
				target.ID, result.PrimaryClass, result.LedgerRows, result.ExpectedLedgerRows,
				result.ProtocolErrors, result.BehaviorErrors, result.Failure)
		}
	}
	return results, nil
}

func qualificationChecksPass(checks map[string]bool) bool {
	if len(checks) == 0 {
		return false
	}
	for _, passed := range checks {
		if !passed {
			return false
		}
	}
	return true
}

func validateSmokeExecution(expected benchrun.Run, target benchrun.Target, execution benchrun.ExecutionResult, plan benchrun.Plan) error {
	run := execution.Run
	if run.SchemaVersion != benchrun.SchemaVersion || run.ID != expected.ID || run.PairID != expected.PairID {
		return errors.New("smoke run identity is incomplete")
	}
	if run.TargetID != target.ID || run.TargetRevision != target.Revision || run.Family != expected.Family || run.Scale != expected.Scale || run.Seed != expected.Seed {
		return errors.New("smoke run target/config identity mismatch")
	}
	if !benchrun.ValidateFailureClass(run.PrimaryClass) || run.PrimaryClass != benchrun.Pass {
		return fmt.Errorf("smoke run has non-passing execution class %q", run.PrimaryClass)
	}
	if len(run.ProtocolErrors) != 0 || len(run.BehaviorErrors) != 0 || run.Failure != "" {
		return errors.New("smoke run contains protocol, behavior, or failure evidence")
	}
	if run.StartedAt.IsZero() || run.EndedAt.IsZero() || run.MeasuredStartedAt.IsZero() || run.MeasuredFinishedAt.IsZero() ||
		!run.EndedAt.After(run.StartedAt) || !run.MeasuredFinishedAt.After(run.MeasuredStartedAt) ||
		run.MeasuredStartedAt.Before(run.StartedAt) || run.MeasuredFinishedAt.After(run.EndedAt) {
		return errors.New("smoke run timestamps are not strictly positive and ordered")
	}
	if plan.MeasureSeconds <= 0 || run.MeasuredFinishedAt.Sub(run.MeasuredStartedAt) <= 0 {
		return errors.New("smoke measured window is invalid")
	}
	if run.ExpectedLedgerRows <= 0 || run.ExpectedMutationRecipients != run.ExpectedLedgerRows || len(execution.Ledger) != run.ExpectedLedgerRows {
		return fmt.Errorf("smoke ledger cardinality is not exact: got %d expected %d recipients %d", len(execution.Ledger), run.ExpectedLedgerRows, run.ExpectedMutationRecipients)
	}

	rowKeys := make(map[string]bool, len(execution.Ledger))
	recipients := make(map[string]bool)
	for _, row := range execution.Ledger {
		if row.SchemaVersion != benchrun.SchemaVersion || row.RunID != run.ID || row.PairID != run.PairID || row.WriterID == "" || row.RecipientID == "" || row.ClientEventID == "" {
			return errors.New("smoke ledger row identity is incomplete")
		}
		if len(row.ExpectedQuerySet) == 0 || len(row.ExpectedRecipientSet) == 0 || !uniqueNonEmpty(row.ExpectedQuerySet) || !uniqueNonEmpty(row.ExpectedRecipientSet) || !contains(row.ExpectedRecipientSet, row.RecipientID) {
			return errors.New("smoke ledger row expected identity is incomplete")
		}
		key := row.ClientEventID + "\x00" + row.RecipientID
		if rowKeys[key] {
			return errors.New("smoke ledger contains duplicate event/recipient evidence")
		}
		rowKeys[key], recipients[row.RecipientID] = true, true
		if row.SubmittedAt.IsZero() || row.AcknowledgementAt.IsZero() || row.ConvergedAt.IsZero() ||
			(row.Coverage != "converged_without_intermediate" && row.CoverAt.IsZero()) ||
			row.SubmittedAt.Before(run.StartedAt) || row.ConvergedAt.After(run.EndedAt) ||
			row.AcknowledgementAt.Before(row.SubmittedAt) || row.CoverAt.Before(row.AcknowledgementAt) || row.ConvergedAt.Before(row.CoverAt) {
			return errors.New("smoke ledger timestamps are not strictly positive and ordered")
		}
		if !benchrun.ValidateCoverage(row.Coverage) || row.Coverage == "none" || row.ExpectedMaterializedDigest == "" || row.ObservedMaterializedDigest == "" {
			return errors.New("smoke ledger semantic evidence is incomplete")
		}
		if row.Coverage == "coalesced" {
			if row.CoveredExpectedMaterializedDigest == "" || row.CoveredExpectedMaterializedDigest != row.ObservedMaterializedDigest || row.CoveredExpectedStateVersion <= row.ExpectedStateVersion || row.ObservedStateVersion < row.CoveredExpectedStateVersion {
				return errors.New("smoke ledger coalesced evidence is invalid")
			}
		} else if row.ExpectedMaterializedDigest != row.ObservedMaterializedDigest {
			return errors.New("smoke ledger semantic digest evidence mismatches")
		}
	}
	if len(recipients) == 0 {
		return errors.New("smoke ledger has no recipient evidence")
	}
	if len(execution.Frames) == 0 {
		return errors.New("smoke frame evidence is empty")
	}
	for _, frame := range execution.Frames {
		if frame.SchemaVersion != benchrun.SchemaVersion || frame.RunID != run.ID || frame.RecipientID == "" || !recipients[frame.RecipientID] || frame.ReceivedAt.IsZero() || frame.ReceivedAt.Before(run.StartedAt) || frame.ReceivedAt.After(run.EndedAt) {
			return errors.New("smoke frame identity or timestamp evidence is invalid")
		}
		if err := benchrun.ValidateMeasurement(frame.PayloadBytes); err != nil {
			return fmt.Errorf("smoke frame payload evidence is invalid: %w", err)
		}
	}
	return nil
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func uniqueNonEmpty(values []string) bool {
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		if value == "" || seen[value] {
			return false
		}
		seen[value] = true
	}
	return true
}

func redactStreamTo(path string) error {
	if path == "" || path == "-" {
		return errors.New("redacted stream output path is required")
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return errors.New("redacted stream output is not a regular file")
		}
		return errors.New("redacted stream output already exists")
	} else if !os.IsNotExist(err) {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	const maxBytes = 1 << 20
	reader := bufio.NewReaderSize(os.Stdin, 32<<10)
	written := 0
	truncated := false
	for {
		chunk, readErr := reader.ReadSlice('\n')
		if len(chunk) > 0 {
			if written < maxBytes {
				redacted := []byte(benchrun.Redact(string(chunk)))
				remaining := maxBytes - written
				if len(redacted) > remaining {
					redacted = redacted[:remaining]
					truncated = true
				}
				if n, writeErr := f.Write(redacted); writeErr != nil {
					return writeErr
				} else {
					written += n
				}
				if len(redacted) < len(chunk) {
					truncated = true
				}
			} else {
				truncated = true
			}
		}
		if readErr == io.EOF {
			break
		}
		if errors.Is(readErr, bufio.ErrBufferFull) {
			continue
		}
		if readErr != nil {
			return readErr
		}
	}
	if truncated {
		marker := []byte("\n[REDACTED OUTPUT TRUNCATED]\n")
		if len(marker) > maxBytes {
			marker = marker[:maxBytes]
		}
		// Keep the file bounded even when the final retained line filled the cap.
		if written+len(marker) > maxBytes {
			return nil
		}
		_, err = f.Write(marker)
	}
	return err
}

type namespaceConfig struct {
	Targets []struct {
		ID               string `json:"id"`
		NetworkNamespace string `json:"network_namespace_id"`
		InitialNamespace string `json:"initial_network_namespace_id"`
	} `json:"targets"`
}

func validateDedicatedNamespace(configPath string) error {
	if runtime.GOOS != "linux" {
		return errors.New("dedicated benchmark namespace validation requires Linux procfs")
	}
	if err := noSymlinkComponents(configPath); err != nil {
		return err
	}
	b, err := readBounded(configPath, 8<<20)
	if err != nil {
		return err
	}
	var cfg namespaceConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		return fmt.Errorf("namespace config is malformed: %w", err)
	}
	wantIDs := []string{"v1", "v2_current", "v2_reference"}
	if len(cfg.Targets) != len(wantIDs) {
		return errors.New("namespace config must contain exactly the Wave 6 triad")
	}
	byID := make(map[string]struct{}, len(cfg.Targets))
	for _, target := range cfg.Targets {
		if _, exists := byID[target.ID]; exists {
			return fmt.Errorf("namespace config duplicates target %q", target.ID)
		}
		byID[target.ID] = struct{}{}
	}
	for _, id := range wantIDs {
		if _, exists := byID[id]; !exists {
			return fmt.Errorf("namespace config is missing target %q", id)
		}
	}
	collectorID, err := readNamespaceID("/proc/self/ns/net")
	if err != nil {
		return fmt.Errorf("collector namespace evidence unavailable: %w", err)
	}
	initial := ""
	for _, target := range cfg.Targets {
		targetID, err := normalizeNamespaceID(target.NetworkNamespace)
		if err != nil {
			return fmt.Errorf("target %s namespace: %w", target.ID, err)
		}
		initialID, err := normalizeNamespaceID(target.InitialNamespace)
		if err != nil {
			return fmt.Errorf("target %s initial namespace: %w", target.ID, err)
		}
		if targetID != collectorID {
			return fmt.Errorf("target %s namespace %s does not match collector %s", target.ID, targetID, collectorID)
		}
		if targetID == initialID {
			return fmt.Errorf("target %s namespace is the initial/host namespace", target.ID)
		}
		if initial == "" {
			initial = initialID
		} else if initial != initialID {
			return errors.New("triad target initial namespace identities differ")
		}
	}
	if err := validateLoopbackOnlyProcNetwork("/proc/self/net"); err != nil {
		return err
	}
	return nil
}

func readBounded(path string, max int64) ([]byte, error) {
	return readStablePath(path, max)
}

func normalizeNamespaceID(value string) (string, error) {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, "net:[") || !strings.HasSuffix(value, "]") {
		return "", errors.New("expected net:[inode] identity")
	}
	digits := strings.TrimSuffix(strings.TrimPrefix(value, "net:["), "]")
	if digits == "" {
		return "", errors.New("namespace inode is empty")
	}
	if _, err := strconv.ParseUint(digits, 10, 64); err != nil {
		return "", errors.New("namespace inode is invalid")
	}
	return "net:[" + digits + "]", nil
}

func readNamespaceID(path string) (string, error) {
	value, err := os.Readlink(path)
	if err != nil {
		return "", err
	}
	return normalizeNamespaceID(value)
}

func validateLoopbackOnlyProcNetwork(netRoot string) error {
	dev, err := os.ReadFile(filepath.Join(netRoot, "dev"))
	if err != nil {
		return fmt.Errorf("network interface evidence unavailable: %w", err)
	}
	if err := validateProcDev(string(dev)); err != nil {
		return err
	}
	route4, err := os.ReadFile(filepath.Join(netRoot, "route"))
	if err != nil {
		return fmt.Errorf("IPv4 route evidence unavailable: %w", err)
	}
	if err := validateProcRoute(string(route4)); err != nil {
		return err
	}
	route6, err := os.ReadFile(filepath.Join(netRoot, "ipv6_route"))
	if err != nil {
		return fmt.Errorf("IPv6 route evidence unavailable: %w", err)
	}
	if err := validateProcIPv6Route(string(route6)); err != nil {
		return err
	}
	return nil
}

func validateProcDev(value string) error {
	seenLoopback, headers := false, 0
	for _, line := range strings.Split(value, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if procDevHeader(line) {
			headers++
			continue
		}
		colon := strings.IndexByte(line, ':')
		if colon <= 0 {
			return errors.New("malformed network interface evidence")
		}
		name := strings.TrimSpace(line[:colon])
		if name == "" || strings.ContainsAny(name, " \t") {
			return errors.New("malformed network interface name")
		}
		if len(strings.Fields(line[colon+1:])) < 8 {
			return errors.New("malformed network interface counters")
		}
		if name != "lo" {
			return fmt.Errorf("external network interface %q is present", name)
		}
		if seenLoopback {
			return errors.New("duplicate loopback network interface evidence")
		}
		seenLoopback = true
	}
	if headers < 2 || !seenLoopback {
		return errors.New("loopback-only network interface evidence is incomplete")
	}
	return nil
}

func procDevHeader(line string) bool {
	fields := strings.Fields(line)
	if len(fields) > 0 && fields[0] == "Inter-|" {
		return strings.Contains(line, "Receive") && strings.Contains(line, "Transmit")
	}
	if len(fields) < 2 || fields[0] != "face" || !strings.HasPrefix(fields[1], "|bytes") {
		return false
	}
	for _, label := range []string{"bytes", "packets", "errs", "drop", "fifo", "frame", "compressed", "multicast", "colls", "carrier"} {
		if !strings.Contains(line, label) {
			return false
		}
	}
	return true
}

func procRouteHeader(line string) bool {
	want := []string{"Iface", "Destination", "Gateway", "Flags", "RefCnt", "Use", "Metric", "Mask", "MTU", "Window", "IRTT"}
	fields := strings.Fields(line)
	if len(fields) < len(want) {
		return false
	}
	for i, value := range want {
		if fields[i] != value {
			return false
		}
	}
	return true
}

func validateProcRoute(value string) error {
	header := false
	for _, line := range strings.Split(value, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if procRouteHeader(line) {
			header = true
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 11 || fields[0] != "lo" {
			return errors.New("external or malformed IPv4 route evidence")
		}
		for _, index := range []int{1, 2, 7} {
			if len(fields[index]) != 8 {
				return errors.New("malformed IPv4 route address field")
			}
			if _, err := strconv.ParseUint(fields[index], 16, 32); err != nil {
				return errors.New("malformed IPv4 route address field")
			}
		}
		if len(fields[3]) != 4 {
			return errors.New("malformed IPv4 route flags field")
		}
		if _, err := strconv.ParseUint(fields[3], 16, 16); err != nil {
			return errors.New("malformed IPv4 route flags field")
		}
		for _, index := range []int{4, 5, 6, 8, 9, 10} {
			if _, err := strconv.ParseUint(fields[index], 10, 64); err != nil {
				return errors.New("malformed IPv4 route counter")
			}
		}
		if fields[2] != "00000000" {
			return errors.New("loopback IPv4 route has a gateway")
		}
		// The dedicated namespace has only the kernel's canonical local route:
		// an all-zero destination/gateway with the loopback mask.  Merely
		// requiring the interface name would allow an external destination to
		// hide behind lo and would not prove the namespace is isolated.
		if fields[3] != "0001" ||
			!((fields[1] == "00000000" && fields[7] == "000000FF") ||
				(fields[1] == "0000007F" && fields[7] == "000000FF")) {
			return errors.New("non-canonical IPv4 loopback route evidence")
		}
	}
	if !header {
		return errors.New("malformed IPv4 route header")
	}
	return nil
}

func validateProcIPv6Route(value string) error {
	for _, line := range strings.Split(value, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 10 {
			return errors.New("malformed IPv6 route evidence")
		}
		for _, index := range []int{0, 2, 4} {
			if len(fields[index]) != 32 {
				return errors.New("malformed IPv6 route address")
			}
			for _, r := range fields[index] {
				if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
					return errors.New("malformed IPv6 route address")
				}
			}
		}
		for _, index := range []int{1, 3} {
			if len(fields[index]) != 2 {
				return errors.New("malformed IPv6 route prefix")
			}
			if _, err := strconv.ParseUint(fields[index], 16, 8); err != nil {
				return errors.New("malformed IPv6 route prefix")
			}
		}
		for _, index := range []int{5, 6, 7, 8} {
			if len(fields[index]) != 8 {
				return errors.New("malformed IPv6 route counter")
			}
			if _, err := strconv.ParseUint(fields[index], 16, 32); err != nil {
				return errors.New("malformed IPv6 route counter")
			}
		}
		if fields[9] != "lo" {
			return fmt.Errorf("external IPv6 route uses %q", fields[9])
		}
		zero := strings.Repeat("0", 32)
		loopback := strings.Repeat("0", 31) + "1"
		defaultRoute := fields[0] == zero && fields[1] == "00"
		localRoute := fields[0] == loopback && fields[1] == "80"
		// Linux's proc ABI reports distinct flags for the null/default and
		// local ::1 routes. Match the complete known values; accepting
		// arbitrary bits or crossing route types would weaken the namespace
		// proof.
		canonicalFlags := (defaultRoute && fields[8] == "00200200") ||
			(localRoute && fields[8] == "80200001")
		if (!defaultRoute && !localRoute) ||
			fields[2] != zero || fields[3] != "00" ||
			fields[4] != zero ||
			!canonicalFlags {
			return errors.New("non-canonical IPv6 loopback route evidence")
		}
	}
	return nil
}

type progressResult struct {
	Valid             bool   `json:"valid"`
	PendingEvidence   bool   `json:"pending_evidence"`
	Transient         bool   `json:"transient,omitempty"`
	CompletedAttempts int    `json:"completed_attempts"`
	FailedAttempts    int    `json:"failed_attempts"`
	TotalAttempts     int    `json:"total_attempts"`
	Error             string `json:"error,omitempty"`
	Warning           string `json:"warning,omitempty"`
}

type progressConfig struct {
	PairID          string `json:"pair_id"`
	Seed            int64  `json:"seed"`
	Family          string `json:"family"`
	Scale           int    `json:"scale"`
	RampSeconds     int    `json:"ramp_seconds"`
	SettleSeconds   int    `json:"settle_seconds"`
	WarmupSeconds   int    `json:"warmup_seconds"`
	WarmupMutations int    `json:"warmup_mutations"`
	MeasureSeconds  int    `json:"measure_seconds"`
	GraceSeconds    int    `json:"grace_seconds"`
	Targets         []struct {
		ID       string `json:"id"`
		Kind     string `json:"kind"`
		Role     string `json:"role"`
		Revision string `json:"revision"`
	} `json:"targets"`
}

func validateProgress(root, configPath string, allowPartial ...bool) (progressResult, error) {
	result := progressResult{TotalAttempts: 21}
	partial := len(allowPartial) > 0 && allowPartial[0]
	if root == "" || !filepath.IsAbs(root) {
		return result, errors.New("progress bundle root must be absolute")
	}
	if err := noSymlinkComponents(root); err != nil {
		return result, err
	}
	if info, err := os.Lstat(root); err != nil {
		if os.IsNotExist(err) {
			result.PendingEvidence = true
			return result, nil
		}
		return result, err
	} else if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return result, errors.New("progress bundle root is not a directory")
	}
	artifacts, err := openArtifactRoot(root)
	if err != nil {
		if partial && os.IsNotExist(err) {
			return progressPending(result, err)
		}
		return result, err
	}
	defer artifacts.Close()
	configBytes, err := readBounded(configPath, 8<<20)
	if err != nil {
		return result, err
	}
	var cfg progressConfig
	if err := json.Unmarshal(configBytes, &cfg); err != nil {
		return result, fmt.Errorf("progress config is malformed: %w", err)
	}
	if cfg.PairID == "" || cfg.Seed <= 0 || cfg.Family != "H-append" || cfg.Scale != 300 || cfg.RampSeconds != 30 || cfg.SettleSeconds != 30 || cfg.WarmupSeconds != 60 || cfg.WarmupMutations != 0 || cfg.MeasureSeconds != 180 || cfg.GraceSeconds != 30 {
		return result, errors.New("progress config is not the canonical H300 contract")
	}
	if len(cfg.Targets) != 3 {
		return result, errors.New("progress config must contain the canonical triad")
	}
	wantTargets := map[string]struct{}{"v1": {}, "v2_reference": {}, "v2_current": {}}
	configTargets := make(map[string]progressConfigTarget, len(cfg.Targets))
	for _, target := range cfg.Targets {
		if _, ok := wantTargets[target.ID]; !ok || target.Role != target.ID || target.Revision == "" {
			return result, errors.New("progress config target identity is not canonical")
		}
		if target.ID == "v1" && target.Kind != "v1" || target.ID != "v1" && target.Kind != "v2" {
			return result, errors.New("progress config target kind is not canonical")
		}
		if _, exists := configTargets[target.ID]; exists {
			return result, errors.New("progress config contains duplicate target identity")
		}
		configTargets[target.ID] = progressConfigTarget{ID: target.ID, Kind: target.Kind, Role: target.Role, Revision: target.Revision}
	}
	if len(configTargets) != len(wantTargets) {
		return result, errors.New("progress config is missing a target")
	}

	var manifest benchrun.Manifest
	if err := readJSONArtifact(artifacts, "manifest.json", &manifest); err != nil {
		if os.IsNotExist(err) {
			result.PendingEvidence = true
			return result, nil
		}
		if partial && isTransientProgressError(err) {
			return progressPending(result, err)
		}
		return result, err
	}
	if manifest.SchemaVersion != benchrun.SchemaVersion || manifest.PairID != cfg.PairID || manifest.Family != cfg.Family || manifest.SubscriberScale != cfg.Scale || manifest.Seed != cfg.Seed {
		return result, errors.New("manifest identity does not match canonical config")
	}
	order, err := benchrun.ThreeTargetSchedule(cfg.Seed, cfg.PairID)
	if err != nil {
		return result, err
	}
	if !reflect.DeepEqual(manifest.TargetOrder, order.Blocks) || !reflect.DeepEqual(manifest.RunOrder, order.Order) {
		return result, errors.New("manifest schedule does not match canonical seeded order")
	}
	if err := benchrun.ValidateManifest(manifest); err != nil {
		return result, fmt.Errorf("manifest validation: %w", err)
	}
	if len(manifest.TargetRevisions) != len(configTargets) {
		return result, errors.New("manifest target revisions are incomplete")
	}
	for id, target := range configTargets {
		if manifest.TargetRevisions[id] != target.Revision {
			return result, fmt.Errorf("manifest revision mismatch for %s", id)
		}
	}
	var plan benchrun.Plan
	if err := readJSONArtifact(artifacts, "plan.json", &plan); err != nil {
		if os.IsNotExist(err) {
			result.PendingEvidence = true
			return result, nil
		}
		if partial && isTransientProgressError(err) {
			return progressPending(result, err)
		}
		return result, err
	}
	if plan.SchemaVersion != benchrun.SchemaVersion || plan.Seed != cfg.Seed || plan.Pairs != 7 || plan.RampSeconds != 30 || plan.SettleSeconds != 30 || plan.WarmupSeconds != 60 || plan.WarmupMutations != 0 || plan.MeasureSeconds != 180 || plan.GraceSeconds != 30 || len(plan.Families) != 1 || plan.Families[0] != "H-append" || len(plan.Scales) != 1 || plan.Scales[0] != 300 {
		return result, errors.New("benchmark plan does not match canonical H300 contract")
	}
	if err := validateProgressTargets(root, manifest, configTargets, artifacts); err != nil {
		if partial && (os.IsNotExist(err) || isTransientProgressError(err)) {
			return progressPending(result, err)
		}
		return result, err
	}

	expected := make(map[string]struct{}, 21)
	for _, block := range order.Blocks {
		for _, id := range block.Order {
			expected[fmt.Sprintf("%s-%02d-%s", cfg.PairID, block.Index, id)] = struct{}{}
		}
	}
	runsRoot := filepath.Join(root, "runs")
	if info, err := os.Lstat(runsRoot); err != nil {
		if os.IsNotExist(err) {
			result.PendingEvidence = true
			return result, nil
		}
		return result, err
	} else if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return result, errors.New("runs artifact root is not a directory")
	}
	seen := make(map[string]struct{}, len(expected))
	walkErr := filepath.WalkDir(runsRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink in benchmark artifacts: %s", filepath.Base(path))
		}
		rel, relErr := filepath.Rel(runsRoot, path)
		if relErr != nil {
			return relErr
		}
		if entry.IsDir() {
			if rel != "." {
				first := strings.Split(rel, string(filepath.Separator))[0]
				if _, ok := expected[first]; !ok {
					return fmt.Errorf("unexpected benchmark run directory %q", first)
				}
			}
			return nil
		}
		if filepath.Base(path) != "run.json" {
			return nil
		}
		id := filepath.Base(filepath.Dir(path))
		if _, ok := expected[id]; !ok {
			return fmt.Errorf("unexpected canonical run artifact %q", id)
		}
		if _, exists := seen[id]; exists {
			return fmt.Errorf("duplicate canonical run artifact %q", id)
		}
		seen[id] = struct{}{}
		rootRel, rootRelErr := filepath.Rel(root, path)
		if rootRelErr != nil || strings.HasPrefix(rootRel, "..") {
			return errors.New("benchmark artifact path escaped bundle root")
		}
		var run benchrun.Run
		if err := readJSONArtifact(artifacts, rootRel, &run); err != nil {
			if partial && (os.IsNotExist(err) || isTransientProgressError(err)) {
				return transientProgressError{err}
			}
			return err
		}
		if err := validateProgressRun(run, id, cfg, manifest, configTargets); err != nil {
			return err
		}
		dirRel := filepath.Dir(rootRel)
		if err := requireRunSiblingsAt(artifacts, dirRel); err != nil {
			result.PendingEvidence = true
			return nil
		}
		if err := validateProgressEvidence(filepath.Dir(path), run, artifacts); err != nil {
			if partial && (os.IsNotExist(err) || isTransientProgressError(err)) {
				return transientProgressError{err}
			}
			return err
		}
		result.CompletedAttempts++
		if run.PrimaryClass != benchrun.Pass {
			result.FailedAttempts++
		}
		return nil
	})
	if walkErr != nil {
		var transient transientProgressError
		if partial && (os.IsNotExist(walkErr) || errors.As(walkErr, &transient)) {
			if !errors.As(walkErr, &transient) {
				transient = transientProgressError{walkErr}
			}
			return progressPending(result, transient)
		}
		return result, walkErr
	}
	if len(seen) != len(expected) {
		result.PendingEvidence = true
	}
	if result.CompletedAttempts == len(expected) && len(seen) == len(expected) && !result.PendingEvidence {
		result.Valid = true
	}
	return result, nil
}

type transientProgressError struct{ err error }

func (e transientProgressError) Error() string { return e.err.Error() }
func (e transientProgressError) Unwrap() error { return e.err }

func progressPending(result progressResult, err error) (progressResult, error) {
	result.PendingEvidence = true
	result.Transient = true
	result.Warning = err.Error()
	return result, nil
}

func isTransientProgressError(err error) bool {
	if err == nil {
		return false
	}
	var transient transientProgressError
	if errors.As(err, &transient) {
		return true
	}
	message := err.Error()
	return strings.Contains(message, "malformed JSONL sibling") ||
		strings.Contains(message, "malformed JSON sibling") ||
		strings.Contains(message, "changed while being read")
}

type progressConfigTarget struct {
	ID, Kind, Role, Revision string
}

func validateProgressTargets(root string, manifest benchrun.Manifest, expected map[string]progressConfigTarget, readers ...*artifactRoot) error {
	var artifacts *artifactRoot
	if len(readers) > 0 {
		artifacts = readers[0]
	}
	for id, want := range expected {
		var target benchrun.Target
		var err error
		if artifacts != nil {
			err = readJSONArtifact(artifacts, filepath.Join("targets", id+".json"), &target)
		} else {
			err = readJSONRegular(filepath.Join(root, "targets", id+".json"), &target)
		}
		if err != nil {
			return err
		}
		if target.SchemaVersion != benchrun.SchemaVersion || target.ID != id || target.Role != id || target.Kind != want.Kind || target.Revision != want.Revision || manifest.TargetRevisions[id] != target.Revision || !target.Qualification.Passed || !qualificationChecksPass(target.Qualification.Checks) {
			return fmt.Errorf("target %s qualification/provenance sibling is invalid", id)
		}
	}
	return nil
}

func validateProgressRun(run benchrun.Run, id string, cfg progressConfig, manifest benchrun.Manifest, targets map[string]progressConfigTarget) error {
	if run.SchemaVersion != benchrun.SchemaVersion || run.ID != id || run.Family != cfg.Family || run.Scale != cfg.Scale || run.Seed != cfg.Seed || run.WarmupMutations != cfg.WarmupMutations || run.PairID == "" || run.TargetRevision == "" || run.StartedAt.IsZero() || run.EndedAt.IsZero() || !run.EndedAt.After(run.StartedAt) {
		return fmt.Errorf("run %s has invalid schema/config identity", id)
	}
	if !benchrun.ValidateFailureClass(run.PrimaryClass) {
		return fmt.Errorf("run %s has invalid execution class", id)
	}
	if run.PrimaryClass == benchrun.Pass && (len(run.ProtocolErrors) != 0 || len(run.BehaviorErrors) != 0) {
		return fmt.Errorf("run %s passing class carries protocol/behavior errors", id)
	}
	parts := strings.Split(run.ID, "-")
	if len(parts) < 3 {
		return fmt.Errorf("run %s has invalid canonical identity", id)
	}
	targetID := run.TargetID
	want, ok := targets[targetID]
	if !ok || parts[len(parts)-1] != targetID || run.TargetRevision != want.Revision || run.PairID != fmt.Sprintf("%s-%02s", cfg.PairID, parts[len(parts)-2]) {
		return fmt.Errorf("run %s target/revision identity is invalid", id)
	}
	block := 0
	if _, err := fmt.Sscanf(parts[len(parts)-2], "%d", &block); err != nil || block < 1 || block > 7 || run.ScheduleBlock != block {
		return fmt.Errorf("run %s schedule block is invalid", id)
	}
	if !containsString(manifest.TargetOrder[block-1].Order, targetID) {
		return fmt.Errorf("run %s target is absent from its canonical block", id)
	}
	return nil
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func readJSONRegular(path string, dst any) error {
	if err := noSymlinkComponents(path); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("artifact is not a regular file: %s", filepath.Base(path))
	}
	b, err := readBounded(path, 8<<20)
	if err != nil {
		return err
	}
	return unmarshalArtifact(filepath.Base(path), b, dst)
}

func readJSONArtifact(root *artifactRoot, relative string, dst any) error {
	b, err := root.read(relative, 8<<20)
	if err != nil {
		return err
	}
	return unmarshalArtifact(filepath.Base(relative), b, dst)
}

func unmarshalArtifact(name string, b []byte, dst any) error {
	if err := json.Unmarshal(b, dst); err != nil {
		var syntaxErr *json.SyntaxError
		if errors.As(err, &syntaxErr) || errors.Is(err, io.ErrUnexpectedEOF) {
			return transientProgressError{fmt.Errorf("malformed artifact %s: %w", name, err)}
		}
		return fmt.Errorf("artifact %s has invalid schema: %w", name, err)
	}
	return nil
}

func noSymlinkComponents(path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("artifact path must be absolute")
	}
	for _, part := range strings.Split(strings.TrimPrefix(path, string(filepath.Separator)), string(filepath.Separator)) {
		if part == "." || part == ".." {
			return errors.New("artifact path contains unsafe component")
		}
	}
	clean := filepath.Clean(path)
	if clean == string(filepath.Separator) {
		return errors.New("artifact path must not be filesystem root")
	}
	for current := string(filepath.Separator); ; {
		rel := strings.TrimPrefix(clean, string(filepath.Separator))
		if current != string(filepath.Separator) {
			rel = strings.TrimPrefix(clean, current+string(filepath.Separator))
		}
		part := rel
		if index := strings.IndexByte(rel, byte(filepath.Separator)); index >= 0 {
			part = rel[:index]
		}
		if part == "" || part == "." || part == ".." {
			return errors.New("artifact path contains unsafe component")
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err == nil && info.Mode()&os.ModeSymlink != 0 {
			return errors.New("artifact path crosses a symlink")
		}
		if current == clean {
			return nil
		}
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if err != nil {
			// Missing parents cannot conceal an existing symlink below them; the
			// final regular-file check still prevents a later link target.
			return nil
		}
	}
}

func requireRunSiblings(dir string) error {
	for _, name := range []string{"ledger.jsonl", "frames.jsonl", "process.jsonl", "runtime-metrics.jsonl", "db-before.json", "db-after.json", "stdout.log", "stderr.log"} {
		info, err := os.Lstat(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fmt.Errorf("run sibling %s is not a regular file", name)
		}
	}
	return nil
}

func requireRunSiblingsAt(root *artifactRoot, dir string) error {
	for _, name := range []string{"ledger.jsonl", "frames.jsonl", "process.jsonl", "runtime-metrics.jsonl", "db-before.json", "db-after.json", "stdout.log", "stderr.log"} {
		if err := root.regular(filepath.Join(dir, name)); err != nil {
			return fmt.Errorf("run sibling %s is not a regular file: %w", name, err)
		}
	}
	return nil
}

func validateProgressEvidence(dir string, run benchrun.Run, root ...*artifactRoot) error {
	readLines := validateJSONLines
	readBytes := readBounded
	if len(root) > 0 && root[0] != nil {
		relativeDir, err := filepath.Rel(root[0].path, dir)
		if err != nil || relativeDir == ".." || strings.HasPrefix(relativeDir, ".."+string(filepath.Separator)) {
			return errors.New("run artifact path escaped bundle root")
		}
		readLines = func(path string) error {
			relative := filepath.Join(relativeDir, filepath.Base(path))
			return validateJSONLinesAt(root[0], relative)
		}
		readBytes = func(path string, max int64) ([]byte, error) {
			return root[0].read(filepath.Join(relativeDir, filepath.Base(path)), max)
		}
	}
	for _, name := range []string{"ledger.jsonl", "frames.jsonl", "process.jsonl", "runtime-metrics.jsonl"} {
		if err := readLines(filepath.Join(dir, name)); err != nil {
			return err
		}
	}
	for _, name := range []string{"db-before.json", "db-after.json"} {
		b, err := readBytes(filepath.Join(dir, name), 8<<20)
		if err != nil {
			return err
		}
		if len(b) > 0 && !json.Valid(b) {
			return fmt.Errorf("malformed JSON sibling %s", name)
		}
	}
	if run.PrimaryClass != benchrun.Pass {
		return nil
	}
	if run.MeasuredStartedAt.IsZero() || run.MeasuredFinishedAt.IsZero() || !run.MeasuredFinishedAt.After(run.MeasuredStartedAt) || run.MeasuredStartedAt.Before(run.StartedAt) || run.MeasuredFinishedAt.After(run.EndedAt) {
		return errors.New("passing run measured timestamps are invalid")
	}
	for name, measurement := range run.Measurements {
		if err := benchrun.ValidateMeasurement(measurement); err != nil {
			return fmt.Errorf("invalid run measurement %s: %w", name, err)
		}
	}
	ledgerBytes, err := readBytes(filepath.Join(dir, "ledger.jsonl"), 8<<20)
	if err != nil {
		return err
	}
	rows := 0
	seen := make(map[string]bool)
	recipients := make(map[string]bool)
	scanner := bufio.NewScanner(bytes.NewReader(ledgerBytes))
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		var row benchrun.LedgerRow
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			return err
		}
		if row.SchemaVersion != benchrun.SchemaVersion || row.RunID != run.ID || row.PairID != run.PairID || row.WriterID == "" || row.RecipientID == "" || row.ClientEventID == "" || len(row.ExpectedQuerySet) == 0 || len(row.ExpectedRecipientSet) == 0 || !uniqueNonEmpty(row.ExpectedQuerySet) || !uniqueNonEmpty(row.ExpectedRecipientSet) || row.SubmittedAt.IsZero() || row.AcknowledgementAt.IsZero() || row.ConvergedAt.IsZero() || (row.Coverage != "converged_without_intermediate" && row.CoverAt.IsZero()) || row.AcknowledgementAt.Before(row.SubmittedAt) || (!row.CoverAt.IsZero() && row.CoverAt.Before(row.AcknowledgementAt)) || row.ConvergedAt.Before(row.AcknowledgementAt) || (!row.CoverAt.IsZero() && row.ConvergedAt.Before(row.CoverAt)) || row.SubmittedAt.Before(run.StartedAt) || row.ConvergedAt.After(run.EndedAt) || !contains(row.ExpectedRecipientSet, row.RecipientID) || !benchrun.ValidateCoverage(row.Coverage) || row.Coverage == "none" || row.ExpectedMaterializedDigest == "" || row.ObservedMaterializedDigest == "" {
			return errors.New("passing run ledger evidence is incomplete")
		}
		key := row.ClientEventID + "\x00" + row.RecipientID
		if seen[key] {
			return errors.New("passing run ledger evidence contains duplicates")
		}
		seen[key] = true
		recipients[row.RecipientID] = true
		if row.Coverage == "coalesced" {
			if row.CoveredExpectedMaterializedDigest == "" || row.CoveredExpectedMaterializedDigest != row.ObservedMaterializedDigest || row.CoveredExpectedStateVersion <= row.ExpectedStateVersion || row.ObservedStateVersion < row.CoveredExpectedStateVersion {
				return errors.New("passing run coalesced ledger evidence is invalid")
			}
		} else if row.ExpectedMaterializedDigest != row.ObservedMaterializedDigest {
			return errors.New("passing run ledger semantic evidence mismatches")
		}
		rows++
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("malformed JSONL sibling %s: %w", filepath.Base(filepath.Join(dir, "ledger.jsonl")), err)
	}
	if rows != run.ExpectedLedgerRows || run.ExpectedLedgerRows <= 0 || run.ExpectedMutationRecipients != run.ExpectedLedgerRows {
		return errors.New("passing run ledger cardinality is not exact")
	}
	framesBytes, err := readBytes(filepath.Join(dir, "frames.jsonl"), 8<<20)
	if err != nil {
		return err
	}
	frameCount := 0
	scanner = bufio.NewScanner(bytes.NewReader(framesBytes))
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		var frame benchrun.Frame
		if err := json.Unmarshal(scanner.Bytes(), &frame); err != nil {
			return err
		}
		if frame.SchemaVersion != benchrun.SchemaVersion || frame.RunID != run.ID || frame.RecipientID == "" || !recipients[frame.RecipientID] || frame.ReceivedAt.IsZero() || frame.ReceivedAt.Before(run.StartedAt) || frame.ReceivedAt.After(run.EndedAt) {
			return errors.New("passing run frame evidence is incomplete")
		}
		if err := benchrun.ValidateMeasurement(frame.PayloadBytes); err != nil {
			return err
		}
		frameCount++
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("malformed JSONL sibling %s: %w", filepath.Base(filepath.Join(dir, "frames.jsonl")), err)
	}
	if frameCount == 0 {
		return errors.New("passing run frame evidence is empty")
	}
	return nil
}

func validateJSONLines(path string) error {
	b, err := readBounded(path, 8<<20)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(bytes.NewReader(b))
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		var value json.RawMessage
		if err := json.Unmarshal(scanner.Bytes(), &value); err != nil {
			return fmt.Errorf("malformed JSONL sibling %s: %w", filepath.Base(path), err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("malformed JSONL sibling %s: %w", filepath.Base(path), err)
	}
	return nil
}

func validateJSONLinesAt(root *artifactRoot, relative string) error {
	b, err := root.read(relative, 8<<20)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(bytes.NewReader(b))
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		var value json.RawMessage
		if err := json.Unmarshal(scanner.Bytes(), &value); err != nil {
			return fmt.Errorf("malformed JSONL sibling %s: %w", filepath.Base(relative), err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("malformed JSONL sibling %s: %w", filepath.Base(relative), err)
	}
	return nil
}

func writeSmokeReport(path string, report smokeReport) error {
	if report.SchemaVersion != benchrun.SchemaVersion || !report.DiagnosticOnly || report.Family != "H-append" || report.Scale != 300 || report.Seed == 0 || report.StartedAt.IsZero() || report.FinishedAt.IsZero() || !report.FinishedAt.After(report.StartedAt) {
		return errors.New("diagnostic smoke report identity or timestamps are invalid")
	}
	var file *os.File
	if path == "-" {
		file = os.Stdout
	} else {
		if err := noSymlinkComponents(path); err != nil {
			return err
		}
		if !filepath.IsAbs(path) {
			return errors.New("diagnostic output must be an absolute path")
		}
		opened, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return err
		}
		file = opened
		defer file.Close()
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

func fail(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}
