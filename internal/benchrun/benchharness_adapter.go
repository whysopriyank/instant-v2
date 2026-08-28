package benchrun

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/instant-v2/instant-v2/internal/benchharness"
)

// BenchharnessDriver adapts the target-neutral WP5-A TargetDriver without
// changing its protocol surface. It intentionally does not provision or
// manufacture metadata; TargetDriver.Qualify remains the gate.
type BenchharnessDriver struct {
	Driver     *benchharness.TargetDriver
	Collectors *LiveCollectors
}

func (a BenchharnessDriver) QualifyTarget(ctx context.Context, target Target) (Qualification, error) {
	if a.Driver == nil {
		return Qualification{}, errors.New("live target driver is not configured")
	}
	q, err := a.Driver.Prepare(ctx)
	out := Qualification{Passed: q.Passed, Checks: map[string]bool{}}
	for name, check := range q.Checks {
		out.Checks[name] = check.Passed
	}
	out.Failure = q.Failure
	if err != nil && out.Failure == "" {
		out.Failure = err.Error()
	}
	return out, err
}

func (a BenchharnessDriver) RunTarget(ctx context.Context, spec RunSpec) (ExecutionResult, error) {
	if a.Driver == nil {
		return ExecutionResult{}, errors.New("live target driver is not configured")
	}
	w, err := benchharness.NewWorkload(benchharness.Family(spec.Run.Family), spec.Run.Scale, spec.Plan.Seed)
	if err != nil {
		return ExecutionResult{}, &RunError{Class: SetupInvalid, Err: err}
	}
	plan := buildBenchharnessRunPlan(spec, w)
	if plan.Mutations <= 0 {
		plan.Mutations = maxInt(1, spec.Plan.MeasureSeconds*8)
	}
	if w.Family == benchharness.FamilyT {
		// Saturation is a fixed 4096-operation contract, independent of an
		// operator's generic measure-duration fallback.
		plan.Mutations = 4096
	}
	collectors := a.Collectors
	if collectors == nil {
		collectors = UnsupportedLiveCollectors("collector not configured")
	}
	defer collectors.Close()
	result := ExecutionResult{}
	result.Run = spec.Run
	result.Run.CollectorProvenance = cloneStringMap(collectors.Provenance)
	if collectors.Database != nil {
		result.DBBefore, _ = collectors.Database.Before(ctx)
	}
	var samplesMu sync.Mutex
	sampleProcess := func(sampleCtx context.Context) {
		if collectors.Process == nil {
			return
		}
		sample, sampleErr := collectors.Process.Sample(sampleCtx)
		if sampleErr != nil && sample.UserCPU.Status == "" {
			sample = failedProcess(Redact(sampleErr.Error()))
		}
		samplesMu.Lock()
		result.Process = append(result.Process, sample)
		samplesMu.Unlock()
	}
	sampleRuntime := func(sampleCtx context.Context) {
		if collectors.Runtime == nil {
			return
		}
		sample, sampleErr := collectors.Runtime.Sample(sampleCtx)
		if sampleErr != nil && sample.AllocBytes.Status == "" {
			sample = failedRuntime(Redact(sampleErr.Error()))
		}
		samplesMu.Lock()
		result.Runtime = append(result.Runtime, sample)
		samplesMu.Unlock()
	}
	// A invokes these callbacks synchronously at the exact measured-window
	// boundaries. Preserve that timestamp on the process evidence so resource
	// deltas exclude setup/warmup and include the end boundary even when the
	// periodic sampler interval is long.
	sampleProcessBoundary := func(sampleCtx context.Context, at time.Time) error {
		if collectors.Process == nil {
			return nil
		}
		sample, sampleErr := collectors.Process.Sample(sampleCtx)
		sample.At = at
		if sampleErr != nil && sample.UserCPU.Status == "" {
			sample = failedProcess(Redact(sampleErr.Error()))
			sample.At = at
		}
		samplesMu.Lock()
		result.Process = append(result.Process, sample)
		samplesMu.Unlock()
		if sampleErr != nil && !unsupportedProcessSample(sample) {
			return sampleErr
		}
		return nil
	}
	plan.OnMeasuredStart = func(at time.Time) error {
		return sampleProcessBoundary(ctx, at)
	}
	plan.OnMeasuredEnd = func(at time.Time) error {
		return sampleProcessBoundary(ctx, at)
	}
	sampleProcess(ctx)
	sampleRuntime(ctx)
	runCtx, cancelRun := context.WithCancel(ctx)
	var samplerWG sync.WaitGroup
	interval := collectors.Interval
	if interval <= 0 {
		interval = time.Second
	}
	samplerWG.Add(1)
	go func() {
		defer samplerWG.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				sampleProcess(runCtx)
				sampleRuntime(runCtx)
			}
		}
	}()
	artifacts, err := a.Driver.Run(runCtx, plan)
	cancelRun()
	samplerWG.Wait()
	sampleProcess(ctx)
	sampleRuntime(ctx)
	if collectors.Database != nil {
		result.DBAfter, _ = collectors.Database.After(ctx)
	}
	result.Run.StartedAt = artifacts.StartedAt
	if result.Run.StartedAt.IsZero() {
		result.Run.StartedAt = spec.Run.StartedAt
	}
	result.Run.EndedAt = artifacts.FinishedAt
	if result.Run.EndedAt.IsZero() {
		result.Run.EndedAt = time.Now().UTC()
	}
	result.Run.MeasuredStartedAt = artifacts.MeasuredStartedAt
	result.Run.MeasuredFinishedAt = artifacts.MeasuredFinishedAt
	result.Run.PrimaryClass = Pass
	if err != nil {
		result.Run.PrimaryClass = classifyBenchError(err)
		result.Run.Failure = Redact(err.Error())
	}
	result.Run.Measurements = map[string]Measurement{}
	result.Run.PhaseDurations = map[string]Measurement{
		"ramp":   durationMeasurement(artifacts.RampDuration),
		"settle": durationMeasurement(artifacts.SettleDuration),
		"warmup": durationMeasurement(artifacts.WarmupDuration),
	}
	result.Run.WarmupMutations = artifacts.WarmupMutations
	result.Run.ProtocolErrors = redactErrors(artifacts.ProtocolErrors)
	result.Run.BehaviorErrors = redactErrors(artifacts.BehaviorErrors)
	result.Run.BehaviorErrors = appendUnique(result.Run.BehaviorErrors, redactErrors(artifacts.Result.BehaviorErrors)...)
	for _, raw := range artifacts.RawFrames {
		if raw.ProtocolError != "" {
			result.Run.ProtocolErrors = appendUnique(result.Run.ProtocolErrors, Redact(raw.ProtocolError))
		}
	}
	if len(result.Run.ProtocolErrors) > 0 && err == nil {
		err = &RunError{Class: TargetProtocolFailure, Err: errors.New(strings.Join(result.Run.ProtocolErrors, "; "))}
	}
	if len(result.Run.BehaviorErrors) > 0 && err == nil {
		err = &RunError{Class: TargetSemanticFailure, Err: errors.New(strings.Join(result.Run.BehaviorErrors, "; "))}
	}
	if err != nil {
		result.Run.PrimaryClass = classifyBenchError(err)
		result.Run.Failure = Redact(err.Error())
	}
	// Evidence taxonomy takes precedence over a generic driver error. A live
	// protocol/behavior failure must never be downgraded to semantic noise just
	// because the target returned an untyped error alongside its evidence.
	if len(result.Run.ProtocolErrors) > 0 {
		result.Run.PrimaryClass = TargetProtocolFailure
	} else if len(result.Run.BehaviorErrors) > 0 {
		result.Run.PrimaryClass = TargetSemanticFailure
	}
	// A process sample is valid only while it identifies the same target
	// process. PID reuse, executable replacement, or a missing signed hash is
	// infrastructure evidence and must not become a claim-eligible run.
	measuredStart, measuredEnd := artifacts.MeasuredStartedAt, artifacts.MeasuredFinishedAt
	if measuredStart.IsZero() {
		measuredStart = artifacts.Evidence.MeasuredStartedAt
	}
	if measuredEnd.IsZero() {
		measuredEnd = artifacts.Evidence.MeasuredFinishedAt
	}
	if !processIdentityStable(result.Process, measuredStart, measuredEnd) {
		result.Run.PrimaryClass = HarnessDefect
		result.Run.Failure = "target process identity changed during measurement"
	}
	var rows []benchharness.LedgerRow
	if artifacts.Result.Ledger != nil {
		rows = artifacts.Result.Ledger.Rows()
	}
	for _, row := range rows {
		coverage := normalizeCoverage(row.Coverage)
		result.Ledger = append(result.Ledger, LedgerRow{SchemaVersion: SchemaVersion, PairID: row.PairID, RunID: row.RunID, WriterID: row.WriterID, RecipientID: row.RecipientID, ClientEventID: row.ClientEventID, ServerTransactionID: row.ServerTransactionID, ExpectedQuerySet: row.ExpectedQuerySet, ExpectedRecipientSet: row.ExpectedRecipientSet, SubmittedAt: row.SubmittedAt, AcknowledgementAt: row.AcknowledgementAt, CoverAt: row.CoverAt, ProcessedTransactionID: row.ProcessedTransactionID, ExpectedMaterializedDigest: row.ExpectedMaterializedDigest, ObservedMaterializedDigest: row.ObservedMaterializedDigest, ExpectedStateVersion: uint64(maxInt64(row.ExpectedStateVersion)), CoveredExpectedStateVersion: uint64(maxInt64(row.CoveredExpectedStateVersion)), CoveredExpectedMaterializedDigest: row.CoveredExpectedMaterializedDigest, ObservedStateVersion: uint64(maxInt64(row.ObservedStateVersion)), Coverage: coverage, RefreshBeforeAck: row.RefreshBeforeAck, BufferedSnapshotAhead: row.BufferedSnapshotAhead, ConvergedAt: row.ConvergedAt, ErrorClass: string(row.ErrorClass), EvidenceRef: row.EvidenceRef})
	}
	expected := expectedLedgerCardinality(w, plan.Mutations)
	result.Run.ExpectedLedgerRows = expected
	result.Run.ExpectedMutationRecipients = expected
	// RawFrames are the parsed wire evidence captured by WP5-A. Persist only
	// bounded redacted digests and exact byte counts; never persist raw payload.
	for _, raw := range artifacts.RawFrames {
		result.Frames = append(result.Frames, mapRawFrame(artifacts.RunID, raw))
	}
	if len(result.Ledger) > 0 {
		recipientConvergence := make([]float64, 0, len(result.Ledger))
		globalByMutation := make(map[string]float64)
		for _, row := range result.Ledger {
			if row.CoverAt.IsZero() || row.SubmittedAt.IsZero() {
				continue
			}
			latency := row.CoverAt.Sub(row.SubmittedAt).Seconds() * 1000
			recipientConvergence = append(recipientConvergence, latency)
			if prior, ok := globalByMutation[row.ClientEventID]; !ok || latency > prior {
				globalByMutation[row.ClientEventID] = latency
			}
		}
		if len(recipientConvergence) > 0 {
			result.Run.Measurements["recipient_convergence_p99"] = Measurement{Status: StatusValue, Value: Quantile(recipientConvergence, .99), Unit: "recipient_semantic_convergence_p99_ms"}
			globalConvergence := make([]float64, 0, len(globalByMutation))
			for _, latency := range globalByMutation {
				globalConvergence = append(globalConvergence, latency)
			}
			result.Run.Measurements["convergence_p99"] = Measurement{Status: StatusValue, Value: Quantile(globalConvergence, .99), Unit: "global_semantic_convergence_p99_ms"}
		}
	}
	affected := 0
	for _, row := range result.Ledger {
		if row.Coverage != "none" && (!row.CoverAt.IsZero() || !row.ConvergedAt.IsZero()) {
			affected++
		}
	}
	// Use A's measured-window cumulative class accounting, not the bounded
	// retained frame sample or setup/cleanup traffic. This remains exact when
	// RawFrames retention truncates.
	appEvidence := artifacts.Evidence.MeasuredByClass[benchharness.FrameApplicationRefresh]
	if appEvidence.Frames > 0 && affected > 0 {
		status := StatusValue
		if appEvidence.Bytes == 0 {
			status = StatusZero
		}
		result.Run.Measurements["wire_bytes"] = Measurement{Status: status, Value: float64(appEvidence.Bytes) / float64(affected), Unit: "wire_bytes_per_affected_recipient"}
	}
	measuredStart, measuredEnd = artifacts.MeasuredStartedAt, artifacts.MeasuredFinishedAt
	if measuredStart.IsZero() {
		measuredStart = artifacts.Evidence.MeasuredStartedAt
	}
	if measuredEnd.IsZero() {
		measuredEnd = artifacts.Evidence.MeasuredFinishedAt
	}
	if measuredStart.IsZero() {
		measuredStart = artifacts.StartedAt
	}
	if measuredEnd.IsZero() {
		measuredEnd = artifacts.FinishedAt
	}
	var maxRSS float64
	for _, sample := range result.Process {
		if sample.At.Before(measuredStart) || sample.At.After(measuredEnd) {
			continue
		}
		// PeakRSS is process-lifetime cumulative and may include provisioning or
		// warm-up. The claim endpoint is the maximum instantaneous RSS sampled
		// inside the measured interval.
		if sample.RSS.Status == StatusValue && sample.RSS.Value > maxRSS {
			maxRSS = sample.RSS.Value
		}
	}
	// Resource CPU is a measured-window delta, never setup/ramp/warmup CPU.
	// Select samples by the harness timestamps before choosing baseline/end.
	if cpu, ok := processWindowCPU(result.Process, measuredStart, measuredEnd, affected); ok {
		result.Run.Measurements["cpu"] = cpu
	}
	if maxRSS > 0 {
		result.Run.Measurements["peak_rss"] = Measurement{Status: StatusValue, Value: maxRSS, Unit: "bytes"}
		if spec.Run.Scale > 0 {
			result.Run.Measurements["peak_rss_per_active_subscriber"] = Measurement{Status: StatusValue, Value: maxRSS / float64(spec.Run.Scale), Unit: "bytes_per_active_subscriber"}
		}
	}
	if measuredEnd.After(measuredStart) {
		elapsed := measuredEnd.Sub(measuredStart).Seconds()
		if w.Family == benchharness.FamilyT {
			if elapsed > 0 {
				result.Run.Measurements["throughput"] = Measurement{Status: StatusValue, Value: float64(artifacts.Result.Acknowledged) / elapsed, Unit: "tx/s"}
			}
		} else {
			// The comparative primary is the global semantic convergence p99;
			// recipient p99 remains a separately reported endpoint.
			if global, ok := result.Run.Measurements["convergence_p99"]; ok {
				result.Run.Measurements["primary"] = global
			}
		}
	}
	return result, err
}

func processIdentityStable(samples []ProcessSample, measuredStart, measuredEnd time.Time) bool {
	var pid int
	var start, hash string
	for _, sample := range samples {
		if !measuredStart.IsZero() && (sample.At.Before(measuredStart) || sample.At.After(measuredEnd)) {
			continue
		}
		if sample.PID <= 0 || sample.StartTime == "" || sample.ExecutableHash == "" {
			return false
		}
		if pid == 0 {
			pid, start, hash = sample.PID, sample.StartTime, sample.ExecutableHash
			continue
		}
		if sample.PID != pid || sample.StartTime != start || !strings.EqualFold(sample.ExecutableHash, hash) {
			return false
		}
	}
	return true
}

// processWindowCPU computes core milliseconds per 1,000 affected coverages
// from the first and last exact measured-window samples. Boundary callbacks
// provide those samples; periodic samples alone are intentionally insufficient
// to claim a complete window.
func processWindowCPU(samples []ProcessSample, measuredStart, measuredEnd time.Time, affected int) (Measurement, bool) {
	if affected <= 0 {
		return Measurement{}, false
	}
	ordered := append([]ProcessSample(nil), samples...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].At.Before(ordered[j].At) })
	var baseline, finish float64
	var baselineAt, finishAt time.Time
	count := 0
	var previousUser, previousSystem float64
	var previousSet bool
	for _, sample := range ordered {
		if sample.At.Before(measuredStart) || sample.At.After(measuredEnd) || !validProcessCPU(sample.UserCPU) || !validProcessCPU(sample.SystemCPU) {
			continue
		}
		if previousSet && (sample.UserCPU.Value < previousUser || sample.SystemCPU.Value < previousSystem) {
			return Measurement{}, false
		}
		previousUser, previousSystem, previousSet = sample.UserCPU.Value, sample.SystemCPU.Value, true
		cpu := sample.UserCPU.Value + sample.SystemCPU.Value
		if baselineAt.IsZero() || sample.At.Before(baselineAt) {
			baseline, baselineAt = cpu, sample.At
		}
		if finishAt.IsZero() || sample.At.After(finishAt) {
			finish, finishAt = cpu, sample.At
		}
		count++
	}
	if count < 2 || baselineAt.IsZero() || finishAt.IsZero() {
		return Measurement{}, false
	}
	delta := finish - baseline
	status := StatusValue
	if delta == 0 {
		status = StatusZero
	}
	return Measurement{Status: status, Value: delta * 1e6 / float64(affected), Unit: "core_ms_per_1000_affected_coverages"}, true
}

func validProcessCPU(m Measurement) bool {
	return m.Status == StatusValue || m.Status == StatusZero
}

func unsupportedProcessSample(sample ProcessSample) bool {
	return sample.UserCPU.Status == StatusUnsupported && sample.SystemCPU.Status == StatusUnsupported && sample.RSS.Status == StatusUnsupported
}

const maxPersistedFrameEvidence = 64 << 10

func mapRawFrame(runID string, raw benchharness.RawFrameEvidence) Frame {
	if runID == "" {
		runID = "unknown-run"
	}
	recipient := raw.RecipientID
	if recipient == "" {
		recipient = raw.ClientID
	}
	kind := raw.Op
	if kind == "" {
		kind = "wire"
	}
	frame := Frame{
		SchemaVersion:          SchemaVersion,
		RunID:                  runID,
		ClientID:               raw.ClientID,
		QueryID:                raw.QueryID,
		RecipientID:            recipient,
		ServerTransactionID:    raw.ServerTransactionID,
		ReceivedAt:             raw.At,
		Kind:                   kind,
		Class:                  string(raw.Class),
		ProcessedTransactionID: raw.ProcessedTransactionID,
		PayloadBytes:           Measurement{Status: StatusValue, Value: float64(raw.PayloadBytes), Unit: "bytes"},
	}
	if raw.PayloadBytes == 0 {
		frame.PayloadBytes = Zero("bytes")
	}
	if raw.ClientEventID != "" {
		frame.ClientEventIDs = []string{raw.ClientEventID}
	}
	if raw.PayloadBytes > 0 {
		frame.PayloadDigest = raw.PayloadDigest
		if frame.PayloadDigest == "" {
			// A's bounded evidence contract normally supplies this digest. Keep
			// an explicit non-content reference if a custom driver omitted it.
			sum := sha256.Sum256([]byte(fmt.Sprintf("payload-bytes:%d", raw.PayloadBytes)))
			frame.PayloadDigest = hex.EncodeToString(sum[:])
		}
		frame.EvidenceRef = "redacted-payload-sha256:" + frame.PayloadDigest
	}
	if raw.ProtocolError != "" {
		frame.ErrorClass = string(TargetProtocolFailure)
	}
	return frame
}

func durationMeasurement(d time.Duration) Measurement {
	if d < 0 {
		return Measurement{Status: StatusFailed, Unit: "s", Error: "negative phase duration"}
	}
	return Measurement{Status: StatusValue, Value: d.Seconds(), Unit: "s"}
}

func redactErrors(in []string) []string {
	out := make([]string, 0, len(in))
	for _, value := range in {
		if value = strings.TrimSpace(Redact(value)); value != "" {
			out = appendUnique(out, value)
		}
	}
	return out
}

func appendUnique(dst []string, values ...string) []string {
	for _, value := range values {
		found := false
		for _, existing := range dst {
			if existing == value {
				found = true
				break
			}
		}
		if !found {
			dst = append(dst, value)
		}
	}
	return dst
}

func normalizeCoverage(c benchharness.Coverage) string {
	switch c {
	case benchharness.CoverageExact, benchharness.CoverageCoalesced, benchharness.CoverageConvergedWithoutIntermediate:
		return string(c)
	default:
		return "none"
	}
}
func classifyBenchError(err error) FailureClass {
	if err == nil {
		return Pass
	}
	var runErr *RunError
	if errors.As(err, &runErr) && runErr.Class != "" {
		return runErr.Class
	}
	var unsupported *benchharness.UnsupportedTargetError
	if errors.As(err, &unsupported) {
		return SetupInvalid
	}
	return TargetSemanticFailure
}
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func maxInt64(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}

var _ TargetDriver = BenchharnessDriver{}

func buildBenchharnessRunPlan(spec RunSpec, workload benchharness.Workload) benchharness.RunPlan {
	mutations := workload.Measured
	if workload.Family == benchharness.FamilyT {
		mutations = 4096
	}
	return benchharness.RunPlan{
		PairID: spec.Run.PairID, RunID: spec.Run.ID, WriterID: "bench-writer", Workload: workload,
		Start: time.Now(), Mutations: mutations, Rate: workload.TxRate,
		RampDuration:     time.Duration(spec.Plan.RampSeconds) * time.Second,
		SettleDuration:   time.Duration(spec.Plan.SettleSeconds) * time.Second,
		WarmupDuration:   time.Duration(spec.Plan.WarmupSeconds) * time.Second,
		WarmupMutations:  spec.Plan.WarmupMutations,
		ConvergenceGrace: time.Duration(spec.Plan.GraceSeconds) * time.Second,
		BehaviorSeconds:  spec.Plan.MeasureSeconds,
	}
}

// expectedLedgerCardinality is derived from the frozen workload assignments,
// never from observed ledger rows. A short/partial ledger therefore remains
// a failed attempt instead of redefining the denominator to make it pass.
func expectedLedgerCardinality(w benchharness.Workload, mutations int) int {
	if w.Family == benchharness.FamilyT {
		mutations = 4096
	}
	if mutations < 0 {
		mutations = 0
	}
	count := 0
	for seq := 1; seq <= mutations; seq++ {
		m := w.Mutation(int64(seq))
		for _, q := range w.Fixture.Queries {
			if q.MatchAll || q.Bucket == m.Bucket || m.Kind == benchharness.MutationRetract || m.Kind == benchharness.MutationReorder {
				count++
			}
		}
	}
	return count
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = Redact(value)
	}
	return out
}
