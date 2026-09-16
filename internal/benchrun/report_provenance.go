package benchrun

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func validateProcessProvenance(dir string, run Run, target Target) error {
	if target.ExecutableHash == "" {
		return nil
	}
	path := filepath.Join(dir, "process.jsonl")
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("run %s process evidence unavailable: %w", run.ID, err)
	}
	defer f.Close()
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 64<<10), MaxJSONLineBytes)
	totalSamples, validCount := 0, 0
	var pid int
	var start string
	for scan.Scan() {
		totalSamples++
		var sample ProcessSample
		if err := json.Unmarshal(scan.Bytes(), &sample); err != nil {
			return fmt.Errorf("run %s malformed process evidence: %w", run.ID, err)
		}
		// Provisioning/cleanup samples are retained for diagnostics, but cannot
		// invalidate a measured resource claim. Live claims validate every
		// inclusive-window sample below against the exact boundaries.
		if !run.MeasuredStartedAt.IsZero() && (sample.At.Before(run.MeasuredStartedAt) || sample.At.After(run.MeasuredFinishedAt)) {
			continue
		}
		unsupported := sample.UserCPU.Status == StatusUnsupported && sample.SystemCPU.Status == StatusUnsupported && sample.RSS.Status == StatusUnsupported
		if run.PrimaryClass == Pass && !unsupported && (sample.At.IsZero() || sample.PID <= 0 || sample.StartTime == "" || sample.ExecutableHash == "") {
			return fmt.Errorf("run %s process evidence lacks stable PID/start/executable identity", run.ID)
		}
		if sample.UserCPU.Status == StatusFailed || sample.SystemCPU.Status == StatusFailed || sample.RSS.Status == StatusFailed {
			if run.PrimaryClass == Pass {
				return fmt.Errorf("run %s process evidence contains failed resource sample", run.ID)
			}
		}
		if sample.ExecutableHash != "" && !strings.EqualFold(sample.ExecutableHash, target.ExecutableHash) {
			return fmt.Errorf("run %s process executable hash does not match signed target %s", run.ID, target.ID)
		}
		if sample.PID > 0 && sample.StartTime != "" {
			validCount++
			if pid == 0 {
				pid, start = sample.PID, sample.StartTime
			} else if sample.PID != pid || sample.StartTime != start {
				return fmt.Errorf("run %s process identity changed during offline verification", run.ID)
			}
		}
	}
	if err := scan.Err(); err != nil {
		return fmt.Errorf("run %s process evidence: %w", run.ID, err)
	}
	if run.PrimaryClass == Pass && totalSamples == 0 {
		return fmt.Errorf("run %s process evidence is empty", run.ID)
	}
	if run.PrimaryClass == Pass && validCount > 0 && validCount < 2 {
		return fmt.Errorf("run %s process evidence lacks exact measured boundary samples", run.ID)
	}
	return nil
}

func isLiveTarget(m Manifest, target Target) bool {
	return target.Endpoint != "" || m.TargetProvenance[target.ID] != ""
}

const collectorClockSkew = time.Minute

// validateCollectorEvidence is intentionally shared by the pair and triad
// loaders. A passing live run is a claim only when every collector's raw
// evidence and provenance can be replayed offline.
func validateCollectorEvidence(dir string, run Run, target Target, databaseRequired bool) error {
	states, err := validateCollectorProvenance(run)
	if err != nil {
		return err
	}
	if err := validateRuntimeEvidence(dir, run, states["runtime"]); err != nil {
		return err
	}
	return validateDatabaseEvidence(dir, run, target, states["database"], databaseRequired)
}

func validateCollectorProvenance(run Run) (map[string]string, error) {
	states := make(map[string]string, 4)
	for _, name := range []string{"process", "runtime", "database", "network"} {
		value := strings.TrimSpace(run.CollectorProvenance[name])
		if value == "" {
			return nil, fmt.Errorf("%s collector provenance is required", name)
		}
		state, detail, ok := strings.Cut(value, ":")
		state, detail = strings.TrimSpace(state), strings.TrimSpace(detail)
		if !ok || detail == "" || (state != "supported" && state != "unsupported" && state != "failed") {
			return nil, fmt.Errorf("%s collector provenance has invalid state", name)
		}
		if run.PrimaryClass == Pass && state == "failed" {
			return nil, fmt.Errorf("%s collector provenance failed for passing run", name)
		}
		if name == "network" && state == "supported" {
			fields := strings.Fields(detail)
			if len(fields) != 2 || !strings.HasPrefix(fields[0], "target=") || !strings.HasPrefix(fields[1], "initial=") {
				return nil, errors.New("network collector provenance has invalid namespace identities")
			}
			networkID, initialID := strings.TrimPrefix(fields[0], "target="), strings.TrimPrefix(fields[1], "initial=")
			if !validNetworkNamespaceEvidenceID(networkID) || !validNetworkNamespaceEvidenceID(initialID) {
				return nil, errors.New("network collector provenance has invalid namespace identities")
			}
			if networkID == initialID {
				return nil, errors.New("network collector provenance identities must differ")
			}
		}
		states[name] = state
	}
	return states, nil
}

func validNetworkNamespaceEvidenceID(value string) bool {
	if !strings.HasPrefix(value, "net:[") || !strings.HasSuffix(value, "]") {
		return false
	}
	digits := strings.TrimSuffix(strings.TrimPrefix(value, "net:["), "]")
	if digits == "" {
		return false
	}
	for _, r := range digits {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func collectorAtWithinRun(at time.Time, run Run) bool {
	if at.IsZero() || run.StartedAt.IsZero() || run.EndedAt.IsZero() || !run.EndedAt.After(run.StartedAt) {
		return false
	}
	start, end := run.StartedAt, run.EndedAt
	if !run.MeasuredStartedAt.IsZero() && run.MeasuredStartedAt.Before(start) {
		start = run.MeasuredStartedAt
	}
	if !run.MeasuredFinishedAt.IsZero() && run.MeasuredFinishedAt.After(end) {
		end = run.MeasuredFinishedAt
	}
	return !at.Before(start.Add(-collectorClockSkew)) && !at.After(end.Add(collectorClockSkew))
}

func validateRuntimeEvidence(dir string, run Run, state string) error {
	f, err := os.Open(filepath.Join(dir, "runtime-metrics.jsonl"))
	if err != nil {
		return fmt.Errorf("run %s runtime metrics evidence unavailable: %w", run.ID, err)
	}
	defer f.Close()
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 64<<10), MaxJSONLineBytes)
	samples := 0
	allUnsupported := true
	for scan.Scan() {
		samples++
		var sample RuntimeSample
		if err := json.Unmarshal(scan.Bytes(), &sample); err != nil {
			return fmt.Errorf("run %s malformed runtime metrics evidence: %w", run.ID, err)
		}
		if !collectorAtWithinRun(sample.At, run) {
			return fmt.Errorf("run %s runtime metrics sample is outside run window", run.ID)
		}
		for _, metric := range []Measurement{sample.AllocBytes, sample.LiveHeap, sample.HeapGoal, sample.GCCycles, sample.GCPause, sample.Goroutines} {
			if err := ValidateMeasurement(metric); err != nil {
				return fmt.Errorf("run %s invalid runtime metrics measurement: %w", run.ID, err)
			}
			if metric.Status == StatusFailed && run.PrimaryClass == Pass {
				return fmt.Errorf("run %s runtime metrics contains failed sample", run.ID)
			}
			if metric.Status != StatusUnsupported {
				allUnsupported = false
			}
		}
	}
	if err := scan.Err(); err != nil {
		return fmt.Errorf("run %s runtime metrics evidence: %w", run.ID, err)
	}
	if samples == 0 {
		return fmt.Errorf("run %s runtime metrics evidence is empty", run.ID)
	}
	if state == "unsupported" && !allUnsupported {
		return fmt.Errorf("run %s runtime metrics contradict unsupported collector provenance", run.ID)
	}
	if state == "supported" && allUnsupported {
		return fmt.Errorf("run %s runtime metrics contradict supported collector provenance", run.ID)
	}
	return nil
}

func validateDatabaseEvidence(dir string, run Run, target Target, state string, databaseRequired bool) error {
	if databaseRequired && state == "unsupported" {
		return fmt.Errorf("run %s database collector is required but unsupported", run.ID)
	}
	readSnapshot := func(name string) (DBSnapshot, error) {
		b, err := readBounded(filepath.Join(dir, name+".json"), SmallArtifactBytes)
		if err != nil {
			return DBSnapshot{}, fmt.Errorf("run %s %s evidence unavailable: %w", run.ID, name, err)
		}
		var snapshot DBSnapshot
		if err := json.Unmarshal(b, &snapshot); err != nil {
			return DBSnapshot{}, fmt.Errorf("run %s malformed %s evidence: %w", run.ID, name, err)
		}
		if !collectorAtWithinRun(snapshot.At, run) {
			return DBSnapshot{}, fmt.Errorf("run %s %s snapshot is outside run window", run.ID, name)
		}
		if state == "supported" && target.PostgresVersion != "" && snapshot.Version != target.PostgresVersion {
			return DBSnapshot{}, fmt.Errorf("run %s %s version does not match target", run.ID, name)
		}
		measurements := []Measurement{snapshot.Connections, snapshot.BlockHits, snapshot.BlockReads, snapshot.TempBytes, snapshot.TempFiles, snapshot.Commits, snapshot.Rollbacks, snapshot.TupleReads, snapshot.TupleWrites, snapshot.WALBytes, snapshot.SlotLag, snapshot.PoolActive, snapshot.PoolIdle}
		allUnsupported := true
		for _, metric := range measurements {
			if err := ValidateMeasurement(metric); err != nil {
				return DBSnapshot{}, fmt.Errorf("run %s invalid %s measurement: %w", run.ID, name, err)
			}
			if metric.Status == StatusFailed && run.PrimaryClass == Pass {
				return DBSnapshot{}, fmt.Errorf("run %s %s contains failed measurement", run.ID, name)
			}
			if metric.Status != StatusUnsupported {
				allUnsupported = false
			}
		}
		if state == "unsupported" && !allUnsupported {
			return DBSnapshot{}, fmt.Errorf("run %s %s contradicts unsupported collector provenance", run.ID, name)
		}
		if databaseRequired && hasUnsupportedDBSnapshotMeasurement(snapshot) {
			return DBSnapshot{}, fmt.Errorf("run %s %s contains unsupported required database measurement", run.ID, name)
		}
		if state == "supported" && allUnsupported {
			return DBSnapshot{}, fmt.Errorf("run %s %s contradicts supported collector provenance", run.ID, name)
		}
		return snapshot, nil
	}
	before, err := readSnapshot("db-before")
	if err != nil {
		return err
	}
	after, err := readSnapshot("db-after")
	if err != nil {
		return err
	}
	if after.At.Before(before.At) {
		return fmt.Errorf("run %s database snapshots are out of order", run.ID)
	}
	return nil
}

func validateLiveResourceEvidence(dir string, run Run, target Target) error {
	if target.ExecutablePath == "" || target.ExecutableHash == "" {
		return fmt.Errorf("run %s live resource evidence lacks signed executable path/hash", run.ID)
	}
	if run.MeasuredStartedAt.IsZero() || run.MeasuredFinishedAt.IsZero() || !run.MeasuredFinishedAt.After(run.MeasuredStartedAt) {
		return fmt.Errorf("run %s live resource evidence lacks measured boundaries", run.ID)
	}
	affected, err := affectedCoveragesFromLedger(dir, run)
	if err != nil {
		return err
	}
	if affected <= 0 {
		return fmt.Errorf("run %s live resource evidence has no affected coverages", run.ID)
	}
	f, err := os.Open(filepath.Join(dir, "process.jsonl"))
	if err != nil {
		return fmt.Errorf("run %s live process evidence unavailable: %w", run.ID, err)
	}
	defer f.Close()
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 64<<10), MaxJSONLineBytes)
	var samples []ProcessSample
	for scan.Scan() {
		var sample ProcessSample
		if err := json.Unmarshal(scan.Bytes(), &sample); err != nil {
			return fmt.Errorf("run %s malformed live process evidence: %w", run.ID, err)
		}
		if sample.At.Before(run.MeasuredStartedAt) || sample.At.After(run.MeasuredFinishedAt) {
			continue
		}
		if sample.At.IsZero() || sample.PID <= 0 || sample.StartTime == "" || sample.ExecutableHash == "" || !strings.EqualFold(sample.ExecutableHash, target.ExecutableHash) {
			return fmt.Errorf("run %s live process sample lacks signed identity", run.ID)
		}
		if !processResourceMeasurement(sample.UserCPU, "cpu_seconds") || !processResourceMeasurement(sample.SystemCPU, "cpu_seconds") || !processResourceMeasurement(sample.RSS, "bytes") {
			return fmt.Errorf("run %s live process sample has unavailable CPU/RSS evidence", run.ID)
		}
		samples = append(samples, sample)
	}
	if err := scan.Err(); err != nil {
		return fmt.Errorf("run %s live process evidence: %w", run.ID, err)
	}
	if len(samples) < 2 {
		return fmt.Errorf("run %s live process evidence requires at least two measured samples", run.ID)
	}
	sort.SliceStable(samples, func(i, j int) bool { return samples[i].At.Before(samples[j].At) })
	var first, last *ProcessSample
	var pid int
	var start string
	var peak float64
	var previousUser, previousSystem float64
	var previousSet bool
	for i := range samples {
		sample := &samples[i]
		if pid == 0 {
			pid, start = sample.PID, sample.StartTime
		} else if sample.PID != pid || sample.StartTime != start {
			return fmt.Errorf("run %s live process identity changed during measured window", run.ID)
		}
		if previousSet && (sample.UserCPU.Value < previousUser || sample.SystemCPU.Value < previousSystem) {
			return fmt.Errorf("run %s live process cumulative CPU counter regressed", run.ID)
		}
		previousUser, previousSystem, previousSet = sample.UserCPU.Value, sample.SystemCPU.Value, true
		if sample.At.Equal(run.MeasuredStartedAt) && first == nil {
			first = sample
		}
		if sample.At.Equal(run.MeasuredFinishedAt) {
			last = sample
		}
		if sample.RSS.Value > peak {
			peak = sample.RSS.Value
		}
	}
	if first == nil || last == nil {
		return fmt.Errorf("run %s live process evidence lacks exact measured boundary samples", run.ID)
	}
	expectedCPU := (last.UserCPU.Value + last.SystemCPU.Value) - (first.UserCPU.Value + first.SystemCPU.Value)
	expectedCPU = expectedCPU * 1e6 / float64(affected)
	expectedCPUStatus := StatusValue
	if expectedCPU == 0 {
		expectedCPUStatus = StatusZero
	}
	if !sameMeasurement(run.Measurements["cpu"], Measurement{Status: expectedCPUStatus, Value: expectedCPU, Unit: "core_ms_per_1000_affected_coverages"}) {
		return fmt.Errorf("run %s fabricated or missing CPU measurement", run.ID)
	}
	expectedRSSStatus := StatusValue
	if peak == 0 {
		expectedRSSStatus = StatusZero
	}
	if !sameMeasurement(run.Measurements["peak_rss"], Measurement{Status: expectedRSSStatus, Value: peak, Unit: "bytes"}) {
		return fmt.Errorf("run %s fabricated or missing peak RSS measurement", run.ID)
	}
	perSubscriber := peak / float64(run.Scale)
	perSubscriberStatus := StatusValue
	if perSubscriber == 0 {
		perSubscriberStatus = StatusZero
	}
	if !sameMeasurement(run.Measurements["peak_rss_per_active_subscriber"], Measurement{Status: perSubscriberStatus, Value: perSubscriber, Unit: "bytes_per_active_subscriber"}) {
		return fmt.Errorf("run %s fabricated or missing per-subscriber RSS measurement", run.ID)
	}
	return nil
}

func processResourceMeasurement(m Measurement, unit string) bool {
	return (m.Status == StatusValue || m.Status == StatusZero) && m.Unit == unit && !math.IsNaN(m.Value) && !math.IsInf(m.Value, 0) && m.Value >= 0
}

func sameMeasurement(got, want Measurement) bool {
	if got.Status != want.Status || got.Unit != want.Unit {
		return false
	}
	if got.Status != StatusValue && got.Status != StatusZero {
		return false
	}
	tol := 1e-9 * math.Max(1, math.Abs(want.Value))
	return math.Abs(got.Value-want.Value) <= tol
}

func affectedCoveragesFromLedger(dir string, run Run) (int, error) {
	f, err := os.Open(filepath.Join(dir, "ledger.jsonl"))
	if err != nil {
		return 0, fmt.Errorf("run %s ledger unavailable for resource denominator: %w", run.ID, err)
	}
	defer f.Close()
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 64<<10), MaxJSONLineBytes)
	count := 0
	for scan.Scan() {
		var row LedgerRow
		if err := json.Unmarshal(scan.Bytes(), &row); err != nil {
			return 0, fmt.Errorf("run %s malformed ledger for resource denominator: %w", run.ID, err)
		}
		if row.Coverage != "none" && (!row.CoverAt.IsZero() || !row.ConvergedAt.IsZero()) {
			count++
		}
	}
	if err := scan.Err(); err != nil {
		return 0, fmt.Errorf("run %s ledger for resource denominator: %w", run.ID, err)
	}
	return count, nil
}
