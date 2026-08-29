package benchrun

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/instant-v2/instant-v2/internal/benchharness"
)

func CanonicalJSON(v any) ([]byte, error) {
	b, e := json.Marshal(v)
	if e != nil {
		return nil, e
	}
	// Decode numbers as json.Number before re-encoding. The default
	// interface decoder converts integers to float64, which silently aliases
	// adjacent int64 authorization seeds above 2^53.
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var x any
	if e = dec.Decode(&x); e != nil {
		return nil, e
	}
	var extra any
	if e = dec.Decode(&extra); e != io.EOF {
		if e == nil {
			return nil, errors.New("canonical JSON contains trailing values")
		}
		return nil, e
	}
	return json.Marshal(x)
}
func DigestJSON(v any) (string, error) {
	b, e := CanonicalJSON(v)
	if e != nil {
		return "", e
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:]), nil
}
func ValidateManifest(m Manifest) error {
	if m.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported schema version %q", m.SchemaVersion)
	}
	if m.BundleID == "" || m.PairID == "" || m.Family == "" || m.SubscriberScale <= 0 {
		return errors.New("manifest missing required provenance")
	}
	if m.Seed == 0 {
		return errors.New("manifest seed must be recorded")
	}
	if len(m.TargetOrder) > 0 || len(m.Comparisons) > 0 || m.TargetRevisions != nil {
		return validateThreeTargetManifestShape(m)
	}
	if len(m.RunOrder) != 7 {
		return errors.New("manifest run order must contain exactly seven attempts")
	}
	ab, ba := 0, 0
	for _, order := range m.RunOrder {
		if order != "AB" && order != "BA" {
			return fmt.Errorf("invalid run order %q", order)
		}
		if order == "AB" {
			ab++
		} else {
			ba++
		}
	}
	if ab < 3 || ba < 3 || ab+ba != 7 {
		return errors.New("run order is not balanced across seven attempts")
	}
	if m.StartedAt.IsZero() {
		return errors.New("manifest start time is required")
	}
	return nil
}

func validateThreeTargetManifestShape(m Manifest) error {
	if len(m.RunOrder) != ThreeTargetBlocks {
		return errors.New("three-target manifest run order must contain seven blocks")
	}
	if err := ValidateThreeTargetOrder(m.TargetOrder); err != nil {
		return err
	}
	for i, block := range m.TargetOrder {
		if m.RunOrder[i] != strings.Join(block.Order, ">") {
			return fmt.Errorf("three-target manifest run order does not match block %d", block.Index)
		}
	}
	if len(m.TargetRevisions) != len(threeTargetIDs) {
		return errors.New("three-target manifest requires three target revisions")
	}
	for _, id := range threeTargetIDs {
		if _, ok := m.TargetRevisions[id]; !ok {
			return fmt.Errorf("three-target manifest missing target revision %s", id)
		}
	}
	comparisons := make(map[string]ComparisonSpec, len(m.Comparisons))
	for _, comparison := range m.Comparisons {
		if _, exists := comparisons[comparison.ID]; exists {
			return fmt.Errorf("duplicate three-target comparison %q", comparison.ID)
		}
		comparisons[comparison.ID] = comparison
	}
	if len(comparisons) != 2 {
		return errors.New("three-target manifest requires exactly two comparisons")
	}
	for _, want := range []struct{ id, baseline, candidate string }{
		{"v1-v2_current", "v1", "v2_current"},
		{"v2_reference-v2_current", "v2_reference", "v2_current"},
	} {
		comparison, ok := comparisons[want.id]
		if !ok || comparison.BaselineID != want.baseline || comparison.CandidateID != want.candidate {
			return fmt.Errorf("invalid three-target comparison %q", want.id)
		}
		if comparison.BaselineRevision != m.TargetRevisions[want.baseline] || comparison.CandidateRevision != m.TargetRevisions[want.candidate] {
			return fmt.Errorf("comparison %q revisions do not match manifest target revisions", want.id)
		}
	}
	return nil
}
func ValidateFailureClass(c FailureClass) bool {
	switch c {
	case Pass, SetupInvalid, HarnessDefect, TargetSemanticFailure, TargetProtocolFailure, TargetResourceExhaustion, TargetCrash, TargetTimeout, InfrastructureNoise, OperatorAbort:
		return true
	default:
		return false
	}
}
func ValidateMeasurement(m Measurement) error {
	switch m.Status {
	case StatusValue:
		if math.IsNaN(m.Value) || math.IsInf(m.Value, 0) {
			return errors.New("measurement value is non-finite")
		}
	case StatusZero:
		if m.Value != 0 {
			return errors.New("zero measurement must have zero value")
		}
	case StatusMissing, StatusUnsupported, StatusNotApplicable, StatusFailed:
		if m.Value != 0 {
			return errors.New("non-observation must not carry a value")
		}
	default:
		return fmt.Errorf("unknown measurement status %q", m.Status)
	}
	if m.Unit == "" {
		return errors.New("measurement unit is required")
	}
	return nil
}
func ValidateCoverage(c string) bool {
	switch c {
	case "exact", "coalesced", "converged_without_intermediate", "none":
		return true
	default:
		return false
	}
}
func validateRunEvidence(dir string, run Run) error {
	if run.WarmupMutations < 0 {
		return fmt.Errorf("run %s has negative warmup mutation count", run.ID)
	}
	for name, phase := range run.PhaseDurations {
		if err := ValidateMeasurement(phase); err != nil {
			return fmt.Errorf("run %s invalid phase duration %s: %w", run.ID, name, err)
		}
	}
	if run.PrimaryClass == Pass && (len(run.ProtocolErrors) > 0 || len(run.BehaviorErrors) > 0) {
		return fmt.Errorf("run %s contains protocol or behavior errors", run.ID)
	}
	ledgerPath, framePath := filepath.Join(dir, "ledger.jsonl"), filepath.Join(dir, "frames.jsonl")
	_, fileLimit, limitErr := bundleArtifactLimits(filepath.Dir(filepath.Dir(dir)))
	if limitErr != nil {
		// Unit tests may validate an isolated run directory without a manifest;
		// retain the standalone writer default in that diagnostic-only case.
		fileLimit = DefaultMaxArtifactBytes
	}
	ledger, err := os.Open(ledgerPath)
	if err != nil {
		return fmt.Errorf("run %s missing ledger: %w", run.ID, err)
	}
	defer ledger.Close()
	if info, e := os.Stat(ledgerPath); e != nil || info.Size() > fileLimit {
		return fmt.Errorf("run %s ledger exceeds artifact budget", run.ID)
	}
	frames, err := os.Open(framePath)
	if err != nil {
		return fmt.Errorf("run %s missing frames: %w", run.ID, err)
	}
	defer frames.Close()
	if info, e := os.Stat(framePath); e != nil || info.Size() > fileLimit {
		return fmt.Errorf("run %s frames exceeds artifact budget", run.ID)
	}
	ledgerCount, covered := 0, 0
	scan := bufio.NewScanner(ledger)
	scan.Buffer(make([]byte, 64<<10), MaxJSONLineBytes)
	for scan.Scan() {
		var row LedgerRow
		if err := json.Unmarshal(scan.Bytes(), &row); err != nil {
			return fmt.Errorf("run %s malformed ledger: %w", run.ID, err)
		}
		if row.SchemaVersion != SchemaVersion {
			return fmt.Errorf("run %s ledger schema mismatch", run.ID)
		}
		if row.RunID != run.ID {
			return fmt.Errorf("run %s ledger cross-link mismatch", run.ID)
		}
		if !ValidateCoverage(row.Coverage) {
			return fmt.Errorf("run %s invalid coverage %q", run.ID, row.Coverage)
		}
		if row.ClientEventID == "" || row.RecipientID == "" {
			return fmt.Errorf("run %s incomplete ledger identity", run.ID)
		}
		if len(row.ExpectedQuerySet) == 0 || len(row.ExpectedRecipientSet) == 0 {
			return fmt.Errorf("run %s ledger row lacks expected query/recipient sets", run.ID)
		}
		if row.SubmittedAt.IsZero() {
			return fmt.Errorf("run %s ledger row lacks submission timestamp", run.ID)
		}
		if row.AcknowledgementAt.IsZero() {
			if run.PrimaryClass == Pass {
				return fmt.Errorf("run %s passing ledger row lacks acknowledgement timestamp", run.ID)
			}
		}
		if row.ErrorClass != "" {
			if run.PrimaryClass == Pass {
				return fmt.Errorf("run %s contains ledger error evidence", run.ID)
			}
		}
		if row.Coverage != "none" {
			if row.ExpectedMaterializedDigest == "" || row.ObservedMaterializedDigest == "" {
				return fmt.Errorf("run %s semantic digest evidence is incomplete", run.ID)
			}
			if row.Coverage == "coalesced" {
				if row.CoveredExpectedMaterializedDigest == "" || row.CoveredExpectedMaterializedDigest != row.ObservedMaterializedDigest || row.CoveredExpectedStateVersion <= row.ExpectedStateVersion || row.ObservedStateVersion < row.CoveredExpectedStateVersion {
					return fmt.Errorf("run %s invalid coalesced prefix/digest evidence", run.ID)
				}
			} else if row.ExpectedMaterializedDigest != row.ObservedMaterializedDigest {
				return fmt.Errorf("run %s semantic digest mismatch", run.ID)
			}
			if row.CoverAt.IsZero() && row.ConvergedAt.IsZero() {
				return fmt.Errorf("run %s covered row has no convergence timestamp", run.ID)
			}
			covered++
		} else if run.PrimaryClass == Pass {
			return fmt.Errorf("run %s passing ledger row has no coverage", run.ID)
		}
		ledgerCount++
	}
	if err := scan.Err(); err != nil {
		return err
	}
	frameCount := 0
	scan = bufio.NewScanner(frames)
	scan.Buffer(make([]byte, 64<<10), MaxJSONLineBytes)
	for scan.Scan() {
		var frame Frame
		if err := json.Unmarshal(scan.Bytes(), &frame); err != nil {
			return fmt.Errorf("run %s malformed frame: %w", run.ID, err)
		}
		if frame.SchemaVersion != SchemaVersion || frame.RunID != run.ID || frame.RecipientID == "" {
			return fmt.Errorf("run %s frame cross-link/schema mismatch", run.ID)
		}
		if err := ValidateMeasurement(frame.PayloadBytes); err != nil {
			return fmt.Errorf("run %s invalid frame bytes: %w", run.ID, err)
		}
		if frame.ErrorClass != "" && run.PrimaryClass == Pass {
			return fmt.Errorf("run %s contains frame error evidence", run.ID)
		}
		frameCount++
	}
	if err := scan.Err(); err != nil {
		return err
	}
	if run.PrimaryClass == Pass && (run.ExpectedLedgerRows <= 0 || run.ExpectedMutationRecipients <= 0 || ledgerCount != run.ExpectedLedgerRows || ledgerCount != run.ExpectedMutationRecipients || frameCount == 0 || covered == 0 || covered != ledgerCount) {
		return fmt.Errorf("run %s passing result lacks semantic evidence", run.ID)
	}
	return nil
}

func validateRunEvidenceBudget(run Run, manifest Manifest) error {
	runLegacy := run.EvidenceMaxFrames == 0 && run.EvidenceMaxBytes == 0 && run.EvidenceMaxRetained == 0
	manifestLegacy := manifest.EvidenceMaxFrames == 0 && manifest.EvidenceMaxBytes == 0 && manifest.EvidenceMaxRetained == 0
	if runLegacy || manifestLegacy {
		if runLegacy && manifestLegacy {
			return nil
		}
		return fmt.Errorf("run %s evidence budget is not completely bound to the manifest", run.ID)
	}
	if run.EvidenceMaxFrames <= 0 || run.EvidenceMaxBytes <= 0 || run.EvidenceMaxRetained <= 0 {
		return fmt.Errorf("run %s evidence budget is incomplete", run.ID)
	}
	if manifest.EvidenceMaxFrames <= 0 || manifest.EvidenceMaxBytes <= 0 || manifest.EvidenceMaxRetained <= 0 {
		return fmt.Errorf("run %s carries evidence budget without manifest binding", run.ID)
	}
	if run.EvidenceMaxFrames != manifest.EvidenceMaxFrames || run.EvidenceMaxBytes != manifest.EvidenceMaxBytes || run.EvidenceMaxRetained != manifest.EvidenceMaxRetained {
		return fmt.Errorf("run %s evidence budget does not match manifest", run.ID)
	}
	return nil
}

func validateManifestEvidenceBudget(manifest Manifest, plan Plan) error {
	if manifest.EvidenceMaxFrames == 0 && manifest.EvidenceMaxBytes == 0 && manifest.EvidenceMaxRetained == 0 {
		return nil
	}
	if manifest.EvidenceMaxFrames == 0 || manifest.EvidenceMaxBytes == 0 || manifest.EvidenceMaxRetained == 0 {
		return errors.New("manifest evidence budget is incomplete")
	}
	budget, err := ContractEvidenceBudget(manifest.Family, manifest.SubscriberScale, plan)
	if err != nil {
		return fmt.Errorf("recompute evidence budget: %w", err)
	}
	if manifest.EvidenceMaxFrames != budget.MaxFrames || manifest.EvidenceMaxBytes != budget.MaxBytes || manifest.EvidenceMaxRetained != budget.MaxRetainedFrames {
		return errors.New("manifest evidence budget does not match contract")
	}
	return nil
}

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

func LoadManifest(root string) (Manifest, error) {
	var m Manifest
	b, e := readBounded(filepath.Join(root, "manifest.json"), SmallArtifactBytes)
	if e != nil {
		return m, e
	}
	if e = json.Unmarshal(b, &m); e != nil {
		return m, e
	}
	return m, ValidateManifest(m)
}

// ReportFromArtifacts only reads the immutable local artifact bundle. It never
// contacts either target; failed attempts remain visible in the result.
func ReportFromArtifacts(root string) (Summary, error) {
	return reportFromArtifacts(root, true)
}
func reportFromArtifacts(root string, verify bool) (Summary, error) {
	m, e := LoadManifest(root)
	if e != nil {
		return Summary{}, e
	}
	if isThreeTargetManifest(m) {
		if verify {
			if e = VerifyChecksums(root); e != nil {
				return Summary{}, e
			}
		}
		return reportThreeTargetArtifacts(root, m)
	}
	if verify {
		e = VerifyChecksums(root)
	}
	if e != nil {
		return Summary{}, e
	}
	var environment Environment
	envBytes, e := readBounded(filepath.Join(root, "environment.json"), SmallArtifactBytes)
	if e != nil {
		return Summary{}, fmt.Errorf("environment.json is required: %w", e)
	}
	if e = json.Unmarshal(envBytes, &environment); e != nil || environment.SchemaVersion != SchemaVersion {
		return Summary{}, errors.New("invalid environment provenance")
	}
	// Bundles with a config hash carry the exact redacted config evidence. A
	// manifest hash alone is insufficient for offline replay, including for
	// synthetic runs where the equivalent minimal config is persisted.
	if m.ConfigHash != "" {
		configBytes, configErr := readBounded(filepath.Join(root, "config.json"), SmallArtifactBytes)
		if configErr != nil {
			return Summary{}, fmt.Errorf("config.json is required: %w", configErr)
		}
		var cfg LiveConfig
		if err := json.Unmarshal(configBytes, &cfg); err != nil {
			return Summary{}, fmt.Errorf("invalid bundled live config: %w", err)
		}
		if digest, err := DigestJSON(CanonicalConfigEvidence(cfg)); err != nil || !strings.EqualFold(digest, m.ConfigHash) {
			return Summary{}, errors.New("bundled live config hash mismatch")
		}
	}
	fixtureBytes, fixtureErr := readBounded(filepath.Join(root, "fixture.json"), SmallArtifactBytes)
	if fixtureErr != nil {
		return Summary{}, fmt.Errorf("fixture.json is required: %w", fixtureErr)
	}
	var fixture FixtureEvidence
	if err := json.Unmarshal(fixtureBytes, &fixture); err != nil {
		return Summary{}, fmt.Errorf("invalid bundled fixture evidence: %w", err)
	}
	if fixture.Family != canonicalBenchmarkFamily(m.Family) || fixture.Scale != m.SubscriberScale || fixture.Seed != m.Seed || len(fixture.EntityIDs) != fixture.SeedEntities || len(fixture.QueryAssignments) == 0 || len(fixture.InitialSemanticHashes) != len(fixture.QueryAssignments) {
		return Summary{}, errors.New("bundled fixture evidence does not match manifest")
	}
	expectedFixture, fixtureBuildErr := BuildFixtureEvidence(fixture.FixtureIDs, fixture.Family, fixture.Scale, fixture.Seed)
	if fixtureBuildErr != nil {
		return Summary{}, fmt.Errorf("reconstruct fixture evidence: %w", fixtureBuildErr)
	}
	actualCanonical, canonicalErr := CanonicalJSON(fixture)
	if canonicalErr != nil {
		return Summary{}, canonicalErr
	}
	expectedCanonical, canonicalErr := CanonicalJSON(expectedFixture)
	if canonicalErr != nil || !bytes.Equal(actualCanonical, expectedCanonical) {
		return Summary{}, errors.New("bundled fixture evidence reconstruction mismatch")
	}
	if digest, err := DigestJSON(fixture); err != nil || m.FixtureHash == "" || !strings.EqualFold(digest, m.FixtureHash) {
		return Summary{}, errors.New("bundled fixture hash mismatch")
	}
	planBytes, planErr := readBounded(filepath.Join(root, "plan.json"), SmallArtifactBytes)
	if planErr != nil {
		return Summary{}, fmt.Errorf("plan.json is required: %w", planErr)
	}
	var plan Plan
	if err := json.Unmarshal(planBytes, &plan); err != nil || plan.SchemaVersion != SchemaVersion {
		return Summary{}, errors.New("invalid benchmark plan evidence")
	}
	if plan.Seed != m.Seed || len(plan.Families) != 1 || canonicalBenchmarkFamily(plan.Families[0]) != canonicalBenchmarkFamily(m.Family) || len(plan.Scales) != 1 || plan.Scales[0] != m.SubscriberScale {
		return Summary{}, errors.New("benchmark plan does not match manifest")
	}
	if err := validateManifestEvidenceBudget(m, plan); err != nil {
		return Summary{}, err
	}
	derivedTotal, derivedFile, budgetErr := ContractArtifactBudget(m.Family, m.SubscriberScale, plan)
	if budgetErr != nil {
		return Summary{}, fmt.Errorf("recompute artifact budget: %w", budgetErr)
	}
	s := Summary{SchemaVersion: SchemaVersion, AggregatorVersion: AggregatorVersion, InputHashes: map[string]string{"manifest.json": fileHash(filepath.Join(root, "manifest.json"))}}
	missingProvenance := []string{}
	if m.ArtifactMaxTotalBytes != derivedTotal || m.ArtifactMaxFileBytes != derivedFile {
		return Summary{}, errors.New("manifest artifact budget evidence mismatch")
	}
	if m.V1SHA == "" {
		missingProvenance = append(missingProvenance, "v1 sha")
	}
	if m.V2SHA == "" {
		missingProvenance = append(missingProvenance, "v2 sha")
	}
	if m.SourceTree == "" {
		missingProvenance = append(missingProvenance, "source tree")
	}
	if m.SchemaHash == "" || m.FixtureHash == "" || m.ConfigHash == "" {
		missingProvenance = append(missingProvenance, "schema/fixture/config hashes")
	}
	if len(m.DatabaseIDs) < 2 || len(m.TargetProvenance) < 2 {
		missingProvenance = append(missingProvenance, "target database/provenance tuple")
	}
	if err := VerifyApproval(m); err != nil {
		missingProvenance = append(missingProvenance, "valid detached approval")
	}
	if err := VerifyContentRoot(root, m); err != nil {
		missingProvenance = append(missingProvenance, "verified immutable content root")
	}
	if len(missingProvenance) > 0 {
		s.ClaimGate.Reasons = append(s.ClaimGate.Reasons, "missing manifest provenance: "+strings.Join(missingProvenance, ", "))
	}
	var err error
	// Run files are the only source of observations. A missing run or metric is
	// deliberately not converted to zero; it leaves the claim gate ineligible.
	type cell struct {
		family string
		scale  int
		pairs  map[string]map[string]Run
	}
	cells := map[string]*cell{}
	runIDs := map[string]bool{}
	err = filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() || filepath.Base(path) != "run.json" {
			return nil
		}
		b, e := readBounded(path, SmallArtifactBytes)
		if e != nil {
			return e
		}
		var r Run
		if e = json.Unmarshal(b, &r); e != nil {
			return e
		}
		if r.SchemaVersion != SchemaVersion {
			return fmt.Errorf("mixed schema version in %s", path)
		}
		if !ValidateFailureClass(r.PrimaryClass) {
			return fmt.Errorf("invalid failure taxonomy in %s", path)
		}
		if e := validateRunEvidence(filepath.Dir(path), r); e != nil {
			return e
		}
		if e := validateRunEvidenceBudget(r, m); e != nil {
			return e
		}
		runIDs[r.ID] = true
		for name, metric := range r.Measurements {
			if e := ValidateMeasurement(metric); e != nil {
				return fmt.Errorf("invalid measurement %s in %s: %w", name, path, e)
			}
		}
		key := fmt.Sprintf("%s/%d", r.Family, r.Scale)
		c := cells[key]
		if c == nil {
			c = &cell{family: r.Family, scale: r.Scale, pairs: map[string]map[string]Run{}}
			cells[key] = c
		}
		if c.pairs[r.PairID] == nil {
			c.pairs[r.PairID] = map[string]Run{}
		}
		c.pairs[r.PairID][r.TargetID] = r
		return nil
	})
	if err != nil {
		return Summary{}, err
	}
	targetIDs := map[string]bool{}
	targetRecords := map[string]Target{}
	qualificationFailed := false
	qualificationFailure := ""
	for _, name := range []string{"v1", "v2", "v2_current", "v2-current"} {
		var target Target
		b, e := readBounded(filepath.Join(root, "targets", name+".json"), SmallArtifactBytes)
		if e != nil {
			continue
		}
		if e = json.Unmarshal(b, &target); e != nil {
			return Summary{}, e
		}
		if target.SchemaVersion != SchemaVersion || target.ID == "" || target.Role == "" || len(target.Qualification.Checks) == 0 {
			return Summary{}, fmt.Errorf("incomplete target qualification: %s", name)
		}
		if err := validateTargetBinding(target); err != nil {
			missingProvenance = append(missingProvenance, "target "+name+" kind binding")
		}
		if target.Revision == "" || target.DatabaseName == "" || target.PostgresVersion == "" || target.InvalidationMode == "" || target.MetadataHash == "" {
			missingProvenance = append(missingProvenance, "target "+name+" provenance")
		}
		mapped := TargetProvenanceTuple(target)
		if m.TargetProvenance[target.ID] != mapped || m.DatabaseIDs[target.ID] != target.DatabaseName {
			missingProvenance = append(missingProvenance, "target "+name+" manifest mapping")
		}
		if !target.Qualification.Passed {
			qualificationFailed = true
			if qualificationFailure == "" {
				qualificationFailure = target.ID + " qualification failed"
			}
		}
		targetIDs[target.ID] = true
		targetRecords[target.ID] = target
		if target.ID == "v1" && target.Revision != "" && (m.V1SHA == "" || m.V1SHA != target.Revision) {
			return Summary{}, errors.New("manifest v1_sha does not match qualified v1 revision")
		}
		if (target.ID == "v2" || target.ID == "v2_current" || target.ID == "v2-current") && target.Revision != "" && (m.V2SHA == "" || m.V2SHA != target.Revision) {
			return Summary{}, errors.New("manifest v2_sha does not match qualified v2 revision")
		}
		if target.ID == "v2_current" || target.ID == "v2-current" {
			targetRecords["v2"] = target
		}
	}
	if !targetIDs["v1"] || !(targetIDs["v2"] || targetIDs["v2_current"] || targetIDs["v2-current"]) {
		return Summary{}, errors.New("both target qualification records are required")
	}
	for _, c := range cells {
		for _, pairRuns := range c.pairs {
			for _, run := range pairRuns {
				if target, ok := targetRecords[run.TargetID]; ok {
					if err := validateProcessProvenance(filepath.Join(root, "runs", run.ID), run, target); err != nil {
						return Summary{}, err
					}
					if run.PrimaryClass == Pass && isLiveTarget(m, target) {
						if err := validateLiveResourceEvidence(filepath.Join(root, "runs", run.ID), run, target); err != nil {
							return Summary{}, err
						}
					}
				}
			}
		}
	}
	if len(missingProvenance) > 0 {
		s.ClaimGate.Reasons = append(s.ClaimGate.Reasons, "missing manifest/target provenance: "+strings.Join(missingProvenance, ", "))
	}
	expected := map[string]bool{}
	for i := 1; i <= 7; i++ {
		expected[fmt.Sprintf("%s-%02d-v1", m.PairID, i)] = true
		expected[fmt.Sprintf("%s-%02d-v2", m.PairID, i)] = true
	}
	if len(runIDs) != len(expected) {
		return Summary{}, fmt.Errorf("expected 14 run artifacts, found %d", len(runIDs))
	}
	for id := range expected {
		if !runIDs[id] {
			return Summary{}, fmt.Errorf("missing run artifact: %s", id)
		}
	}
	for id := range runIDs {
		if !expected[id] {
			return Summary{}, fmt.Errorf("unexpected run artifact: %s", id)
		}
	}
	keys := make([]string, 0, len(cells))
	for k := range cells {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		c := cells[key]
		var obs []PairObservation
		pairIDs := make([]string, 0, len(c.pairs))
		for pair := range c.pairs {
			pairIDs = append(pairIDs, pair)
		}
		sort.Strings(pairIDs)
		for _, pair := range pairIDs {
			targets := c.pairs[pair]
			o := PairObservation{PairID: pair}
			v1, ok1 := targets["v1"]
			v2, ok2 := targets["v2"]
			if !ok2 {
				v2, ok2 = targets["v2_current"]
			}
			if !ok2 {
				v2, ok2 = targets["v2-current"]
			}
			if !ok1 || !ok2 {
				o.Failed = true
				o.Failure = HarnessDefect
				obs = append(obs, o)
				continue
			}
			if v1.PrimaryClass == "" {
				o.Failed = true
				o.Failure = HarnessDefect
			} else if v1.PrimaryClass != Pass {
				o.Failed = true
				o.Failure = v1.PrimaryClass
			} else if v2.PrimaryClass == "" {
				o.Failed = true
				o.Failure = HarnessDefect
			} else if v2.PrimaryClass != Pass {
				o.Failed = true
				o.Failure = v2.PrimaryClass
			}
			metricName := "primary"
			if _, ok := v1.Measurements["throughput"]; ok {
				metricName = "throughput"
			}
			if !o.Failed {
				if m1, ok := v1.Measurements[metricName]; ok {
					if m2, ok2 := v2.Measurements[metricName]; ok2 && m1.Status == StatusValue && m2.Status == StatusValue && validPrimaryUnit(metricName, m1.Unit) && validPrimaryUnit(metricName, m2.Unit) && m1.Unit == m2.Unit {
						o.V1 = m1.Value
						o.V2 = m2.Value
					} else {
						o.Failed = true
						o.Failure = HarnessDefect
					}
				} else {
					o.Failed = true
					o.Failure = HarnessDefect
				}
			}
			obs = append(obs, o)
		}
		metricName := "primary"
		direction := LowerIsBetter
		if canonicalBenchmarkFamily(c.family) == string(benchharness.FamilyT) {
			// Family, rather than whichever pair happens to sort first, selects
			// the contract metric. This prevents a missing first attempt from
			// silently changing throughput aggregation into the primary alias.
			metricName, direction = "throughput", HigherIsBetter
		}
		cs := AggregateDirection(obs, metricName, endpointUnit(c.pairs, metricName), primaryDenominator(metricName), m.Seed, direction)
		cs.Family = c.family
		cs.Scale = c.scale
		cs.EndpointClaims = map[string]ClaimGate{"primary": cs.ClaimGate}
		endpointDirections := map[string]MetricDirection{
			"convergence_p99":                LowerIsBetter,
			"recipient_convergence_p99":      LowerIsBetter,
			"wire_bytes":                     LowerIsBetter,
			"cpu":                            LowerIsBetter,
			"peak_rss":                       LowerIsBetter,
			"peak_rss_per_active_subscriber": LowerIsBetter,
		}
		if canonicalBenchmarkFamily(c.family) == string(benchharness.FamilyT) {
			endpointDirections["throughput"] = HigherIsBetter
		}
		endpointNames := make([]string, 0, len(endpointDirections))
		for endpoint := range endpointDirections {
			endpointNames = append(endpointNames, endpoint)
		}
		sort.Strings(endpointNames)
		for _, endpoint := range endpointNames {
			endpointDirection := endpointDirections[endpoint]
			endpointObs := endpointObservations(c.pairs, endpoint)
			endpointSummary := AggregateDirection(endpointObs, endpoint, endpointUnit(c.pairs, endpoint), endpointDenominator(endpoint), m.Seed, endpointDirection)
			if metric, ok := endpointSummary.Metrics[endpoint]; ok {
				if cs.Metrics == nil {
					cs.Metrics = map[string]MetricSummary{}
				}
				cs.Metrics[endpoint] = metric
			}
			cs.EndpointClaims[endpoint] = endpointSummary.ClaimGate
		}
		if qualificationFailed {
			cs.ClaimGate.Eligible = false
			cs.ClaimGate.Reasons = append(cs.ClaimGate.Reasons, qualificationFailure)
		}
		// A cell claim is the conjunction of every endpoint claim. This keeps
		// missing p99, wire, CPU, or RSS evidence from being hidden by a valid
		// primary metric.
		for _, endpoint := range endpointNamesWithPrimary(cs.EndpointClaims) {
			gate := cs.EndpointClaims[endpoint]
			if !gate.Eligible {
				cs.ClaimGate.Eligible = false
				cs.ClaimGate.Reasons = append(cs.ClaimGate.Reasons, endpoint+" endpoint claim ineligible")
			}
		}
		s.Cells = append(s.Cells, cs)
		if !cs.ClaimGate.Eligible {
			s.ClaimGate.Reasons = append(s.ClaimGate.Reasons, cs.ClaimGate.Reasons...)
		}
	}
	s.ClaimGate.Eligible = len(s.Cells) > 0 && !qualificationFailed && len(missingProvenance) == 0
	if qualificationFailed {
		if qualificationFailure == "" {
			qualificationFailure = "target qualification failed"
		}
		s.ClaimGate.Reasons = append(s.ClaimGate.Reasons, qualificationFailure)
	}
	direction := ""
	for _, c := range s.Cells {
		if !c.ClaimGate.Eligible {
			s.ClaimGate.Eligible = false
		}
		if c.Sign.Direction != "" && c.Sign.Direction != "tie" {
			if direction == "" {
				direction = c.Sign.Direction
			} else if direction != c.Sign.Direction {
				s.ClaimGate.Eligible = false
				s.ClaimGate.Reasons = append(s.ClaimGate.Reasons, "contradictory effect directions across scales")
			}
		}
	}
	normalizeSummaryReasons(&s)
	return s, nil
}

func isThreeTargetManifest(m Manifest) bool {
	return len(m.TargetOrder) > 0 || len(m.Comparisons) > 0 || len(m.TargetRevisions) > 0
}

func endpointNamesWithPrimary(claims map[string]ClaimGate) []string {
	names := make([]string, 0, len(claims))
	for name := range claims {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func normalizeReasons(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, reason := range in {
		if reason != "" && !seen[reason] {
			seen[reason] = true
			out = append(out, reason)
		}
	}
	sort.Strings(out)
	return out
}

func normalizeSummaryReasons(s *Summary) {
	if s == nil {
		return
	}
	s.ClaimGate.Reasons = normalizeReasons(s.ClaimGate.Reasons)
	for i := range s.Cells {
		normalizeCellReasons(&s.Cells[i])
	}
	for i := range s.Comparisons {
		s.Comparisons[i].ClaimGate.Reasons = normalizeReasons(s.Comparisons[i].ClaimGate.Reasons)
		for j := range s.Comparisons[i].Cells {
			normalizeCellReasons(&s.Comparisons[i].Cells[j])
		}
	}
}

func normalizeFailureClasses(in []FailureClass) []FailureClass {
	if len(in) == 0 {
		return nil
	}
	seen := map[FailureClass]bool{}
	out := make([]FailureClass, 0, len(in))
	for _, item := range in {
		if !seen[item] {
			seen[item] = true
			out = append(out, item)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func endpointObservations(pairs map[string]map[string]Run, metric string) []PairObservation {
	obs := make([]PairObservation, 0, len(pairs))
	pairIDs := make([]string, 0, len(pairs))
	for pair := range pairs {
		pairIDs = append(pairIDs, pair)
	}
	sort.Strings(pairIDs)
	for _, pair := range pairIDs {
		targets := pairs[pair]
		o := PairObservation{PairID: pair}
		v1, ok1 := targets["v1"]
		v2, ok2 := targets["v2"]
		if !ok2 {
			v2, ok2 = targets["v2_current"]
		}
		if !ok2 {
			v2, ok2 = targets["v2-current"]
		}
		if !ok1 || !ok2 || v1.PrimaryClass != Pass || v2.PrimaryClass != Pass {
			o.Failed = true
			if ok1 && v1.PrimaryClass != Pass {
				o.Failure = v1.PrimaryClass
			} else if ok2 && v2.PrimaryClass != Pass {
				o.Failure = v2.PrimaryClass
			} else {
				o.Failure = HarnessDefect
			}
			obs = append(obs, o)
			continue
		}
		m1, has1 := v1.Measurements[metric]
		m2, has2 := v2.Measurements[metric]
		if !has1 || !has2 || !validMetricObservation(m1) || !validMetricObservation(m2) || !validEndpointUnit(metric, m1.Unit) || !validEndpointUnit(metric, m2.Unit) || m1.Unit != m2.Unit {
			o.Failed, o.Failure = true, HarnessDefect
		} else {
			o.V1, o.V2 = m1.Value, m2.Value
		}
		obs = append(obs, o)
	}
	return obs
}

func endpointUnit(pairs map[string]map[string]Run, metric string) string {
	ids := make([]string, 0, len(pairs))
	for id := range pairs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if run, ok := pairs[id]["v1"]; ok {
			if m, ok := run.Measurements[metric]; ok && m.Unit != "" {
				return m.Unit
			}
		}
	}
	return "contract unit"
}

func validEndpointUnit(metric, unit string) bool {
	if unit == "" {
		return false
	}
	if expected := map[string]string{
		"convergence_p99":                "global_semantic_convergence_p99_ms",
		"recipient_convergence_p99":      "recipient_semantic_convergence_p99_ms",
		"throughput":                     "tx/s",
		"wire_bytes":                     "wire_bytes_per_affected_recipient",
		"cpu":                            "core_ms_per_1000_affected_coverages",
		"peak_rss":                       "bytes",
		"peak_rss_per_active_subscriber": "bytes_per_active_subscriber",
	}[metric]; expected != "" {
		return unit == expected
	}
	return true
}

func validPrimaryUnit(metric, unit string) bool {
	if metric == "throughput" {
		return unit == "tx/s"
	}
	// Synthetic executors historically used ms for the primary alias; retain
	// that diagnostic representation while requiring the live contract unit.
	return unit == "global_semantic_convergence_p99_ms" || unit == "ms"
}

func primaryDenominator(metric string) string {
	if metric == "throughput" {
		return "measured seconds"
	}
	return "global semantic p99 samples"
}

func endpointDenominator(metric string) string {
	switch metric {
	case "convergence_p99":
		return "global semantic p99 samples"
	case "recipient_convergence_p99":
		return "affected-recipient coverages"
	case "throughput":
		return "measured seconds"
	case "wire_bytes":
		return "affected-recipient coverages"
	case "cpu":
		return "1,000 affected coverages"
	case "peak_rss", "peak_rss_per_active_subscriber":
		return "active subscribers"
	default:
		return "contract denominator"
	}
}

func validMetricObservation(m Measurement) bool {
	return m.Status == StatusValue || m.Status == StatusZero
}
func RenderMarkdown(s Summary) string {
	normalizeSummaryReasons(&s)
	var b strings.Builder
	b.WriteString("# Benchmark report\n\n")
	b.WriteString(fmt.Sprintf("Schema: `%s`; aggregator: `%s`\n\n", s.SchemaVersion, s.AggregatorVersion))
	if len(s.Comparisons) > 0 {
		for _, comparison := range s.Comparisons {
			b.WriteString(fmt.Sprintf("# Comparison `%s`: `%s` (%s) → `%s` (%s)\n\n", comparison.ID, comparison.BaselineID, comparison.BaselineRevision, comparison.CandidateID, comparison.CandidateRevision))
			b.WriteString(fmt.Sprintf("Comparison claim eligible: %t\n\n", comparison.ClaimGate.Eligible))
			for _, cell := range comparison.Cells {
				renderCell(&b, cell)
			}
		}
		return b.String()
	}
	for _, c := range s.Cells {
		renderCell(&b, c)
	}
	return b.String()
}

func renderCell(b *strings.Builder, c CellSummary) {
	b.WriteString(fmt.Sprintf("## %s @ %d subscribers\n\nAttempts: %d\n\n", c.Family, c.Scale, c.Attempts))
	if len(c.Ratios) > 0 {
		b.WriteString(fmt.Sprintf("Median paired ratio: %.4f; 95%% CI [%.4f, %.4f]\n\n", c.CIpoint(), c.CI.Lower, c.CI.Upper))
	}
	b.WriteString(fmt.Sprintf("Claim eligible: %t\n", c.ClaimGate.Eligible))
	if len(c.ClaimGate.Reasons) > 0 {
		b.WriteString("Reasons: " + strings.Join(c.ClaimGate.Reasons, "; ") + "\n")
	}
	endpoints := make([]string, 0, len(c.EndpointClaims))
	for endpoint := range c.EndpointClaims {
		endpoints = append(endpoints, endpoint)
	}
	sort.Strings(endpoints)
	for _, endpoint := range endpoints {
		gate := c.EndpointClaims[endpoint]
		b.WriteString(fmt.Sprintf("Endpoint %s claim eligible: %t", endpoint, gate.Eligible))
		if len(gate.Reasons) > 0 {
			b.WriteString(" (" + strings.Join(gate.Reasons, "; ") + ")")
		}
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
}
func (c CellSummary) CIpoint() float64 {
	if len(c.Ratios) == 0 {
		return 0
	}
	xs := append([]float64(nil), c.Ratios...)
	sort.Float64s(xs)
	return mathExpMedian(xs)
}
func mathExpMedian(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	return exp(xs[len(xs)/2])
}

// split out for a compact report package without exposing math in the schema.
func exp(x float64) float64 { return math.Exp(x) }
func fileHash(path string) string {
	sum, e := hashFileBounded(path, SmallArtifactBytes)
	if e != nil {
		return ""
	}
	return hex.EncodeToString(sum[:])
}
