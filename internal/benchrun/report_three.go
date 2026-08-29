package benchrun

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/instant-v2/instant-v2/internal/benchharness"
)

// reportThreeTargetArtifacts is deliberately separate from the historical
// two-target reader. This keeps the old v1/v2 aliases intact while making the
// three-target comparison surfaces explicit and independently gated.
func reportThreeTargetArtifacts(root string, m Manifest) (Summary, error) {
	if err := validateThreeTargetManifestShape(m); err != nil {
		return Summary{}, err
	}
	if err := validateThreeTargetSharedEvidence(root, m); err != nil {
		return Summary{}, err
	}
	s := Summary{
		SchemaVersion:     SchemaVersion,
		AggregatorVersion: AggregatorVersion,
		InputHashes:       map[string]string{"manifest.json": fileHash(filepath.Join(root, "manifest.json"))},
	}
	missingProvenance := []string{}
	for _, field := range []struct {
		value string
		name  string
	}{
		{m.SourceTree, "source tree"},
		{m.SchemaHash, "schema hash"},
		{m.FixtureHash, "fixture hash"},
		{m.ConfigHash, "config hash"},
	} {
		if field.value == "" {
			missingProvenance = append(missingProvenance, field.name)
		}
	}
	if len(m.DatabaseIDs) < len(threeTargetIDs) || len(m.TargetProvenance) < len(threeTargetIDs) {
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

	targetRecords, qualificationFailed, qualificationFailure, err := loadThreeTargetRecords(root, m, &missingProvenance)
	if err != nil {
		return Summary{}, err
	}
	if len(missingProvenance) > 0 && len(s.ClaimGate.Reasons) == 0 {
		s.ClaimGate.Reasons = append(s.ClaimGate.Reasons, "missing manifest/target provenance: "+strings.Join(missingProvenance, ", "))
	}

	type triadCell struct {
		family string
		scale  int
		pairs  map[string]map[string]Run
	}
	cells := map[string]*triadCell{}
	runIDs := map[string]bool{}
	err = filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() || filepath.Base(path) != "run.json" {
			return nil
		}
		b, readErr := readBounded(path, SmallArtifactBytes)
		if readErr != nil {
			return readErr
		}
		var run Run
		if readErr = json.Unmarshal(b, &run); readErr != nil {
			return readErr
		}
		if run.SchemaVersion != SchemaVersion {
			return fmt.Errorf("mixed schema version in %s", path)
		}
		if !ValidateFailureClass(run.PrimaryClass) {
			return fmt.Errorf("invalid failure taxonomy in %s", path)
		}
		if !isThreeTargetID(run.TargetID) {
			return fmt.Errorf("run %s references unsupported target %q", run.ID, run.TargetID)
		}
		if target, ok := targetRecords[run.TargetID]; !ok {
			return fmt.Errorf("run %s references missing target %q", run.ID, run.TargetID)
		} else {
			if run.TargetRevision != target.Revision {
				return fmt.Errorf("run %s target revision does not match %s", run.ID, run.TargetID)
			}
			if run.ScheduleBlock < 1 || run.ScheduleBlock > ThreeTargetBlocks {
				return fmt.Errorf("run %s has invalid schedule block %d", run.ID, run.ScheduleBlock)
			}
			block := m.TargetOrder[run.ScheduleBlock-1]
			inBlock := false
			for _, id := range block.Order {
				if id == run.TargetID {
					inBlock = true
					break
				}
			}
			if !inBlock || run.PairID != fmt.Sprintf("%s-%02d", m.PairID, run.ScheduleBlock) || run.ID != fmt.Sprintf("%s-%s", run.PairID, run.TargetID) || run.Family != m.Family || run.Scale != m.SubscriberScale || run.Seed != m.Seed {
				return fmt.Errorf("run %s does not match its manifest schedule", run.ID)
			}
		}
		if readErr = validateRunEvidence(filepath.Dir(path), run); readErr != nil {
			return readErr
		}
		for name, metric := range run.Measurements {
			if readErr = ValidateMeasurement(metric); readErr != nil {
				return fmt.Errorf("invalid measurement %s in %s: %w", name, path, readErr)
			}
		}
		runIDs[run.ID] = true
		key := fmt.Sprintf("%s/%d", run.Family, run.Scale)
		cell := cells[key]
		if cell == nil {
			cell = &triadCell{family: run.Family, scale: run.Scale, pairs: map[string]map[string]Run{}}
			cells[key] = cell
		}
		if cell.pairs[run.PairID] == nil {
			cell.pairs[run.PairID] = map[string]Run{}
		}
		if _, exists := cell.pairs[run.PairID][run.TargetID]; exists {
			return fmt.Errorf("duplicate run for pair %s target %s", run.PairID, run.TargetID)
		}
		cell.pairs[run.PairID][run.TargetID] = run
		return nil
	})
	if err != nil {
		return Summary{}, err
	}
	if err := validateThreeTargetRunSet(m, runIDs); err != nil {
		return Summary{}, err
	}

	cellKeys := make([]string, 0, len(cells))
	for key := range cells {
		cellKeys = append(cellKeys, key)
	}
	sort.Strings(cellKeys)
	for _, key := range cellKeys {
		cell := cells[key]
		comparisonSummaries := make([]ComparisonSummary, 0, len(m.Comparisons))
		for _, comparison := range m.Comparisons {
			cs := aggregateThreeTargetComparison(cell.pairs, cell.family, cell.scale, comparison, m.Seed)
			comparisonSummaries = append(comparisonSummaries, ComparisonSummary{ID: comparison.ID, BaselineID: comparison.BaselineID, CandidateID: comparison.CandidateID, BaselineRevision: comparison.BaselineRevision, CandidateRevision: comparison.CandidateRevision, Cells: []CellSummary{cs}, ClaimGate: cs.ClaimGate})
			s.Cells = append(s.Cells, cs)
		}
		s.Comparisons = append(s.Comparisons, comparisonSummaries...)
	}
	if qualificationFailed {
		s.ClaimGate.Reasons = append(s.ClaimGate.Reasons, qualificationFailure)
	}
	s.ClaimGate.Eligible = len(s.Cells) > 0 && !qualificationFailed && len(missingProvenance) == 0
	for _, comparison := range s.Comparisons {
		if !comparison.ClaimGate.Eligible {
			s.ClaimGate.Eligible = false
			s.ClaimGate.Reasons = append(s.ClaimGate.Reasons, comparison.ID+" comparison claim ineligible")
		}
	}
	normalizeSummaryReasons(&s)
	return s, nil
}

func validateThreeTargetSharedEvidence(root string, m Manifest) error {
	var environment Environment
	b, err := readBounded(filepath.Join(root, "environment.json"), SmallArtifactBytes)
	if err != nil {
		return fmt.Errorf("environment.json is required: %w", err)
	}
	if err = json.Unmarshal(b, &environment); err != nil || environment.SchemaVersion != SchemaVersion {
		return errors.New("invalid environment provenance")
	}
	if m.ConfigHash != "" {
		b, err = readBounded(filepath.Join(root, "config.json"), SmallArtifactBytes)
		if err != nil {
			return fmt.Errorf("config.json is required: %w", err)
		}
		var cfg LiveConfig
		if err = json.Unmarshal(b, &cfg); err != nil {
			return fmt.Errorf("invalid bundled live config: %w", err)
		}
		if digest, digestErr := DigestJSON(CanonicalConfigEvidence(cfg)); digestErr != nil || !strings.EqualFold(digest, m.ConfigHash) {
			return errors.New("bundled live config hash mismatch")
		}
	}
	b, err = readBounded(filepath.Join(root, "fixture.json"), SmallArtifactBytes)
	if err != nil {
		return fmt.Errorf("fixture.json is required: %w", err)
	}
	var fixture FixtureEvidence
	if err = json.Unmarshal(b, &fixture); err != nil {
		return fmt.Errorf("invalid bundled fixture evidence: %w", err)
	}
	if fixture.Family != canonicalBenchmarkFamily(m.Family) || fixture.Scale != m.SubscriberScale || fixture.Seed != m.Seed || len(fixture.EntityIDs) != fixture.SeedEntities || len(fixture.QueryAssignments) == 0 || len(fixture.InitialSemanticHashes) != len(fixture.QueryAssignments) {
		return errors.New("bundled fixture evidence does not match manifest")
	}
	expectedFixture, err := BuildFixtureEvidence(fixture.FixtureIDs, fixture.Family, fixture.Scale, fixture.Seed)
	if err != nil {
		return fmt.Errorf("reconstruct fixture evidence: %w", err)
	}
	actual, err := CanonicalJSON(fixture)
	if err != nil {
		return err
	}
	expected, err := CanonicalJSON(expectedFixture)
	if err != nil || !bytes.Equal(actual, expected) {
		return errors.New("bundled fixture evidence reconstruction mismatch")
	}
	if digest, digestErr := DigestJSON(fixture); digestErr != nil || m.FixtureHash == "" || !strings.EqualFold(digest, m.FixtureHash) {
		return errors.New("bundled fixture hash mismatch")
	}
	b, err = readBounded(filepath.Join(root, "plan.json"), SmallArtifactBytes)
	if err != nil {
		return fmt.Errorf("plan.json is required: %w", err)
	}
	var plan Plan
	if err = json.Unmarshal(b, &plan); err != nil || plan.SchemaVersion != SchemaVersion {
		return errors.New("invalid benchmark plan evidence")
	}
	if plan.Seed != m.Seed || len(plan.Families) != 1 || canonicalBenchmarkFamily(plan.Families[0]) != canonicalBenchmarkFamily(m.Family) || len(plan.Scales) != 1 || plan.Scales[0] != m.SubscriberScale {
		return errors.New("benchmark plan does not match manifest")
	}
	derivedTotal, derivedFile, err := ContractArtifactBudgetForTargets(m.Family, m.SubscriberScale, plan, len(threeTargetIDs))
	if err != nil {
		return fmt.Errorf("recompute artifact budget: %w", err)
	}
	if m.ArtifactMaxTotalBytes != derivedTotal || m.ArtifactMaxFileBytes != derivedFile {
		return errors.New("manifest artifact budget evidence mismatch")
	}
	return nil
}

func loadThreeTargetRecords(root string, m Manifest, missing *[]string) (map[string]Target, bool, string, error) {
	records := make(map[string]Target, len(threeTargetIDs))
	qualificationFailed := false
	failure := ""
	for _, id := range threeTargetIDs {
		b, err := readBounded(filepath.Join(root, "targets", id+".json"), SmallArtifactBytes)
		if err != nil {
			return nil, false, "", fmt.Errorf("missing target qualification: %s", id)
		}
		var target Target
		if err = json.Unmarshal(b, &target); err != nil {
			return nil, false, "", err
		}
		if target.SchemaVersion != SchemaVersion || target.ID != id || target.Role != id || len(target.Qualification.Checks) == 0 {
			return nil, false, "", fmt.Errorf("incomplete target qualification: %s", id)
		}
		if target.Revision == "" || target.DatabaseName == "" || target.PostgresVersion == "" || target.InvalidationMode == "" || target.MetadataHash == "" {
			*missing = append(*missing, "target "+id+" provenance")
		}
		mapped := strings.Join([]string{target.Revision, target.DirtyHash, target.OutputPlugin, target.InvalidationMode, target.DatabaseName, target.PostgresVersion, target.ProcessPIDEnv, target.ProcessPIDFile, target.FixturePath, target.FixtureHash, target.ExecutablePath, target.ExecutableHash, target.MetadataHash}, "|")
		if m.TargetProvenance[id] != mapped || m.DatabaseIDs[id] != target.DatabaseName || m.TargetRevisions[id] != target.Revision {
			*missing = append(*missing, "target "+id+" manifest mapping")
		}
		if !target.Qualification.Passed {
			qualificationFailed = true
			if failure == "" {
				failure = id + " qualification failed"
			}
		}
		records[id] = target
	}
	return records, qualificationFailed, failure, nil
}

func validateThreeTargetRunSet(m Manifest, runIDs map[string]bool) error {
	expected := make(map[string]bool, ThreeTargetBlocks*len(threeTargetIDs))
	for _, block := range m.TargetOrder {
		for _, targetID := range block.Order {
			expected[fmt.Sprintf("%s-%02d-%s", m.PairID, block.Index, targetID)] = true
		}
	}
	if len(expected) != ThreeTargetBlocks*len(threeTargetIDs) || len(runIDs) != len(expected) {
		return fmt.Errorf("expected %d three-target run artifacts, found %d", len(expected), len(runIDs))
	}
	for id := range expected {
		if !runIDs[id] {
			return fmt.Errorf("missing run artifact: %s", id)
		}
	}
	for id := range runIDs {
		if !expected[id] {
			return fmt.Errorf("unexpected run artifact: %s", id)
		}
	}
	return nil
}

func aggregateThreeTargetComparison(pairs map[string]map[string]Run, family string, scale int, comparison ComparisonSpec, seed int64) CellSummary {
	obs := make([]PairObservation, 0, len(pairs))
	pairIDs := make([]string, 0, len(pairs))
	for pairID := range pairs {
		pairIDs = append(pairIDs, pairID)
	}
	sort.Strings(pairIDs)
	metricName := "primary"
	direction := LowerIsBetter
	if canonicalBenchmarkFamily(family) == string(benchharness.FamilyT) {
		metricName = "throughput"
		direction = HigherIsBetter
	}
	for _, pairID := range pairIDs {
		pairRuns := pairs[pairID]
		baseline, baselineOK := pairRuns[comparison.BaselineID]
		candidate, candidateOK := pairRuns[comparison.CandidateID]
		o := PairObservation{PairID: pairID}
		if !baselineOK || !candidateOK {
			o.Failed, o.Failure = true, HarnessDefect
		} else if baseline.PrimaryClass != Pass {
			o.Failed, o.Failure = true, baseline.PrimaryClass
		} else if candidate.PrimaryClass != Pass {
			o.Failed, o.Failure = true, candidate.PrimaryClass
		} else {
			m1, ok1 := baseline.Measurements[metricName]
			m2, ok2 := candidate.Measurements[metricName]
			if !ok1 || !ok2 || m1.Status != StatusValue || m2.Status != StatusValue || !validPrimaryUnit(metricName, m1.Unit) || !validPrimaryUnit(metricName, m2.Unit) || m1.Unit != m2.Unit {
				o.Failed, o.Failure = true, HarnessDefect
			} else {
				o.V1, o.V2 = m1.Value, m2.Value
			}
		}
		obs = append(obs, o)
	}
	cs := AggregateDirection(obs, metricName, triadMetricUnit(obs, pairs, comparison, metricName), primaryDenominator(metricName), seed, direction)
	cs.ComparisonID = comparison.ID
	cs.BaselineID = comparison.BaselineID
	cs.CandidateID = comparison.CandidateID
	cs.BaselineRevision = comparison.BaselineRevision
	cs.CandidateRevision = comparison.CandidateRevision
	cs.Family = family
	cs.Scale = scale
	if metric, ok := cs.Metrics[metricName]; ok {
		metric.AbsoluteBaseline = metric.AbsoluteV1
		metric.AbsoluteCandidate = metric.AbsoluteV2
		metric.AbsoluteV1 = Missing(metric.Unit)
		metric.AbsoluteV2 = Missing(metric.Unit)
		cs.Metrics[metricName] = metric
	}
	endpointDirections := map[string]MetricDirection{
		"convergence_p99":                LowerIsBetter,
		"recipient_convergence_p99":      LowerIsBetter,
		"wire_bytes":                     LowerIsBetter,
		"cpu":                            LowerIsBetter,
		"peak_rss":                       LowerIsBetter,
		"peak_rss_per_active_subscriber": LowerIsBetter,
	}
	if canonicalBenchmarkFamily(family) == string(benchharness.FamilyT) {
		endpointDirections["throughput"] = HigherIsBetter
	}
	cs.EndpointClaims = map[string]ClaimGate{"primary": cs.ClaimGate}
	for endpoint, endpointDirection := range endpointDirections {
		endpointObs := triadEndpointObservations(pairs, endpoint, comparison)
		endpointSummary := AggregateDirection(endpointObs, endpoint, triadMetricUnit(endpointObs, pairs, comparison, endpoint), endpointDenominator(endpoint), seed, endpointDirection)
		if metric, ok := endpointSummary.Metrics[endpoint]; ok {
			metric.AbsoluteBaseline = metric.AbsoluteV1
			metric.AbsoluteCandidate = metric.AbsoluteV2
			metric.AbsoluteV1 = Missing(metric.Unit)
			metric.AbsoluteV2 = Missing(metric.Unit)
			if cs.Metrics == nil {
				cs.Metrics = map[string]MetricSummary{}
			}
			cs.Metrics[endpoint] = metric
		}
		cs.EndpointClaims[endpoint] = endpointSummary.ClaimGate
	}
	for endpoint, gate := range cs.EndpointClaims {
		if !gate.Eligible {
			cs.ClaimGate.Eligible = false
			cs.ClaimGate.Reasons = append(cs.ClaimGate.Reasons, endpoint+" endpoint claim ineligible")
		}
	}
	normalizeCellReasons(&cs)
	return cs
}

func triadEndpointObservations(pairs map[string]map[string]Run, metric string, comparison ComparisonSpec) []PairObservation {
	obs := make([]PairObservation, 0, len(pairs))
	pairIDs := make([]string, 0, len(pairs))
	for pairID := range pairs {
		pairIDs = append(pairIDs, pairID)
	}
	sort.Strings(pairIDs)
	for _, pairID := range pairIDs {
		pairRuns := pairs[pairID]
		baseline, baselineOK := pairRuns[comparison.BaselineID]
		candidate, candidateOK := pairRuns[comparison.CandidateID]
		o := PairObservation{PairID: pairID}
		if !baselineOK || !candidateOK || baseline.PrimaryClass != Pass || candidate.PrimaryClass != Pass {
			o.Failed = true
			o.Failure = HarnessDefect
			if baselineOK && baseline.PrimaryClass != Pass {
				o.Failure = baseline.PrimaryClass
			} else if candidateOK && candidate.PrimaryClass != Pass {
				o.Failure = candidate.PrimaryClass
			}
			obs = append(obs, o)
			continue
		}
		m1, ok1 := baseline.Measurements[metric]
		m2, ok2 := candidate.Measurements[metric]
		if !ok1 || !ok2 || !validMetricObservation(m1) || !validMetricObservation(m2) || !validEndpointUnit(metric, m1.Unit) || !validEndpointUnit(metric, m2.Unit) || m1.Unit != m2.Unit {
			o.Failed, o.Failure = true, HarnessDefect
		} else {
			o.V1, o.V2 = m1.Value, m2.Value
		}
		obs = append(obs, o)
	}
	return obs
}

func triadMetricUnit(obs []PairObservation, pairs map[string]map[string]Run, comparison ComparisonSpec, metric string) string {
	for _, pairID := range sortedPairIDs(pairs) {
		pairRuns := pairs[pairID]
		for _, id := range []string{comparison.BaselineID, comparison.CandidateID} {
			if run, ok := pairRuns[id]; ok {
				if measurement, ok := run.Measurements[metric]; ok && measurement.Unit != "" {
					return measurement.Unit
				}
			}
		}
	}
	return "contract unit"
}

func sortedPairIDs(pairs map[string]map[string]Run) []string {
	ids := make([]string, 0, len(pairs))
	for id := range pairs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func normalizeCellReasons(cell *CellSummary) {
	if cell == nil {
		return
	}
	cell.ClaimGate.Reasons = normalizeReasons(cell.ClaimGate.Reasons)
	cell.Failures = normalizeFailureClasses(cell.Failures)
	for name, gate := range cell.EndpointClaims {
		gate.Reasons = normalizeReasons(gate.Reasons)
		cell.EndpointClaims[name] = gate
	}
}
