package benchrun

import (
	"sort"

	"github.com/instant-v2/instant-v2/internal/benchharness"
)

func aggregatePairReport(input validatedReport) Summary {
	m, cells := input.manifest, input.cells
	qualificationFailed, qualificationFailure := input.qualificationFailed, input.qualificationFailure
	missingProvenance := input.missingProvenance
	s := input.summary()
	keys := make([]string, 0, len(cells))
	for k := range cells {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		c := cells[key]
		cs := aggregatePairCell(c, m.Seed, qualificationFailed, qualificationFailure)
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
	return s
}

func aggregateThreeTargetReport(input validatedReport) Summary {
	m, cells := input.manifest, input.cells
	qualificationFailed, qualificationFailure := input.qualificationFailed, input.qualificationFailure
	missingProvenance := input.missingProvenance
	s := input.summary()
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
	return s
}

func aggregateThreeTargetComparison(pairs map[string]map[string]Run, family string, scale int, comparison ComparisonSpec, seed int64) CellSummary {
	metricName := "primary"
	direction := LowerIsBetter
	if canonicalBenchmarkFamily(family) == string(benchharness.FamilyT) {
		metricName = "throughput"
		direction = HigherIsBetter
	}
	obs := primaryThreeTargetObservations(pairs, comparison, metricName)
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

func aggregatePairCell(c *reportCell, seed int64, qualificationFailed bool, qualificationFailure string) CellSummary {
	obs := primaryPairObservations(c.pairs)
	metricName := "primary"
	direction := LowerIsBetter
	if canonicalBenchmarkFamily(c.family) == string(benchharness.FamilyT) {
		// Family, rather than whichever pair happens to sort first, selects
		// the contract metric. This prevents a missing first attempt from
		// silently changing throughput aggregation into the primary alias.
		metricName, direction = "throughput", HigherIsBetter
	}
	cs := AggregateDirection(obs, metricName, endpointUnit(c.pairs, metricName), primaryDenominator(metricName), seed, direction)
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
		endpointSummary := AggregateDirection(endpointObs, endpoint, endpointUnit(c.pairs, endpoint), endpointDenominator(endpoint), seed, endpointDirection)
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
	return cs
}
