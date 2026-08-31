package benchrun

import (
	"sort"
)

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

func primaryPairObservations(pairs map[string]map[string]Run) []PairObservation {
	var obs []PairObservation
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
	return obs
}

func primaryThreeTargetObservations(pairs map[string]map[string]Run, comparison ComparisonSpec, metricName string) []PairObservation {
	obs := make([]PairObservation, 0, len(pairs))
	for _, pairID := range sortedPairIDs(pairs) {
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
	return obs
}
