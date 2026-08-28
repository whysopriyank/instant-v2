package benchrun

import (
	"math"
	"math/rand"
	"sort"
)

const AggregatorVersion = "benchrun-stats-v1"

type MetricDirection string

const (
	LowerIsBetter  MetricDirection = "lower_is_better"
	HigherIsBetter MetricDirection = "higher_is_better"
)

func LogRatios(v1, v2 []float64) []float64 {
	n := len(v1)
	if len(v2) < n {
		n = len(v2)
	}
	out := make([]float64, 0, n)
	for i := 0; i < n; i++ {
		if v1[i] > 0 && v2[i] > 0 && !math.IsNaN(v1[i]) && !math.IsNaN(v2[i]) {
			out = append(out, math.Log(v2[i]/v1[i]))
		}
	}
	return out
}
func Median(xs []float64) float64 {
	if len(xs) == 0 {
		return math.NaN()
	}
	ys := append([]float64(nil), xs...)
	sort.Float64s(ys)
	return ys[len(ys)/2]
}
func Quantile(xs []float64, q float64) float64 {
	if len(xs) == 0 {
		return math.NaN()
	}
	ys := append([]float64(nil), xs...)
	sort.Float64s(ys)
	if q <= 0 {
		return ys[0]
	}
	if q >= 1 {
		return ys[len(ys)-1]
	}
	return ys[int(math.Ceil(q*float64(len(ys)))-1)]
}

// BootstrapCI returns a percentile CI for the median paired log-ratio. The
// deterministic seed and resample count are part of the report provenance.
func BootstrapCI(logRatios []float64, resamples int, seed int64) CI {
	if resamples < 10000 {
		resamples = 10000
	}
	if len(logRatios) == 0 {
		return CI{Level: .95, Resamples: resamples, Seed: seed}
	}
	r := rand.New(rand.NewSource(seed))
	values := make([]float64, resamples)
	sample := make([]float64, len(logRatios))
	for i := 0; i < resamples; i++ {
		for j := range sample {
			sample[j] = logRatios[r.Intn(len(logRatios))]
		}
		values[i] = Median(sample)
	}
	return CI{Lower: math.Exp(Quantile(values, .025)), Upper: math.Exp(Quantile(values, .975)), Level: .95, Resamples: resamples, Seed: seed}
}
func SignTest(logRatios []float64) SignResult {
	var pos, neg, ties int
	for _, x := range logRatios {
		switch {
		case x > 0:
			pos++
		case x < 0:
			neg++
		default:
			ties++
		}
	}
	n := pos + neg
	p := 1.0
	if n > 0 {
		k := pos
		if neg < k {
			k = neg
		}
		p = 2 * binomialTail(n, k)
		if p > 1 {
			p = 1
		}
	}
	dir := "tie"
	if pos > neg {
		dir = "v2_higher"
	} else if neg > pos {
		dir = "v2_lower"
	}
	return SignResult{Positive: pos, Negative: neg, Ties: ties, PValue: p, Direction: dir}
}
func binomialTail(n, k int) float64 {
	if k < 0 {
		return 0
	}
	sum := 0.0
	for i := 0; i <= k; i++ {
		sum += math.Exp(logChoose(n, i) - float64(n)*math.Ln2)
	}
	return sum
}
func logChoose(n, k int) float64 {
	a, _ := math.Lgamma(float64(n + 1))
	b, _ := math.Lgamma(float64(k + 1))
	c, _ := math.Lgamma(float64(n - k + 1))
	return a - b - c
}

type PairObservation struct {
	PairID  string
	V1      float64
	V2      float64
	Failed  bool
	Failure FailureClass
}

func Aggregate(obs []PairObservation, metric, unit, denominator string, seed int64) CellSummary {
	return AggregateDirection(obs, metric, unit, denominator, seed, LowerIsBetter)
}
func AggregateDirection(obs []PairObservation, metric, unit, denominator string, seed int64, direction MetricDirection) CellSummary {
	ratios := make([]float64, 0, len(obs))
	failures := []FailureClass{}
	for _, o := range obs {
		if o.Failed {
			failures = append(failures, o.Failure)
			continue
		}
		r := LogRatios([]float64{o.V1}, []float64{o.V2})
		ratios = append(ratios, r...)
	}
	ci := BootstrapCI(ratios, 10000, seed)
	gate := ClaimGate{Eligible: true}
	if len(obs) != 7 {
		gate.Reasons = append(gate.Reasons, "exactly seven attempts required")
	}
	if len(ratios) != 7 {
		gate.Reasons = append(gate.Reasons, "missing metric observations")
	}
	if len(failures) > 0 {
		gate.Reasons = append(gate.Reasons, "attempt failure present")
	}
	if ci.Lower <= 1 && ci.Upper >= 1 {
		gate.Reasons = append(gate.Reasons, "paired confidence interval includes 1.0")
	}
	s := SignTest(ratios)
	medianOK := (direction == LowerIsBetter && Median(ratios) < 0) || (direction == HigherIsBetter && Median(ratios) > 0)
	if len(ratios) > 0 && !medianOK {
		gate.Reasons = append(gate.Reasons, "median ratio is not in the declared direction")
	}
	signOK := (direction == LowerIsBetter && s.Direction == "v2_lower") || (direction == HigherIsBetter && s.Direction == "v2_higher")
	if len(ratios) > 0 && !signOK {
		gate.Reasons = append(gate.Reasons, "paired sign direction does not match the declared direction")
	}
	if len(obs) != 7 || len(ratios) != 7 || len(failures) != 0 || (ci.Lower <= 1 && ci.Upper >= 1) || !medianOK || !signOK {
		gate.Eligible = false
	}
	median := Missing("ratio")
	p99 := Missing("ratio")
	absV1, absV2 := Missing(unit), Missing(unit)
	if len(ratios) > 0 {
		median = Measurement{Status: StatusValue, Value: math.Exp(Median(ratios)), Unit: "ratio"}
		p99 = Measurement{Status: StatusValue, Value: math.Exp(quantile(ratios, .99)), Unit: "ratio"}
		v1, v2 := make([]float64, 0, len(obs)), make([]float64, 0, len(obs))
		for _, o := range obs {
			if !o.Failed {
				v1 = append(v1, o.V1)
				v2 = append(v2, o.V2)
			}
		}
		if len(v1) > 0 {
			absV1 = Measurement{Status: StatusValue, Value: Median(v1), Unit: unit}
			absV2 = Measurement{Status: StatusValue, Value: Median(v2), Unit: unit}
		}
	}
	return CellSummary{Attempts: len(obs), Ratios: ratios, CI: ci, Sign: s, Direction: direction, Metrics: map[string]MetricSummary{metric: {Name: metric, Unit: unit, Denominator: denominator, Samples: len(ratios), Median: median, P99: p99, AbsoluteV1: absV1, AbsoluteV2: absV2}}, Failures: failures, ClaimGate: gate}
}

func quantile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	x := append([]float64(nil), values...)
	sort.Float64s(x)
	if p <= 0 {
		return x[0]
	}
	if p >= 1 {
		return x[len(x)-1]
	}
	idx := int(math.Ceil(p*float64(len(x)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(x) {
		idx = len(x) - 1
	}
	return x[idx]
}
