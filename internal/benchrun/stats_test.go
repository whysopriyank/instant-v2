package benchrun

import (
	"testing"
)

func TestStatsSyntheticAndFailureRetention(t *testing.T) {
	obs := []PairObservation{{V1: 10, V2: 5}, {V1: 10, V2: 5}, {V1: 10, V2: 5}, {V1: 10, V2: 5}, {V1: 10, V2: 5}, {V1: 10, V2: 5}, {V1: 10, V2: 5, Failed: true, Failure: TargetTimeout}}
	s := Aggregate(obs, "p99", "ms", "recipient", 9)
	if s.Attempts != 7 || len(s.Failures) != 1 {
		t.Fatalf("attempt/failure retention: %+v", s)
	}
	if s.Sign.Direction != "v2_lower" {
		t.Fatalf("sign result: %+v", s.Sign)
	}
	if s.CI.Resamples != 10000 {
		t.Fatalf("bootstrap count: %+v", s.CI)
	}
	good := make([]PairObservation, 7)
	for i := range good {
		good[i] = PairObservation{V1: 10, V2: 5}
	}
	gs := Aggregate(good, "p99", "ms", "recipient", 9)
	if !gs.ClaimGate.Eligible {
		t.Fatalf("uniform lower result should be eligible: %+v", gs.ClaimGate)
	}
	higher := make([]PairObservation, 7)
	for i := range higher {
		higher[i] = PairObservation{V1: 5, V2: 10}
	}
	hs := AggregateDirection(higher, "throughput", "tx/s", "committed", 9, HigherIsBetter)
	if !hs.ClaimGate.Eligible || hs.Sign.Direction != "v2_higher" {
		t.Fatalf("uniform higher result should be eligible: %+v", hs)
	}
}
