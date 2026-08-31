package benchharness

import (
	"testing"
)

func TestEvidenceBudgetForH300AdmitsObservedFullSnapshotTraffic(t *testing.T) {
	workload, err := NewWorkload(FamilyH, 300, 17)
	if err != nil {
		t.Fatal(err)
	}
	budget, err := EvidenceBudgetForWorkload(workload, 180*8)
	if err != nil {
		t.Fatal(err)
	}
	const observedBytes int64 = 85_902_606_202
	if budget.MaxBytes <= observedBytes {
		t.Fatalf("H300 derived evidence budget=%d does not admit observed cumulative wire bytes=%d", budget.MaxBytes, observedBytes)
	}
	if budget.MaxFrames != defaultEvidenceMaxFrames || budget.MaxRetainedFrames != defaultEvidenceMaxRetainedFrames {
		t.Fatalf("derived budget changed frame/sample hard bounds: %#v", budget)
	}
	if budget.MaxBytes > MaxAbsoluteEvidenceBytes {
		t.Fatalf("derived H300 budget crossed hard byte ceiling: %d", budget.MaxBytes)
	}
}

func TestEvidenceBudgetForWorkloadUsesBucketCardinalityAndRejectsOverflow(t *testing.T) {
	h, err := NewWorkload(FamilyH, 300, 17)
	if err != nil {
		t.Fatal(err)
	}
	x, err := NewWorkload(FamilyX, 300, 17)
	if err != nil {
		t.Fatal(err)
	}
	hBudget, err := EvidenceBudgetForWorkload(h, 0)
	if err != nil {
		t.Fatal(err)
	}
	xBudget, err := EvidenceBudgetForWorkload(x, 0)
	if err != nil {
		t.Fatal(err)
	}
	if xBudget.MaxBytes >= hBudget.MaxBytes {
		t.Fatalf("bucketed X workload did not use its smaller recipient cardinality: H=%d X=%d", hBudget.MaxBytes, xBudget.MaxBytes)
	}
	x.Fixture.Queries[0].MatchAll = true
	customFixtureBudget, err := EvidenceBudgetForWorkload(x, 0)
	if err != nil {
		t.Fatal(err)
	}
	if customFixtureBudget.MaxBytes != hBudget.MaxBytes {
		t.Fatalf("non-canonical X fixture incorrectly used bucket optimization: H=%d custom=%d", hBudget.MaxBytes, customFixtureBudget.MaxBytes)
	}
	if _, err := EvidenceBudgetForWorkload(h, int(^uint(0)>>1)); err == nil {
		t.Fatal("overflowing workload cardinality was accepted")
	}
	warmupBudget, err := EvidenceBudgetForWorkloadWithWarmup(h, 180*8, 100)
	if err != nil {
		t.Fatal(err)
	}
	if warmupBudget.MaxBytes <= hBudget.MaxBytes {
		t.Fatalf("warm-up traffic was omitted from cumulative budget: measured=%d with_warmup=%d", hBudget.MaxBytes, warmupBudget.MaxBytes)
	}
	h.TxRate = 1.5
	if _, err := EvidenceBudgetForWorkloadWithWarmup(h, 0, 1); err == nil {
		t.Fatal("non-integral workload rate bypassed fail-closed budget derivation")
	}
}

func TestEvidenceBudgetIncludesBoundedLifecycleSources(t *testing.T) {
	w, err := NewWorkload(FamilyH, 300, 17)
	if err != nil {
		t.Fatal(err)
	}
	const mutations = 180 * 8
	budget, err := EvidenceBudgetForWorkload(w, mutations)
	if err != nil {
		t.Fatal(err)
	}
	queryCount := int64(w.Subscribers)
	refreshFrames := int64(mutations) * queryCount
	// The shared bound must cover the worst transport. SSE consumes a
	// handshake before the protocol init acknowledgement; WS has the smaller
	// shape, so both use the SSE-safe two-frame session setup allowance.
	readinessFrames := int64(2*maxEvidenceReadinessAttempts) * (2*queryCount + 2)
	lifecycleFrames := readinessFrames + 4*queryCount + 4*queryCount + 21 + int64(mutations) + evidenceLifecycleControlFrames
	want := (refreshFrames + lifecycleFrames) * maxEvidenceFrameBytes
	if want < defaultEvidenceMaxBytes {
		want = defaultEvidenceMaxBytes
	}
	if budget.MaxBytes != want {
		t.Fatalf("lifecycle-derived budget=%d want=%d (refresh=%d lifecycle=%d)", budget.MaxBytes, want, refreshFrames, lifecycleFrames)
	}
	s, err := NewWorkload(FamilyS, 300, 17)
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewWorkload(FamilyR, 300, 17)
	if err != nil {
		t.Fatal(err)
	}
	sBudget, err := EvidenceBudgetForWorkload(s, 0)
	if err != nil {
		t.Fatal(err)
	}
	rBudget, err := EvidenceBudgetForWorkload(r, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rBudget.MaxBytes <= sBudget.MaxBytes {
		t.Fatalf("R reconnect schedule was omitted from lifecycle budget: S=%d R=%d", sBudget.MaxBytes, rBudget.MaxBytes)
	}
}

func TestEvidenceBudgetIncludesSaturationWriterSetups(t *testing.T) {
	w, err := NewWorkload(FamilyT, 300, 17)
	if err != nil {
		t.Fatal(err)
	}
	budget, err := EvidenceBudgetForWorkload(w, 0)
	if err != nil {
		t.Fatal(err)
	}
	const mutations = 4096
	queries := int64(w.Subscribers)
	readiness := int64(2*maxEvidenceReadinessAttempts) * (2*queries + 2)
	lifecycle := readiness + 4*queries + 4*queries + 21 + mutations + 16 + evidenceLifecycleControlFrames
	perBucket := (queries + int64(w.Cohorts) - 1) / int64(w.Cohorts)
	want := (int64(mutations)*perBucket + lifecycle) * maxEvidenceFrameBytes
	if budget.MaxBytes != want {
		t.Fatalf("saturation writer/session lifecycle budget=%d want=%d", budget.MaxBytes, want)
	}
}

func TestEvidenceBudgetMaximumContractWorkloadsStayUnderHardCeiling(t *testing.T) {
	for _, family := range contractFamilies {
		t.Run(string(family), func(t *testing.T) {
			w, err := NewWorkload(family, 2000, 17)
			if err != nil {
				t.Fatal(err)
			}
			budget, err := EvidenceBudgetForWorkload(w, 0)
			if err != nil {
				t.Fatal(err)
			}
			if budget.MaxBytes > MaxAbsoluteEvidenceBytes {
				t.Fatalf("maximum contract workload crossed evidence hard ceiling: %d", budget.MaxBytes)
			}
		})
	}
}

func TestEvidenceBudgetOperatorOverrideCannotExceedHardCeilings(t *testing.T) {
	for name, budget := range map[string]EvidenceBudget{
		"frames":   {MaxFrames: defaultEvidenceMaxFrames + 1},
		"bytes":    {MaxBytes: MaxAbsoluteEvidenceBytes + 1},
		"retained": {MaxRetainedFrames: defaultEvidenceMaxRetainedFrames + 1},
		"negative": {MaxBytes: -1},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateEvidenceBudget(budget); err == nil {
				t.Fatalf("operator evidence budget %#v bypassed hard ceiling", budget)
			}
		})
	}
}
