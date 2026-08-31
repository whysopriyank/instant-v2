package benchharness

import (
	"context"
	"testing"
	"time"
)

func TestLedgerRejectedAckCannotCoverBufferedObservation(t *testing.T) {
	l := NewLedger()
	k := LedgerKey{PairID: "p", RunID: "r", WriterID: "w", RecipientID: "c", ClientEventID: "e"}
	if err := l.AddExpected(k, Mutation{EventID: "e"}); err != nil {
		t.Fatal(err)
	}
	l.SetExpectation(k, []string{"q"}, []string{"c"}, "expected", 1)
	if err := l.Observe(k, Observation{ObservedDigest: "expected", ExpectedDigest: "expected", LatestExpectedDigest: "ahead", Prefix: 2, ExpectedPrefix: 1, At: time.Now(), Applicable: true}); err != nil {
		t.Fatal(err)
	}
	l.Acknowledged(k, Ack{Accepted: false}, time.Now(), nil)
	row, _ := l.Expected(k)
	if row.Coverage != CoverageMissing || row.ErrorClass != ErrorTarget || row.CoverAt.IsZero() == false {
		t.Fatalf("rejected ack covered row: %#v", row)
	}
}

func TestLedgerFinalSnapshotDivergenceInvalidatesCoverage(t *testing.T) {
	l := NewLedger()
	k := LedgerKey{PairID: "p", RunID: "r", WriterID: "w", RecipientID: "c", ClientEventID: "e"}
	if err := l.AddExpected(k, Mutation{EventID: "e"}); err != nil {
		t.Fatal(err)
	}
	l.SetExpectation(k, []string{"q"}, []string{"c"}, "expected", 1)
	l.Acknowledged(k, Ack{Accepted: true}, time.Now(), nil)
	_ = l.Observe(k, Observation{ObservedDigest: "expected", ExpectedDigest: "expected", Prefix: 1, ExpectedPrefix: 1, At: time.Now(), Applicable: true})
	l.Invalidate(k, ErrorTarget, "final snapshot digest does not match prefix oracle")
	row, _ := l.Expected(k)
	if row.Coverage != CoverageMissing || row.ErrorClass != ErrorTarget {
		t.Fatalf("divergence did not invalidate coverage: %#v", row)
	}
}

func TestLedgerCoverageAndRefreshBeforeAck(t *testing.T) {
	l := NewLedger()
	k := LedgerKey{PairID: "p", RunID: "r", WriterID: "w", RecipientID: "c", ClientEventID: "e"}
	if err := l.AddExpected(k, Mutation{EventID: "e"}); err != nil {
		t.Fatal(err)
	}
	submitted := time.Now()
	l.Submitted(k, submitted)
	receipt := submitted.Add(time.Millisecond)
	if err := l.Observe(k, Observation{ExpectedDigest: "d", ObservedDigest: "d", ExpectedPrefix: 1, Prefix: 1, ProvesIntermediate: true, Applicable: true, At: receipt}); err != nil {
		t.Fatal(err)
	}
	l.Acknowledged(k, Ack{ServerTransactionID: "tx", Accepted: true}, receipt.Add(time.Millisecond), nil)
	rows := l.Rows()
	if len(rows) != 1 || rows[0].Coverage != CoverageExact || !rows[0].RefreshBeforeAck {
		t.Fatalf("row %#v", rows)
	}
}

func TestLedgerBuffersAheadAndFirstValidProofWins(t *testing.T) {
	l := NewLedger()
	k := LedgerKey{PairID: "p", RunID: "r", WriterID: "w", RecipientID: "c", ClientEventID: "e"}
	if err := l.AddExpected(k, Mutation{EventID: "e"}); err != nil {
		t.Fatal(err)
	}
	l.SetExpectation(k, []string{"q"}, []string{"c"}, "prefix-1", 1)
	ahead := time.Now()
	if err := l.Observe(k, Observation{ExpectedDigest: "prefix-1", LatestExpectedDigest: "prefix-2", ObservedDigest: "prefix-2", Prefix: 2, ExpectedPrefix: 1, At: ahead, Applicable: true}); err != nil {
		t.Fatal(err)
	}
	row, _ := l.Expected(k)
	if row.Coverage != CoverageMissing || !row.BufferedSnapshotAhead || !row.CoverAt.IsZero() {
		t.Fatalf("ahead receipt covered too early: %#v", row)
	}
	l.Acknowledged(k, Ack{ServerTransactionID: "tx", Accepted: true}, ahead.Add(time.Millisecond), nil)
	row, _ = l.Expected(k)
	if row.Coverage != CoverageCoalesced || row.CoverAt.IsZero() || row.ExpectedStateVersion != 1 || row.ExpectedMaterializedDigest != "prefix-1" || row.CoveredExpectedStateVersion != 2 || row.CoveredExpectedMaterializedDigest != "prefix-2" {
		t.Fatalf("buffered receipt not reconciled: %#v", row)
	}

	l2 := NewLedger()
	k2 := k
	k2.ClientEventID = "e2"
	if err := l2.AddExpected(k2, Mutation{EventID: "e2"}); err != nil {
		t.Fatal(err)
	}
	l2.SetExpectation(k2, []string{"q"}, []string{"c"}, "good", 1)
	_ = l2.Observe(k2, Observation{ExpectedDigest: "good", ObservedDigest: "bad", Prefix: 1, ExpectedPrefix: 1, Applicable: true, At: ahead})
	_ = l2.Observe(k2, Observation{ExpectedDigest: "good", ObservedDigest: "good", Prefix: 1, ExpectedPrefix: 1, Applicable: true, At: ahead.Add(time.Millisecond)})
	row, _ = l2.Expected(k2)
	if row.Coverage != CoverageConvergedWithoutIntermediate || row.CoverAt.IsZero() {
		t.Fatalf("valid proof did not win after invalid: %#v", row)
	}
}

func TestLedgerCoalescedProofRejectsMismatchedLaterDigestWithoutOverwritingOriginal(t *testing.T) {
	l := NewLedger()
	k := LedgerKey{PairID: "p", RunID: "r", WriterID: "w", RecipientID: "c", ClientEventID: "e"}
	if err := l.AddExpected(k, Mutation{EventID: "e"}); err != nil {
		t.Fatal(err)
	}
	l.SetExpectation(k, []string{"q"}, []string{"c"}, "prefix-1", 1)
	l.Acknowledged(k, Ack{Accepted: true}, time.Now(), nil)
	if err := l.Observe(k, Observation{ExpectedDigest: "prefix-1", LatestExpectedDigest: "prefix-2", ObservedDigest: "wrong-prefix-2", Prefix: 2, ExpectedPrefix: 1, Applicable: true, At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	row, _ := l.Expected(k)
	if row.Coverage != CoverageMissing || row.ExpectedStateVersion != 1 || row.ExpectedMaterializedDigest != "prefix-1" || row.CoveredExpectedStateVersion != 0 || row.CoveredExpectedMaterializedDigest != "" {
		t.Fatalf("mismatched coalesced proof changed ledger evidence: %#v", row)
	}
}

func TestExecuteUsesBlockingScheduleAndLedger(t *testing.T) {
	w, _ := NewWorkload(FamilyH, 300, 11)
	ctx := context.Background()
	var submitted []string
	result, err := Execute(ctx, RunPlan{PairID: "p", RunID: "r", WriterID: "w", Workload: w, Start: time.Now().Add(2 * time.Millisecond), Mutations: 2, Rate: 1000}, RunHooks{Submit: func(_ context.Context, m Mutation) (Ack, error) {
		submitted = append(submitted, m.EventID)
		return Ack{Accepted: true, ServerTransactionID: "tx"}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(submitted) != 2 || result.Submitted != 2 || result.Acknowledged != 2 {
		t.Fatalf("result %#v submitted %v", result, submitted)
	}
	if result.ExpectedMutations != 2 || result.ExpectedRows != 600 || result.MeasuredStartedAt.IsZero() || result.MeasuredFinishedAt.IsZero() || result.MeasuredFinishedAt.Before(result.MeasuredStartedAt) {
		t.Fatalf("measurement contract fields missing: %#v", result)
	}
	if len(result.Slips) != 2 || len(result.Ledger.Rows()) != 600 {
		t.Fatalf("schedule/ledger sizes %d/%d", len(result.Slips), len(result.Ledger.Rows()))
	}
}

func TestExecuteWarmupIsBaselineNotMeasuredLedger(t *testing.T) {
	w, _ := NewWorkload(FamilyH, 300, 29)
	var submitted []Mutation
	result, err := Execute(context.Background(), RunPlan{PairID: "p", RunID: "r", WriterID: "w", Workload: w, Mutations: 1, Rate: 1000, WarmupMutations: 2, WarmupStart: 1_000_000}, RunHooks{Submit: func(_ context.Context, m Mutation) (Ack, error) {
		submitted = append(submitted, m)
		return Ack{Accepted: true}, nil
	}, FinalSnapshot: func(_ context.Context, _, query string) (Materialized, error) {
		o := NewPrefixOracle(w.Fixture)
		for i := 0; i < 2; i++ {
			o.Append(w.Mutation(1_000_000 + int64(i)))
		}
		o.Append(w.Mutation(1))
		return o.Materialize(query, 3)
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(submitted) != 1 || result.WarmupMutations != 2 || len(result.Ledger.Rows()) != 300 {
		t.Fatalf("warmup contaminated measured run: submitted=%d warmup=%d rows=%d", len(submitted), result.WarmupMutations, len(result.Ledger.Rows()))
	}
	for _, row := range result.Ledger.Rows() {
		if row.ExpectedStateVersion != 3 {
			t.Fatalf("measured prefix omitted warmup baseline: %d", row.ExpectedStateVersion)
		}
	}
}
