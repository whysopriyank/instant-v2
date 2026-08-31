package benchharness

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestExecuteMeasuredBoundaryCallbacksAreExactAndFailClosed(t *testing.T) {
	workload := Workload{Family: FamilyH, Subscribers: 1, Fixture: Fixture{Queries: []Query{{ID: "q", MatchAll: true}}}}
	t.Run("exact-order", func(t *testing.T) {
		var events []string
		var started, finished time.Time
		result, err := Execute(context.Background(), RunPlan{
			PairID: "p", RunID: "r", Workload: workload, Mutations: 1, Rate: 1000,
			OnMeasuredStart: func(at time.Time) error { events = append(events, "start"); started = at; return nil },
			OnMeasuredEnd:   func(at time.Time) error { events = append(events, "end"); finished = at; return nil },
		}, RunHooks{Submit: func(context.Context, Mutation) (Ack, error) { return Ack{Accepted: true}, nil }})
		if err != nil {
			t.Fatal(err)
		}
		if len(events) != 2 || events[0] != "start" || events[1] != "end" || !started.Equal(result.MeasuredStartedAt) || !finished.Equal(result.MeasuredFinishedAt) || finished.Before(started) {
			t.Fatalf("measurement callbacks did not bracket measured work exactly: events=%v result=%#v", events, result)
		}
	})
	t.Run("start-failure", func(t *testing.T) {
		var submits atomic.Int64
		var ends atomic.Int64
		result, err := Execute(context.Background(), RunPlan{
			PairID: "p", RunID: "r", Workload: workload, Mutations: 1, Rate: 1000,
			OnMeasuredStart: func(time.Time) error { return errors.New("start sample failed") },
			OnMeasuredEnd:   func(time.Time) error { ends.Add(1); return nil },
		}, RunHooks{Submit: func(context.Context, Mutation) (Ack, error) { submits.Add(1); return Ack{Accepted: true}, nil }})
		if err == nil || !strings.Contains(err.Error(), "measured-start callback") || submits.Load() != 0 || ends.Load() != 1 || result.MeasuredStartedAt.IsZero() || result.MeasuredFinishedAt.IsZero() {
			t.Fatalf("start callback failure was not fail-closed: err=%v submits=%d ends=%d result=%#v", err, submits.Load(), ends.Load(), result)
		}
	})
	t.Run("end-failure", func(t *testing.T) {
		result, err := Execute(context.Background(), RunPlan{
			PairID: "p", RunID: "r", Workload: workload, Mutations: 1, Rate: 1000,
			OnMeasuredEnd: func(time.Time) error { return errors.New("end sample failed") },
		}, RunHooks{Submit: func(context.Context, Mutation) (Ack, error) { return Ack{Accepted: true}, nil }})
		if err == nil || !strings.Contains(err.Error(), "measured-end callback") || result.MeasuredFinishedAt.Before(result.MeasuredStartedAt) {
			t.Fatalf("end callback failure was not fail-closed: err=%v result=%#v", err, result)
		}
	})
}

func TestExecuteReconcilesSemanticReceipt(t *testing.T) {
	w, _ := NewWorkload(FamilyH, 300, 13)
	receipts := make(chan Receipt, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result, err := Execute(ctx, RunPlan{PairID: "p", RunID: "r", WriterID: "w", Workload: w, Start: time.Now(), Mutations: 1, Rate: 1000, ConvergenceGrace: time.Second}, RunHooks{Submit: func(_ context.Context, m Mutation) (Ack, error) {
		oracle := NewPrefixOracle(w.Fixture)
		oracle.Append(m)
		state, _ := oracle.Materialize(w.Fixture.Queries[0].ID, 1)
		receipts <- Receipt{PairID: "p", RunID: "r", WriterID: "w", RecipientID: "client-" + w.Fixture.Queries[0].ID, QueryID: w.Fixture.Queries[0].ID, ClientEventID: m.EventID, Observed: state, Prefix: 1, ProvesIntermediate: true, At: time.Now()}
		close(receipts)
		return Ack{Accepted: true}, nil
	}, Receipts: receipts})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		rows := result.Ledger.Rows()
		if len(rows) > 0 && rows[0].Coverage == CoverageExact {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("receipt did not reconcile: %#v", result.Ledger.Rows()[0])
}

func TestExecuteRunsFinalSnapshotForEveryQuery(t *testing.T) {
	w, _ := NewWorkload(FamilyH, 300, 19)
	var snapshots atomic.Int64
	result, err := Execute(context.Background(), RunPlan{PairID: "p", RunID: "r", WriterID: "w", Workload: w, Mutations: 1, Rate: 1000}, RunHooks{Submit: func(_ context.Context, m Mutation) (Ack, error) { return Ack{Accepted: true}, nil }, FinalSnapshot: func(_ context.Context, recipient, query string) (Materialized, error) {
		snapshots.Add(1)
		o := NewPrefixOracle(w.Fixture)
		o.Append(w.Mutation(1))
		return o.Materialize(query, 1)
	}})
	if err != nil {
		t.Fatal(err)
	}
	if snapshots.Load() != int64(w.Subscribers) {
		t.Fatalf("snapshot calls %d", snapshots.Load())
	}
	for _, row := range result.Ledger.Rows() {
		if row.Coverage != CoverageConvergedWithoutIntermediate {
			t.Fatalf("final coverage %#v", row.Coverage)
		}
	}
	if result.MeasuredStartedAt.IsZero() || result.MeasuredFinishedAt.IsZero() || !result.MeasuredFinishedAt.Before(result.FinishedAt) {
		t.Fatalf("measured phase was not closed before finalization: measured=%v..%v finished=%v", result.MeasuredStartedAt, result.MeasuredFinishedAt, result.FinishedAt)
	}
}

func TestSaturationUsesEightClosedLoopWriters(t *testing.T) {
	w, _ := NewWorkload(FamilyT, 300, 17)
	var mu sync.Mutex
	seen := make(map[string]int)
	result, err := Execute(context.Background(), RunPlan{PairID: "p", RunID: "r", Workload: w, Mutations: 16}, RunHooks{Submit: func(_ context.Context, m Mutation) (Ack, error) {
		mu.Lock()
		seen[m.EventID]++
		mu.Unlock()
		return Ack{Accepted: true, ServerTransactionID: fmt.Sprint(m.Sequence)}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Submitted != 16 || result.Acknowledged != 16 {
		t.Fatalf("saturation result %#v", result)
	}
	if len(seen) != 16 {
		t.Fatalf("duplicate/missing events %d", len(seen))
	}
	writers := map[string]bool{}
	for _, row := range result.Ledger.Rows() {
		writers[row.WriterID] = true
	}
	if len(writers) != 8 {
		t.Fatalf("writer count %d", len(writers))
	}
}

func TestSaturationUsesIndependentWriterHook(t *testing.T) {
	w, _ := NewWorkload(FamilyT, 300, 31)
	var mu sync.Mutex
	seen := map[int]int{}
	result, err := Execute(context.Background(), RunPlan{PairID: "p", RunID: "r", Workload: w, Mutations: 32}, RunHooks{SubmitWriter: func(_ context.Context, writer int, m Mutation) (Ack, error) {
		mu.Lock()
		seen[writer]++
		mu.Unlock()
		return Ack{Accepted: true, ServerTransactionID: fmt.Sprint(m.Sequence)}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Acknowledged != 32 || len(seen) < 2 {
		t.Fatalf("independent writer hook was not used: acknowledged=%d writers=%v", result.Acknowledged, seen)
	}
}

func TestSaturationCancellationIsNotSilentWhenContractIncomplete(t *testing.T) {
	w, _ := NewWorkload(FamilyT, 300, 33)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int64
	result, err := Execute(ctx, RunPlan{PairID: "p", RunID: "r", Workload: w, Mutations: 32}, RunHooks{SubmitWriter: func(callCtx context.Context, _ int, m Mutation) (Ack, error) {
		if calls.Add(1) == 1 {
			cancel()
			return Ack{Accepted: true, ServerTransactionID: fmt.Sprint(m.Sequence)}, nil
		}
		return Ack{}, callCtx.Err()
	}})
	if err == nil {
		t.Fatalf("incomplete cancelled saturation returned success: %#v", result)
	}
	if result.ExpectedMutations != 32 || result.ExpectedRows <= 0 || result.Acknowledged >= result.ExpectedMutations {
		t.Fatalf("cancellation contract fields/result mismatch: %#v", result)
	}
}

func TestSaturationRejectsUnorderableAcknowledgements(t *testing.T) {
	w, _ := NewWorkload(FamilyT, 300, 43)
	_, err := Execute(context.Background(), RunPlan{PairID: "p", RunID: "r", Workload: w, Mutations: 8}, RunHooks{SubmitWriter: func(context.Context, int, Mutation) (Ack, error) {
		return Ack{Accepted: true}, nil
	}})
	var unsupported *UnsupportedTargetError
	if !errors.As(err, &unsupported) || unsupported.Check != "t_tx_order" {
		t.Fatalf("unorderable T acknowledgement was accepted: %v", err)
	}
}

func TestClientBehaviorsRunAffectedCohortConcurrently(t *testing.T) {
	w := Workload{Family: FamilyR, Subscribers: 30}
	var active, maxActive atomic.Int64
	started := time.Now()
	runClientBehaviorsAt(context.Background(), w, 30, time.Millisecond, func(context.Context, int, int, ClientBehavior) error {
		current := active.Add(1)
		for {
			previous := maxActive.Load()
			if current <= previous || maxActive.CompareAndSwap(previous, current) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		active.Add(-1)
		return nil
	}, nil)
	if maxActive.Load() < 3 {
		t.Fatalf("affected reconnect cohort was serialized: maximum concurrent callbacks=%d", maxActive.Load())
	}
	if elapsed := time.Since(started); elapsed > 150*time.Millisecond {
		t.Fatalf("behavior ticks were excessively serialized: %v", elapsed)
	}
}

func TestSaturationPrefixFollowsAcknowledgedCommitOrder(t *testing.T) {
	w, _ := NewWorkload(FamilyT, 300, 23)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var once sync.Once
	result, err := Execute(context.Background(), RunPlan{PairID: "p", RunID: "r", Workload: w, Mutations: 8}, RunHooks{Submit: func(_ context.Context, m Mutation) (Ack, error) {
		if m.Sequence == 1 {
			once.Do(func() { close(firstStarted) })
			<-releaseFirst
		}
		if m.Sequence == 2 {
			<-firstStarted
			close(releaseFirst)
			return Ack{Accepted: true, ServerTransactionID: "2"}, nil
		}
		return Ack{Accepted: true, ServerTransactionID: fmt.Sprint(m.Sequence)}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	var p1, p2 int64
	for _, row := range result.Ledger.Rows() {
		switch row.ClientEventID {
		case "bench/23/000001":
			p1 = row.ExpectedStateVersion
		case "bench/23/000002":
			p2 = row.ExpectedStateVersion
		}
	}
	if p1 == 0 || p2 == 0 || p1 >= p2 {
		t.Fatalf("prefixes were not assigned by numeric server transaction order: sequence1=%d sequence2=%d", p1, p2)
	}
}
