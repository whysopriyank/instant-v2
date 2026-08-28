package benchharness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
)

func TestContractWorkloadsDeterministicMatrix(t *testing.T) {
	a, b := ContractWorkloads(7), ContractWorkloads(7)
	if len(a) != 24 || len(b) != len(a) {
		t.Fatalf("matrix lengths %d/%d", len(a), len(b))
	}
	for i := range a {
		if a[i].Family != b[i].Family || a[i].Subscribers != b[i].Subscribers || a[i].Mutation(3) != b[i].Mutation(3) {
			t.Fatalf("cell %d is not deterministic", i)
		}
	}
	if got := a[0].Mutation(1).EventID; got != "bench/7/000001" {
		t.Fatalf("event id %q", got)
	}
	for _, cell := range a {
		for id := range cell.Fixture.Entities {
			if _, err := uuid.Parse(id); err != nil {
				t.Fatalf("fixture entity id %q is not product-compatible UUID: %v", id, err)
			}
		}
		got := cell.Mutation(1).EntityID
		if _, err := uuid.Parse(got); err != nil {
			t.Fatalf("mutation entity id %q is not product-compatible UUID: %v", got, err)
		}
	}
	first, _ := NewWorkload(FamilyX, 300, 7)
	second, _ := NewWorkload(FamilyX, 300, 7)
	if first.Mutation(1).EntityID != second.Mutation(1).EntityID {
		t.Fatal("deterministic UUID entity id changed between identical manifests")
	}
	if _, err := NewWorkload(Family("bad"), 300, 1); err == nil {
		t.Fatal("unknown family accepted")
	}
	w, _ := NewWorkload(FamilyS, 300, 1)
	if got := w.Behavior(0, 10); !got.PauseReads || got.PauseFor != 2*time.Second {
		t.Fatalf("slow-reader behavior %#v", got)
	}
	if got := w.Behavior(1, 10); got.PauseReads {
		t.Fatal("non-cohort client paused")
	}
	r, _ := NewWorkload(FamilyR, 300, 1)
	if got := r.Behavior(0, 30); !got.Reconnect || got.Backoff < 500*time.Millisecond || got.Backoff > 1500*time.Millisecond {
		t.Fatalf("reconnect behavior %#v", got)
	}
}

func TestBlockingSchedulerDoesNotDropAndRecordsSlip(t *testing.T) {
	s, err := NewBlockingScheduler(100, time.Now().Add(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	items := []Mutation{{Sequence: 1}, {Sequence: 2}, {Sequence: 3}}
	var got []int64
	if err := s.Run(context.Background(), items, func(_ context.Context, m Mutation) error {
		got = append(got, m.Sequence)
		if m.Sequence == 1 {
			time.Sleep(25 * time.Millisecond)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Fatalf("submitted sequence %v", got)
	}
	slips := s.Slips()
	if len(slips) != 3 {
		t.Fatalf("slip count %d", len(slips))
	}
	if slips[1].Slip < slips[0].Slip {
		t.Fatalf("slip did not reflect blocking: %#v", slips)
	}
}

func TestPrefixOracleFullDeltaAndCanonicalDigest(t *testing.T) {
	w, _ := NewWorkload(FamilyH, 300, 3)
	o := NewPrefixOracle(w.Fixture)
	q := w.Fixture.Queries[0].ID
	before, _ := o.ExpectedDigest(q, 0)
	m := w.Mutation(1)
	o.Append(m)
	after, _ := o.Materialize(q, 1)
	afterDigest, _ := after.Digest()
	if before == afterDigest {
		t.Fatal("mutation did not change digest")
	}
	delta := Delta{QueryID: q, Adds: []Entity{{ID: m.EntityID, Bucket: 0, Rank: 1, Attributes: map[string]any{"value": m.Marker}}}}
	replayed, err := ApplyDelta(Materialized{QueryID: q, Entities: map[string]Entity{}}, delta)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := replayed.Entities[m.EntityID]; !ok {
		t.Fatal("delta add missing")
	}
	if _, err := o.ExpectedDigest("missing", 1); err == nil {
		t.Fatal("missing query accepted")
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
	l.Observe(k, Observation{ExpectedDigest: "d", ObservedDigest: "d", ExpectedPrefix: 1, Prefix: 1, ProvesIntermediate: true, Applicable: true, At: receipt})
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

func TestStallDetector(t *testing.T) {
	d := NewStallDetector(time.Second)
	now := time.Now()
	if d.Stalled(now.Add(2 * time.Second)) {
		t.Fatal("stalled before write")
	}
	d.ObserveWrite(now)
	if d.Stalled(now.Add(500 * time.Millisecond)) {
		t.Fatal("wrong early stall state")
	}
	if !d.Stalled(now.Add(2 * time.Second)) {
		t.Fatal("stall not detected")
	}
	d.ObserveReceipt(now.Add(2 * time.Second))
	if d.Stalled(now.Add(3 * time.Second)) {
		t.Fatal("receipt did not clear stall")
	}
}

func TestSafetyGuards(t *testing.T) {
	for _, raw := range []string{"http://127.0.0.1:1234/runtime/session", "http://localhost:1/runtime/session"} {
		if err := ValidateLoopbackURL(raw); err != nil {
			t.Fatalf("loopback rejected: %v", err)
		}
	}
	if err := ValidateLoopbackURL("http://example.com"); err == nil {
		t.Fatal("public URL accepted")
	}
	if err := ValidateLoopbackURL("http://localhost.localdomain:1"); err == nil {
		t.Fatal("localhost.localdomain shortcut accepted")
	}
	if err := ValidateDatabaseURL("postgres://u:p@127.0.0.1:5432/instant_bench_x"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateDatabaseURL("postgres://u:p@127.0.0.1:5432/postgres"); err == nil {
		t.Fatal("non-benchmark DB accepted")
	}
	for _, raw := range []string{
		"postgres://u:p@127.0.0.1:5432/instant_bench_x?host=192.0.2.1",
		"postgres://u:p@127.0.0.1:5432/instant_bench_x?hostaddr=192.0.2.1",
		"host=127.0.0.1,192.0.2.1 port=5432 dbname=instant_bench_x",
		"host=127.0.0.1 hostaddr=192.0.2.1 dbname=instant_bench_x",
		"host=127.0.0.1 service=unsafe dbname=instant_bench_x",
		"host=127.0.0.1 fallback=192.0.2.1 dbname=instant_bench_x",
	} {
		if err := ValidateDatabaseURL(raw); err == nil {
			t.Fatalf("unsafe database DSN accepted: %q", raw)
		}
	}
	if name, err := DatabaseName("host=127.0.0.1 port=5432 dbname=instant_bench_keyword user=bench"); err != nil || name != "instant_bench_keyword" {
		t.Fatalf("keyword DSN name %q/%v", name, err)
	}
	root := t.TempDir()
	if _, err := SafeBundlePath(root, "../outside"); err == nil {
		t.Fatal("path traversal accepted")
	}
	if got, err := SafeBundlePath(root, "runs/a.json"); err != nil || filepath.Dir(got) != filepath.Join(root, "runs") {
		t.Fatalf("safe path %q/%v", got, err)
	}
	if err := (ResetGuard{}).Validate("instant_bench_x"); err == nil {
		t.Fatal("empty marker accepted")
	}
	_ = os.ErrNotExist
}

func TestEventWriterStructuredOutput(t *testing.T) {
	var got []byte
	w := NewEventWriter(func(b []byte) error { got = append(got, b...); return nil }, 1000)
	if err := w.Emit("begin", map[string]any{"run": "r"}); err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 {
		t.Fatal("event not emitted")
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

func TestQualificationRejectsNonLoopbackBeforeChecker(t *testing.T) {
	called := false
	q := Qualify(context.Background(), "v2", SessionOptions{URL: "https://example.com"}, fakeHealth{called: &called})
	if q.Passed || called {
		t.Fatalf("qualification did not fail closed: %#v called=%v", q, called)
	}
}

type fakeHealth struct{ called *bool }

func (f fakeHealth) Health(context.Context) error         { *f.called = true; return nil }
func (f fakeHealth) LiveProbe(context.Context, int) error { return nil }

func TestSafeBundlePathRootSymlinkRejected(t *testing.T) {
	dir := t.TempDir()
	target := t.TempDir()
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skip("symlink unavailable")
	}
	if _, err := SafeBundlePath(link, "x"); err == nil {
		t.Fatal("symlink root accepted")
	}
}

func TestSSESessionMessageEncodingShape(t *testing.T) {
	var posts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("data: {\"op\":\"init-ok\",\"session-id\":\"s\",\"sse-token\":\"t\",\"machine-id\":\"m\"}\n\ndata: {broken\n\n"))
			return
		}
		var body struct {
			AppID     string            `json:"app_id"`
			MachineID string            `json:"machine_id"`
			SessionID string            `json:"session_id"`
			SSEToken  string            `json:"sse_token"`
			Messages  []json.RawMessage `json:"messages"`
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil || body.AppID != "app" || body.MachineID != "m" || body.SessionID != "s" || body.SSEToken != "t" || len(body.Messages) != 1 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		posts.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	s, err := DialSSE(context.Background(), SessionOptions{URL: srv.URL, AppID: "app", EventBuffer: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ack, err := s.Send(context.Background(), SessionMessage{Op: "init", ClientEventID: "e"})
	if err != nil || !ack.Accepted || posts.Load() != 1 {
		t.Fatalf("SSE send %v/%#v posts=%d", err, ack, posts.Load())
	}
	var got []SessionEvent
	for ev := range s.Events() {
		got = append(got, ev)
	}
	if len(got) != 2 || got[0].Op != "init-ok" || got[1].Op != "protocol-error" {
		t.Fatalf("events %#v", got)
	}
}

func TestWebSocketTransactWaitsForCorrelatedAck(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		_, data, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		var req map[string]any
		if json.Unmarshal(data, &req) != nil {
			return
		}
		id, _ := req["client-event-id"].(string)
		body, _ := json.Marshal(map[string]any{"op": "transact-ok", "client-event-id": id, "tx-id": "tx-7", "processed-tx-id": "ptx-7"})
		_ = conn.Write(r.Context(), websocket.MessageText, body)
	}))
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	s, err := DialWebSocket(context.Background(), SessionOptions{URL: wsURL})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ack, err := s.Send(ctx, SessionMessage{Op: "transact", ClientEventID: "event-7"})
	if err != nil {
		t.Fatal(err)
	}
	if !ack.Accepted || ack.ServerTransactionID != "tx-7" || ack.ProcessedTransactionID != "ptx-7" {
		t.Fatalf("ack %#v", ack)
	}
}
