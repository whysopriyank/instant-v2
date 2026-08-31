package benchharness

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestTargetDriverRejectsRAndSBehaviorWindowBeyondCanonical(t *testing.T) {
	for _, family := range []Family{FamilyR, FamilyS} {
		w, err := NewWorkload(family, 300, 17)
		if err != nil {
			t.Fatal(err)
		}
		d := &TargetDriver{cfg: TargetConfig{ID: "test-target"}}
		_, err = d.Run(context.Background(), RunPlan{Workload: w, BehaviorSeconds: w.DurationSeconds + 1})
		if err == nil || !strings.Contains(err.Error(), "behavior window") {
			t.Fatalf("family %s accepted behavior window %ds beyond canonical %ds: %v", family, w.DurationSeconds+1, w.DurationSeconds, err)
		}
		_, err = Execute(context.Background(), RunPlan{Workload: w, BehaviorSeconds: w.DurationSeconds + 1}, RunHooks{
			Submit: func(context.Context, Mutation) (Ack, error) { return Ack{Accepted: true}, nil },
		})
		if err == nil || !strings.Contains(err.Error(), "behavior window") {
			t.Fatalf("Execute accepted family %s behavior window %ds beyond canonical %ds: %v", family, w.DurationSeconds+1, w.DurationSeconds, err)
		}
	}
}

func TestCanonicalRAndSBehaviorWindowIsAccepted(t *testing.T) {
	for _, family := range []Family{FamilyR, FamilyS} {
		w, err := NewWorkload(family, 300, 17)
		if err != nil {
			t.Fatal(err)
		}
		if err := ValidateBehaviorWindow(w, w.DurationSeconds); err != nil {
			t.Fatalf("family %s canonical behavior window was rejected: %v", family, err)
		}
	}
}

func TestTargetDriverRunProvisionsPhasesAndEightTWriterSessions(t *testing.T) {
	health := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer health.Close()
	dialer := &countingRunDialer{}
	var provisions atomic.Int64
	probeMutation := Mutation{Sequence: 1, EventID: "qualification-probe", Kind: MutationAppend, EntityID: "00000000-0000-4000-8000-000000000099", Bucket: 0, Rank: 1, Marker: "probe-only"}
	w := Workload{Family: FamilyT, Subscribers: 1, Seed: 37, Fixture: Fixture{Entities: map[string]Entity{"todo-1": {ID: "todo-1", Attributes: map[string]any{"value": float64(0)}}}, Queries: []Query{{ID: "q", MatchAll: true}}}, Measured: 8}
	driver, err := NewTargetDriver(TargetConfig{
		ID: "v2", Kind: TargetV2, Transport: TransportWebSocket,
		SessionURL: "ws://127.0.0.1:1/runtime/session", HealthURL: health.URL,
		AppID: "00000000-0000-0000-0000-000000000001", Revision: "rev", DatabaseName: "instant_bench_run", PostgresVersion: "17.11", InvalidationMode: "post-commit",
		MetadataProbe: func(context.Context) (TargetMetadata, error) {
			return TargetMetadata{Revision: "rev", DatabaseName: "instant_bench_run", PostgresVersion: "17.11", InvalidationMode: "post-commit"}, nil
		},
		Provisioner: func(context.Context) error {
			provisions.Add(1)
			dialer.probePresent.Store(false)
			atomic.StoreInt64(&dialer.value, 0)
			return nil
		}, Dialer: dialer, EvidenceBudget: EvidenceBudget{MaxRetainedFrames: 1},
		QueryBuilder:       func(Query) (any, error) { return map[string]any{"todos": map[string]any{}}, nil },
		TransactionBuilder: func(Mutation) ([]any, error) { return []any{[]any{"update", "uuid"}}, nil },
		Probe: NewFourClientSemanticProbe(w.Fixture.Queries[0], probeMutation, func(initial, refreshed Refresh) error {
			if initial.Full == nil || refreshed.Full == nil || mustDigest(*initial.Full) == mustDigest(*refreshed.Full) {
				return errors.New("probe did not observe semantic change")
			}
			return nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := driver.Run(context.Background(), RunPlan{PairID: "p", RunID: "r", Workload: w, Mutations: 8, Rate: 1000, RampDuration: 2 * time.Millisecond, SettleDuration: 2 * time.Millisecond, WarmupDuration: 2 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if provisions.Load() != 2 || !artifacts.Qualification.Passed {
		t.Fatalf("run did not provision/qualify attempt: provisions=%d qualification=%#v", provisions.Load(), artifacts.Qualification)
	}
	dialer.mu.Lock()
	writerCount := 0
	for _, id := range dialer.ids {
		if strings.Contains(id, "-writer-") {
			writerCount++
		}
	}
	dialer.mu.Unlock()
	if writerCount != 8 {
		t.Fatalf("T did not open eight independent writer sessions: %d", writerCount)
	}
	dialer.mu.Lock()
	probeTransactions := 0
	for _, eventID := range dialer.transactionIDs {
		if eventID == probeMutation.EventID {
			probeTransactions++
		}
	}
	sessions := append([]*countingRunSession(nil), dialer.sessions...)
	dialer.mu.Unlock()
	if probeTransactions != 1 {
		t.Fatalf("qualification probe was repeated during measured run: %d transactions", probeTransactions)
	}
	measuredSnapshots := 0
	for _, session := range sessions {
		if !strings.Contains(session.clientID, "client-") && !strings.Contains(session.clientID, "-final") {
			continue
		}
		session.mu.Lock()
		for _, containsProbe := range session.snapshots {
			measuredSnapshots++
			if containsProbe {
				session.mu.Unlock()
				t.Fatalf("probe entity leaked into measured/final initial snapshot for %s", session.clientID)
			}
		}
		session.mu.Unlock()
	}
	if measuredSnapshots == 0 {
		t.Fatal("did not inspect any measured/final initial snapshots")
	}
	if len(artifacts.RawFrames) == 0 {
		t.Fatal("run returned no raw frame evidence")
	}
	measuredRefreshes := artifacts.Evidence.MeasuredByClass[FrameApplicationRefresh]
	allRefreshes := artifacts.Evidence.ByClass[FrameApplicationRefresh]
	if artifacts.Evidence.ObservedFrames < int64(len(artifacts.RawFrames)) || artifacts.Evidence.StreamDigest == "" || artifacts.Evidence.Overflow || artifacts.Evidence.MeasuredStartedAt.IsZero() || !artifacts.Evidence.MeasuredStartedAt.Equal(artifacts.MeasuredStartedAt) || !artifacts.Evidence.MeasuredFinishedAt.Equal(artifacts.MeasuredFinishedAt) || artifacts.MeasuredStartedAt.IsZero() || artifacts.MeasuredFinishedAt.Before(artifacts.MeasuredStartedAt) || measuredRefreshes.Frames <= int64(len(artifacts.RawFrames)) || allRefreshes.Frames <= measuredRefreshes.Frames {
		t.Fatalf("run evidence/measurement contract missing: %#v", artifacts)
	}
	if artifacts.ExpectedMutations != 8 || artifacts.ExpectedRows != 8 {
		t.Fatalf("run expected contract missing: mutations=%d rows=%d", artifacts.ExpectedMutations, artifacts.ExpectedRows)
	}
	if artifacts.RampDuration != 2*time.Millisecond || artifacts.SettleDuration != 2*time.Millisecond {
		t.Fatalf("phase controls not retained: %#v", artifacts)
	}
}

func TestFinalSnapshotterDeduplicatesIdenticalWireQueries(t *testing.T) {
	dialer := &countingRunDialer{}
	driver, err := NewTargetDriver(TargetConfig{
		ID: "v2", Kind: TargetV2, Transport: TransportWebSocket,
		SessionURL: "ws://127.0.0.1:1/runtime/session", HealthURL: "http://127.0.0.1:1/health",
		AppID: "00000000-0000-0000-0000-000000000001", Revision: "rev", DatabaseName: "instant_bench_run", PostgresVersion: "17.11", InvalidationMode: "post-commit",
		Dialer:       dialer,
		QueryBuilder: func(Query) (any, error) { return map[string]any{"todos": map[string]any{}}, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture := Fixture{
		Entities: map[string]Entity{"todo-1": {ID: "todo-1", Attributes: map[string]any{"value": float64(0)}}},
		Queries:  []Query{{ID: "q-1", MatchAll: true}, {ID: "q-2", MatchAll: true}},
	}
	snapshot := driver.finalSnapshotter(fixture, newEvidenceCollector(EvidenceBudget{}))
	for _, queryID := range []string{"q-1", "q-2"} {
		got, snapshotErr := snapshot(context.Background(), "client-"+queryID, queryID)
		if snapshotErr != nil {
			t.Fatal(snapshotErr)
		}
		if got.QueryID != queryID {
			t.Fatalf("snapshot query id = %q, want %q", got.QueryID, queryID)
		}
	}
	dialer.mu.Lock()
	sessions := append([]*countingRunSession(nil), dialer.sessions...)
	dialer.mu.Unlock()
	if len(sessions) != 1 {
		t.Fatalf("final verification opened %d sessions, want 1", len(sessions))
	}
	sessions[0].mu.Lock()
	snapshots := len(sessions[0].snapshots)
	sessions[0].mu.Unlock()
	if snapshots != 1 {
		t.Fatalf("identical final wire queries produced %d subscriptions, want 1", snapshots)
	}
}
