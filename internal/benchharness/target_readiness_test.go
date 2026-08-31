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

func TestTargetDriverRunRetriesTransientCleanFixtureReadiness(t *testing.T) {
	health := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer health.Close()
	dialer := &countingRunDialer{}
	dialer.staleCleanOnce.Store(true)
	var provisions atomic.Int64
	probeID := "00000000-0000-4000-8000-000000000099"
	fixture := Fixture{Entities: map[string]Entity{"todo-1": {ID: "todo-1", Attributes: map[string]any{"value": float64(0)}}}, Queries: []Query{{ID: "q", MatchAll: true}}}
	driver, err := NewTargetDriver(TargetConfig{
		ID: "v2", Kind: TargetV2, Transport: TransportWebSocket,
		SessionURL: "ws://127.0.0.1:1/runtime/session", HealthURL: health.URL,
		AppID: "00000000-0000-0000-0000-000000000001", Revision: "rev", DatabaseName: "instant_bench_ready", PostgresVersion: "17.11", InvalidationMode: "post-commit",
		MetadataProbe: func(context.Context) (TargetMetadata, error) {
			return TargetMetadata{Revision: "rev", DatabaseName: "instant_bench_ready", PostgresVersion: "17.11", InvalidationMode: "post-commit"}, nil
		},
		Provisioner: func(context.Context) error {
			provisions.Add(1)
			dialer.provisions.Add(1)
			if provisions.Load() == 1 {
				dialer.probePresent.Store(false)
			} else {
				dialer.probePresent.Store(true)
			}
			atomic.StoreInt64(&dialer.value, 0)
			return nil
		},
		Dialer: dialer, QueryBuilder: func(Query) (any, error) { return map[string]any{"todos": map[string]any{}}, nil },
		TransactionBuilder: func(Mutation) ([]any, error) { return []any{[]any{"update", "uuid"}}, nil },
		Probe: NewFourClientSemanticProbe(fixture.Queries[0], Mutation{EventID: "qualification-probe", EntityID: probeID, Marker: "probe-only"}, func(initial, refreshed Refresh) error {
			if initial.Full == nil || refreshed.Full == nil || mustDigest(*initial.Full) == mustDigest(*refreshed.Full) {
				return errors.New("probe did not observe semantic change")
			}
			return nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	workload := Workload{Family: FamilyH, Subscribers: 1, Seed: 37, Fixture: fixture, Measured: 1}
	artifacts, err := driver.Run(context.Background(), RunPlan{PairID: "p", RunID: "ready", Workload: workload, Mutations: 600, Rate: 1000})
	if err != nil {
		t.Fatalf("transient clean fixture state was not retried: %v", err)
	}
	if artifacts.Evidence.MaxBytes <= defaultEvidenceMaxBytes {
		t.Fatalf("target run did not apply derived workload evidence budget: %#v", artifacts.Evidence)
	}
	if got := dialer.provisions.Load(); got != 2 {
		t.Fatalf("provision count = %d, want 2", got)
	}
	if got := dialer.staleCleanOnce.Load(); got {
		t.Fatal("test did not exercise transient stale clean-fixture state")
	}
}

func TestSemanticReadinessDeadlineIsBoundedAndRetainsLastObservation(t *testing.T) {
	var attempts atomic.Int64
	started := time.Now()
	err := waitForSemanticReadiness(context.Background(), 35*time.Millisecond, func(context.Context) error {
		attempts.Add(1)
		return errors.New("fixture is still converging")
	})
	if err == nil || !strings.Contains(err.Error(), "semantic readiness deadline") || !strings.Contains(err.Error(), "fixture is still converging") {
		t.Fatalf("readiness failure did not retain bounded observation: %v", err)
	}
	if attempts.Load() < 2 || time.Since(started) > time.Second {
		t.Fatalf("readiness retry was not bounded: attempts=%d elapsed=%v", attempts.Load(), time.Since(started))
	}
}

func TestSemanticReadinessHonorsParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var attempts atomic.Int64
	err := waitForSemanticReadiness(ctx, time.Second, func(context.Context) error {
		attempts.Add(1)
		cancel()
		return errors.New("not ready")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("parent cancellation was not returned: %v", err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("cancellation triggered extra readiness probes: %d", attempts.Load())
	}
}

func TestTargetDriverRejectsStaleProbeDuringCleanFixtureVerification(t *testing.T) {
	dialer := &countingRunDialer{}
	probeID := "00000000-0000-4000-8000-000000000099"
	driver, err := NewTargetDriver(TargetConfig{
		ID: "v2", Kind: TargetV2, Transport: TransportWebSocket,
		SessionURL: "ws://127.0.0.1:1/runtime/session", HealthURL: "http://127.0.0.1:1/health",
		AppID: "00000000-0000-0000-0000-000000000001", Revision: "rev", DatabaseName: "instant_bench_run", PostgresVersion: "17.11", InvalidationMode: "post-commit",
		Dialer:       dialer,
		QueryBuilder: func(Query) (any, error) { return map[string]any{"todos": map[string]any{}}, nil },
		Probe:        LiveRefreshProbe{Mutation: Mutation{EntityID: probeID}},
	})
	if err != nil {
		t.Fatal(err)
	}
	dialer.probePresent.Store(true)
	atomic.StoreInt64(&dialer.value, 1)
	fixture := Fixture{Entities: map[string]Entity{"todo-1": {ID: "todo-1", Attributes: map[string]any{"value": float64(0)}}}, Queries: []Query{{ID: "q", MatchAll: true}}}
	err = driver.verifyCleanFixture(context.Background(), fixture, "stale", newEvidenceCollector(EvidenceBudget{}))
	if err == nil || !strings.Contains(err.Error(), "qualification probe entity") {
		t.Fatalf("stale probe was accepted: %v", err)
	}
	dialer.mu.Lock()
	opened := len(dialer.sessions)
	dialer.mu.Unlock()
	if opened != 1 {
		t.Fatalf("clean verification did not use exactly one isolated snapshot session: %d", opened)
	}
}

func TestCleanFixtureVerificationDeduplicatesIdenticalWireQueries(t *testing.T) {
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
	if err := driver.verifyCleanFixture(context.Background(), fixture, "dedupe", newEvidenceCollector(EvidenceBudget{})); err != nil {
		t.Fatal(err)
	}
	dialer.mu.Lock()
	sessions := append([]*countingRunSession(nil), dialer.sessions...)
	dialer.mu.Unlock()
	if len(sessions) != 1 {
		t.Fatalf("clean verification opened %d sessions, want 1", len(sessions))
	}
	sessions[0].mu.Lock()
	snapshots := len(sessions[0].snapshots)
	sessions[0].mu.Unlock()
	if snapshots != 1 {
		t.Fatalf("identical wire queries produced %d subscriptions, want 1", snapshots)
	}
}

func TestTargetDriverRejectsFixtureDivergenceDuringCleanFixtureVerification(t *testing.T) {
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
	atomic.StoreInt64(&dialer.value, 1)
	fixture := Fixture{Entities: map[string]Entity{"todo-1": {ID: "todo-1", Attributes: map[string]any{"value": float64(0)}}}, Queries: []Query{{ID: "q", MatchAll: true}}}
	err = driver.verifyCleanFixture(context.Background(), fixture, "diverged", newEvidenceCollector(EvidenceBudget{}))
	if err == nil || !strings.Contains(err.Error(), "diverges after qualification cleanup") {
		t.Fatalf("fixture divergence was accepted: %v", err)
	}
}
