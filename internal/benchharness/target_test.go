package benchharness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDecodeWireRefreshUsesComputationsAndNumericWatermark(t *testing.T) {
	event := SessionEvent{Op: "refresh-ok", At: time.Now(), Payload: json.RawMessage(`{
		"processed-tx-id": 17,
		"query-id": "wrong-top-level-value",
		"computations": [{
			"instaql-query": {"todos": {}},
			"instaql-result": {"data": {"todos": [{"id":"todo-1","value":"marker"}]}}
		}]
	}`)}
	refresh, err := DecodeWireRefresh(event, "q-1")
	if err != nil {
		t.Fatal(err)
	}
	if refresh.ProcessedTransactionID != "17" || refresh.StateVersion != 17 {
		t.Fatalf("watermark %#v", refresh)
	}
	if refresh.Full == nil || refresh.Full.QueryID != "q-1" {
		t.Fatalf("missing semantic full result %#v", refresh)
	}
	entity, ok := refresh.Full.Entities["todo-1"]
	if !ok || entity.Attributes["value"] != "marker" {
		t.Fatalf("computation result was not materialized: %#v", refresh.Full.Entities)
	}
}

func TestDecodeAddQuerySnapshotUsesInlineInitialResult(t *testing.T) {
	query := json.RawMessage(`{"todos":{}}`)
	event := SessionEvent{Op: "add-query-ok", At: time.Now(), Payload: json.RawMessage(`{
		"q":{"todos":{}},
		"processed-tx-id":17,
		"result":{"data":{"todos":[{"id":"todo-1","value":"marker"}]}}
	}`)}
	refresh, available, err := decodeAddQuerySnapshot(event, "q-1", nil, query)
	if err != nil {
		t.Fatal(err)
	}
	if !available || refresh.Full == nil || refresh.StateVersion != 17 {
		t.Fatalf("inline initial snapshot not decoded: available=%t refresh=%#v", available, refresh)
	}
	if got := refresh.Full.Entities["todo-1"].Attributes["value"]; got != "marker" {
		t.Fatalf("initial entity value = %#v", got)
	}
}

func TestDecodeAddQuerySnapshotFallsBackWhenResultAbsent(t *testing.T) {
	_, available, err := decodeAddQuerySnapshot(SessionEvent{Op: "add-query-ok", Payload: json.RawMessage(`{"op":"add-query-ok"}`)}, "q-1", nil, json.RawMessage(`{"todos":{}}`))
	if err != nil || available {
		t.Fatalf("result-less add-query should use refresh fallback: available=%t err=%v", available, err)
	}
}

func TestEvidenceCollectorCountsMillionsWithoutRetainingPayloads(t *testing.T) {
	collector := newEvidenceCollector(EvidenceBudget{MaxFrames: 2_000_000, MaxBytes: 100_000_000, MaxRetainedFrames: 8})
	payload := json.RawMessage(`{"op":"refresh-ok","computations":[]}`)
	event := SessionEvent{Op: "refresh-ok", Payload: payload, At: time.Now()}
	for i := 0; i < 1_000_000; i++ {
		collector.record(event, "client-0", "q-0", "recipient-0", "")
	}
	frames, stats := collector.snapshot()
	if len(frames) != 8 || stats.ObservedFrames != 1_000_000 || stats.ObservedBytes != int64(len(payload))*1_000_000 {
		t.Fatalf("bounded evidence accounting mismatch: retained=%d stats=%#v", len(frames), stats)
	}
	if !stats.Truncated || stats.Overflow || stats.StreamDigest == "" {
		t.Fatalf("unexpected bounded evidence state: %#v", stats)
	}
	if got := stats.ByClass[FrameApplicationRefresh]; got.Frames != 1_000_000 || got.Bytes != int64(len(payload))*1_000_000 {
		t.Fatalf("application refresh classification mismatch: %#v", stats.ByClass)
	}
	if len(frames[0].PayloadDigest) != 64 || frames[0].PayloadBytes != int64(len(payload)) {
		t.Fatalf("payload was not represented by bounded digest/size: %#v", frames[0])
	}

	overflow := newEvidenceCollector(EvidenceBudget{MaxFrames: 3, MaxBytes: 6, MaxRetainedFrames: 2})
	for i := 0; i < 10; i++ {
		overflow.record(SessionEvent{Op: "refresh-ok", Payload: []byte("123")}, "c", "q", "r", "")
	}
	_, overflowStats := overflow.snapshot()
	if !overflowStats.Overflow || overflowStats.ObservedFrames != 10 || overflowStats.ObservedBytes != 30 {
		t.Fatalf("budget overflow was not explicit: %#v", overflowStats)
	}
}

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

func TestTargetConfigReadinessTimeoutUsesCanonicalTwentySecondCap(t *testing.T) {
	config := TargetConfig{
		ID: "target", Kind: TargetV1, Transport: TransportWebSocket,
		SessionURL: "ws://127.0.0.1:80/runtime/session", HealthURL: "http://127.0.0.1:80/health",
		AppID: "00000000-0000-0000-0000-000000000001", Revision: "rev-1",
		DatabaseName: "instant_bench_test", PostgresVersion: "16", InvalidationMode: "logical", OutputPlugin: "wal2json",
	}
	config.Probe.Timeout = 30 * time.Second
	if err := validateTargetConfig(config); err == nil {
		t.Fatal("readiness timeout above canonical 20 seconds was accepted")
	}
	config.Probe.Timeout = 20 * time.Second
	if err := validateTargetConfig(config); err != nil {
		t.Fatalf("canonical 20-second readiness timeout was rejected: %v", err)
	}
	config.Probe.Timeout = 0
	if err := validateTargetConfig(config); err != nil {
		t.Fatalf("zero readiness timeout (default fallback) was rejected: %v", err)
	}
}

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

func TestEvidenceCollectorClassifiesControlAndProtocolFrames(t *testing.T) {
	collector := newEvidenceCollector(EvidenceBudget{MaxRetainedFrames: 16})
	frames := []SessionEvent{
		{Op: "init-ok", Payload: []byte("init")},
		{Op: "add-query-ok", Payload: []byte("query")},
		{Op: "transact-ok", Payload: []byte("ack")},
		{Op: "refresh-ok", Payload: []byte("refresh")},
		{Op: "protocol-error", Payload: []byte("bad")},
		{Op: "refresh-ok", Payload: []byte("decode-failed")},
	}
	for i, ev := range frames {
		protocolErr := ""
		if i == len(frames)-1 {
			protocolErr = "malformed refresh"
		}
		collector.record(ev, "client", "query", "recipient", protocolErr)
	}
	retained, stats := collector.snapshot()
	if got := stats.ByClass[FrameSessionInit]; got.Frames != 1 || got.Bytes != 4 {
		t.Fatalf("session-init classification mismatch: %#v", stats.ByClass)
	}
	if got := stats.ByClass[FrameQueryLifecycle]; got.Frames != 1 || got.Bytes != 5 {
		t.Fatalf("query classification mismatch: %#v", stats.ByClass)
	}
	if got := stats.ByClass[FrameTransactionAck]; got.Frames != 1 || got.Bytes != 3 {
		t.Fatalf("ack classification mismatch: %#v", stats.ByClass)
	}
	if got := stats.ByClass[FrameApplicationRefresh]; got.Frames != 1 || got.Bytes != 7 {
		t.Fatalf("application refresh classification mismatch: %#v", stats.ByClass)
	}
	if got := stats.ByClass[FrameProtocolError]; got.Frames != 2 || got.Bytes != 3+13 {
		t.Fatalf("protocol classification mismatch: %#v", stats.ByClass)
	}
	if len(retained) != len(frames) || retained[0].Class != FrameSessionInit || retained[3].Class != FrameApplicationRefresh || retained[5].Class != FrameProtocolError {
		t.Fatalf("raw frame class metadata mismatch: %#v", retained)
	}
}

func TestEvidenceCollectorMeasuredWindowExcludesSetupAndRetainsTotals(t *testing.T) {
	collector := newEvidenceCollector(EvidenceBudget{MaxRetainedFrames: 1})
	started := time.Unix(100, 0)
	setupPayload := []byte("setup-refresh")
	collector.record(SessionEvent{Op: "refresh-ok", Payload: setupPayload, At: started.Add(-time.Second)}, "client", "q", "recipient", "")
	collector.beginMeasured(started)
	measuredBytes := int64(0)
	for i, payload := range [][]byte{[]byte("measured-a"), []byte("measured-bb"), []byte("measured-ccc")} {
		measuredBytes += int64(len(payload))
		collector.record(SessionEvent{Op: "refresh-ok", Payload: payload, At: started.Add(time.Duration(i+1) * time.Second / 4)}, "client", "q", "recipient", "")
	}
	finished := time.Unix(101, 0)
	collector.endMeasured(finished)
	retained, stats := collector.snapshot()
	all := stats.ByClass[FrameApplicationRefresh]
	measured := stats.MeasuredByClass[FrameApplicationRefresh]
	if all.Frames != 4 || all.Bytes != int64(len(setupPayload))+measuredBytes {
		t.Fatalf("cumulative application evidence mismatch: %#v", stats)
	}
	if measured.Frames != 3 || measured.Bytes != measuredBytes {
		t.Fatalf("measured application evidence includes setup frames: %#v", stats.MeasuredByClass)
	}
	if !stats.MeasuredStartedAt.Equal(started) || !stats.MeasuredFinishedAt.Equal(finished) {
		t.Fatalf("measured boundary timestamps were not retained: %#v", stats)
	}
	if len(retained) != 1 || retained[0].PayloadBytes != int64(len(setupPayload)) {
		t.Fatalf("bounded sample does not prove pre-window exclusion: %#v", retained)
	}
}

func TestEvidenceCollectorMeasuredWindowUsesEventTimeNotProcessingTime(t *testing.T) {
	collector := newEvidenceCollector(EvidenceBudget{MaxRetainedFrames: 16})
	started := time.Unix(200, 0)
	finished := started.Add(time.Second)
	collector.beginMeasured(started)
	// This frame is consumed after the start callback but was emitted before
	// it; event time must keep it out of measured totals.
	collector.record(SessionEvent{Op: "refresh-ok", Payload: []byte("queued-before"), At: started.Add(-time.Nanosecond)}, "client", "q", "recipient", "")
	collector.endMeasured(finished)
	// This frame is consumed during convergence after the end callback, but
	// its server timestamp lies inside the measured interval and must count.
	collector.record(SessionEvent{Op: "refresh-ok", Payload: []byte("queued-during"), At: started.Add(500 * time.Millisecond)}, "client", "q", "recipient", "")
	// Missing event time is never treated as a local processing timestamp.
	collector.record(SessionEvent{Op: "refresh-ok", Payload: []byte("missing-time")}, "client", "q", "recipient", "")
	_, stats := collector.snapshot()
	measured := stats.MeasuredByClass[FrameApplicationRefresh]
	if measured.Frames != 1 || measured.Bytes != int64(len("queued-during")) {
		t.Fatalf("measured classification used processing state/time: %#v", stats.MeasuredByClass)
	}
}

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

func TestDecodeWireRefreshParsesV1NodeList(t *testing.T) {
	event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(`{
		"processed-tx-id": 4,
		"computations": [{
			"instaql-query": {"todos": {}},
			"instaql-result": [{"data":{"datalog-result":{"join-rows":[[
				["todo-1","attr-id","marker"]
			]]}}}]
		}]
	}`)}
	refresh, err := DecodeWireRefresh(event, "q-1")
	if err != nil {
		t.Fatal(err)
	}
	if refresh.Full == nil || refresh.Full.Entities["todo-1"].Attributes["attr-id"] != "marker" {
		t.Fatalf("node-list not materialized: %#v", refresh.Full)
	}
}

func TestV1TargetAcceptsLegacyEmptyRefreshAsNoop(t *testing.T) {
	query := json.RawMessage(`{"todos":{}}`)
	session := &TargetSession{
		driver:      &TargetDriver{cfg: TargetConfig{Kind: TargetV1}},
		wireQueries: map[string]json.RawMessage{"q-1": query},
	}
	event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(`{
		"op":"refresh-ok",
		"processed-tx-id":17,
		"processed-isn":"0/0/17",
		"computations":[],
		"attrs":[]
	}`)}
	refresh, err := session.decodeRefresh(event, "q-1")
	if err != nil {
		t.Fatalf("V1 empty legacy refresh is a valid no-op: %v", err)
	}
	if refresh.ProcessedTransactionID != "17" || refresh.Full != nil || refresh.Delta != nil {
		t.Fatalf("V1 empty legacy refresh was not preserved as a no-op: %#v", refresh)
	}
	if refresh.StateVersion != 0 {
		t.Fatalf("metadata-only refresh must not become semantic state version: %#v", refresh)
	}
}

func TestV1TargetAcceptsThreeComponentISN(t *testing.T) {
	session := &TargetSession{driver: &TargetDriver{cfg: TargetConfig{Kind: TargetV1}}}
	event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(`{
		"op":"refresh-ok",
		"processed-tx-id":17,
		"processed-isn":"0/0/17",
		"computations":[],
		"attrs":[]
	}`)}
	if _, err := session.decodeRefresh(event, "q-1"); err != nil {
		t.Fatalf("V1 three-component ISN was rejected: %v", err)
	}
}

func TestV1TargetAcceptsLegacyComputationEnvelope(t *testing.T) {
	query := json.RawMessage(`{"todos":{}}`)
	session := &TargetSession{
		driver:      &TargetDriver{cfg: TargetConfig{Kind: TargetV1}},
		wireQueries: map[string]json.RawMessage{"q-1": query},
	}
	event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(`{
		"op":"refresh-ok",
		"processed-tx-id":17,
		"processed-isn":"0/0/17",
		"computations":[{
			"instaql-query":{"todos":{}},
			"instaql-query-hash":123,
			"instaql-result":[{"data":{"datalog-result":{"join-rows":[[["todo-1","value","marker"]]]}},"child-nodes":[]}],
			"result-meta":null,
			"result-changed?":true,
			"duration-ms":1,
			"instaql-topic?":false
		}],
		"attrs":[]
	}`)}
	refresh, err := session.decodeRefresh(event, "q-1")
	if err != nil {
		t.Fatalf("source-shaped V1 computation was rejected: %v", err)
	}
	if refresh.Kind != RefreshFull || refresh.Full == nil || refresh.Full.Entities["todo-1"].Attributes["value"] != "marker" {
		t.Fatalf("source-shaped V1 computation was not materialized: %#v", refresh)
	}
}

func TestV1TargetRejectsMalformedLegacyEnvelope(t *testing.T) {
	base := `"processed-tx-id":17,"processed-isn":"0/0/17","computations":[],"attrs":[]`
	for name, payload := range map[string]string{
		"missing op":          `{` + base + `}`,
		"wrong isn type":      `{"op":"refresh-ok","processed-tx-id":17,"processed-isn":23,"computations":[],"attrs":[]}`,
		"two isn components":  `{"op":"refresh-ok","processed-tx-id":17,"processed-isn":"0/17","computations":[],"attrs":[]}`,
		"four isn components": `{"op":"refresh-ok","processed-tx-id":17,"processed-isn":"0/0/17/99","computations":[],"attrs":[]}`,
		"malformed isn":       `{"op":"refresh-ok","processed-tx-id":17,"processed-isn":"0/0/nope","computations":[],"attrs":[]}`,
		"missing attrs noop":  `{"op":"refresh-ok","processed-tx-id":17,"processed-isn":"0/0/17","computations":[]}`,
		"attrs wrong type":    `{"op":"refresh-ok","processed-tx-id":17,"processed-isn":"0/0/17","computations":[],"attrs":{}}`,
	} {
		t.Run(name, func(t *testing.T) {
			session := &TargetSession{driver: &TargetDriver{cfg: TargetConfig{Kind: TargetV1}}}
			event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(payload)}
			if _, err := session.decodeRefresh(event, "q-1"); err == nil {
				t.Fatalf("malformed V1 envelope was accepted: %s", payload)
			}
		})
	}
}

func TestV1TargetRejectsDeltaRefreshEnvelope(t *testing.T) {
	session := &TargetSession{driver: &TargetDriver{cfg: TargetConfig{Kind: TargetV1}}}
	event := SessionEvent{Op: "refresh-ok-delta", Payload: json.RawMessage(`{
		"op":"refresh-ok-delta",
		"processed-tx-id":17,
		"processed-isn":"0/0/17",
		"computations":[{"instaql-query":{"todos":{}},"delta":{"ops":[]}}]
	}`)}
	if _, err := session.decodeRefresh(event, "q-1"); err == nil {
		t.Fatal("V1 legacy decoder accepted a V2 delta envelope")
	}
}

func TestV2TargetStillRejectsEmptyComputations(t *testing.T) {
	session := &TargetSession{driver: &TargetDriver{cfg: TargetConfig{Kind: TargetV2}}}
	event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(`{
		"op":"refresh-ok",
		"processed-tx-id":17,
		"processed-isn":"0/0/17",
		"computations":[]
	}`)}
	if _, err := session.decodeRefresh(event, "q-1"); err == nil || !strings.Contains(err.Error(), "refresh missing computations") {
		t.Fatalf("V2 empty refresh was not rejected fail-closed: %v", err)
	}
}

func TestV1NoopRefreshDoesNotBecomeProtocolReceipt(t *testing.T) {
	client := &TargetSession{
		driver:      &TargetDriver{cfg: TargetConfig{Kind: TargetV1}},
		wireQueries: map[string]json.RawMessage{"q-1": json.RawMessage(`{"todos":{}}`)},
	}
	event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(`{
		"op":"refresh-ok",
		"processed-tx-id":17,
		"processed-isn":"0/0/17",
		"computations":[],
		"attrs":[]
	}`)}
	items, err := (&TargetDriver{}).decodeReceipts(client, event, "q-1", "recipient", newCommitPrefixes(nil))
	if err != nil || items != nil {
		t.Fatalf("V1 metadata-only refresh should be ignored, items=%#v err=%v", items, err)
	}
}

func TestDecodeWireRefreshNormalizesUUIDNodeListAttributes(t *testing.T) {
	event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(`{
		"processed-tx-id": 4,
		"computations": [{
			"instaql-query": {"todos": {}},
			"instaql-result": [{"data":{"datalog-result":{"join-rows":[[
				["00000000-0000-4000-8000-000000000010","00000000-0000-4000-8000-000000000100","00000000-0000-4000-8000-000000000010"],
				["00000000-0000-4000-8000-000000000010","00000000-0000-4000-8000-000000000001","marker"],
				["00000000-0000-4000-8000-000000000010","00000000-0000-4000-8000-000000000002",7],
				["00000000-0000-4000-8000-000000000010","00000000-0000-4000-8000-000000000003",2]
			]]}}}]
		}]
	}`)}
	aliases := map[string]string{
		"id":     "00000000-0000-4000-8000-000000000100",
		"value":  "00000000-0000-4000-8000-000000000001",
		"bucket": "00000000-0000-4000-8000-000000000002",
		"rank":   "00000000-0000-4000-8000-000000000003",
	}
	refresh, err := DecodeWireRefreshWithAliases(event, "q-1", aliases)
	if err != nil {
		t.Fatal(err)
	}
	entity := refresh.Full.Entities["00000000-0000-4000-8000-000000000010"]
	if entity.Attributes["value"] != "marker" || entity.Bucket != 7 || entity.Rank != 2 {
		t.Fatalf("UUID attributes were not normalized: %#v", entity)
	}
	if _, present := entity.Attributes["id"]; present {
		t.Fatalf("identity UUID leaked into semantic attributes: %#v", entity.Attributes)
	}
	for _, wireKey := range []string{aliases["value"], aliases["bucket"], aliases["rank"]} {
		if _, present := entity.Attributes[wireKey]; present {
			t.Fatalf("wire UUID leaked into semantic attributes: %#v", entity.Attributes)
		}
	}
}

func TestNormalizeWireAttributeCanonicalizesUUIDAliases(t *testing.T) {
	alias := "00000000-0000-4000-8000-000000000100"
	if got := normalizeWireAttribute(strings.ToUpper(alias), map[string]string{"id": alias}); got != "id" {
		t.Fatalf("uppercase UUID alias normalized as %q, want id", got)
	}
}

func TestDecodeWireRefreshRejectsNonCanonicalEntityUUIDCasing(t *testing.T) {
	entityID := "ABCDEFAB-CDEF-ABCD-EFAB-CDEFABCDEFAB"
	idAttr := "00000000-0000-4000-8000-000000000100"
	event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(fmt.Sprintf(`{
		"computations": [{"instaql-query":{"todos":{}},"instaql-result":[{"data":{"datalog-result":{"join-rows":[[
			[%q,%q,%q]
		]]}}}]}]
	}`, entityID, idAttr, entityID))}
	_, err := DecodeWireRefreshWithAliases(event, "q-1", map[string]string{"id": strings.ToLower(idAttr)})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "lowercase") {
		t.Fatalf("noncanonical entity UUID casing was accepted: %v", err)
	}
}

func TestCompareMetadataRejectsWeakIdentityCatalog(t *testing.T) {
	want := IdentityAttributeMetadata{ID: "00000000-0000-0000-0000-000000000100", EntityType: "bench_items", Label: "id", ValueType: "blob", Cardinality: "one", Unique: true, Indexed: true, Required: true, Primary: true, Identity: true}
	cfg := TargetConfig{Kind: TargetV1, Revision: "rev", DatabaseName: "instant_bench_v1", PostgresVersion: "17", InvalidationMode: "logical", OutputPlugin: "wal2json", IdentityAttribute: want}
	for _, tc := range []struct {
		name   string
		mutate func(*IdentityAttributeMetadata)
		want   string
	}{
		{name: "missing", mutate: func(got *IdentityAttributeMetadata) { *got = IdentityAttributeMetadata{} }, want: "identity"},
		{name: "unindexed", mutate: func(got *IdentityAttributeMetadata) { got.Indexed = false }, want: "indexed"},
		{name: "optional", mutate: func(got *IdentityAttributeMetadata) { got.Required = false }, want: "required"},
		{name: "required without primary", mutate: func(got *IdentityAttributeMetadata) { got.Primary = false }, want: "primary"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := want
			tc.mutate(&got)
			err := compareMetadata(cfg, TargetMetadata{Revision: "rev", DatabaseName: "instant_bench_v1", PostgresVersion: "17", InvalidationMode: "logical", OutputPlugin: "wal2json", IdentityAttribute: got}, func(string, bool, string) {})
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.want)) {
				t.Fatalf("weak identity catalog was accepted: %v", err)
			}
		})
	}
	if err := compareMetadata(cfg, TargetMetadata{Revision: "rev", DatabaseName: "instant_bench_v1", PostgresVersion: "17", InvalidationMode: "logical", OutputPlugin: "wal2json", IdentityAttribute: want}, func(string, bool, string) {}); err != nil {
		t.Fatalf("exact identity catalog was rejected: %v", err)
	}
}

func TestDecodeWireRefreshRejectsMissingIdentityForResultEntity(t *testing.T) {
	entityID := "00000000-0000-4000-8000-000000000010"
	idAttr := "00000000-0000-4000-8000-000000000100"
	valueAttr := "00000000-0000-4000-8000-000000000001"
	event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(fmt.Sprintf(`{
		"computations": [{
			"instaql-query": {"todos": {}},
			"instaql-result": [{"data":{"datalog-result":{"join-rows":[[
				[%q,%q,"marker"]
			]]}}}]
		}]
	}`, entityID, valueAttr))}
	_, err := DecodeWireRefreshWithAliases(event, "q-1", map[string]string{"id": idAttr, "value": valueAttr})
	if err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("missing identity row was accepted: %v", err)
	}
}

func TestDecodeWireRefreshRejectsInvalidDuplicateAndConflictingIdentity(t *testing.T) {
	entityID := "00000000-0000-4000-8000-000000000010"
	idAttr := "00000000-0000-4000-8000-000000000100"
	valueAttr := "00000000-0000-4000-8000-000000000001"
	aliases := map[string]string{"id": idAttr, "value": valueAttr}
	for _, tc := range []struct {
		name string
		rows string
		want string
	}{
		{name: "invalid", rows: fmt.Sprintf(`[%q,%q,"not-a-uuid"]`, entityID, idAttr), want: "UUID"},
		{name: "duplicate", rows: fmt.Sprintf(`[%q,%q,%q],[%q,%q,%q]`, entityID, idAttr, entityID, entityID, idAttr, entityID), want: "duplicate"},
		{name: "conflicting", rows: fmt.Sprintf(`[%q,%q,%q],[%q,%q,%q]`, entityID, idAttr, entityID, entityID, idAttr, "00000000-0000-4000-8000-000000000011"), want: "identity"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(fmt.Sprintf(`{
				"computations": [{"instaql-query":{"todos":{}},"instaql-result":[{"data":{"datalog-result":{"join-rows":[[%s]]}}}]}]
			}`, tc.rows))}
			_, err := DecodeWireRefreshWithAliases(event, "q-1", aliases)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.want)) {
				t.Fatalf("%s identity was accepted: %v", tc.name, err)
			}
		})
	}
}

func TestDecodeWireRefreshNormalizesObjectIdentityAliasBeforeOmitting(t *testing.T) {
	entityID := "00000000-0000-4000-8000-000000000010"
	idAttr := "00000000-0000-4000-8000-000000000100"
	valueAttr := "00000000-0000-4000-8000-000000000001"
	event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(fmt.Sprintf(`{
		"computations": [{"instaql-query":{"todos":{}},"instaql-result":{"data":{"todos":[{"id":%q,%q:%q,%q:"marker"}]}}}]
	}`, entityID, idAttr, entityID, valueAttr))}
	refresh, err := DecodeWireRefreshWithAliases(event, "q-1", map[string]string{"id": idAttr, "value": valueAttr})
	if err != nil {
		t.Fatal(err)
	}
	entity := refresh.Full.Entities[entityID]
	if entity.Attributes["value"] != "marker" {
		t.Fatalf("object value was not normalized: %#v", entity)
	}
	if _, ok := entity.Attributes["id"]; ok {
		t.Fatalf("object identity leaked into attributes: %#v", entity.Attributes)
	}
	if _, ok := entity.Attributes[idAttr]; ok {
		t.Fatalf("object identity alias leaked into attributes: %#v", entity.Attributes)
	}
}

func TestDecodeWireRefreshRejectsInvalidObjectIdentity(t *testing.T) {
	event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(`{
		"computations": [{"instaql-query":{"todos":{}},"instaql-result":{"data":{"todos":[{"id":"not-a-uuid","value":"marker"}]}}}]
	}`)}
	_, err := DecodeWireRefreshWithAliases(event, "q-1", map[string]string{"id": "00000000-0000-4000-8000-000000000100"})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "identity") {
		t.Fatalf("invalid object identity was accepted: %v", err)
	}
}

func TestDecodeWireRefreshCombinesMatchingComputations(t *testing.T) {
	event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(`{
		"computations": [
			{"instaql-query":{"posts":{}},"instaql-result":{"data":{"posts":[{"id":"post-1","value":"unrelated-before"}]} }},
			{"instaql-query":{"todos":{}},"instaql-result":{"data":{"todos":[{"id":"todo-1","value":"first"}]} }},
			{"instaql-query":{"todos":{}},"instaql-result":{"data":{"todos":[{"id":"todo-2","value":"second"}]} }},
			{"instaql-query":{"posts":{}},"instaql-result":{"data":{"posts":[{"id":"post-2","value":"unrelated-after"}]}}}
		]
	}`)}
	refresh, err := DecodeWireRefreshForQuery(event, "q-1", json.RawMessage(`{"todos":{}}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	if refresh.Full == nil || len(refresh.Full.Entities) != 2 {
		t.Fatalf("matching computations were not combined: %#v", refresh.Full)
	}
	if refresh.Full.Entities["todo-1"].Attributes["value"] != "first" || refresh.Full.Entities["todo-2"].Attributes["value"] != "second" {
		t.Fatalf("matching computation results were lost: %#v", refresh.Full.Entities)
	}
	if _, present := refresh.Full.Entities["post-1"]; present {
		t.Fatal("unrelated computation before matching query was decoded")
	}
	if _, present := refresh.Full.Entities["post-2"]; present {
		t.Fatal("unrelated computation after matching query was decoded")
	}
}

func TestDecodeWireRefreshCombinesMatchingDeltas(t *testing.T) {
	event := SessionEvent{Op: "refresh-ok-delta", Payload: json.RawMessage(`{
		"processed-tx-id": 9,
		"computations": [
			{"instaql-query":{"posts":{}},"delta":{"ops":[{"op":"add","id":"post-1","entity":{"id":"post-1","value":"ignore"}}]}},
			{"instaql-query":{"todos":{}},"delta":{"ops":[{"op":"add","id":"todo-1","entity":{"id":"todo-1","value":"first"}}]}},
			{"instaql-query":{"todos":{}},"delta":{"ops":[{"op":"update","id":"todo-2","entity":{"id":"todo-2","value":"second","bucket":3}}]}},
			{"instaql-query":{"posts":{}},"delta":{"ops":[{"op":"remove","id":"post-2"}]}}
		]
	}`)}
	refresh, err := DecodeWireRefreshForQuery(event, "q-1", json.RawMessage(`{"todos":{}}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	if refresh.Kind != RefreshDelta || refresh.Delta == nil || len(refresh.Delta.Adds) != 1 || len(refresh.Delta.Updates) != 1 || len(refresh.Delta.Removes) != 0 {
		t.Fatalf("matching deltas were not combined: %#v", refresh)
	}
	if refresh.Delta.Adds[0].ID != "todo-1" || refresh.Delta.Adds[0].Attributes["value"] != "first" || refresh.Delta.Updates[0].Bucket != 3 {
		t.Fatalf("matching delta payloads were not materialized: %#v", refresh.Delta)
	}
}

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

type scriptedTargetSession struct {
	mu        sync.Mutex
	events    chan SessionEvent
	closed    chan struct{}
	broadcast func(SessionEvent)
}

func newScriptedTargetSession() *scriptedTargetSession {
	return &scriptedTargetSession{events: make(chan SessionEvent, 16), closed: make(chan struct{})}
}

func (s *scriptedTargetSession) Send(_ context.Context, msg SessionMessage) (Ack, error) {
	switch msg.Op {
	case "init":
		s.events <- SessionEvent{Op: "init-ok", ClientEventID: msg.ClientEventID}
	case "add-query":
		s.events <- SessionEvent{Op: "add-query-ok", ClientEventID: msg.ClientEventID}
		s.events <- scriptedRefresh(0, "initial")
	case "transact":
		s.events <- SessionEvent{Op: "transact-ok", ClientEventID: msg.ClientEventID, TxID: "1", ServerTransactionID: "1", At: time.Now()}
		if s.broadcast != nil {
			s.broadcast(scriptedRefresh(1, "changed"))
		} else {
			s.events <- scriptedRefresh(1, "changed")
		}
		return Ack{Accepted: true, ServerTransactionID: "1"}, nil
	}
	return Ack{Accepted: true}, nil
}
func (s *scriptedTargetSession) Events() <-chan SessionEvent { return s.events }
func (s *scriptedTargetSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.closed:
	default:
		close(s.closed)
		close(s.events)
	}
	return nil
}

type scriptedDialer struct {
	mu       sync.Mutex
	sessions []*scriptedTargetSession
}

type countingRunDialer struct {
	mu             sync.Mutex
	sessions       []*countingRunSession
	nextTx         int64
	value          int64
	ids            []string
	transactionIDs []string
	probePresent   atomic.Bool
	provisions     atomic.Int64
	staleCleanOnce atomic.Bool
}

type countingRunSession struct {
	dialer    *countingRunDialer
	events    chan SessionEvent
	clientID  string
	mu        sync.Mutex
	closed    bool
	snapshots []bool
}

func (d *countingRunDialer) Dial(_ context.Context, opts SessionOptions) (Session, error) {
	s := &countingRunSession{dialer: d, events: make(chan SessionEvent, 64), clientID: opts.ClientID}
	d.mu.Lock()
	d.sessions = append(d.sessions, s)
	d.ids = append(d.ids, opts.ClientID)
	d.mu.Unlock()
	return s, nil
}

func (s *countingRunSession) push(ev SessionEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.events <- ev
}

func (s *countingRunSession) Send(_ context.Context, msg SessionMessage) (Ack, error) {
	switch msg.Op {
	case "init":
		s.push(SessionEvent{Op: "init-ok", ClientEventID: msg.ClientEventID})
	case "add-query":
		s.push(SessionEvent{Op: "add-query-ok", ClientEventID: msg.ClientEventID})
		staleClean := strings.Contains(s.clientID, "clean-check") && s.dialer.provisions.Load() >= 2 && s.dialer.staleCleanOnce.CompareAndSwap(true, false)
		if staleClean {
			s.dialer.probePresent.Store(true)
		}
		s.mu.Lock()
		s.snapshots = append(s.snapshots, s.dialer.probePresent.Load())
		s.mu.Unlock()
		s.push(s.dialer.refresh())
		if staleClean {
			s.dialer.probePresent.Store(false)
		}
	case "transact":
		if msg.ClientEventID == "qualification-probe" {
			s.dialer.probePresent.Store(true)
		}
		s.dialer.mu.Lock()
		s.dialer.transactionIDs = append(s.dialer.transactionIDs, msg.ClientEventID)
		s.dialer.mu.Unlock()
		tx := atomic.AddInt64(&s.dialer.nextTx, 1)
		atomic.StoreInt64(&s.dialer.value, tx)
		s.push(SessionEvent{Op: "transact-ok", ClientEventID: msg.ClientEventID, TxID: fmt.Sprint(tx), ProcessedTxID: fmt.Sprint(tx), ServerTransactionID: fmt.Sprint(tx), ProcessedTransactionID: fmt.Sprint(tx)})
		s.dialer.broadcast(s.dialer.refresh())
		return Ack{Accepted: true, ServerTransactionID: fmt.Sprint(tx), ProcessedTransactionID: fmt.Sprint(tx)}, nil
	}
	return Ack{Accepted: true}, nil
}

func (s *countingRunSession) Events() <-chan SessionEvent { return s.events }
func (s *countingRunSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.events)
	}
	return nil
}

func (d *countingRunDialer) refresh() SessionEvent {
	value := atomic.LoadInt64(&d.value)
	todos := []any{map[string]any{"id": "todo-1", "value": value}}
	if d.probePresent.Load() {
		todos = append(todos, map[string]any{"id": "00000000-0000-4000-8000-000000000099", "value": "probe-only"})
	}
	b, _ := json.Marshal(map[string]any{"processed-tx-id": value, "computations": []any{map[string]any{"instaql-query": map[string]any{"todos": map[string]any{}}, "instaql-result": map[string]any{"data": map[string]any{"todos": todos}}}}})
	return SessionEvent{Op: "refresh-ok", Payload: b, ProcessedTransactionID: fmt.Sprint(value), At: time.Now()}
}

func (d *countingRunDialer) broadcast(ev SessionEvent) {
	d.mu.Lock()
	sessions := append([]*countingRunSession(nil), d.sessions...)
	d.mu.Unlock()
	for _, session := range sessions {
		session.push(ev)
	}
}

func (d *scriptedDialer) Dial(context.Context, SessionOptions) (Session, error) {
	s := newScriptedTargetSession()
	s.broadcast = func(event SessionEvent) {
		d.mu.Lock()
		defer d.mu.Unlock()
		for _, target := range d.sessions {
			select {
			case target.events <- event:
			case <-target.closed:
			}
		}
	}
	d.mu.Lock()
	d.sessions = append(d.sessions, s)
	d.mu.Unlock()
	return s, nil
}

func scriptedRefresh(version int, value string) SessionEvent {
	b, _ := json.Marshal(map[string]any{"processed-tx-id": version, "computations": []any{map[string]any{"instaql-query": map[string]any{"todos": map[string]any{}}, "instaql-result": map[string]any{"data": map[string]any{"todos": []any{map[string]any{"id": "todo-1", "value": value}}}}}}})
	return SessionEvent{Op: "refresh-ok", Payload: b, ProcessedTransactionID: "1", At: time.Now()}
}

func TestTargetDriverQualifyRunsActualSessionProbe(t *testing.T) {
	health := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer health.Close()
	app := "00000000-0000-0000-0000-000000000001"
	dialer := &scriptedDialer{}
	driver, err := NewTargetDriver(TargetConfig{
		ID: "v2", Kind: TargetV2, Transport: TransportWebSocket,
		SessionURL: "ws://127.0.0.1:1/runtime/session", HealthURL: health.URL,
		AppID: app, Revision: "rev", DatabaseName: "instant_bench_probe", PostgresVersion: "17.11", InvalidationMode: "post-commit",
		MetadataProbe: func(context.Context) (TargetMetadata, error) {
			return TargetMetadata{Revision: "rev", DatabaseName: "instant_bench_probe", PostgresVersion: "17.11", InvalidationMode: "post-commit"}, nil
		},
		Provisioner: func(context.Context) error { return nil },
		Dialer:      dialer, QueryBuilder: func(Query) (any, error) { return map[string]any{"todos": map[string]any{}}, nil },
		TransactionBuilder: func(Mutation) ([]any, error) { return []any{[]any{"add-triple", "e", "a", "marker"}}, nil },
		Probe: LiveRefreshProbe{Query: Query{ID: "q"}, Mutation: Mutation{EventID: "e"}, Validate: func(initial, refreshed Refresh) error {
			if initial.Full == nil || refreshed.Full == nil {
				return errors.New("missing full refresh")
			}
			if mustDigest(*initial.Full) == mustDigest(*refreshed.Full) {
				return errors.New("refresh did not change semantic state")
			}
			return nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	qualification, err := driver.Qualify(context.Background())
	if err != nil || !qualification.Passed || !qualification.Checks["live_refresh"].Passed {
		t.Fatalf("qualification failed: %#v / %v", qualification, err)
	}
}

func TestCheckAdminRoutesUsesCanonicalAdminPrefix(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("admin probe used %s instead of POST", r.Method)
		}
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	want := []string{"/admin/query", "/admin/transact", "/admin/subscribe-query"}
	for _, base := range []string{server.URL, server.URL + "/admin/"} {
		mu.Lock()
		paths = nil
		mu.Unlock()
		driver := &TargetDriver{cfg: TargetConfig{AdminBaseURL: base}, client: server.Client()}
		var check QualificationCheck
		if err := driver.checkAdminRoutes(context.Background(), func(name string, passed bool, details string) {
			if name == "admin_routes" {
				check = QualificationCheck{Passed: passed, Details: details}
			}
		}); err != nil || !check.Passed {
			t.Fatalf("admin route probe failed for base %q: %v / %#v", base, err, check)
		}
		mu.Lock()
		got := append([]string(nil), paths...)
		mu.Unlock()
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("admin route probe for base %q used %v, want %v", base, got, want)
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

func TestTargetSessionReconnectReplaysQueriesAndRetainsRawEvidence(t *testing.T) {
	dialer := &scriptedDialer{}
	driver, err := NewTargetDriver(TargetConfig{
		ID: "v2", Kind: TargetV2, Transport: TransportWebSocket,
		SessionURL: "ws://127.0.0.1:1/runtime/session", HealthURL: "http://127.0.0.1:1/health",
		AppID: "00000000-0000-0000-0000-000000000001", Revision: "rev", DatabaseName: "instant_bench_probe", PostgresVersion: "17.11", InvalidationMode: "post-commit",
		Provisioner: func(context.Context) error { return nil }, MetadataProbe: func(context.Context) (TargetMetadata, error) {
			return TargetMetadata{Revision: "rev", DatabaseName: "instant_bench_probe", PostgresVersion: "17.11", InvalidationMode: "post-commit"}, nil
		},
		Dialer: dialer, QueryBuilder: func(Query) (any, error) { return map[string]any{"todos": map[string]any{}}, nil }, TransactionBuilder: func(Mutation) ([]any, error) { return nil, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	session, err := driver.OpenSession(context.Background(), "reconnect-client")
	if err != nil {
		t.Fatal(err)
	}
	query := Query{ID: "q-reconnect", MatchAll: true}
	if _, err := session.Subscribe(context.Background(), query); err != nil {
		t.Fatal(err)
	}
	if got := len(session.RawFrames()); got < 3 {
		t.Fatalf("raw handshake/query evidence missing: %d", got)
	}
	if err := session.Reconnect(context.Background()); err != nil {
		t.Fatal(err)
	}
	dialer.mu.Lock()
	sessions := len(dialer.sessions)
	dialer.mu.Unlock()
	if sessions != 2 {
		t.Fatalf("reconnect did not create replacement transport: %d", sessions)
	}
	if got := len(session.RawFrames()); got < 6 {
		t.Fatalf("reconnect evidence/query replay missing: %d", got)
	}
	_ = session.Close()
}

func TestTargetSessionPauseReadsHonorsDeclaredDuration(t *testing.T) {
	s := &TargetSession{}
	s.PauseReads(25 * time.Millisecond)
	started := time.Now()
	if err := s.waitRead(context.Background()); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 20*time.Millisecond {
		t.Fatalf("pause ended before declared duration: %v", elapsed)
	}
}

func TestFourClientSemanticProbeHasFrozenContractDefaults(t *testing.T) {
	probe := NewFourClientSemanticProbe(Query{ID: "q"}, Mutation{EventID: "e"}, func(Refresh, Refresh) error { return nil })
	if probe.Query.ID != "q" || probe.Mutation.EventID != "e" || probe.Timeout <= 0 || probe.Validate == nil {
		t.Fatalf("invalid standard probe: %#v", probe)
	}
}

func TestDecodeReceiptErrorIsReturnedToRunAdapter(t *testing.T) {
	driver := &TargetDriver{cfg: TargetConfig{DecodeRefresh: func(SessionEvent, string) (Refresh, error) { return Refresh{}, errors.New("malformed computations") }}}
	client := &TargetSession{driver: driver, states: map[string]Materialized{}}
	_, err := driver.decodeReceipts(client, SessionEvent{Op: "refresh-ok", At: time.Now()}, "q", "client-q", newCommitPrefixes(nil))
	if err == nil || !strings.Contains(err.Error(), "malformed computations") {
		t.Fatalf("decoder error was swallowed: %v", err)
	}
}

func TestCommitPrefixesMapsNumericWatermarkWithoutWireEventID(t *testing.T) {
	w, _ := NewWorkload(FamilyH, 300, 47)
	q := w.Fixture.Queries[0]
	m1, m2 := w.Mutation(1), w.Mutation(2)
	o := NewPrefixOracle(w.Fixture)
	o.Append(m1)
	d1, _ := o.ExpectedDigest(q.ID, 1)
	o.Append(m2)
	d2, _ := o.ExpectedDigest(q.ID, 2)
	cp := newCommitPrefixes(nil)
	cp.Record(m1, Ack{Accepted: true, ServerTransactionID: "101", ProcessedTransactionID: "101"}, 1)
	cp.Record(m2, Ack{Accepted: true, ServerTransactionID: "102", ProcessedTransactionID: "102"}, 2)

	exact, ok := cp.ResolveAll(Receipt{QueryID: q.ID, RecipientID: "client-" + q.ID, ProcessedTransactionID: "101", Observed: mustMaterialized(o, q.ID, 1)})
	if !ok || len(exact) != 1 || exact[0].ClientEventID != m1.EventID || exact[0].Prefix != 1 {
		t.Fatalf("numeric exact watermark was not correlated: %#v", exact)
	}
	if !exact[0].ProvesIntermediate {
		t.Fatalf("single exact receipt did not prove its intermediate prefix: %#v", exact[0])
	}
	coalesced, ok := cp.ResolveAll(Receipt{QueryID: q.ID, RecipientID: "client-" + q.ID, ProcessedTransactionID: "102", Observed: mustMaterialized(o, q.ID, 2)})
	if !ok || len(coalesced) != 1 || coalesced[0].ClientEventID != m2.EventID {
		t.Fatalf("numeric coalesced watermark did not emit only newly covered event: %#v", coalesced)
	}
	if coalesced[0].Prefix != 2 {
		t.Fatalf("coalesced receipts used wrong watermark: %#v", coalesced)
	}
	if !coalesced[0].ProvesIntermediate {
		t.Fatalf("single newly emitted receipt at its exact watermark was not exact: %#v", coalesced[0])
	}
	if repeated, ok := cp.ResolveAll(Receipt{QueryID: q.ID, RecipientID: "client-" + q.ID, ProcessedTransactionID: "102", Observed: mustMaterialized(o, q.ID, 2)}); !ok || len(repeated) != 0 {
		t.Fatalf("repeated coalesced watermark was not bounded: %#v/%t", repeated, ok)
	}
	// A first observation at watermark 102 must still expand the entire
	// committed prefix; the incremental gate above has already emitted 101.
	cpAll := newCommitPrefixes(nil)
	cpAll.Record(m1, Ack{Accepted: true, ServerTransactionID: "101", ProcessedTransactionID: "101"}, 1)
	cpAll.Record(m2, Ack{Accepted: true, ServerTransactionID: "102", ProcessedTransactionID: "102"}, 2)
	all, ok := cpAll.ResolveAll(Receipt{QueryID: q.ID, RecipientID: "client-" + q.ID, ProcessedTransactionID: "102", Observed: mustMaterialized(o, q.ID, 2)})
	if !ok || len(all) != 2 {
		t.Fatalf("initial coalesced watermark did not expand committed prefix: %#v", all)
	}
	if all[0].ProvesIntermediate || all[1].ProvesIntermediate {
		t.Fatalf("multi-event expansion was mislabeled exact: %#v", all)
	}

	l := NewLedger()
	for i, mutation := range []Mutation{m1, m2} {
		key := LedgerKey{PairID: "p", RunID: "r", WriterID: "writer-0", RecipientID: "client-" + q.ID, ClientEventID: mutation.EventID}
		if err := l.AddExpected(key, mutation); err != nil {
			t.Fatal(err)
		}
		digest := d1
		if i == 1 {
			digest = d2
		}
		l.SetExpectation(key, []string{q.ID}, []string{key.RecipientID}, digest, i+1)
		l.Acknowledged(key, Ack{Accepted: true}, time.Now(), nil)
		observation := all[i]
		observationDigest := mustDigest(observation.Observed)
		latest := ""
		if i == 0 {
			latest = d2
		}
		if err := l.Observe(key, Observation{ObservedDigest: observationDigest, ExpectedDigest: digest, LatestExpectedDigest: latest, Prefix: observation.Prefix, ExpectedPrefix: i + 1, Applicable: true, At: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	rows := l.Rows()
	if rows[0].Coverage == CoverageMissing || rows[1].Coverage == CoverageMissing {
		t.Fatalf("exact/coalesced ledger coverage was not recorded: %#v", rows)
	}
}

func TestCommitPrefixesDoesNotSuppressChangedProofAtSameWatermark(t *testing.T) {
	w, _ := NewWorkload(FamilyH, 300, 49)
	q, mutation := w.Fixture.Queries[0], w.Mutation(1)
	o := NewPrefixOracle(w.Fixture)
	o.Append(mutation)
	good := mustMaterialized(o, q.ID, 1)
	bad := Materialized{QueryID: q.ID, Entities: map[string]Entity{}}
	cp := newCommitPrefixes(nil)
	cp.Record(mutation, Ack{Accepted: true, ProcessedTransactionID: "101"}, 1)
	if first, ok := cp.ResolveAll(Receipt{QueryID: q.ID, RecipientID: "client-" + q.ID, ProcessedTransactionID: "101", Observed: bad}); !ok || len(first) != 1 {
		t.Fatalf("initial proof was not emitted: %#v/%t", first, ok)
	}
	if retry, ok := cp.ResolveAll(Receipt{QueryID: q.ID, RecipientID: "client-" + q.ID, ProcessedTransactionID: "101", Observed: good}); !ok || len(retry) != 1 || retry[0].ClientEventID != mutation.EventID {
		t.Fatalf("changed proof at same watermark was suppressed: %#v/%t", retry, ok)
	}
}

func mustMaterialized(o *PrefixOracle, queryID string, prefix int) Materialized {
	m, err := o.Materialize(queryID, prefix)
	if err != nil {
		panic(err)
	}
	return m
}

func TestDialSSEAcceptsV1TransportHandshake(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("test server does not support streaming")
		}
		_, _ = w.Write([]byte("data: {\"op\":\"sse-init\",\"machine-id\":\"00000000-0000-0000-0000-000000000002\",\"session-id\":\"00000000-0000-0000-0000-000000000003\",\"sse-token\":\"00000000-0000-0000-0000-000000000004\"}\n\n"))
		flusher.Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()
	app := "00000000-0000-0000-0000-000000000001"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sess, err := DialSSE(ctx, SessionOptions{URL: srv.URL, AppID: app})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	if !strings.Contains(sess.(*SSESession).opts.URL, "app_id=") {
		t.Fatalf("resolved POST URL omitted app_id: %q", sess.(*SSESession).opts.URL)
	}
}
