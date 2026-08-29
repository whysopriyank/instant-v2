package benchrun

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/benchharness"
)

type scriptedDriver struct {
	called    bool
	qualifies int
	runs      int
}

func (d *scriptedDriver) QualifyTarget(context.Context, Target) (Qualification, error) {
	d.qualifies++
	return Qualification{Passed: true, Checks: map[string]bool{"script": true}}, nil
}
func (d *scriptedDriver) RunTarget(_ context.Context, s RunSpec) (ExecutionResult, error) {
	d.called = true
	d.runs++
	now := time.Now().UTC()
	run := s.Run
	value := 10.0
	if s.Target.ID == "v2" {
		value = 5
	}
	run.Measurements = map[string]Measurement{"primary": {Status: StatusValue, Value: value, Unit: "ms"}}
	run.ExpectedLedgerRows = 1
	run.ExpectedMutationRecipients = 1
	return ExecutionResult{Run: run,
		Ledger: []LedgerRow{{SchemaVersion: SchemaVersion, RunID: s.Run.ID, PairID: s.Run.PairID, WriterID: "scripted", RecipientID: "recipient", ClientEventID: "event", ExpectedQuerySet: []string{"query"}, ExpectedRecipientSet: []string{"recipient"}, ExpectedMaterializedDigest: "digest", ObservedMaterializedDigest: "digest", Coverage: "exact", SubmittedAt: now, AcknowledgementAt: now, CoverAt: now, ConvergedAt: now}},
		Frames: []Frame{{SchemaVersion: SchemaVersion, RunID: s.Run.ID, RecipientID: "recipient", ReceivedAt: now, Kind: "refresh", PayloadBytes: Zero("bytes")}},
	}, nil
}

func TestTargetDriverAdapterDelegatesWithoutFallback(t *testing.T) {
	d := &scriptedDriver{}
	a := TargetDriverAdapter{Driver: d}
	q, e := a.Qualify(context.Background(), Target{ID: "v1"})
	if e != nil || !q.Passed {
		t.Fatalf("qualification: %+v %v", q, e)
	}
	_, e = a.Execute(context.Background(), RunSpec{Run: Run{ID: "r"}})
	if e != nil || !d.called {
		t.Fatalf("execute delegation: %v %t", e, d.called)
	}
	if _, e = (TargetDriverAdapter{}).Execute(context.Background(), RunSpec{}); e == nil {
		t.Fatal("nil adapter produced success")
	}
}

func TestTargetDriverAdapterPairRunnerScriptsQualificationAndFourteenAttempts(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<26)
	if err != nil {
		t.Fatal(err)
	}
	d := &scriptedDriver{}
	r := PairRunner{Writer: w, Executor: TargetDriverAdapter{Driver: d}, Manifest: Manifest{
		SchemaVersion: SchemaVersion, BundleID: "b", PairID: "p", Family: "H", SubscriberScale: 300, Seed: 9, V1SHA: "v1", V2SHA: "v2", SourceTree: "test", SchemaHash: "schema", FixtureHash: "fixture", ConfigHash: "config",
		RunOrder: []string{"AB", "BA", "AB", "BA", "AB", "BA", "AB"}, StartedAt: time.Now().UTC(),
	}, Plan: Plan{SchemaVersion: SchemaVersion, Seed: 9, Pairs: 7}, Targets: []Target{
		{SchemaVersion: SchemaVersion, ID: "v1", Role: "v1"}, {SchemaVersion: SchemaVersion, ID: "v2", Role: "v2_current"},
	}}
	s, err := r.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if d.qualifies != 2 || d.runs != 14 || len(s.Cells) != 1 || s.ClaimGate.Eligible {
		t.Fatalf("scripted live path calls/claim: qualifies=%d runs=%d summary=%+v", d.qualifies, d.runs, s)
	}
}

func TestRawFrameMappingRedactsAndPreservesEvidenceMetadata(t *testing.T) {
	frame := mapRawFrame("run-1", benchharness.RawFrameEvidence{
		ClientID:               "client-1",
		QueryID:                "query-1",
		RecipientID:            "recipient-1",
		Op:                     "refresh-ok",
		Class:                  benchharness.FrameApplicationRefresh,
		ClientEventID:          "event-1",
		ServerTransactionID:    "tx-1",
		ProcessedTransactionID: "processed-1",
		At:                     time.Unix(10, 0).UTC(),
		PayloadBytes:           63,
		PayloadDigest:          "digest",
		ProtocolError:          "protocol token=secret",
	})
	if frame.RunID != "run-1" || frame.ClientID != "client-1" || frame.QueryID != "query-1" || frame.ServerTransactionID != "tx-1" || frame.Class != string(benchharness.FrameApplicationRefresh) {
		t.Fatalf("frame metadata not mapped: %+v", frame)
	}
	if frame.PayloadBytes.Status != StatusValue || frame.PayloadBytes.Value != 63 {
		t.Fatalf("payload size not exact: %+v", frame.PayloadBytes)
	}
	if frame.PayloadDigest == "" || !strings.HasPrefix(frame.EvidenceRef, "redacted-payload-sha256:") {
		t.Fatalf("redacted evidence digest missing: %+v", frame)
	}
	if strings.Contains(frame.EvidenceRef, "secret") || frame.ErrorClass != string(TargetProtocolFailure) {
		t.Fatalf("secret/error leaked or error not classified: %+v", frame)
	}
}

func TestLiveCollectorsSupportedAndPlanPhases(t *testing.T) {
	if _, err := os.Stat("/proc"); err != nil {
		t.Skip("procfs process collector is unavailable")
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	endpoint := "http://" + listener.Addr().String() + "/health"
	process, err := (ProcProcessCollector{PID: os.Getpid(), ExecutablePath: executable, EndpointURLs: []string{endpoint}}).Sample(context.Background())
	if err != nil {
		if os.IsNotExist(err) {
			t.Skip("procfs does not expose the test process")
		}
		t.Fatal(err)
	}
	if process.PID != os.Getpid() || process.StartTime == "" || process.ExecutableHash == "" || process.RSS.Status != StatusValue || process.Threads.Status != StatusValue || process.FDs.Status != StatusValue {
		t.Fatalf("incomplete process sample: %+v", process)
	}
	if _, err := (ProcProcessCollector{PID: os.Getpid(), ExecutablePath: executable, ExpectedHash: "0000000000000000000000000000000000000000000000000000000000000000", EndpointURLs: []string{endpoint}}).Sample(context.Background()); err == nil {
		t.Fatal("mismatched signed executable hash was accepted")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("go_memstats_alloc_bytes 10\ngo_memstats_heap_alloc_bytes 20\ngo_memstats_heap_goal_bytes 30\ngo_gc_duration_seconds_count 2\ngo_gc_duration_seconds_sum 0.5\ngo_goroutines 4\n"))
	}))
	defer server.Close()
	runtimeSample, err := (PrometheusRuntimeCollector{URL: server.URL}).Sample(context.Background())
	if err != nil || runtimeSample.AllocBytes.Status != StatusValue || runtimeSample.GCCycles.Value != 2 || runtimeSample.GCPause.Value != 0.5 {
		t.Fatalf("incomplete runtime sample: %+v %v", runtimeSample, err)
	}
	unsupported := UnsupportedLiveCollectors("test")
	if sample, _ := unsupported.Process.Sample(context.Background()); sample.RSS.Status != StatusUnsupported {
		t.Fatalf("unsupported process was not explicit: %+v", sample)
	}
	w, err := benchharness.NewWorkload(benchharness.FamilyH, 300, 7)
	if err != nil {
		t.Fatal(err)
	}
	plan := buildBenchharnessRunPlan(RunSpec{Run: Run{PairID: "p", ID: "r"}, Plan: Plan{RampSeconds: 2, SettleSeconds: 3, WarmupSeconds: 4, WarmupMutations: 5, GraceSeconds: 6, MeasureSeconds: 7}}, w)
	if plan.RampDuration != 2*time.Second || plan.SettleDuration != 3*time.Second || plan.WarmupDuration != 4*time.Second || plan.WarmupMutations != 5 {
		t.Fatalf("plan phase settings not propagated: %+v", plan)
	}
}

func TestParsePrometheusSupportsUnlabeledAndLabeledSamples(t *testing.T) {
	metrics := parsePrometheus("go_memstats_alloc_bytes 10\n" +
		"go_gc_duration_seconds_count{quantile=\"0.5\"} 2\n")
	if metrics["go_memstats_alloc_bytes"] != 10 {
		t.Fatalf("unlabeled sample parsed incorrectly: %+v", metrics)
	}
	if metrics["go_gc_duration_seconds_count"] != 2 {
		t.Fatalf("labeled sample parsed incorrectly: %+v", metrics)
	}
	if _, ok := metrics["go_memstats_alloc_bytes 10"]; ok {
		t.Fatalf("sample value leaked into metric name: %+v", metrics)
	}
}

func TestClassifyLedgerEvidence(t *testing.T) {
	tests := []struct {
		name string
		rows []benchharness.LedgerRow
		want FailureClass
	}{
		{name: "pass", rows: []benchharness.LedgerRow{{Coverage: benchharness.CoverageExact}}, want: Pass},
		{name: "missing coverage", rows: []benchharness.LedgerRow{{Coverage: benchharness.CoverageMissing}}, want: TargetSemanticFailure},
		{name: "target", rows: []benchharness.LedgerRow{{Coverage: benchharness.CoverageMissing, ErrorClass: benchharness.ErrorTarget}}, want: TargetSemanticFailure},
		{name: "infrastructure", rows: []benchharness.LedgerRow{{Coverage: benchharness.CoverageMissing, ErrorClass: benchharness.ErrorInfrastructure}}, want: HarnessDefect},
		{name: "protocol precedence", rows: []benchharness.LedgerRow{{Coverage: benchharness.CoverageMissing, ErrorClass: benchharness.ErrorInfrastructure}, {Coverage: benchharness.CoverageMissing, ErrorClass: benchharness.ErrorProtocol}}, want: TargetProtocolFailure},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, failure := classifyLedgerEvidence(tt.rows)
			if got != tt.want {
				t.Fatalf("classification = %q, want %q", got, tt.want)
			}
			if got != Pass && failure == "" {
				t.Fatal("failed ledger classification lacks failure reason")
			}
		})
	}
}

func TestLoadLiveConfigAndDefaultBuilders(t *testing.T) {
	dir := t.TempDir()
	meta := map[string]any{"revision": "abc", "database_name": "instant_bench_v1", "postgres_version": "17.1", "invalidation_mode": "direct", "output_plugin": "wal2json", "dirty_tree_hash": "clean"}
	mb, _ := json.Marshal(meta)
	mp := filepath.Join(dir, "v1-meta.json")
	if e := os.WriteFile(mp, mb, 0600); e != nil {
		t.Fatal(e)
	}
	meta["output_plugin"] = "pgoutput"
	meta["database_name"] = "instant_bench_v2"
	mb, _ = json.Marshal(meta)
	mp2 := filepath.Join(dir, "v2-meta.json")
	if e := os.WriteFile(mp2, mb, 0600); e != nil {
		t.Fatal(e)
	}
	common := func(id, db, metadata, transport string) LiveTargetConfig {
		sha, _ := hashFile("/usr/bin/true")
		prefix := "BENCH_" + strings.ToUpper(id) + "_"
		return LiveTargetConfig{ID: id, Kind: id, Transport: transport, SessionURL: map[string]string{"v1": "ws://127.0.0.1/runtime/session", "v2": "http://127.0.0.1/runtime/sse"}[id], HealthURL: "http://127.0.0.1/health", AppID: "00000000-0000-0000-0000-00000000000" + id[len(id)-1:], Revision: "abc", DirtyTreeHash: "clean", DatabaseName: db, PostgresVersion: "17.1", InvalidationMode: "direct", OutputPlugin: map[string]string{"v1": "wal2json", "v2": ""}[id], MetadataFile: metadata, ProvisionedMarker: db, ProvisionCommand: []string{"/usr/bin/true"}, ProvisionCommandSHA256: sha, DatabaseURLEnv: prefix + "DATABASE_URL", AdminTokenEnv: prefix + "ADMIN_TOKEN", RefreshTokenEnv: prefix + "REFRESH_TOKEN", RuntimeTokenEnv: prefix + "RUNTIME_TOKEN", ProcessPIDEnv: prefix + "PID", ProcessExecutablePath: "/usr/bin/true", ProcessExecutableSHA256: sha, MarkerQuery: "SELECT current_database(), current_setting('server_version'), $1", ProbeEntityID: "00000000-0000-0000-0000-000000000003"}
	}
	ids := FixtureIDs{EntityType: "bench_items", ValueAttr: "value", BucketAttr: "bucket", RankAttr: "rank", ValueAttrID: "00000000-0000-0000-0000-000000000001", BucketAttrID: "00000000-0000-0000-0000-000000000002", RankAttrID: "00000000-0000-0000-0000-000000000003"}
	fixture, err := BuildFixtureEvidence(ids, "H-append", 300, 1)
	if err != nil {
		t.Fatal(err)
	}
	fixtureBytes, _ := json.Marshal(fixture)
	fixturePath := filepath.Join(dir, "fixture.json")
	if err := os.WriteFile(fixturePath, fixtureBytes, 0600); err != nil {
		t.Fatal(err)
	}
	fixtureHash, _ := DigestJSON(fixture)
	cfg := LiveConfig{PairID: "p", Seed: 1, Family: "H-append", Scale: 300, V1SHA: "abc", V2SHA: "abc", FixturePath: fixturePath, FixtureHash: fixtureHash, Fixture: ids, Targets: []LiveTargetConfig{common("v1", "instant_bench_v1", mp, "websocket"), common("v2", "instant_bench_v2", mp2, "sse")}, Executables: map[string]string{"v1": common("v1", "instant_bench_v1", mp, "websocket").ProcessExecutableSHA256, "v2": common("v2", "instant_bench_v2", mp2, "sse").ProcessExecutableSHA256}}
	cfg.ConfigHash, _ = DigestJSON(CanonicalConfigEvidence(cfg))
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	t.Setenv(TrustedApprovalPublicKeyEnv, hex.EncodeToString(pub))
	tuple, _ := LiveConfigAuthorizationTuple(cfg)
	cfg.AuthorizationSignature = hex.EncodeToString(ed25519.Sign(priv, tuple))
	path := filepath.Join(dir, "config.json")
	b, _ := json.Marshal(cfg)
	if e := os.WriteFile(path, b, 0600); e != nil {
		t.Fatal(e)
	}
	loaded, e := LoadLiveConfig(path)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = loaded.Executor(); e != nil {
		t.Fatal(e)
	}
	q, e := DefaultInstaQLQuery(loaded.Fixture, benchharness.Query{MatchAll: true})
	if e != nil || q == nil {
		t.Fatalf("default query: %#v %v", q, e)
	}
	steps, e := DefaultTransactionSteps(loaded.Fixture, benchharness.Mutation{EntityID: "e", Marker: "bench/m", Kind: benchharness.MutationAppend})
	if e != nil || len(steps) != 3 {
		t.Fatalf("default tx steps: %#v %v", steps, e)
	}
}

func TestProbeValidatorRequiresMutationChangeAndMarker(t *testing.T) {
	dir := t.TempDir()
	meta := map[string]any{"revision": "abc", "database_name": "instant_bench_v1", "postgres_version": "17.1", "invalidation_mode": "direct", "dirty_tree_hash": "", "output_plugin": "wal2json"}
	b, _ := json.Marshal(meta)
	path := filepath.Join(dir, "meta.json")
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	cfg := LiveTargetConfig{ID: "v1", Revision: "abc", DatabaseName: "instant_bench_v1", PostgresVersion: "17.1", InvalidationMode: "direct", OutputPlugin: "wal2json", MetadataFile: path, ProbeEntityID: "probe-id"}
	tc, err := cfg.driverConfig(FixtureIDs{EntityType: "x", ValueAttr: "value", ValueAttrID: "00000000-0000-0000-0000-000000000001"}, "", "", "H", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := tc.AttributeAliases["value"]; got != "00000000-0000-0000-0000-000000000001" {
		t.Fatalf("value attribute alias = %q", got)
	}
	initial := benchharness.Refresh{QueryID: "probe-query", Kind: benchharness.RefreshFull, Full: &benchharness.Materialized{QueryID: "probe-query", Entities: map[string]benchharness.Entity{}}}
	unchanged := benchharness.Refresh{QueryID: "probe-query", Kind: benchharness.RefreshFull, Full: &benchharness.Materialized{QueryID: "probe-query", Entities: map[string]benchharness.Entity{}}}
	if err := tc.Probe.Validate(initial, unchanged); err == nil {
		t.Fatal("unchanged probe was accepted")
	}
	changed := unchanged
	changed.Full = &benchharness.Materialized{QueryID: "probe-query", Entities: map[string]benchharness.Entity{"probe-id": {ID: "probe-id", Attributes: map[string]any{"value": "bench/probe"}}}}
	if err := tc.Probe.Validate(initial, changed); err != nil {
		t.Fatalf("valid probe rejected: %v", err)
	}
}

func TestDefaultMutationShapesCoverAllFamilies(t *testing.T) {
	ids := FixtureIDs{EntityType: "bench_items", ValueAttr: "value", BucketAttr: "bucket", RankAttr: "rank", ValueAttrID: "00000000-0000-0000-0000-000000000001", BucketAttrID: "00000000-0000-0000-0000-000000000002", RankAttrID: "00000000-0000-0000-0000-000000000003"}
	families := []benchharness.Family{benchharness.FamilyH, benchharness.FamilyX, benchharness.FamilyM, benchharness.FamilyO, benchharness.FamilyS, benchharness.FamilyR, benchharness.FamilyC, benchharness.FamilyT}
	for _, family := range families {
		w, e := benchharness.NewWorkload(family, 300, 7)
		if e != nil {
			t.Fatal(e)
		}
		for _, kind := range []benchharness.MutationKind{benchharness.MutationAppend, benchharness.MutationUpdate, benchharness.MutationRetract, benchharness.MutationReorder} {
			m := w.Mutation(1)
			m.Kind = kind
			m.EntityID = "00000000-0000-0000-0000-000000000001"
			steps, e := DefaultTransactionSteps(ids, m)
			if e != nil {
				t.Fatalf("%s/%s: %v", family, kind, e)
			}
			if len(steps) == 0 {
				t.Fatalf("%s/%s empty steps", family, kind)
			}
		}
	}
}
