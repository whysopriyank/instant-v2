package benchrun

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/benchharness"
)

func TestSchemaRoundTripAndMeasurementStates(t *testing.T) {
	in := Run{SchemaVersion: SchemaVersion, ID: "r1", PairID: "p1", TargetID: "v2", Family: "H", Scale: 300, Seed: 7, MeasuredStartedAt: time.Unix(10, 0).UTC(), MeasuredFinishedAt: time.Unix(11, 0).UTC(), PrimaryClass: Pass, Measurements: map[string]Measurement{"zero": Zero("bytes"), "missing": Missing("bytes"), "unsupported": Unsupported("bytes", "network namespace unavailable")}}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out Run
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Measurements["zero"].Status != StatusZero || out.Measurements["missing"].Status != StatusMissing || out.Measurements["unsupported"].Status != StatusUnsupported {
		t.Fatalf("states lost in round trip: %+v", out.Measurements)
	}
	if !out.MeasuredStartedAt.Equal(in.MeasuredStartedAt) || !out.MeasuredFinishedAt.Equal(in.MeasuredFinishedAt) {
		t.Fatalf("measured boundaries lost in round trip: %v..%v", out.MeasuredStartedAt, out.MeasuredFinishedAt)
	}
}

func TestArtifactWriterRedactsAndVerifies(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteJSON("runs/r1/stdout.log", map[string]string{"authorization": "Bearer secret", "token": "abc", "safe": "ok"}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(root, "runs/r1/stdout.log"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "secret") || strings.Contains(string(b), "abc") {
		t.Fatalf("secret was retained: %s", b)
	}
	uri := Redact("postgres://bench:secret@localhost/instant_bench_x")
	if strings.Contains(uri, "secret") || !strings.Contains(uri, "[REDACTED]@") {
		t.Fatalf("URI credentials not redacted: %s", uri)
	}
	for _, header := range []string{"Authorization: Bearer very secret value", "Authorization: Basic dXNlcjpwYXNz"} {
		redacted := Redact(header)
		if strings.Contains(redacted, "secret") || strings.Contains(redacted, "dXNlcjpwYXNz") {
			t.Fatalf("authorization header leaked: %s", redacted)
		}
	}
	if err := w.Finalize(); err != nil {
		t.Fatal(err)
	}
	if err := VerifyChecksums(root); err != nil {
		t.Fatal(err)
	}
}

func TestCanonicalJSONPreservesLargeIntegerSeeds(t *testing.T) {
	first, err := CanonicalJSON(struct {
		Seed int64 `json:"seed"`
	}{Seed: 9007199254740993})
	if err != nil {
		t.Fatal(err)
	}
	second, err := CanonicalJSON(struct {
		Seed int64 `json:"seed"`
	}{Seed: 9007199254740994})
	if err != nil {
		t.Fatal(err)
	}
	if string(first) == string(second) || string(first) != `{"seed":9007199254740993}` {
		t.Fatalf("large integer canonicalization aliased or changed: %s / %s", first, second)
	}
}

func TestProcessWindowCPUUsesExactMeasuredBoundaries(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	end := start.Add(time.Second)
	samples := []ProcessSample{
		{At: start.Add(-time.Minute), UserCPU: Measurement{Status: StatusValue, Value: 99, Unit: "cpu_seconds"}, SystemCPU: Zero("cpu_seconds")},
		{At: start, UserCPU: Measurement{Status: StatusValue, Value: 10, Unit: "cpu_seconds"}, SystemCPU: Zero("cpu_seconds")},
		{At: end, UserCPU: Measurement{Status: StatusValue, Value: 11, Unit: "cpu_seconds"}, SystemCPU: Zero("cpu_seconds")},
	}
	m, ok := processWindowCPU(samples, start, end, 1000)
	if !ok || m.Status != StatusValue || m.Value != 1000 || m.Unit != "core_ms_per_1000_affected_coverages" {
		t.Fatalf("unexpected measured CPU: ok=%t measurement=%+v", ok, m)
	}
}

func TestProcessWindowCPURejectsCounterRegressionAndReversedWindow(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	end := start.Add(time.Second)
	samples := []ProcessSample{
		{At: start, UserCPU: Measurement{Status: StatusValue, Value: 1, Unit: "cpu_seconds"}, SystemCPU: Zero("cpu_seconds")},
		{At: start.Add(500 * time.Millisecond), UserCPU: Measurement{Status: StatusValue, Value: .5, Unit: "cpu_seconds"}, SystemCPU: Zero("cpu_seconds")},
		{At: end, UserCPU: Measurement{Status: StatusValue, Value: 2, Unit: "cpu_seconds"}, SystemCPU: Zero("cpu_seconds")},
	}
	if _, ok := processWindowCPU(samples, start, end, 1000); ok {
		t.Fatal("intermediate cumulative CPU regression was accepted")
	}
	if _, ok := processWindowCPU(samples, end, start, 1000); ok {
		t.Fatal("reversed measured window was accepted")
	}
}

func TestEndpointObservationsRejectUnitMismatch(t *testing.T) {
	pairs := make(map[string]map[string]Run, 7)
	for i := 0; i < 7; i++ {
		id := fmt.Sprintf("pair-%d", i)
		v1 := Run{PrimaryClass: Pass, Measurements: map[string]Measurement{"wire_bytes": {Status: StatusValue, Value: 10, Unit: "bytes_per_affected_recipient"}}}
		v2 := Run{PrimaryClass: Pass, Measurements: map[string]Measurement{"wire_bytes": {Status: StatusValue, Value: 5, Unit: "bytes"}}}
		pairs[id] = map[string]Run{"v1": v1, "v2": v2}
	}
	obs := endpointObservations(pairs, "wire_bytes")
	if len(obs) != 7 {
		t.Fatalf("observations=%d", len(obs))
	}
	for _, o := range obs {
		if !o.Failed || o.Failure != HarnessDefect {
			t.Fatalf("unit mismatch was accepted: %+v", o)
		}
	}
	if gate := AggregateDirection(obs, "wire_bytes", "bytes", "affected-recipient coverages", 1, LowerIsBetter).ClaimGate; gate.Eligible {
		t.Fatal("unit mismatch produced an eligible claim")
	}
}

func TestLiveResourceEvidenceRejectsFabricationAndRequiresBoundaries(t *testing.T) {
	makeFixture := func(t *testing.T) (string, Run, Target) {
		t.Helper()
		dir := t.TempDir()
		start := time.Unix(100, 0).UTC()
		end := start.Add(time.Second)
		run := Run{ID: "run-1", Scale: 10, MeasuredStartedAt: start, MeasuredFinishedAt: end, Measurements: map[string]Measurement{
			"cpu":                            {Status: StatusValue, Value: 1e6, Unit: "core_ms_per_1000_affected_coverages"},
			"peak_rss":                       {Status: StatusValue, Value: 200, Unit: "bytes"},
			"peak_rss_per_active_subscriber": {Status: StatusValue, Value: 20, Unit: "bytes_per_active_subscriber"},
		}}
		row := LedgerRow{RunID: run.ID, Coverage: "exact", RecipientID: "recipient", ClientEventID: "event", CoverAt: end, ConvergedAt: end}
		writeJSONL := func(name string, value any) {
			b, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, name), append(b, '\n'), 0600); err != nil {
				t.Fatal(err)
			}
		}
		writeJSONL("ledger.jsonl", row)
		processSamples := []ProcessSample{
			{At: start, PID: 42, StartTime: "proc-start", ExecutableHash: "exec-hash", UserCPU: Measurement{Status: StatusValue, Value: 1, Unit: "cpu_seconds"}, SystemCPU: Zero("cpu_seconds"), RSS: Measurement{Status: StatusValue, Value: 100, Unit: "bytes"}},
			{At: end, PID: 42, StartTime: "proc-start", ExecutableHash: "exec-hash", UserCPU: Measurement{Status: StatusValue, Value: 2, Unit: "cpu_seconds"}, SystemCPU: Zero("cpu_seconds"), RSS: Measurement{Status: StatusValue, Value: 200, Unit: "bytes"}},
		}
		var processBytes []byte
		for _, sample := range processSamples {
			b, err := json.Marshal(sample)
			if err != nil {
				t.Fatal(err)
			}
			processBytes = append(processBytes, b...)
			processBytes = append(processBytes, '\n')
		}
		if err := os.WriteFile(filepath.Join(dir, "process.jsonl"), processBytes, 0600); err != nil {
			t.Fatal(err)
		}
		target := Target{ID: "v1", Endpoint: "http://127.0.0.1", ExecutablePath: "/bin/app", ExecutableHash: "exec-hash"}
		return dir, run, target
	}
	dir, run, target := makeFixture(t)
	if err := validateLiveResourceEvidence(dir, run, target); err != nil {
		t.Fatalf("valid evidence rejected: %v", err)
	}
	for name, mutate := range map[string]func(*Run, *Target, []ProcessSample){
		"missing boundaries": func(r *Run, _ *Target, samples []ProcessSample) { r.MeasuredFinishedAt = time.Time{} },
		"empty executable":   func(_ *Run, target *Target, _ []ProcessSample) { target.ExecutableHash = "" },
		"fabricated cpu": func(r *Run, _ *Target, _ []ProcessSample) {
			r.Measurements["cpu"] = Measurement{Status: StatusValue, Value: 7, Unit: "core_ms_per_1000_affected_coverages"}
		},
		"fabricated rss": func(r *Run, _ *Target, _ []ProcessSample) {
			r.Measurements["peak_rss"] = Measurement{Status: StatusValue, Value: 999, Unit: "bytes"}
		},
		"missing cpu": func(r *Run, _ *Target, _ []ProcessSample) {
			delete(r.Measurements, "cpu")
		},
		"PID drift": func(_ *Run, _ *Target, samples []ProcessSample) {
			samples[1].PID = 43
		},
		"start drift": func(_ *Run, _ *Target, samples []ProcessSample) {
			samples[1].StartTime = "different-start"
		},
		"unsupported RSS": func(_ *Run, _ *Target, samples []ProcessSample) {
			samples[0].RSS = Unsupported("bytes", "collector unavailable")
		},
		"all unsupported": func(_ *Run, _ *Target, samples []ProcessSample) {
			for i := range samples {
				samples[i].UserCPU = Unsupported("cpu_seconds", "collector unavailable")
				samples[i].SystemCPU = Unsupported("cpu_seconds", "collector unavailable")
				samples[i].RSS = Unsupported("bytes", "collector unavailable")
			}
		},
	} {
		d, r, tg := makeFixture(t)
		var samples []ProcessSample
		b, err := os.ReadFile(filepath.Join(d, "process.jsonl"))
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range bytes.Split(bytes.TrimSpace(b), []byte("\n")) {
			var sample ProcessSample
			if err := json.Unmarshal(line, &sample); err != nil {
				t.Fatal(err)
			}
			samples = append(samples, sample)
		}
		mutate(&r, &tg, samples)
		writeProcessSamples(d, samples)
		if err := validateLiveResourceEvidence(d, r, tg); err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}
}

func writeProcessSamples(dir string, samples []ProcessSample) {
	var b []byte
	for _, sample := range samples {
		line, _ := json.Marshal(sample)
		b = append(b, line...)
		b = append(b, '\n')
	}
	_ = os.WriteFile(filepath.Join(dir, "process.jsonl"), b, 0600)
}

func TestRedactConsumesQuotedSecretValues(t *testing.T) {
	input := `dsn='postgres://bench:top secret@localhost/instant_bench_x?note=escaped\'suffix' cookie="session value with spaces\"and suffix" auth: "Bearer value with spaces"`
	redacted := Redact(input)
	for _, leaked := range []string{"top secret", "escaped", "suffix", "session value", "Bearer value"} {
		if strings.Contains(redacted, leaked) {
			t.Fatalf("quoted secret leaked %q: %s", leaked, redacted)
		}
	}
}

func TestArtifactWriterRedactionPreservesJSONWithQuotedDSNError(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	record := map[string]any{
		"failure": `unsafe benchmark database DSN: "postgresql://bench:top-secret@127.0.0.1/instant_bench_v1" host must be explicit`,
		"token":   "also-secret",
	}
	if err := w.WriteJSON("target.json", record); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(root, "target.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(b) {
		t.Fatalf("redaction produced invalid JSON: %s", b)
	}
	if strings.Contains(string(b), "top-secret") || strings.Contains(string(b), "also-secret") {
		t.Fatalf("secret was retained: %s", b)
	}
	var decoded map[string]any
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatal(err)
	}
}

func TestProcessCollectorRequiresEndpointBindingWhenRequested(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("procfs endpoint ownership is Linux-specific")
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	good := ProcProcessCollector{PID: os.Getpid(), ExecutablePath: executable, EndpointURLs: []string{fmt.Sprintf("http://127.0.0.1:%d/health", port)}}
	if _, err := good.Sample(context.Background()); err != nil {
		t.Fatalf("listener owned by sampled PID was not accepted: %v", err)
	}
	bad := good
	bad.EndpointURLs = []string{"http://127.0.0.1:1/health"}
	if _, err := bad.Sample(context.Background()); err == nil {
		t.Fatal("unowned endpoint was accepted for resource collector")
	}
}

func TestProcessCollectorRereadsPIDFileAtEachSample(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("procfs PID files are Linux-specific")
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	pidPath := filepath.Join(t.TempDir(), "target.pid")
	port := listener.Addr().(*net.TCPAddr).Port
	if err := os.WriteFile(pidPath, []byte(fmt.Sprint(os.Getpid())), 0600); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	collector := ProcProcessCollector{PIDFile: pidPath, ExecutablePath: executable, EndpointURLs: []string{fmt.Sprintf("http://127.0.0.1:%d/health", port)}}
	if _, err := collector.Sample(context.Background()); err != nil {
		t.Fatalf("initial PID file sample failed: %v", err)
	}
	if err := os.WriteFile(pidPath, []byte("1"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := collector.Sample(context.Background()); err == nil {
		t.Fatal("stale PID file value was accepted")
	}
}

func TestProcessIdentityIgnoresPreWindowReplacement(t *testing.T) {
	start := time.Unix(10, 0).UTC()
	end := start.Add(time.Second)
	identity := func(at time.Time, pid int) ProcessSample {
		return ProcessSample{At: at, PID: pid, StartTime: "start", ExecutableHash: "hash"}
	}
	samples := []ProcessSample{identity(start.Add(-time.Second), 10), identity(start, 20), identity(end, 20)}
	if !processIdentityStable(samples, start, end) {
		t.Fatal("pre-window process replacement poisoned measured identity")
	}
	samples[2].PID = 21
	if processIdentityStable(samples, start, end) {
		t.Fatal("measured-window process identity drift was accepted")
	}
}

func TestArtifactWriterRejectsUnsafePathsAndCaps(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write("../escape", []byte("x")); err == nil {
		t.Fatal("path traversal accepted")
	}
	if err := w.Write("big", []byte("1234")); err == nil {
		t.Fatal("size cap accepted oversized file")
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if err := w.Write("link", []byte("x")); err == nil {
		t.Fatal("symlink escape accepted")
	}
}

func TestPairScheduleIsSeededAndBalanced(t *testing.T) {
	a, err := PairOrder(42, "p1")
	if err != nil {
		t.Fatal(err)
	}
	b, err := PairOrder(42, "p1")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(a, ",") != strings.Join(b, ",") {
		t.Fatal("schedule not deterministic")
	}
	var ab, ba int
	for _, x := range a {
		if x == "AB" {
			ab++
		}
		if x == "BA" {
			ba++
		}
	}
	if ab != 4 || ba != 3 {
		t.Fatalf("unbalanced schedule: %v", a)
	}
}

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

func TestOfflineReportReplay(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	m := Manifest{SchemaVersion: SchemaVersion, BundleID: "b1", PairID: "p1", Family: "H", SubscriberScale: 300, Seed: 1, RunOrder: []string{"AB", "BA", "AB", "BA", "AB", "BA", "AB"}, StartedAt: time.Now().UTC()}
	if err := w.WriteJSON("manifest.json", m); err != nil {
		t.Fatal(err)
	}
	if err := w.Finalize(); err != nil {
		t.Fatal(err)
	}
	if _, err := ReportFromArtifacts(root); err == nil {
		t.Fatal("incomplete bundle was accepted")
	}
}

func TestReportRejectsOneRowExpectedCardinalityBypass(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC()
	run := Run{SchemaVersion: SchemaVersion, ID: "r", PairID: "p", TargetID: "v1", Family: "H", Scale: 300, Seed: 1, PrimaryClass: Pass, ExpectedLedgerRows: 2, ExpectedMutationRecipients: 2}
	row := LedgerRow{SchemaVersion: SchemaVersion, RunID: "r", PairID: "p", WriterID: "w", RecipientID: "recipient", ClientEventID: "event", ExpectedQuerySet: []string{"query"}, ExpectedRecipientSet: []string{"recipient"}, ExpectedMaterializedDigest: "digest", ObservedMaterializedDigest: "digest", Coverage: "exact", SubmittedAt: now, AcknowledgementAt: now, CoverAt: now, ConvergedAt: now}
	frame := Frame{SchemaVersion: SchemaVersion, RunID: "r", RecipientID: "recipient", ReceivedAt: now, Kind: "refresh", PayloadBytes: Zero("bytes")}
	if err := os.WriteFile(filepath.Join(dir, "ledger.jsonl"), mustJSONLForTest(row), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "frames.jsonl"), mustJSONLForTest(frame), 0600); err != nil {
		t.Fatal(err)
	}
	if err := validateRunEvidence(dir, run); err == nil {
		t.Fatal("one-row ledger bypassed expected cardinality")
	}
}

func mustJSONLForTest(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return append(b, '\n')
}

func TestOfflineProcessProvenanceRejectsMissingAndDriftingIdentity(t *testing.T) {
	dir := t.TempDir()
	run := Run{ID: "run", PrimaryClass: Pass}
	target := Target{ID: "v1", ExecutableHash: "signed-hash"}
	path := filepath.Join(dir, "process.jsonl")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := validateProcessProvenance(dir, run, target); err == nil {
		t.Fatal("empty process evidence accepted for a passing run")
	}
	now := time.Now().UTC()
	valid := func(pid int, start string) ProcessSample {
		return ProcessSample{At: now, PID: pid, StartTime: start, ExecutableHash: "signed-hash", UserCPU: Zero("cpu_seconds"), SystemCPU: Zero("cpu_seconds"), RSS: Zero("bytes")}
	}
	if err := os.WriteFile(path, append(mustJSONLForTest(valid(7, "start")), nil...), 0600); err != nil {
		t.Fatal(err)
	}
	if err := validateProcessProvenance(dir, run, target); err == nil {
		t.Fatal("single boundary process sample accepted")
	}
	if err := os.WriteFile(path, append(mustJSONLForTest(valid(7, "start")), mustJSONLForTest(valid(8, "reused"))...), 0600); err != nil {
		t.Fatal(err)
	}
	if err := validateProcessProvenance(dir, run, target); err == nil {
		t.Fatal("PID/start drift accepted in process evidence")
	}
	if err := os.WriteFile(path, append(mustJSONLForTest(valid(7, "start")), mustJSONLForTest(valid(7, "start"))...), 0600); err != nil {
		t.Fatal(err)
	}
	if err := validateProcessProvenance(dir, run, target); err != nil {
		t.Fatalf("stable process evidence rejected: %v", err)
	}
}

func TestBenchmarkDSNGuards(t *testing.T) {
	for _, dsn := range []string{"postgres://user:password@localhost/instant_bench_x", "postgres://localhost/production", "host=shared-db dbname=x", "postgres://user:password@10.0.0.1/instant_bench_x"} {
		if dsn == "postgres://user:password@localhost/instant_bench_x" {
			if err := ValidateBenchmarkDSN(dsn); err != nil {
				t.Fatalf("local credential-bearing DSN should be accepted: %v", err)
			}
			continue
		}
		if ValidateBenchmarkDSN(dsn) == nil {
			t.Fatalf("unsafe DSN accepted: %q", dsn)
		}
	}
	if err := ValidateBenchmarkDSN("host=127.0.0.1 port=5432 dbname=instant_bench_x"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateBenchmarkDSN("postgres://localhost/instant_bench_x?host=remote.example"); err == nil {
		t.Fatal("URI host override bypassed loopback validation")
	}
}

func TestUnsupportedCollectorsDoNotBecomeZero(t *testing.T) {
	p, _ := UnsupportedProcessCollector{Reason: "test"}.Sample(context.Background())
	if p.RSS.Status != StatusUnsupported || p.RSS.Value != 0 {
		t.Fatalf("unsupported process became zero: %+v", p.RSS)
	}
	d, _ := UnsupportedDatabaseCollector{Reason: "test"}.Before(context.Background())
	if d.Commits.Status != StatusUnsupported {
		t.Fatalf("unsupported database status lost: %+v", d.Commits)
	}
	n, _ := (UnsupportedCollector{Reason: "network namespace unavailable"}).Sample(context.Background())
	if n.Status != StatusUnsupported {
		t.Fatalf("unsupported network status lost: %+v", n)
	}
}

func TestChecksumRejectsExtraAndTamperAfterFinalize(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Write("one.json", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if err = w.Finalize(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "extra.json"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = VerifyChecksums(root); err == nil {
		t.Fatal("extra file was accepted")
	}
	if err = os.Remove(filepath.Join(root, "extra.json")); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "one.json"), []byte("tampered"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = VerifyChecksums(root); err == nil {
		t.Fatal("tamper was accepted")
	}
}

func TestArtifactWriterConcurrentWritersRemainBounded(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); _ = w.Write(fmt.Sprintf("%02d.json", i), []byte("{}")) }(i)
	}
	wg.Wait()
	if err := w.Finalize(); err != nil {
		t.Fatal(err)
	}
	if err := VerifyChecksums(root); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactWriterJSONLStreamingCleansPartialAndTracksTotal(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 128)
	if err != nil {
		t.Fatal(err)
	}
	records := make([]any, 0, 100)
	for i := 0; i < cap(records); i++ {
		records = append(records, map[string]int{"index": i, "value": i})
	}
	if err := w.WriteJSONL("runs/oversized.jsonl", records); err == nil {
		t.Fatal("oversized JSONL was accepted")
	}
	if _, err := os.Stat(filepath.Join(root, "runs", "oversized.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("partial JSONL remained: %v", err)
	}
	entries, err := filepath.Glob(filepath.Join(root, "runs", ".benchrun-jsonl-*"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("temporary JSONL files remained: %v %v", entries, err)
	}
	if w.total != 0 {
		t.Fatalf("failed JSONL changed total accounting: %d", w.total)
	}
}

func TestArtifactWriterJSONLStreamsTypedSlice(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	rows := []LedgerRow{{SchemaVersion: SchemaVersion, RunID: "run-typed", PairID: "pair", WriterID: "writer", RecipientID: "recipient", ClientEventID: "event", ExpectedQuerySet: []string{"query"}, ExpectedRecipientSet: []string{"recipient"}, ExpectedMaterializedDigest: "digest", Coverage: "none"}}
	if err := w.WriteJSONL("runs/typed.jsonl", rows); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, "runs", "typed.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var got LedgerRow
	if err := json.Unmarshal(bytes.TrimSpace(data), &got); err != nil {
		t.Fatal(err)
	}
	if got.RunID != rows[0].RunID || got.RecipientID != rows[0].RecipientID {
		t.Fatalf("typed JSONL row changed: %+v", got)
	}
}

func TestArtifactWriterContractBudgetIsIdempotent(t *testing.T) {
	w, err := NewArtifactWriter(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.ConfigureContractBudget(1024, 512); err != nil {
		t.Fatal(err)
	}
	if err := w.ConfigureContractBudget(1024, 512); err != nil {
		t.Fatalf("identical budget was not idempotent: %v", err)
	}
	if err := w.ConfigureContractBudget(2048, 512); err == nil {
		t.Fatal("different repeated budget was accepted")
	}
}

func TestVerifyChecksumsIgnoresUnsignedLargeManifestBudget(t *testing.T) {
	root := t.TempDir()
	m := Manifest{SchemaVersion: SchemaVersion, BundleID: "unsigned", PairID: "pair", Family: "H-append", SubscriberScale: 300, Seed: 7, RunOrder: []string{"AB", "BA", "AB", "BA", "AB", "BA", "AB"}, StartedAt: time.Unix(1, 0).UTC(), ArtifactMaxTotalBytes: MaxAbsoluteArtifactBudget, ArtifactMaxFileBytes: MaxAbsoluteArtifactBudget}
	manifest, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), append(manifest, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	largePath := filepath.Join(root, "huge.jsonl")
	f, err := os.Create(largePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(DefaultMaxArtifactBytes + 1); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	manifestHash, err := hashFileStringBounded(filepath.Join(root, "manifest.json"), SmallArtifactBytes)
	if err != nil {
		t.Fatal(err)
	}
	checksums := fmt.Sprintf("%s  manifest.json\n%s  huge.jsonl\n", manifestHash, strings.Repeat("0", 64))
	if err := os.WriteFile(filepath.Join(root, "checksums.sha256"), []byte(checksums), 0600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyChecksums(root); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("unsigned large artifact was not rejected under conservative limits: %v", err)
	}
}

func TestArtifactWriterJSONLPublicationRollbackOnTempUnlinkFailure(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	removeCalls := 0
	w.removeFile = func(path string) error {
		removeCalls++
		if removeCalls == 1 {
			return errors.New("injected temp unlink failure")
		}
		return os.Remove(path)
	}
	if err := w.WriteJSONL("runs/retry.jsonl", []any{map[string]string{"ok": "value"}}); err == nil {
		t.Fatal("post-link temp unlink failure was hidden")
	}
	if w.total != 0 {
		t.Fatalf("failed publication changed total accounting: %d", w.total)
	}
	if _, err := os.Stat(filepath.Join(root, "runs", "retry.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("published destination survived rollback: %v", err)
	}
	entries, err := filepath.Glob(filepath.Join(root, "runs", ".benchrun-jsonl-*"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("temporary JSONL survived deferred cleanup: %v %v", entries, err)
	}
	w.removeFile = os.Remove
	if err := w.WriteJSONL("runs/retry.jsonl", []any{map[string]string{"ok": "value"}}); err != nil {
		t.Fatalf("retry after rollback failed: %v", err)
	}
	if w.total == 0 {
		t.Fatal("successful retry did not update total accounting")
	}
}

func TestArtifactWriterJSONLRollbackFailureIsReported(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	removeCalls := 0
	w.removeFile = func(path string) error {
		removeCalls++
		if removeCalls <= 2 {
			return fmt.Errorf("injected remove failure %d", removeCalls)
		}
		return os.Remove(path)
	}
	err = w.WriteJSONL("runs/rollback-failure.jsonl", []any{map[string]string{"ok": "value"}})
	if err == nil || !strings.Contains(err.Error(), "injected remove failure 1") || !strings.Contains(err.Error(), "injected remove failure 2") {
		t.Fatalf("rollback failures were not both reported: %v", err)
	}
	if w.total != 0 {
		t.Fatalf("failed publication changed total accounting: %d", w.total)
	}
	if _, statErr := os.Stat(filepath.Join(root, "runs", "rollback-failure.jsonl")); statErr != nil {
		t.Fatalf("published destination was unexpectedly removed after rollback failure: %v", statErr)
	}
	entries, globErr := filepath.Glob(filepath.Join(root, "runs", ".benchrun-jsonl-*"))
	if globErr != nil || len(entries) != 0 {
		t.Fatalf("temporary JSONL survived cleanup: %v %v", entries, globErr)
	}
}

func TestCoalescedLedgerEvidenceRequiresCoveredPrefixAndDigest(t *testing.T) {
	root := t.TempDir()
	run := Run{ID: "run-coalesced", PrimaryClass: Pass, ExpectedLedgerRows: 1, ExpectedMutationRecipients: 1}
	now := time.Now().UTC()
	row := LedgerRow{SchemaVersion: SchemaVersion, RunID: run.ID, PairID: "p", WriterID: "w", RecipientID: "r", ClientEventID: "e", ExpectedQuerySet: []string{"q"}, ExpectedRecipientSet: []string{"r"}, SubmittedAt: now, AcknowledgementAt: now, CoverAt: now, ConvergedAt: now, ExpectedMaterializedDigest: "original", CoveredExpectedMaterializedDigest: "covered", ObservedMaterializedDigest: "covered", ExpectedStateVersion: 1, CoveredExpectedStateVersion: 2, ObservedStateVersion: 2, Coverage: "coalesced"}
	frame := Frame{SchemaVersion: SchemaVersion, RunID: run.ID, RecipientID: "r", ReceivedAt: now, Kind: "refresh", PayloadBytes: Zero("bytes")}
	write := func(name string, value any) {
		b, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name), append(b, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("ledger.jsonl", row)
	write("frames.jsonl", frame)
	if err := validateRunEvidence(root, run); err != nil {
		t.Fatalf("valid coalesced evidence rejected: %v", err)
	}
	row.CoveredExpectedMaterializedDigest = "original"
	write("ledger.jsonl", row)
	if err := validateRunEvidence(root, run); err == nil {
		t.Fatal("coalesced digest bypass was accepted")
	}
}

func TestContractArtifactBudgetScalesBeyondDefault(t *testing.T) {
	w, err := NewArtifactWriter(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := configureContractArtifactBudget(w, "H-append", 300, Plan{Seed: 7, MeasureSeconds: 180}); err != nil {
		t.Fatal(err)
	}
	if w.MaxBytes <= DefaultMaxArtifactBytes || w.MaxFileBytes <= DefaultMaxArtifactBytes {
		t.Fatalf("contract budget did not exceed default: total=%d file=%d", w.MaxBytes, w.MaxFileBytes)
	}
	w2000, err := NewArtifactWriter(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := configureContractArtifactBudget(w2000, "H-append", 2000, Plan{Seed: 7, MeasureSeconds: 180}); err != nil {
		t.Fatal(err)
	}
	if w2000.MaxBytes <= w.MaxBytes {
		t.Fatalf("2000 scale budget did not grow: 300=%d 2000=%d", w.MaxBytes, w2000.MaxBytes)
	}
}

func TestContractLedgerBoundCoversWorstShapeAndScalesWithCardinality(t *testing.T) {
	makeBound := func(scale int) (int64, int64, error) {
		w, err := benchharness.NewWorkload(benchharness.FamilyH, scale, 7)
		if err != nil {
			return 0, 0, err
		}
		bound, err := contractLedgerRowBound(scale, len(w.Fixture.Queries))
		if err != nil {
			return 0, 0, err
		}
		row := LedgerRow{SchemaVersion: SchemaVersion, PairID: "p", RunID: "r", WriterID: "writer", RecipientID: "recipient", ClientEventID: "event", ExpectedQuerySet: make([]string, len(w.Fixture.Queries)), ExpectedRecipientSet: make([]string, scale), ExpectedMaterializedDigest: strings.Repeat("a", 64), ObservedMaterializedDigest: strings.Repeat("b", 64), Coverage: "coalesced", CoveredExpectedMaterializedDigest: strings.Repeat("b", 64)}
		for i := range row.ExpectedQuerySet {
			row.ExpectedQuerySet[i] = fmt.Sprintf("query-%06d", i)
		}
		for i := range row.ExpectedRecipientSet {
			row.ExpectedRecipientSet[i] = fmt.Sprintf("recipient-%06d", i)
		}
		encoded, err := json.Marshal(row)
		if err != nil {
			return 0, 0, err
		}
		return bound, int64(len(encoded)), nil
	}
	bound300, size300, err := makeBound(300)
	if err != nil {
		t.Fatal(err)
	}
	if size300 >= bound300 {
		t.Fatalf("worst-shape row exceeds bound: size=%d bound=%d", size300, bound300)
	}
	bound2000, _, err := makeBound(2000)
	if err != nil {
		t.Fatal(err)
	}
	if bound2000 <= bound300*5 {
		t.Fatalf("row bound did not scale with repeated recipient sets: 300=%d 2000=%d", bound300, bound2000)
	}
	if _, err := contractLedgerRowBound(int(^uint(0)>>1), int(^uint(0)>>1)); err == nil {
		t.Fatal("overflowing ledger bound was accepted")
	}
}

func TestPairRunnerRetainsFourteenRunsAndFailedAttempt(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<26)
	if err != nil {
		t.Fatal(err)
	}
	r := PairRunner{Writer: w, Executor: SyntheticExecutor{FailAttempt: 3, FailClass: TargetTimeout}, Manifest: Manifest{SchemaVersion: SchemaVersion, BundleID: "b", PairID: "p", Family: "H", SubscriberScale: 300, Seed: 9, RunOrder: []string{"AB", "BA", "AB", "BA", "AB", "BA", "AB"}, StartedAt: time.Now().UTC()}, Plan: Plan{SchemaVersion: SchemaVersion, Seed: 9, Pairs: 7}, Targets: []Target{{SchemaVersion: SchemaVersion, ID: "v1", Role: "v1"}, {SchemaVersion: SchemaVersion, ID: "v2", Role: "v2_current"}}}
	s, err := r.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Cells) != 1 || s.Cells[0].Attempts != 7 || len(s.Cells[0].Failures) == 0 || s.ClaimGate.Eligible {
		t.Fatalf("unexpected summary: %+v", s)
	}
	count := 0
	failed := false
	err = filepath.Walk(root, func(path string, info os.FileInfo, e error) error {
		if e != nil {
			return e
		}
		if filepath.Base(path) != "run.json" {
			return nil
		}
		count++
		b, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		if strings.Contains(string(b), string(TargetTimeout)) {
			failed = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 14 || !failed {
		t.Fatalf("runs=%d failed=%t", count, failed)
	}
	if _, err = os.Stat(filepath.Join(root, "summary.json")); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(root, "report.md")); err != nil {
		t.Fatal(err)
	}
}

func TestPairRunnerRetainsProtocolAndInfrastructureFailures(t *testing.T) {
	for _, class := range []FailureClass{TargetProtocolFailure, InfrastructureNoise} {
		root := t.TempDir()
		w, e := NewArtifactWriter(root, 1<<26)
		if e != nil {
			t.Fatal(e)
		}
		r := PairRunner{Writer: w, Executor: SyntheticExecutor{FailAttempt: 2, FailClass: class}, Manifest: Manifest{SchemaVersion: SchemaVersion, BundleID: "b", PairID: "p", Family: "H", SubscriberScale: 300, Seed: 11, RunOrder: []string{"AB", "BA", "AB", "BA", "AB", "BA", "AB"}, StartedAt: time.Now().UTC()}, Plan: Plan{SchemaVersion: SchemaVersion, Seed: 11, Pairs: 7}, Targets: []Target{{SchemaVersion: SchemaVersion, ID: "v1", Role: "v1"}, {SchemaVersion: SchemaVersion, ID: "v2", Role: "v2_current"}}}
		s, e := r.Run(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		if len(s.Cells) != 1 || len(s.Cells[0].Failures) == 0 || s.Cells[0].Failures[0] != class {
			t.Fatalf("class %s not retained: %+v", class, s.Cells)
		}
	}
}
