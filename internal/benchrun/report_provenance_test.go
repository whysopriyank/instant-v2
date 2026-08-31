package benchrun

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

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

func TestProcessCollectorRequiresEndpointBindingWhenRequested(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("procfs endpoint ownership is Linux-specific")
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
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
	defer func() { _ = listener.Close() }()
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
