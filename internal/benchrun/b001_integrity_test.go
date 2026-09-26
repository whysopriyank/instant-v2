package benchrun

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func syntheticTriadBundle(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writer, err := NewArtifactWriter(root, 1<<28)
	if err != nil {
		t.Fatal(err)
	}
	runner := PairRunner{
		Writer: writer, Executor: SyntheticExecutor{},
		Manifest: Manifest{
			SchemaVersion: SchemaVersion, BundleID: "b001-triad", PairID: "b001-triad",
			Family: "H-append", SubscriberScale: 300, Seed: 17, StartedAt: time.Unix(1, 0).UTC(),
		},
		Plan: Plan{SchemaVersion: SchemaVersion, Seed: 17, Pairs: 7},
		Targets: []Target{
			{SchemaVersion: SchemaVersion, ID: "v1", Role: "v1", Revision: "v1-sha"},
			{SchemaVersion: SchemaVersion, ID: "v2_reference", Role: "v2_reference", Revision: "reference-sha"},
			{SchemaVersion: SchemaVersion, ID: "v2_current", Role: "v2_current", Revision: "current-sha"},
		},
	}
	if _, err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	return root
}

func makeTriadTargetLive(t *testing.T, root, id string) {
	t.Helper()
	path := filepath.Join(root, "targets", id+".json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var target Target
	if err := json.Unmarshal(b, &target); err != nil {
		t.Fatal(err)
	}
	target.Endpoint = "http://127.0.0.1:8080/runtime/session"
	target.ExecutablePath = "/usr/local/bin/instantd"
	target.ExecutableHash = "signed-executable-hash"
	if err := os.WriteFile(path, mustJSON(target), 0600); err != nil {
		t.Fatal(err)
	}
}

func syntheticPairBundle(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writer, err := NewArtifactWriter(root, 1<<28)
	if err != nil {
		t.Fatal(err)
	}
	runner := PairRunner{
		Writer: writer, Executor: SyntheticExecutor{},
		Manifest: Manifest{
			SchemaVersion: SchemaVersion, BundleID: "b001-pair", PairID: "b001-pair",
			Family: "H-append", SubscriberScale: 300, Seed: 17, V1SHA: "v1-sha", V2SHA: "current-sha", RunOrder: []string{"AB", "BA", "AB", "BA", "AB", "BA", "AB"}, StartedAt: time.Unix(1, 0).UTC(),
		},
		Plan: Plan{SchemaVersion: SchemaVersion, Seed: 17, Pairs: 7},
		Targets: []Target{
			{SchemaVersion: SchemaVersion, ID: "v1", Role: "v1", Revision: "v1-sha"},
			{SchemaVersion: SchemaVersion, ID: "v2", Role: "v2_current", Revision: "current-sha"},
		},
	}
	if _, err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	return root
}

func makeLiveBundle(t *testing.T, triad bool) string {
	t.Helper()
	root := syntheticPairBundle(t)
	if triad {
		root = syntheticTriadBundle(t)
	}
	targetIDs := []string{"v1", "v2"}
	if triad {
		targetIDs = []string{"v1", "v2_reference", "v2_current"}
	}
	for _, id := range targetIDs {
		makeTriadTargetLive(t, root, id)
		targetPath := filepath.Join(root, "targets", id+".json")
		b, err := os.ReadFile(targetPath)
		if err != nil {
			t.Fatal(err)
		}
		var target Target
		if err := json.Unmarshal(b, &target); err != nil {
			t.Fatal(err)
		}
		target.PostgresVersion = "17.0"
		if err := os.WriteFile(targetPath, mustJSON(target), 0600); err != nil {
			t.Fatal(err)
		}
	}
	runPaths, err := filepath.Glob(filepath.Join(root, "runs", "*"))
	if err != nil {
		t.Fatal(err)
	}
	for _, runPath := range runPaths {
		b, err := os.ReadFile(filepath.Join(runPath, "run.json"))
		if err != nil {
			t.Fatal(err)
		}
		var run Run
		if err := json.Unmarshal(b, &run); err != nil {
			t.Fatal(err)
		}
		start, end := run.StartedAt, run.EndedAt
		if start.IsZero() {
			start = time.Now().UTC()
			end = start.Add(time.Second)
		}
		if !end.After(start) {
			end = start.Add(time.Second)
		}
		run.MeasuredStartedAt, run.MeasuredFinishedAt = start, end
		run.Measurements["cpu"] = Zero("core_ms_per_1000_affected_coverages")
		run.Measurements["peak_rss"] = Measurement{Status: StatusValue, Value: 100, Unit: "bytes"}
		run.Measurements["peak_rss_per_active_subscriber"] = Measurement{Status: StatusValue, Value: 100.0 / float64(run.Scale), Unit: "bytes_per_active_subscriber"}
		run.CollectorProvenance = map[string]string{
			"process":  "supported: pid=42",
			"runtime":  "supported: prometheus endpoint http://127.0.0.1:9090/metrics",
			"database": "supported: pg_stat_database/pg_stat_wal via BENCH_DATABASE_URL",
			"network":  "unsupported: isolated network namespace not configured",
		}
		if err := os.WriteFile(filepath.Join(runPath, "run.json"), mustJSON(run), 0600); err != nil {
			t.Fatal(err)
		}
		process := []ProcessSample{
			{At: start, PID: 42, StartTime: "start-" + run.ID, ExecutableHash: "signed-executable-hash", UserCPU: Zero("cpu_seconds"), SystemCPU: Zero("cpu_seconds"), RSS: Measurement{Status: StatusValue, Value: 100, Unit: "bytes"}},
			{At: end, PID: 42, StartTime: "start-" + run.ID, ExecutableHash: "signed-executable-hash", UserCPU: Zero("cpu_seconds"), SystemCPU: Zero("cpu_seconds"), RSS: Measurement{Status: StatusValue, Value: 100, Unit: "bytes"}},
		}
		var processLines []byte
		for _, sample := range process {
			processLines = append(processLines, mustJSONLForTest(sample)...)
		}
		if err := os.WriteFile(filepath.Join(runPath, "process.jsonl"), processLines, 0600); err != nil {
			t.Fatal(err)
		}
		runtimeSamples := []RuntimeSample{{At: start, AllocBytes: Zero("bytes"), LiveHeap: Zero("bytes"), HeapGoal: Zero("bytes"), GCCycles: Zero("cycles"), GCPause: Zero("seconds"), Goroutines: Zero("count")}, {At: end, AllocBytes: Zero("bytes"), LiveHeap: Zero("bytes"), HeapGoal: Zero("bytes"), GCCycles: Zero("cycles"), GCPause: Zero("seconds"), Goroutines: Zero("count")}}
		var runtimeLines []byte
		for _, sample := range runtimeSamples {
			runtimeLines = append(runtimeLines, mustJSONLForTest(sample)...)
		}
		if err := os.WriteFile(filepath.Join(runPath, "runtime-metrics.jsonl"), runtimeLines, 0600); err != nil {
			t.Fatal(err)
		}
		dbBefore := syntheticDBSnapshot(start)
		dbAfter := syntheticDBSnapshot(end)
		if err := os.WriteFile(filepath.Join(runPath, "db-before.json"), mustJSON(dbBefore), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(runPath, "db-after.json"), mustJSON(dbAfter), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func syntheticDBSnapshot(at time.Time) DBSnapshot {
	return DBSnapshot{
		At: at, Version: "17.0", Connections: Zero("count"), BlockHits: Zero("count"), BlockReads: Zero("count"),
		TempBytes: Zero("bytes"), TempFiles: Zero("count"), Commits: Zero("count"), Rollbacks: Zero("count"),
		TupleReads: Zero("count"), TupleWrites: Zero("count"), WALBytes: Zero("bytes"), SlotLag: Zero("bytes"),
		PoolActive: Zero("count"), PoolIdle: Zero("count"),
	}
}

func mutateLiveRun(t *testing.T, root string, triad bool, mutate func(*Run, string)) {
	t.Helper()
	pattern := "*-v1"
	if !triad {
		pattern = "*-v1"
	}
	runPaths, err := filepath.Glob(filepath.Join(root, "runs", pattern))
	if err != nil || len(runPaths) == 0 {
		t.Fatalf("find live run: paths=%v err=%v", runPaths, err)
	}
	path := runPaths[0]
	b, err := os.ReadFile(filepath.Join(path, "run.json"))
	if err != nil {
		t.Fatal(err)
	}
	var run Run
	if err := json.Unmarshal(b, &run); err != nil {
		t.Fatal(err)
	}
	mutate(&run, path)
	if err := os.WriteFile(filepath.Join(path, "run.json"), mustJSON(run), 0600); err != nil {
		t.Fatal(err)
	}
}

func forLiveModes(t *testing.T, fn func(t *testing.T, triad bool)) {
	t.Helper()
	for _, triad := range []bool{false, true} {
		name := "pair"
		if triad {
			name = "triad"
		}
		t.Run(name, func(t *testing.T) { fn(t, triad) })
	}
}

func TestThreeTargetOfflineReportRejectsApplicableProcessEvidenceOmission(t *testing.T) {
	root := syntheticTriadBundle(t)
	makeTriadTargetLive(t, root, "v1")

	if _, err := reportFromArtifacts(root, false); err == nil || !strings.Contains(err.Error(), "process evidence is empty") {
		t.Fatalf("triad report did not reject omitted live process evidence: %v", err)
	}
}

func TestThreeTargetOfflineReportRejectsApplicableLiveResourceEvidenceWithoutBoundaries(t *testing.T) {
	root := syntheticTriadBundle(t)
	makeTriadTargetLive(t, root, "v1")
	process := []ProcessSample{
		{At: time.Now().UTC(), PID: 42, StartTime: "start", ExecutableHash: "signed-executable-hash", UserCPU: Zero("cpu_seconds"), SystemCPU: Zero("cpu_seconds"), RSS: Zero("bytes")},
		{At: time.Now().UTC().Add(time.Second), PID: 42, StartTime: "start", ExecutableHash: "signed-executable-hash", UserCPU: Zero("cpu_seconds"), SystemCPU: Zero("cpu_seconds"), RSS: Zero("bytes")},
	}
	var lines []byte
	for _, sample := range process {
		lines = append(lines, mustJSONLForTest(sample)...)
	}
	runPaths, err := filepath.Glob(filepath.Join(root, "runs", "*-v1"))
	if err != nil {
		t.Fatal(err)
	}
	if len(runPaths) != ThreeTargetBlocks {
		t.Fatalf("v1 run count=%d, want %d", len(runPaths), ThreeTargetBlocks)
	}
	for _, runPath := range runPaths {
		if err := os.WriteFile(filepath.Join(runPath, "process.jsonl"), lines, 0600); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := reportFromArtifacts(root, false); err == nil || !strings.Contains(err.Error(), "live resource evidence lacks measured boundaries") {
		t.Fatalf("triad report did not reject live resource evidence without boundaries: %v", err)
	}
}

func TestOfflineReportRejectsLiveRuntimeEvidenceFailuresPairAndTriad(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*testing.T, string)
		wantErr string
	}{
		{name: "omitted", mutate: func(t *testing.T, runPath string) {
			if err := os.Remove(filepath.Join(runPath, "runtime-metrics.jsonl")); err != nil {
				t.Fatal(err)
			}
		}, wantErr: "runtime metrics evidence unavailable"},
		{name: "corrupt", mutate: func(t *testing.T, runPath string) {
			if err := os.WriteFile(filepath.Join(runPath, "runtime-metrics.jsonl"), []byte("not-json\n"), 0600); err != nil {
				t.Fatal(err)
			}
		}, wantErr: "malformed runtime metrics evidence"},
		{name: "stale", mutate: func(t *testing.T, runPath string) {
			sample := RuntimeSample{At: time.Unix(1, 0).UTC(), AllocBytes: Zero("bytes"), LiveHeap: Zero("bytes"), HeapGoal: Zero("bytes"), GCCycles: Zero("cycles"), GCPause: Zero("seconds"), Goroutines: Zero("count")}
			if err := os.WriteFile(filepath.Join(runPath, "runtime-metrics.jsonl"), mustJSONLForTest(sample), 0600); err != nil {
				t.Fatal(err)
			}
		}, wantErr: "runtime metrics sample is outside run window"},
		{name: "collector-mismatch", mutate: func(t *testing.T, runPath string) {
			sample := RuntimeSample{At: time.Now().UTC(), AllocBytes: Unsupported("bytes", "mismatch"), LiveHeap: Unsupported("bytes", "mismatch"), HeapGoal: Unsupported("bytes", "mismatch"), GCCycles: Unsupported("cycles", "mismatch"), GCPause: Unsupported("seconds", "mismatch"), Goroutines: Unsupported("count", "mismatch")}
			if err := os.WriteFile(filepath.Join(runPath, "runtime-metrics.jsonl"), mustJSONLForTest(sample), 0600); err != nil {
				t.Fatal(err)
			}
		}, wantErr: "runtime metrics contradict supported collector provenance"},
	}
	forLiveModes(t, func(t *testing.T, triad bool) {
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				root := makeLiveBundle(t, triad)
				mutateLiveRun(t, root, triad, func(_ *Run, runPath string) { tc.mutate(t, runPath) })
				if _, err := reportFromArtifacts(root, false); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("%s report accepted invalid live runtime evidence: %v", tc.name, err)
				}
			})
		}
	})
}

func TestOfflineReportRejectsLiveDatabaseEvidenceFailuresPairAndTriad(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*testing.T, string)
		wantErr string
	}{
		{name: "omitted", mutate: func(t *testing.T, runPath string) {
			if err := os.Remove(filepath.Join(runPath, "db-before.json")); err != nil {
				t.Fatal(err)
			}
		}, wantErr: "db-before evidence unavailable"},
		{name: "corrupt", mutate: func(t *testing.T, runPath string) {
			if err := os.WriteFile(filepath.Join(runPath, "db-before.json"), []byte("not-json"), 0600); err != nil {
				t.Fatal(err)
			}
		}, wantErr: "malformed db-before evidence"},
		{name: "stale", mutate: func(t *testing.T, runPath string) {
			if err := os.WriteFile(filepath.Join(runPath, "db-before.json"), mustJSON(syntheticDBSnapshot(time.Unix(1, 0).UTC())), 0600); err != nil {
				t.Fatal(err)
			}
		}, wantErr: "db-before snapshot is outside run window"},
		{name: "version-mismatch", mutate: func(t *testing.T, runPath string) {
			b, err := os.ReadFile(filepath.Join(runPath, "db-before.json"))
			if err != nil {
				t.Fatal(err)
			}
			var snapshot DBSnapshot
			if err := json.Unmarshal(b, &snapshot); err != nil {
				t.Fatal(err)
			}
			snapshot.Version = "16.0"
			if err := os.WriteFile(filepath.Join(runPath, "db-before.json"), mustJSON(snapshot), 0600); err != nil {
				t.Fatal(err)
			}
		}, wantErr: "db-before version does not match target"},
	}
	forLiveModes(t, func(t *testing.T, triad bool) {
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				root := makeLiveBundle(t, triad)
				mutateLiveRun(t, root, triad, func(_ *Run, runPath string) { tc.mutate(t, runPath) })
				if _, err := reportFromArtifacts(root, false); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("%s report accepted invalid live database evidence: %v", tc.name, err)
				}
			})
		}
	})
}

func TestOfflineReportRejectsLiveCollectorAndNetworkProvenanceFailuresPairAndTriad(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Run)
		wantErr string
	}{
		{name: "collector-omitted", mutate: func(run *Run) { delete(run.CollectorProvenance, "network") }, wantErr: "network collector provenance is required"},
		{name: "network-mismatch", mutate: func(run *Run) { run.CollectorProvenance["network"] = "supported: target=net:[42] initial=net:[42]" }, wantErr: "network collector provenance identities must differ"},
		{name: "failed-process", mutate: func(run *Run) { run.CollectorProvenance["process"] = "failed: process exited" }, wantErr: "process collector provenance failed for passing run"},
	}
	forLiveModes(t, func(t *testing.T, triad bool) {
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				root := makeLiveBundle(t, triad)
				mutateLiveRun(t, root, triad, func(run *Run, _ string) { tc.mutate(run) })
				if _, err := reportFromArtifacts(root, false); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("%s report accepted invalid collector provenance: %v", tc.name, err)
				}
			})
		}
	})
}

type b001SerializationFailure struct{}

func (b001SerializationFailure) MarshalJSON() ([]byte, error) {
	return nil, errors.New("injected serialization failure")
}

func TestArtifactWriterSerializationFailureDoesNotPublishArtifact(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteJSON("bad.json", b001SerializationFailure{}); err == nil {
		t.Fatal("serialization failure was accepted")
	}
	if _, err := os.Stat(filepath.Join(root, "bad.json")); !os.IsNotExist(err) {
		t.Fatalf("failed JSON artifact was published: %v", err)
	}
	if err := w.WriteJSONL("bad.jsonl", []b001SerializationFailure{{}}); err == nil {
		t.Fatal("JSONL serialization failure was accepted")
	}
	if _, err := os.Stat(filepath.Join(root, "bad.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("failed JSONL artifact was published: %v", err)
	}
}

func TestArtifactWriterRewriteJSONSerializationFailurePreservesArtifact(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteJSON("record.json", map[string]string{"status": "original"}); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(filepath.Join(root, "record.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.RewriteJSON("record.json", b001SerializationFailure{}); err == nil {
		t.Fatal("rewrite serialization failure was accepted")
	}
	got, err := os.ReadFile(filepath.Join(root, "record.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(original) {
		t.Fatalf("failed rewrite changed artifact: got %q want %q", got, original)
	}
}

func TestRedactJSONBytesRejectsMalformedInput(t *testing.T) {
	if _, err := redactJSONBytes([]byte(`{"token":`), true); err == nil {
		t.Fatal("malformed JSON redaction input was accepted")
	}
}
