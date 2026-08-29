package benchrun

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPairRunnerThreeTargetModeWritesTwentyOneRunsAndTwoComparisons(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<28)
	if err != nil {
		t.Fatal(err)
	}
	targets := []Target{
		{SchemaVersion: SchemaVersion, ID: "v1", Role: "v1", Revision: "v1-sha"},
		{SchemaVersion: SchemaVersion, ID: "v2_reference", Role: "v2_reference", Revision: "v2-reference-sha"},
		{SchemaVersion: SchemaVersion, ID: "v2_current", Role: "v2_current", Revision: "v2-current-sha"},
	}
	runner := PairRunner{
		Writer:   w,
		Executor: SyntheticExecutor{},
		Manifest: Manifest{SchemaVersion: SchemaVersion, BundleID: "three-bundle", PairID: "three", Family: "H-append", SubscriberScale: 300, Seed: 17, StartedAt: time.Unix(1, 0).UTC()},
		Plan:     Plan{SchemaVersion: SchemaVersion, Seed: 17, Pairs: 7},
		Targets:  targets,
	}
	summary, err := runner.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(summary.Comparisons) != 2 || len(summary.Cells) != 2 {
		t.Fatalf("summary comparisons/cells = %d/%d, want 2/2: %+v", len(summary.Comparisons), len(summary.Cells), summary)
	}
	if got := summary.Comparisons[0].ID + "," + summary.Comparisons[1].ID; got != "v1-v2_current,v2_reference-v2_current" {
		t.Fatalf("comparison order = %q", got)
	}
	if summary.Comparisons[0].Cells[0].BaselineRevision != "v1-sha" || summary.Comparisons[0].Cells[0].CandidateRevision != "v2-current-sha" {
		t.Fatalf("first comparison revisions not preserved: %+v", summary.Comparisons[0])
	}
	if summary.Comparisons[1].Cells[0].BaselineRevision != "v2-reference-sha" || summary.Comparisons[1].Cells[0].CandidateRevision != "v2-current-sha" {
		t.Fatalf("second comparison revisions not preserved: %+v", summary.Comparisons[1])
	}
	runCount := 0
	if err := filepath.Walk(filepath.Join(root, "runs"), func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !info.IsDir() && filepath.Base(path) == "run.json" {
			runCount++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if runCount != 21 {
		t.Fatalf("run artifact count = %d, want 21", runCount)
	}
	manifestBytes, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	if err := ValidateManifest(manifest); err != nil {
		t.Fatal(err)
	}
	if err := ValidateThreeTargetOrder(manifest.TargetOrder); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"v1", "v2_reference", "v2_current"} {
		var target Target
		b, err := os.ReadFile(filepath.Join(root, "targets", id+".json"))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(b, &target); err != nil {
			t.Fatal(err)
		}
		want := "v2"
		if id == "v1" {
			want = "v1"
		}
		if target.Kind != want {
			t.Fatalf("target %s kind=%q, want %q", id, target.Kind, want)
		}
	}
}

func TestPairRunnerRejectsForgedTargetKind(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<28)
	if err != nil {
		t.Fatal(err)
	}
	runner := PairRunner{
		Writer: w, Executor: SyntheticExecutor{},
		Manifest: Manifest{SchemaVersion: SchemaVersion, BundleID: "kind", PairID: "kind", Family: "H-append", SubscriberScale: 300, Seed: 17, StartedAt: time.Unix(1, 0).UTC()},
		Plan:     Plan{SchemaVersion: SchemaVersion, Seed: 17, Pairs: 7},
		Targets: []Target{
			{SchemaVersion: SchemaVersion, ID: "v1", Role: "v1", Kind: "v2"},
			{SchemaVersion: SchemaVersion, ID: "v2", Role: "v2_current", Kind: "v2"},
		},
	}
	if _, err := runner.Run(context.Background()); err == nil {
		t.Fatal("runner accepted V1 target bound to V2 kind")
	}
}

func TestThreeTargetOfflineReportFlagsForgedTargetKind(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<28)
	if err != nil {
		t.Fatal(err)
	}
	runner := PairRunner{
		Writer: w, Executor: SyntheticExecutor{},
		Manifest: Manifest{SchemaVersion: SchemaVersion, BundleID: "kind-triad", PairID: "kind-triad", Family: "H-append", SubscriberScale: 300, Seed: 17, StartedAt: time.Unix(1, 0).UTC()},
		Plan:     Plan{SchemaVersion: SchemaVersion, Seed: 17, Pairs: 7},
		Targets: []Target{
			{SchemaVersion: SchemaVersion, ID: "v1", Role: "v1", Revision: "v1-sha"},
			{SchemaVersion: SchemaVersion, ID: "v2_reference", Role: "v2_reference", Revision: "reference-sha"},
			{SchemaVersion: SchemaVersion, ID: "v2_current", Role: "v2_current", Revision: "current-sha"},
		},
	}
	if _, err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "targets", "v2_current.json")
	var target Target
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &target); err != nil {
		t.Fatal(err)
	}
	target.Kind = "v1"
	if err := os.WriteFile(path, mustJSON(target), 0600); err != nil {
		t.Fatal(err)
	}
	manifest, err := LoadManifest(root)
	if err != nil {
		t.Fatal(err)
	}
	missing := []string{}
	if _, _, _, err := loadThreeTargetRecords(root, manifest, &missing); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(missing, "\n"), "kind binding") {
		t.Fatalf("forged target kind did not produce a binding finding: %v", missing)
	}
	summary, err := reportFromArtifacts(root, false)
	if err != nil {
		t.Fatal(err)
	}
	if summary.ClaimGate.Eligible || !strings.Contains(strings.Join(summary.ClaimGate.Reasons, "\n"), "kind binding") {
		t.Fatalf("forged target kind was not gated: %+v", summary.ClaimGate)
	}
}

func TestThreeTargetOfflineReportRejectsOmittedOrForgedRunEvidenceBudget(t *testing.T) {
	for name, mutate := range map[string]func(*Run){
		"omitted": func(run *Run) {
			run.EvidenceMaxFrames = 0
			run.EvidenceMaxBytes = 0
			run.EvidenceMaxRetained = 0
		},
		"forged": func(run *Run) { run.EvidenceMaxBytes++ },
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			writer, err := NewArtifactWriter(root, 1<<28)
			if err != nil {
				t.Fatal(err)
			}
			runner := PairRunner{
				Writer: writer, Executor: SyntheticExecutor{},
				Manifest: Manifest{SchemaVersion: SchemaVersion, BundleID: "budget-triad", PairID: "budget-triad", Family: "H-append", SubscriberScale: 300, Seed: 17, StartedAt: time.Unix(1, 0).UTC()},
				Plan:     Plan{SchemaVersion: SchemaVersion, Seed: 17, Pairs: 7},
				Targets: []Target{
					{SchemaVersion: SchemaVersion, ID: "v1", Role: "v1", Revision: "v1-sha"},
					{SchemaVersion: SchemaVersion, ID: "v2_reference", Role: "v2_reference", Revision: "v2-reference-sha"},
					{SchemaVersion: SchemaVersion, ID: "v2_current", Role: "v2_current", Revision: "v2-current-sha"},
				},
			}
			if _, err := runner.Run(context.Background()); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "runs", "budget-triad-01-v1", "run.json")
			var run Run
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(b, &run); err != nil {
				t.Fatal(err)
			}
			manifest, err := LoadManifest(root)
			if err != nil {
				t.Fatal(err)
			}
			if run.EvidenceMaxFrames != manifest.EvidenceMaxFrames || run.EvidenceMaxBytes != manifest.EvidenceMaxBytes || run.EvidenceMaxRetained != manifest.EvidenceMaxRetained {
				t.Fatalf("runner did not initialize run evidence budget: run=%+v manifest=%+v", run, manifest)
			}
			mutate(&run)
			if err := os.WriteFile(path, mustJSON(run), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := reportFromArtifacts(root, false); err == nil {
				t.Fatalf("offline triad report accepted %s run evidence budget", name)
			}
		})
	}
}

type qualificationFailureExecutor struct {
	SyntheticExecutor
}

func (e qualificationFailureExecutor) Qualify(_ context.Context, target Target) (Qualification, error) {
	if target.ID == "v2_reference" {
		return Qualification{Passed: false, Checks: map[string]bool{"preflight": false}, Failure: "historical target failed preflight"}, nil
	}
	return Qualification{Passed: true, Checks: map[string]bool{"preflight": true}}, nil
}

func TestPairRunnerThreeTargetModeRetainsHistoricalQualificationFailure(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<28)
	if err != nil {
		t.Fatal(err)
	}
	runner := PairRunner{
		Writer:   w,
		Executor: qualificationFailureExecutor{},
		Manifest: Manifest{SchemaVersion: SchemaVersion, BundleID: "three-bundle", PairID: "three", Family: "H-append", SubscriberScale: 300, Seed: 19, StartedAt: time.Unix(1, 0).UTC()},
		Plan:     Plan{SchemaVersion: SchemaVersion, Seed: 19, Pairs: 7},
		Targets: []Target{
			{SchemaVersion: SchemaVersion, ID: "v1", Role: "v1", Revision: "v1-sha"},
			{SchemaVersion: SchemaVersion, ID: "v2_reference", Role: "v2_reference", Revision: "v2-reference-sha"},
			{SchemaVersion: SchemaVersion, ID: "v2_current", Role: "v2_current", Revision: "v2-current-sha"},
		},
	}
	summary, err := runner.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if summary.ClaimGate.Eligible {
		t.Fatal("historical qualification failure produced an eligible claim")
	}
	if len(summary.Comparisons) != 2 {
		t.Fatalf("comparisons=%d, want 2", len(summary.Comparisons))
	}
	for _, comparison := range summary.Comparisons {
		if comparison.ClaimGate.Eligible {
			t.Fatalf("comparison %s hid historical qualification failure", comparison.ID)
		}
	}
	for _, id := range []string{"v1", "v2_reference", "v2_current"} {
		if _, err := os.Stat(filepath.Join(root, "targets", id+".json")); err != nil {
			t.Fatalf("target qualification artifact %s: %v", id, err)
		}
	}
	manifest, err := LoadManifest(root)
	if err != nil {
		t.Fatal(err)
	}
	failedRunPath := filepath.Join(root, "runs", "three-01-v2_reference", "run.json")
	failedRunBytes, err := os.ReadFile(failedRunPath)
	if err != nil {
		t.Fatal(err)
	}
	var failedRun Run
	if err := json.Unmarshal(failedRunBytes, &failedRun); err != nil {
		t.Fatal(err)
	}
	if failedRun.PrimaryClass != SetupInvalid || failedRun.EvidenceMaxFrames != manifest.EvidenceMaxFrames || failedRun.EvidenceMaxBytes != manifest.EvidenceMaxBytes || failedRun.EvidenceMaxRetained != manifest.EvidenceMaxRetained {
		t.Fatalf("qualification-failed run lost bound evidence budget: run=%+v manifest=%+v", failedRun, manifest)
	}
}
