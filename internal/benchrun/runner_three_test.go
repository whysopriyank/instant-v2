package benchrun

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
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
}
