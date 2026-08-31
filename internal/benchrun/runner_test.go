package benchrun

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

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
