package main

import (
	"context"
	"encoding/json"
	"github.com/instant-v2/instant-v2/internal/benchrun"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestValidateProgressRejectsNoncanonicalManifest(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(root, "config.json")
	configValue := map[string]any{
		"pair_id": "wave6", "seed": 17, "family": "H-append", "scale": 300,
		"ramp_seconds": 30, "settle_seconds": 30, "warmup_seconds": 60,
		"warmup_mutations": 0, "measure_seconds": 180, "grace_seconds": 30,
		"targets": []map[string]any{
			{"id": "v1", "role": "v1", "kind": "v1", "revision": "v1"},
			{"id": "v2_reference", "role": "v2_reference", "kind": "v2", "revision": "ref"},
			{"id": "v2_current", "role": "v2_current", "kind": "v2", "revision": "cur"},
		},
	}
	writeTestJSON(t, config, configValue)
	output := filepath.Join(root, "bundle")
	if err := os.Mkdir(output, 0o700); err != nil {
		t.Fatal(err)
	}
	writeTestJSON(t, filepath.Join(output, "manifest.json"), map[string]any{"schema_version": "wrong"})
	if _, err := validateProgress(output, config); err == nil {
		t.Fatal("noncanonical manifest was accepted")
	}
}

func TestValidateProgressAcceptsCompleteCanonicalBundle(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(root, "bundle")
	writer, err := benchrun.NewArtifactWriter(bundle, 1<<28)
	if err != nil {
		t.Fatal(err)
	}
	targets := []benchrun.Target{
		{SchemaVersion: benchrun.SchemaVersion, ID: "v1", Role: "v1", Kind: "v1", Revision: "v1"},
		{SchemaVersion: benchrun.SchemaVersion, ID: "v2_reference", Role: "v2_reference", Kind: "v2", Revision: "ref"},
		{SchemaVersion: benchrun.SchemaVersion, ID: "v2_current", Role: "v2_current", Kind: "v2", Revision: "cur"},
	}
	pairID, seed := "wave6", int64(17)
	runner := benchrun.PairRunner{
		Writer: writer, Executor: benchrun.SyntheticExecutor{}, Targets: targets,
		Manifest: benchrun.Manifest{SchemaVersion: benchrun.SchemaVersion, BundleID: "bundle-wave6", PairID: pairID, Family: "H-append", SubscriberScale: 300, Seed: seed, StartedAt: time.Unix(1, 0).UTC()},
		Plan:     benchrun.Plan{SchemaVersion: benchrun.SchemaVersion, Seed: seed, Families: []string{"H-append"}, Scales: []int{300}, Pairs: 7, RampSeconds: 30, SettleSeconds: 30, WarmupSeconds: 60, MeasureSeconds: 180, GraceSeconds: 30},
	}
	if _, err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	err = filepath.Walk(bundle, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() || filepath.Base(path) != "run.json" {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var run benchrun.Run
		if err := json.Unmarshal(b, &run); err != nil {
			return err
		}
		run.StartedAt = now.Add(-time.Hour)
		run.MeasuredStartedAt = now.Add(-time.Minute)
		run.MeasuredFinishedAt = now
		run.EndedAt = now.Add(time.Second)
		updated, err := json.Marshal(run)
		if err != nil {
			return err
		}
		return os.WriteFile(path, updated, 0o600)
	})
	if err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(root, "config.json")
	writeTestJSON(t, config, map[string]any{
		"pair_id": pairID, "seed": seed, "family": "H-append", "scale": 300,
		"ramp_seconds": 30, "settle_seconds": 30, "warmup_seconds": 60, "warmup_mutations": 0,
		"measure_seconds": 180, "grace_seconds": 30,
		"targets": []map[string]any{
			{"id": "v1", "role": "v1", "kind": "v1", "revision": "v1"},
			{"id": "v2_reference", "role": "v2_reference", "kind": "v2", "revision": "ref"},
			{"id": "v2_current", "role": "v2_current", "kind": "v2", "revision": "cur"},
		},
	})
	progress, err := validateProgress(bundle, config)
	if err != nil || !progress.Valid || progress.CompletedAttempts != 21 || progress.FailedAttempts != 0 {
		t.Fatalf("complete canonical bundle was not accepted: progress=%+v err=%v", progress, err)
	}

	t.Run("failed_qualification_is_not_complete", func(t *testing.T) {
		path := filepath.Join(bundle, "targets", "v1.json")
		var target benchrun.Target
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(b, &target); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = os.WriteFile(path, b, 0o600) }()
		target.Qualification.Passed = false
		writeTestJSON(t, path, target)
		progress, err := validateProgress(bundle, config)
		if err == nil && progress.Valid {
			t.Fatalf("failed qualification was accepted as complete: progress=%+v", progress)
		}
	})

	t.Run("failed_qualification_check_is_not_complete", func(t *testing.T) {
		path := filepath.Join(bundle, "targets", "v1.json")
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var target benchrun.Target
		if err := json.Unmarshal(b, &target); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = os.WriteFile(path, b, 0o600) }()
		for check := range target.Qualification.Checks {
			target.Qualification.Checks[check] = false
			break
		}
		writeTestJSON(t, path, target)
		progress, err := validateProgress(bundle, config)
		if err == nil && progress.Valid {
			t.Fatalf("failed qualification check was accepted as complete: progress=%+v", progress)
		}
	})

	t.Run("partial_run_is_pending_when_live", func(t *testing.T) {
		path := filepath.Join(bundle, "runs")
		var runJSON string
		err := filepath.Walk(path, func(path string, info os.FileInfo, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if !info.IsDir() && filepath.Base(path) == "run.json" && runJSON == "" {
				runJSON = path
			}
			return nil
		})
		if err != nil || runJSON == "" {
			t.Fatalf("could not find a run artifact: %v", err)
		}
		original, err := os.ReadFile(runJSON)
		if err != nil {
			t.Fatal(err)
		}
		partialReady := make(chan struct{})
		restore := make(chan struct{})
		go func() {
			if err := os.WriteFile(runJSON, []byte(`{"schema_version":"bench-v1"`), 0o600); err != nil {
				return
			}
			close(partialReady)
			<-restore
			_ = os.WriteFile(runJSON, original, 0o600)
		}()
		select {
		case <-partialReady:
		case <-time.After(2 * time.Second):
			t.Fatal("partial artifact writer did not publish")
		}
		defer close(restore)
		progress, err := validateProgress(bundle, config, true)
		if err != nil || !progress.PendingEvidence || progress.Valid {
			t.Fatalf("partial live artifact was not pending: progress=%+v err=%v", progress, err)
		}
	})
}

func writeTestJSON(t *testing.T, path string, value any) {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
}
