package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/instant-v2/instant-v2/internal/benchrun"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

type progressResult struct {
	Valid             bool   `json:"valid"`
	PendingEvidence   bool   `json:"pending_evidence"`
	Transient         bool   `json:"transient,omitempty"`
	CompletedAttempts int    `json:"completed_attempts"`
	FailedAttempts    int    `json:"failed_attempts"`
	TotalAttempts     int    `json:"total_attempts"`
	Error             string `json:"error,omitempty"`
	Warning           string `json:"warning,omitempty"`
}

type progressConfig struct {
	PairID          string `json:"pair_id"`
	Seed            int64  `json:"seed"`
	Family          string `json:"family"`
	Scale           int    `json:"scale"`
	RampSeconds     int    `json:"ramp_seconds"`
	SettleSeconds   int    `json:"settle_seconds"`
	WarmupSeconds   int    `json:"warmup_seconds"`
	WarmupMutations int    `json:"warmup_mutations"`
	MeasureSeconds  int    `json:"measure_seconds"`
	GraceSeconds    int    `json:"grace_seconds"`
	Targets         []struct {
		ID       string `json:"id"`
		Kind     string `json:"kind"`
		Role     string `json:"role"`
		Revision string `json:"revision"`
	} `json:"targets"`
}

func validateProgress(root, configPath string, allowPartial ...bool) (progressResult, error) {
	result := progressResult{TotalAttempts: 21}
	partial := len(allowPartial) > 0 && allowPartial[0]
	if root == "" || !filepath.IsAbs(root) {
		return result, errors.New("progress bundle root must be absolute")
	}
	if err := noSymlinkComponents(root); err != nil {
		return result, err
	}
	if info, err := os.Lstat(root); err != nil {
		if os.IsNotExist(err) {
			result.PendingEvidence = true
			return result, nil
		}
		return result, err
	} else if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return result, errors.New("progress bundle root is not a directory")
	}
	artifacts, err := openArtifactRoot(root)
	if err != nil {
		if partial && os.IsNotExist(err) {
			return progressPending(result, err)
		}
		return result, err
	}
	defer func() { _ = artifacts.Close() }() // Descriptor cleanup does not replace the evidence verdict.
	configBytes, err := readBounded(configPath, 8<<20)
	if err != nil {
		return result, err
	}
	var cfg progressConfig
	if err := json.Unmarshal(configBytes, &cfg); err != nil {
		return result, fmt.Errorf("progress config is malformed: %w", err)
	}
	if cfg.PairID == "" || cfg.Seed <= 0 || cfg.Family != "H-append" || cfg.Scale != 300 || cfg.RampSeconds != 30 || cfg.SettleSeconds != 30 || cfg.WarmupSeconds != 60 || cfg.WarmupMutations != 0 || cfg.MeasureSeconds != 180 || cfg.GraceSeconds != 30 {
		return result, errors.New("progress config is not the canonical H300 contract")
	}
	if len(cfg.Targets) != 3 {
		return result, errors.New("progress config must contain the canonical triad")
	}
	wantTargets := map[string]struct{}{"v1": {}, "v2_reference": {}, "v2_current": {}}
	configTargets := make(map[string]progressConfigTarget, len(cfg.Targets))
	for _, target := range cfg.Targets {
		if _, ok := wantTargets[target.ID]; !ok || target.Role != target.ID || target.Revision == "" {
			return result, errors.New("progress config target identity is not canonical")
		}
		if target.ID == "v1" && target.Kind != "v1" || target.ID != "v1" && target.Kind != "v2" {
			return result, errors.New("progress config target kind is not canonical")
		}
		if _, exists := configTargets[target.ID]; exists {
			return result, errors.New("progress config contains duplicate target identity")
		}
		configTargets[target.ID] = progressConfigTarget{ID: target.ID, Kind: target.Kind, Role: target.Role, Revision: target.Revision}
	}
	if len(configTargets) != len(wantTargets) {
		return result, errors.New("progress config is missing a target")
	}

	var manifest benchrun.Manifest
	if err := readJSONArtifact(artifacts, "manifest.json", &manifest); err != nil {
		if os.IsNotExist(err) {
			result.PendingEvidence = true
			return result, nil
		}
		if partial && isTransientProgressError(err) {
			return progressPending(result, err)
		}
		return result, err
	}
	if manifest.SchemaVersion != benchrun.SchemaVersion || manifest.PairID != cfg.PairID || manifest.Family != cfg.Family || manifest.SubscriberScale != cfg.Scale || manifest.Seed != cfg.Seed {
		return result, errors.New("manifest identity does not match canonical config")
	}
	order, err := benchrun.ThreeTargetSchedule(cfg.Seed, cfg.PairID)
	if err != nil {
		return result, err
	}
	if !reflect.DeepEqual(manifest.TargetOrder, order.Blocks) || !reflect.DeepEqual(manifest.RunOrder, order.Order) {
		return result, errors.New("manifest schedule does not match canonical seeded order")
	}
	if err := benchrun.ValidateManifest(manifest); err != nil {
		return result, fmt.Errorf("manifest validation: %w", err)
	}
	if len(manifest.TargetRevisions) != len(configTargets) {
		return result, errors.New("manifest target revisions are incomplete")
	}
	for id, target := range configTargets {
		if manifest.TargetRevisions[id] != target.Revision {
			return result, fmt.Errorf("manifest revision mismatch for %s", id)
		}
	}
	var plan benchrun.Plan
	if err := readJSONArtifact(artifacts, "plan.json", &plan); err != nil {
		if os.IsNotExist(err) {
			result.PendingEvidence = true
			return result, nil
		}
		if partial && isTransientProgressError(err) {
			return progressPending(result, err)
		}
		return result, err
	}
	if plan.SchemaVersion != benchrun.SchemaVersion || plan.Seed != cfg.Seed || plan.Pairs != 7 || plan.RampSeconds != 30 || plan.SettleSeconds != 30 || plan.WarmupSeconds != 60 || plan.WarmupMutations != 0 || plan.MeasureSeconds != 180 || plan.GraceSeconds != 30 || len(plan.Families) != 1 || plan.Families[0] != "H-append" || len(plan.Scales) != 1 || plan.Scales[0] != 300 {
		return result, errors.New("benchmark plan does not match canonical H300 contract")
	}
	if err := validateProgressTargets(root, manifest, configTargets, artifacts); err != nil {
		if partial && (os.IsNotExist(err) || isTransientProgressError(err)) {
			return progressPending(result, err)
		}
		return result, err
	}

	expected := make(map[string]struct{}, 21)
	for _, block := range order.Blocks {
		for _, id := range block.Order {
			expected[fmt.Sprintf("%s-%02d-%s", cfg.PairID, block.Index, id)] = struct{}{}
		}
	}
	runsRoot := filepath.Join(root, "runs")
	if info, err := os.Lstat(runsRoot); err != nil {
		if os.IsNotExist(err) {
			result.PendingEvidence = true
			return result, nil
		}
		return result, err
	} else if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return result, errors.New("runs artifact root is not a directory")
	}
	seen := make(map[string]struct{}, len(expected))
	walkErr := filepath.WalkDir(runsRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink in benchmark artifacts: %s", filepath.Base(path))
		}
		rel, relErr := filepath.Rel(runsRoot, path)
		if relErr != nil {
			return relErr
		}
		if entry.IsDir() {
			if rel != "." {
				first := strings.Split(rel, string(filepath.Separator))[0]
				if _, ok := expected[first]; !ok {
					return fmt.Errorf("unexpected benchmark run directory %q", first)
				}
			}
			return nil
		}
		if filepath.Base(path) != "run.json" {
			return nil
		}
		id := filepath.Base(filepath.Dir(path))
		if _, ok := expected[id]; !ok {
			return fmt.Errorf("unexpected canonical run artifact %q", id)
		}
		if _, exists := seen[id]; exists {
			return fmt.Errorf("duplicate canonical run artifact %q", id)
		}
		seen[id] = struct{}{}
		rootRel, rootRelErr := filepath.Rel(root, path)
		if rootRelErr != nil || strings.HasPrefix(rootRel, "..") {
			return errors.New("benchmark artifact path escaped bundle root")
		}
		var run benchrun.Run
		if err := readJSONArtifact(artifacts, rootRel, &run); err != nil {
			if partial && (os.IsNotExist(err) || isTransientProgressError(err)) {
				return transientProgressError{err}
			}
			return err
		}
		if err := validateProgressRun(run, id, cfg, manifest, configTargets); err != nil {
			return err
		}
		dirRel := filepath.Dir(rootRel)
		if err := requireRunSiblingsAt(artifacts, dirRel); err != nil {
			result.PendingEvidence = true
			return nil
		}
		if err := validateProgressEvidence(filepath.Dir(path), run, artifacts); err != nil {
			if partial && (os.IsNotExist(err) || isTransientProgressError(err)) {
				return transientProgressError{err}
			}
			return err
		}
		result.CompletedAttempts++
		if run.PrimaryClass != benchrun.Pass {
			result.FailedAttempts++
		}
		return nil
	})
	if walkErr != nil {
		var transient transientProgressError
		if partial && (os.IsNotExist(walkErr) || errors.As(walkErr, &transient)) {
			if !errors.As(walkErr, &transient) {
				transient = transientProgressError{walkErr}
			}
			return progressPending(result, transient)
		}
		return result, walkErr
	}
	if len(seen) != len(expected) {
		result.PendingEvidence = true
	}
	if result.CompletedAttempts == len(expected) && len(seen) == len(expected) && !result.PendingEvidence {
		result.Valid = true
	}
	return result, nil
}

type transientProgressError struct{ err error }

func (e transientProgressError) Error() string { return e.err.Error() }

func (e transientProgressError) Unwrap() error { return e.err }

func progressPending(result progressResult, err error) (progressResult, error) {
	result.PendingEvidence = true
	result.Transient = true
	result.Warning = err.Error()
	return result, nil
}

func isTransientProgressError(err error) bool {
	if err == nil {
		return false
	}
	var transient transientProgressError
	if errors.As(err, &transient) {
		return true
	}
	message := err.Error()
	return strings.Contains(message, "malformed JSONL sibling") ||
		strings.Contains(message, "malformed JSON sibling") ||
		strings.Contains(message, "changed while being read")
}
