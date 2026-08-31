package main

import (
	"fmt"
	"github.com/instant-v2/instant-v2/internal/benchrun"
	"path/filepath"
	"strings"
)

type progressConfigTarget struct {
	ID, Kind, Role, Revision string
}

func validateProgressTargets(root string, manifest benchrun.Manifest, expected map[string]progressConfigTarget, readers ...*artifactRoot) error {
	var artifacts *artifactRoot
	if len(readers) > 0 {
		artifacts = readers[0]
	}
	for id, want := range expected {
		var target benchrun.Target
		var err error
		if artifacts != nil {
			err = readJSONArtifact(artifacts, filepath.Join("targets", id+".json"), &target)
		} else {
			err = readJSONRegular(filepath.Join(root, "targets", id+".json"), &target)
		}
		if err != nil {
			return err
		}
		if target.SchemaVersion != benchrun.SchemaVersion || target.ID != id || target.Role != id || target.Kind != want.Kind || target.Revision != want.Revision || manifest.TargetRevisions[id] != target.Revision || !target.Qualification.Passed || !qualificationChecksPass(target.Qualification.Checks) {
			return fmt.Errorf("target %s qualification/provenance sibling is invalid", id)
		}
	}
	return nil
}

func validateProgressRun(run benchrun.Run, id string, cfg progressConfig, manifest benchrun.Manifest, targets map[string]progressConfigTarget) error {
	if run.SchemaVersion != benchrun.SchemaVersion || run.ID != id || run.Family != cfg.Family || run.Scale != cfg.Scale || run.Seed != cfg.Seed || run.WarmupMutations != cfg.WarmupMutations || run.PairID == "" || run.TargetRevision == "" || run.StartedAt.IsZero() || run.EndedAt.IsZero() || !run.EndedAt.After(run.StartedAt) {
		return fmt.Errorf("run %s has invalid schema/config identity", id)
	}
	if !benchrun.ValidateFailureClass(run.PrimaryClass) {
		return fmt.Errorf("run %s has invalid execution class", id)
	}
	if run.PrimaryClass == benchrun.Pass && (len(run.ProtocolErrors) != 0 || len(run.BehaviorErrors) != 0) {
		return fmt.Errorf("run %s passing class carries protocol/behavior errors", id)
	}
	parts := strings.Split(run.ID, "-")
	if len(parts) < 3 {
		return fmt.Errorf("run %s has invalid canonical identity", id)
	}
	targetID := run.TargetID
	want, ok := targets[targetID]
	if !ok || parts[len(parts)-1] != targetID || run.TargetRevision != want.Revision || run.PairID != fmt.Sprintf("%s-%02s", cfg.PairID, parts[len(parts)-2]) {
		return fmt.Errorf("run %s target/revision identity is invalid", id)
	}
	block := 0
	if _, err := fmt.Sscanf(parts[len(parts)-2], "%d", &block); err != nil || block < 1 || block > 7 || run.ScheduleBlock != block {
		return fmt.Errorf("run %s schedule block is invalid", id)
	}
	if !containsString(manifest.TargetOrder[block-1].Order, targetID) {
		return fmt.Errorf("run %s target is absent from its canonical block", id)
	}
	return nil
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
