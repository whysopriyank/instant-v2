package benchrun

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

func loadThreeTargetRuns(root string, m Manifest, targetRecords map[string]Target) (map[string]*reportCell, error) {
	cells := map[string]*reportCell{}
	runIDs := map[string]bool{}
	err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() || filepath.Base(path) != "run.json" {
			return nil
		}
		b, readErr := readBounded(path, SmallArtifactBytes)
		if readErr != nil {
			return readErr
		}
		var run Run
		if readErr = json.Unmarshal(b, &run); readErr != nil {
			return readErr
		}
		if run.SchemaVersion != SchemaVersion {
			return fmt.Errorf("mixed schema version in %s", path)
		}
		if !ValidateFailureClass(run.PrimaryClass) {
			return fmt.Errorf("invalid failure taxonomy in %s", path)
		}
		if !isThreeTargetID(run.TargetID) {
			return fmt.Errorf("run %s references unsupported target %q", run.ID, run.TargetID)
		}
		if target, ok := targetRecords[run.TargetID]; !ok {
			return fmt.Errorf("run %s references missing target %q", run.ID, run.TargetID)
		} else {
			if run.TargetRevision != target.Revision {
				return fmt.Errorf("run %s target revision does not match %s", run.ID, run.TargetID)
			}
			if run.ScheduleBlock < 1 || run.ScheduleBlock > ThreeTargetBlocks {
				return fmt.Errorf("run %s has invalid schedule block %d", run.ID, run.ScheduleBlock)
			}
			block := m.TargetOrder[run.ScheduleBlock-1]
			inBlock := false
			for _, id := range block.Order {
				if id == run.TargetID {
					inBlock = true
					break
				}
			}
			if !inBlock || run.PairID != fmt.Sprintf("%s-%02d", m.PairID, run.ScheduleBlock) || run.ID != fmt.Sprintf("%s-%s", run.PairID, run.TargetID) || run.Family != m.Family || run.Scale != m.SubscriberScale || run.Seed != m.Seed {
				return fmt.Errorf("run %s does not match its manifest schedule", run.ID)
			}
		}
		if readErr = validateRunEvidence(filepath.Dir(path), run); readErr != nil {
			return readErr
		}
		if readErr = validateRunEvidenceBudget(run, m); readErr != nil {
			return readErr
		}
		for name, metric := range run.Measurements {
			if readErr = ValidateMeasurement(metric); readErr != nil {
				return fmt.Errorf("invalid measurement %s in %s: %w", name, path, readErr)
			}
		}
		runIDs[run.ID] = true
		key := fmt.Sprintf("%s/%d", run.Family, run.Scale)
		cell := cells[key]
		if cell == nil {
			cell = &reportCell{family: run.Family, scale: run.Scale, pairs: map[string]map[string]Run{}}
			cells[key] = cell
		}
		if cell.pairs[run.PairID] == nil {
			cell.pairs[run.PairID] = map[string]Run{}
		}
		if _, exists := cell.pairs[run.PairID][run.TargetID]; exists {
			return fmt.Errorf("duplicate run for pair %s target %s", run.PairID, run.TargetID)
		}
		cell.pairs[run.PairID][run.TargetID] = run
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := validateThreeTargetRunSet(m, runIDs); err != nil {
		return nil, err
	}

	return cells, nil
}

func loadThreeTargetRecords(root string, m Manifest, missing *[]string) (map[string]Target, bool, string, error) {
	records := make(map[string]Target, len(threeTargetIDs))
	qualificationFailed := false
	failure := ""
	for _, id := range threeTargetIDs {
		b, err := readBounded(filepath.Join(root, "targets", id+".json"), SmallArtifactBytes)
		if err != nil {
			return nil, false, "", fmt.Errorf("missing target qualification: %s", id)
		}
		var target Target
		if err = json.Unmarshal(b, &target); err != nil {
			return nil, false, "", err
		}
		if target.SchemaVersion != SchemaVersion || target.ID != id || target.Role != id || len(target.Qualification.Checks) == 0 {
			return nil, false, "", fmt.Errorf("incomplete target qualification: %s", id)
		}
		if err := validateTargetBinding(target); err != nil {
			*missing = append(*missing, "target "+id+" kind binding")
		}
		if target.Revision == "" || target.DatabaseName == "" || target.PostgresVersion == "" || target.InvalidationMode == "" || target.MetadataHash == "" {
			*missing = append(*missing, "target "+id+" provenance")
		}
		mapped := TargetProvenanceTuple(target)
		if m.TargetProvenance[id] != mapped || m.DatabaseIDs[id] != target.DatabaseName || m.TargetRevisions[id] != target.Revision {
			*missing = append(*missing, "target "+id+" manifest mapping")
		}
		if !target.Qualification.Passed {
			qualificationFailed = true
			if failure == "" {
				failure = id + " qualification failed"
			}
		}
		records[id] = target
	}
	return records, qualificationFailed, failure, nil
}

func validateThreeTargetRunSet(m Manifest, runIDs map[string]bool) error {
	expected := make(map[string]bool, ThreeTargetBlocks*len(threeTargetIDs))
	for _, block := range m.TargetOrder {
		for _, targetID := range block.Order {
			expected[fmt.Sprintf("%s-%02d-%s", m.PairID, block.Index, targetID)] = true
		}
	}
	if len(expected) != ThreeTargetBlocks*len(threeTargetIDs) || len(runIDs) != len(expected) {
		return fmt.Errorf("expected %d three-target run artifacts, found %d", len(expected), len(runIDs))
	}
	for id := range expected {
		if !runIDs[id] {
			return fmt.Errorf("missing run artifact: %s", id)
		}
	}
	for id := range runIDs {
		if !expected[id] {
			return fmt.Errorf("unexpected run artifact: %s", id)
		}
	}
	return nil
}
