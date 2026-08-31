package benchrun

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func validateSharedReportEvidence(root string, m Manifest, targetCount int) error {
	var bundledFixtureIDs *FixtureIDs
	var environment Environment
	b, err := readBounded(filepath.Join(root, "environment.json"), SmallArtifactBytes)
	if err != nil {
		return fmt.Errorf("environment.json is required: %w", err)
	}
	if err = json.Unmarshal(b, &environment); err != nil || environment.SchemaVersion != SchemaVersion {
		return errors.New("invalid environment provenance")
	}
	if m.ConfigHash != "" {
		b, err = readBounded(filepath.Join(root, "config.json"), SmallArtifactBytes)
		if err != nil {
			return fmt.Errorf("config.json is required: %w", err)
		}
		var cfg LiveConfig
		if err = json.Unmarshal(b, &cfg); err != nil {
			return fmt.Errorf("invalid bundled live config: %w", err)
		}
		if digest, digestErr := DigestJSON(CanonicalConfigEvidence(cfg)); digestErr != nil || !strings.EqualFold(digest, m.ConfigHash) {
			return errors.New("bundled live config hash mismatch")
		}
		fixtureIDs := cfg.Fixture
		bundledFixtureIDs = &fixtureIDs
	}
	b, err = readBounded(filepath.Join(root, "fixture.json"), SmallArtifactBytes)
	if err != nil {
		return fmt.Errorf("fixture.json is required: %w", err)
	}
	var fixture FixtureEvidence
	if err = json.Unmarshal(b, &fixture); err != nil {
		return fmt.Errorf("invalid bundled fixture evidence: %w", err)
	}
	if bundledFixtureIDs != nil && *bundledFixtureIDs != fixture.FixtureIDs {
		return errors.New("bundled live config fixture does not match fixture evidence")
	}
	if fixture.Family != canonicalBenchmarkFamily(m.Family) || fixture.Scale != m.SubscriberScale || fixture.Seed != m.Seed || len(fixture.EntityIDs) != fixture.SeedEntities || len(fixture.QueryAssignments) == 0 || len(fixture.InitialSemanticHashes) != len(fixture.QueryAssignments) {
		return errors.New("bundled fixture evidence does not match manifest")
	}
	expectedFixture, err := BuildFixtureEvidence(fixture.FixtureIDs, fixture.Family, fixture.Scale, fixture.Seed)
	if err != nil {
		return fmt.Errorf("reconstruct fixture evidence: %w", err)
	}
	actual, err := CanonicalJSON(fixture)
	if err != nil {
		return err
	}
	expected, err := CanonicalJSON(expectedFixture)
	if err != nil || !bytes.Equal(actual, expected) {
		return errors.New("bundled fixture evidence reconstruction mismatch")
	}
	if digest, digestErr := DigestJSON(fixture); digestErr != nil || m.FixtureHash == "" || !strings.EqualFold(digest, m.FixtureHash) {
		return errors.New("bundled fixture hash mismatch")
	}
	b, err = readBounded(filepath.Join(root, "plan.json"), SmallArtifactBytes)
	if err != nil {
		return fmt.Errorf("plan.json is required: %w", err)
	}
	var plan Plan
	if err = json.Unmarshal(b, &plan); err != nil || plan.SchemaVersion != SchemaVersion {
		return errors.New("invalid benchmark plan evidence")
	}
	if plan.Seed != m.Seed || len(plan.Families) != 1 || canonicalBenchmarkFamily(plan.Families[0]) != canonicalBenchmarkFamily(m.Family) || len(plan.Scales) != 1 || plan.Scales[0] != m.SubscriberScale {
		return errors.New("benchmark plan does not match manifest")
	}
	if err := validateManifestEvidenceBudget(m, plan); err != nil {
		return err
	}
	derivedTotal, derivedFile, err := ContractArtifactBudgetForTargets(m.Family, m.SubscriberScale, plan, targetCount)
	if err != nil {
		return fmt.Errorf("recompute artifact budget: %w", err)
	}
	if m.ArtifactMaxTotalBytes != derivedTotal || m.ArtifactMaxFileBytes != derivedFile {
		return errors.New("manifest artifact budget evidence mismatch")
	}
	return nil
}

func pairManifestProvenance(root string, m Manifest) []string {
	missingProvenance := []string{}
	if m.V1SHA == "" {
		missingProvenance = append(missingProvenance, "v1 sha")
	}
	if m.V2SHA == "" {
		missingProvenance = append(missingProvenance, "v2 sha")
	}
	if m.SourceTree == "" {
		missingProvenance = append(missingProvenance, "source tree")
	}
	if m.SchemaHash == "" || m.FixtureHash == "" || m.ConfigHash == "" {
		missingProvenance = append(missingProvenance, "schema/fixture/config hashes")
	}
	if len(m.DatabaseIDs) < 2 || len(m.TargetProvenance) < 2 {
		missingProvenance = append(missingProvenance, "target database/provenance tuple")
	}
	if err := VerifyApproval(m); err != nil {
		missingProvenance = append(missingProvenance, "valid detached approval")
	}
	if err := VerifyContentRoot(root, m); err != nil {
		missingProvenance = append(missingProvenance, "verified immutable content root")
	}
	return missingProvenance
}

func threeTargetManifestProvenance(root string, m Manifest) []string {
	missingProvenance := []string{}
	for _, field := range []struct {
		value string
		name  string
	}{
		{m.SourceTree, "source tree"},
		{m.SchemaHash, "schema hash"},
		{m.FixtureHash, "fixture hash"},
		{m.ConfigHash, "config hash"},
	} {
		if field.value == "" {
			missingProvenance = append(missingProvenance, field.name)
		}
	}
	if len(m.DatabaseIDs) < len(threeTargetIDs) || len(m.TargetProvenance) < len(threeTargetIDs) {
		missingProvenance = append(missingProvenance, "target database/provenance tuple")
	}
	if err := VerifyApproval(m); err != nil {
		missingProvenance = append(missingProvenance, "valid detached approval")
	}
	if err := VerifyContentRoot(root, m); err != nil {
		missingProvenance = append(missingProvenance, "verified immutable content root")
	}
	return missingProvenance
}

func loadPairRuns(root string, m Manifest) (map[string]*reportCell, map[string]bool, error) {
	cells := map[string]*reportCell{}
	runIDs := map[string]bool{}
	err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() || filepath.Base(path) != "run.json" {
			return nil
		}
		b, e := readBounded(path, SmallArtifactBytes)
		if e != nil {
			return e
		}
		var r Run
		if e = json.Unmarshal(b, &r); e != nil {
			return e
		}
		if r.SchemaVersion != SchemaVersion {
			return fmt.Errorf("mixed schema version in %s", path)
		}
		if !ValidateFailureClass(r.PrimaryClass) {
			return fmt.Errorf("invalid failure taxonomy in %s", path)
		}
		if e := validateRunEvidence(filepath.Dir(path), r); e != nil {
			return e
		}
		if e := validateRunEvidenceBudget(r, m); e != nil {
			return e
		}
		runIDs[r.ID] = true
		for name, metric := range r.Measurements {
			if e := ValidateMeasurement(metric); e != nil {
				return fmt.Errorf("invalid measurement %s in %s: %w", name, path, e)
			}
		}
		key := fmt.Sprintf("%s/%d", r.Family, r.Scale)
		c := cells[key]
		if c == nil {
			c = &reportCell{family: r.Family, scale: r.Scale, pairs: map[string]map[string]Run{}}
			cells[key] = c
		}
		if c.pairs[r.PairID] == nil {
			c.pairs[r.PairID] = map[string]Run{}
		}
		c.pairs[r.PairID][r.TargetID] = r
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return cells, runIDs, nil
}

func loadPairTargetRecords(root string, m Manifest, missing *[]string) (map[string]Target, bool, string, error) {
	targetIDs := map[string]bool{}
	targetRecords := map[string]Target{}
	qualificationFailed := false
	qualificationFailure := ""
	for _, name := range []string{"v1", "v2", "v2_current", "v2-current"} {
		var target Target
		b, e := readBounded(filepath.Join(root, "targets", name+".json"), SmallArtifactBytes)
		if e != nil {
			continue
		}
		if e = json.Unmarshal(b, &target); e != nil {
			return nil, false, "", e
		}
		if target.SchemaVersion != SchemaVersion || target.ID == "" || target.Role == "" || len(target.Qualification.Checks) == 0 {
			return nil, false, "", fmt.Errorf("incomplete target qualification: %s", name)
		}
		if err := validateTargetBinding(target); err != nil {
			*missing = append(*missing, "target "+name+" kind binding")
		}
		if target.Revision == "" || target.DatabaseName == "" || target.PostgresVersion == "" || target.InvalidationMode == "" || target.MetadataHash == "" {
			*missing = append(*missing, "target "+name+" provenance")
		}
		mapped := TargetProvenanceTuple(target)
		if m.TargetProvenance[target.ID] != mapped || m.DatabaseIDs[target.ID] != target.DatabaseName {
			*missing = append(*missing, "target "+name+" manifest mapping")
		}
		if !target.Qualification.Passed {
			qualificationFailed = true
			if qualificationFailure == "" {
				qualificationFailure = target.ID + " qualification failed"
			}
		}
		targetIDs[target.ID] = true
		targetRecords[target.ID] = target
		if target.ID == "v1" && target.Revision != "" && (m.V1SHA == "" || m.V1SHA != target.Revision) {
			return nil, false, "", errors.New("manifest v1_sha does not match qualified v1 revision")
		}
		if (target.ID == "v2" || target.ID == "v2_current" || target.ID == "v2-current") && target.Revision != "" && (m.V2SHA == "" || m.V2SHA != target.Revision) {
			return nil, false, "", errors.New("manifest v2_sha does not match qualified v2 revision")
		}
		if target.ID == "v2_current" || target.ID == "v2-current" {
			targetRecords["v2"] = target
		}
	}
	if !targetIDs["v1"] || (!targetIDs["v2"] && !targetIDs["v2_current"] && !targetIDs["v2-current"]) {
		return nil, false, "", errors.New("both target qualification records are required")
	}
	return targetRecords, qualificationFailed, qualificationFailure, nil
}

func validatePairProcessEvidence(root string, m Manifest, cells map[string]*reportCell, targetRecords map[string]Target) error {
	for _, c := range cells {
		for _, pairRuns := range c.pairs {
			for _, run := range pairRuns {
				if target, ok := targetRecords[run.TargetID]; ok {
					if err := validateProcessProvenance(filepath.Join(root, "runs", run.ID), run, target); err != nil {
						return err
					}
					if run.PrimaryClass == Pass && isLiveTarget(m, target) {
						if err := validateLiveResourceEvidence(filepath.Join(root, "runs", run.ID), run, target); err != nil {
							return err
						}
					}
				}
			}
		}
	}
	return nil
}

func validatePairRunSet(m Manifest, runIDs map[string]bool) error {
	expected := map[string]bool{}
	for i := 1; i <= 7; i++ {
		expected[fmt.Sprintf("%s-%02d-v1", m.PairID, i)] = true
		expected[fmt.Sprintf("%s-%02d-v2", m.PairID, i)] = true
	}
	if len(runIDs) != len(expected) {
		return fmt.Errorf("expected 14 run artifacts, found %d", len(runIDs))
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
