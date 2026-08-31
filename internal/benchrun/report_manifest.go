package benchrun

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
)

func ValidateManifest(m Manifest) error {
	if m.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported schema version %q", m.SchemaVersion)
	}
	if m.BundleID == "" || m.PairID == "" || m.Family == "" || m.SubscriberScale <= 0 {
		return errors.New("manifest missing required provenance")
	}
	if m.Seed == 0 {
		return errors.New("manifest seed must be recorded")
	}
	if len(m.TargetOrder) > 0 || len(m.Comparisons) > 0 || m.TargetRevisions != nil {
		return validateThreeTargetManifestShape(m)
	}
	if len(m.RunOrder) != 7 {
		return errors.New("manifest run order must contain exactly seven attempts")
	}
	ab, ba := 0, 0
	for _, order := range m.RunOrder {
		if order != "AB" && order != "BA" {
			return fmt.Errorf("invalid run order %q", order)
		}
		if order == "AB" {
			ab++
		} else {
			ba++
		}
	}
	if ab < 3 || ba < 3 || ab+ba != 7 {
		return errors.New("run order is not balanced across seven attempts")
	}
	if m.StartedAt.IsZero() {
		return errors.New("manifest start time is required")
	}
	return nil
}

func validateThreeTargetManifestShape(m Manifest) error {
	if len(m.RunOrder) != ThreeTargetBlocks {
		return errors.New("three-target manifest run order must contain seven blocks")
	}
	if err := ValidateThreeTargetOrder(m.TargetOrder); err != nil {
		return err
	}
	for i, block := range m.TargetOrder {
		if m.RunOrder[i] != strings.Join(block.Order, ">") {
			return fmt.Errorf("three-target manifest run order does not match block %d", block.Index)
		}
	}
	if len(m.TargetRevisions) != len(threeTargetIDs) {
		return errors.New("three-target manifest requires three target revisions")
	}
	for _, id := range threeTargetIDs {
		if _, ok := m.TargetRevisions[id]; !ok {
			return fmt.Errorf("three-target manifest missing target revision %s", id)
		}
	}
	comparisons := make(map[string]ComparisonSpec, len(m.Comparisons))
	for _, comparison := range m.Comparisons {
		if _, exists := comparisons[comparison.ID]; exists {
			return fmt.Errorf("duplicate three-target comparison %q", comparison.ID)
		}
		comparisons[comparison.ID] = comparison
	}
	if len(comparisons) != 2 {
		return errors.New("three-target manifest requires exactly two comparisons")
	}
	for _, want := range []struct{ id, baseline, candidate string }{
		{"v1-v2_current", "v1", "v2_current"},
		{"v2_reference-v2_current", "v2_reference", "v2_current"},
	} {
		comparison, ok := comparisons[want.id]
		if !ok || comparison.BaselineID != want.baseline || comparison.CandidateID != want.candidate {
			return fmt.Errorf("invalid three-target comparison %q", want.id)
		}
		if comparison.BaselineRevision != m.TargetRevisions[want.baseline] || comparison.CandidateRevision != m.TargetRevisions[want.candidate] {
			return fmt.Errorf("comparison %q revisions do not match manifest target revisions", want.id)
		}
	}
	return nil
}

func ValidateFailureClass(c FailureClass) bool {
	switch c {
	case Pass, SetupInvalid, HarnessDefect, TargetSemanticFailure, TargetProtocolFailure, TargetResourceExhaustion, TargetCrash, TargetTimeout, InfrastructureNoise, OperatorAbort:
		return true
	default:
		return false
	}
}

func ValidateMeasurement(m Measurement) error {
	switch m.Status {
	case StatusValue:
		if math.IsNaN(m.Value) || math.IsInf(m.Value, 0) {
			return errors.New("measurement value is non-finite")
		}
	case StatusZero:
		if m.Value != 0 {
			return errors.New("zero measurement must have zero value")
		}
	case StatusMissing, StatusUnsupported, StatusNotApplicable, StatusFailed:
		if m.Value != 0 {
			return errors.New("non-observation must not carry a value")
		}
	default:
		return fmt.Errorf("unknown measurement status %q", m.Status)
	}
	if m.Unit == "" {
		return errors.New("measurement unit is required")
	}
	return nil
}

func ValidateCoverage(c string) bool {
	switch c {
	case "exact", "coalesced", "converged_without_intermediate", "none":
		return true
	default:
		return false
	}
}

func LoadManifest(root string) (Manifest, error) {
	var m Manifest
	b, e := readBounded(filepath.Join(root, "manifest.json"), SmallArtifactBytes)
	if e != nil {
		return m, e
	}
	if e = json.Unmarshal(b, &m); e != nil {
		return m, e
	}
	return m, ValidateManifest(m)
}

func isThreeTargetManifest(m Manifest) bool {
	return len(m.TargetOrder) > 0 || len(m.Comparisons) > 0 || len(m.TargetRevisions) > 0
}
