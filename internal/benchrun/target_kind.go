package benchrun

import (
	"fmt"
	"strings"
)

// expectedTargetKind is the canonical identity binding used at both the live
// adapter seam and offline artifact replay. The legacy v2 ID is retained as a
// two-target alias for v2_current, but it can never identify a V1 target.
func expectedTargetKind(id, role string) (string, error) {
	id = strings.TrimSpace(id)
	role = strings.TrimSpace(role)
	switch id {
	case "v1":
		if role != "v1" {
			return "", fmt.Errorf("target %s must use role v1 (got %q)", id, role)
		}
		return "v1", nil
	case "v2_reference":
		if role != "v2_reference" {
			return "", fmt.Errorf("target %s must use role v2_reference (got %q)", id, role)
		}
		return "v2", nil
	case "v2_current":
		if role != "v2_current" {
			return "", fmt.Errorf("target %s must use role v2_current (got %q)", id, role)
		}
		return "v2", nil
	case "v2":
		if role != "" && role != "v2" && role != "v2_current" {
			return "", fmt.Errorf("target %s must use the legacy v2/current role (got %q)", id, role)
		}
		return "v2", nil
	case "v2-current":
		if role != "v2-current" && role != "v2_current" {
			return "", fmt.Errorf("target %s must use a current-v2 role (got %q)", id, role)
		}
		return "v2", nil
	default:
		return "", fmt.Errorf("unsupported target identity %q", id)
	}
}

func validateTargetBinding(target Target) error {
	want, err := expectedTargetKind(target.ID, target.Role)
	if err != nil {
		return err
	}
	if target.Kind == "" {
		return fmt.Errorf("target %s kind is required", target.ID)
	}
	if target.Kind != want {
		return fmt.Errorf("target %s role %s requires kind %s (got %s)", target.ID, target.Role, want, target.Kind)
	}
	return nil
}

// bindTargetKind fills the kind for in-memory synthetic callers before an
// artifact is written. Live config is required to carry the same explicit
// value; this fallback is only for the pre-artifact synthetic construction
// path and never makes an offline target record valid by itself.
func bindTargetKind(target *Target) error {
	if target == nil {
		return fmt.Errorf("target is required")
	}
	want, err := expectedTargetKind(target.ID, target.Role)
	if err != nil {
		return err
	}
	if target.Kind == "" {
		target.Kind = want
		return nil
	}
	return validateTargetBinding(*target)
}

func bindTargetKinds(targets []Target) error {
	for i := range targets {
		if err := bindTargetKind(&targets[i]); err != nil {
			return err
		}
	}
	return nil
}

// TargetProvenanceTuple is the canonical manifest mapping for a target
// artifact. Kind is deliberately part of the signed tuple so an artifact
// cannot be relabeled as a different protocol implementation offline.
func TargetProvenanceTuple(target Target) string {
	return strings.Join([]string{target.Kind, target.Revision, target.DirtyHash, target.OutputPlugin, target.InvalidationMode, target.DatabaseName, target.PostgresVersion, target.ProcessPIDEnv, target.ProcessPIDFile, target.FixturePath, target.FixtureHash, target.ExecutablePath, target.ExecutableHash, target.MetadataHash}, "|")
}
