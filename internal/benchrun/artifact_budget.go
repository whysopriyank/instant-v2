package benchrun

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const DefaultMaxArtifactBytes int64 = 128 << 20

// MaxAbsoluteArtifactBudget is a hard safety ceiling for contract-derived
// bundles. It is deliberately independent of the historical default so large
// valid JSONL workloads can be measured without unbounded reads or writes.
const MaxAbsoluteArtifactBudget int64 = 64 << 40
const SmallArtifactBytes int64 = 8 << 20
const MaxJSONLineBytes int = 1 << 20

// workload cardinality. An explicit operator cap is never raised; the
// default budget may grow to accommodate the contract's real JSONL volume.
func (w *ArtifactWriter) ConfigureContractBudget(total, perFile int64) error {
	if total <= 0 || perFile <= 0 || perFile > total || total > MaxAbsoluteArtifactBudget || perFile > MaxAbsoluteArtifactBudget {
		return errors.New("invalid contract artifact budget")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	effectiveTotal, effectiveFile := total, perFile
	if w.operatorBudget {
		effectiveTotal = w.MaxBytes
		if effectiveTotal <= 0 || effectiveTotal > MaxAbsoluteArtifactBudget {
			return errors.New("invalid operator artifact budget")
		}
		if effectiveFile > w.MaxFileBytes {
			effectiveFile = w.MaxFileBytes
		}
	}
	if w.contractBudgetConfigured {
		if w.MaxBytes != effectiveTotal || w.MaxFileBytes != effectiveFile {
			return errors.New("artifact budget was already configured differently")
		}
		return nil
	}
	if w.finalized || w.total != 0 {
		return errors.New("artifact budget must be configured before writes")
	}
	if !w.operatorBudget {
		w.MaxBytes = total
		w.MaxFileBytes = perFile
	} else {
		if perFile < w.MaxFileBytes {
			w.MaxFileBytes = perFile
		}
	}
	w.contractBudgetConfigured = true
	return nil
}

func validateBundleSize(root string, totalLimit, fileLimit int64) error {
	if err := validateArtifactLimits(totalLimit, fileLimit); err != nil {
		return err
	}
	files, err := bundleFiles(root)
	if err != nil {
		return err
	}
	var total int64
	for rel := range files {
		f, size, err := openRegularArtifact(filepath.Join(root, rel))
		if err != nil {
			return err
		}
		_ = f.Close()
		if size > fileLimit || total > totalLimit-size {
			return errors.New("bundle exceeds artifact budget")
		}
		total += size
	}
	return nil
}

func validateArtifactLimits(total, perFile int64) error {
	if total <= 0 || perFile <= 0 || perFile > total || total > MaxAbsoluteArtifactBudget || perFile > MaxAbsoluteArtifactBudget {
		return errors.New("invalid artifact budget")
	}
	return nil
}

// bundleArtifactLimits reads the signed manifest's derived limits. Until a
// trusted detached approval is present, or if the small manifest/plan
// evidence is malformed or inconsistent, callers get conservative defaults so
// attacker-selected large limits cannot unlock hashing or allocation.
func bundleArtifactLimits(root string) (int64, int64, error) {
	defaults := func(err error) (int64, int64, error) {
		return DefaultMaxArtifactBytes, DefaultMaxArtifactBytes, err
	}
	manifestPath := filepath.Join(root, "manifest.json")
	if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
		return DefaultMaxArtifactBytes, DefaultMaxArtifactBytes, nil
	} else if err != nil {
		return defaults(err)
	}
	b, err := readBounded(manifestPath, SmallArtifactBytes)
	if err != nil {
		return defaults(err)
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return defaults(fmt.Errorf("invalid manifest budget evidence: %w", err))
	}
	if m.ArtifactMaxTotalBytes == 0 && m.ArtifactMaxFileBytes == 0 {
		return DefaultMaxArtifactBytes, DefaultMaxArtifactBytes, nil
	}
	if err := validateArtifactLimits(m.ArtifactMaxTotalBytes, m.ArtifactMaxFileBytes); err != nil {
		return defaults(fmt.Errorf("invalid manifest budget evidence: %w", err))
	}
	planBytes, err := readBounded(filepath.Join(root, "plan.json"), SmallArtifactBytes)
	if err != nil {
		return defaults(err)
	}
	var plan Plan
	if err := json.Unmarshal(planBytes, &plan); err != nil {
		return defaults(err)
	}
	if plan.Seed != m.Seed || len(plan.Families) != 1 || canonicalBenchmarkFamily(plan.Families[0]) != canonicalBenchmarkFamily(m.Family) || len(plan.Scales) != 1 || plan.Scales[0] != m.SubscriberScale {
		return defaults(errors.New("manifest and plan do not match"))
	}
	if err := validateManifestEvidenceBudget(m, plan); err != nil {
		return defaults(err)
	}
	targetCount, err := manifestTargetCount(m)
	if err != nil {
		return defaults(err)
	}
	derivedTotal, derivedFile, err := ContractArtifactBudgetForTargets(m.Family, m.SubscriberScale, plan, targetCount)
	if err != nil || derivedTotal != m.ArtifactMaxTotalBytes || derivedFile != m.ArtifactMaxFileBytes {
		return defaults(errors.New("manifest artifact budget does not match contract"))
	}
	if err := VerifyApproval(m); err != nil {
		return defaults(err)
	}
	return m.ArtifactMaxTotalBytes, m.ArtifactMaxFileBytes, nil
}

// manifestTargetCount derives the artifact cardinality from the signed
// manifest contract. It deliberately never inspects run rows: observed rows
// are untrusted and must not be able to unlock larger artifact limits.
func manifestTargetCount(m Manifest) (int, error) {
	if !isThreeTargetManifest(m) {
		return 2, nil
	}
	if err := validateThreeTargetManifestShape(m); err != nil {
		return 0, err
	}
	if len(m.TargetRevisions) != len(threeTargetIDs) {
		return 0, errors.New("three-target manifest target count is not canonical")
	}
	for _, id := range threeTargetIDs {
		if _, ok := m.TargetRevisions[id]; !ok {
			return 0, fmt.Errorf("three-target manifest missing target revision %s", id)
		}
	}
	return len(threeTargetIDs), nil
}
