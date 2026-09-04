package benchrun

import (
	"path/filepath"
	"strings"
)

// reportCell groups run observations without filling in missing targets or metrics.
type reportCell struct {
	family string
	scale  int
	pairs  map[string]map[string]Run
}

// validatedReport is the boundary between bundle I/O and pure aggregation.
// Loaders retain the pair/triad-specific validation order and claim findings.
type validatedReport struct {
	manifest             Manifest
	manifestHash         string
	cells                map[string]*reportCell
	missingProvenance    []string
	provenanceReasons    []string
	qualificationFailed  bool
	qualificationFailure string
}

func (input validatedReport) summary() Summary {
	return Summary{
		SchemaVersion:     SchemaVersion,
		AggregatorVersion: AggregatorVersion,
		InputHashes:       map[string]string{"manifest.json": input.manifestHash},
		ClaimGate:         ClaimGate{Reasons: input.provenanceReasons},
	}
}

func (input *validatedReport) recordProvenance(label string) {
	if len(input.missingProvenance) > 0 {
		input.provenanceReasons = append(input.provenanceReasons, label+strings.Join(input.missingProvenance, ", "))
	}
}

// ReportFromArtifacts only reads the immutable local artifact bundle. It never
// contacts either target; failed attempts remain visible in the result.
func ReportFromArtifacts(root string) (Summary, error) {
	return reportFromArtifacts(root, true)
}

func reportFromArtifacts(root string, verify bool) (Summary, error) {
	m, err := LoadManifest(root)
	if err != nil {
		return Summary{}, err
	}
	if verify {
		if err := VerifyChecksums(root); err != nil {
			return Summary{}, err
		}
	}
	if isThreeTargetManifest(m) {
		input, err := loadThreeTargetReport(root, m)
		if err != nil {
			return Summary{}, err
		}
		return aggregateThreeTargetReport(input), nil
	}
	input, err := loadPairReport(root, m)
	if err != nil {
		return Summary{}, err
	}
	return aggregatePairReport(input), nil
}

func loadPairReport(root string, m Manifest) (validatedReport, error) {
	if err := validateSharedReportEvidence(root, m, 2); err != nil {
		return validatedReport{}, err
	}
	// Preserve the historical empty diagnostic hash if the reread fails.
	manifestHash, _ := hashFileStringBounded(filepath.Join(root, "manifest.json"), SmallArtifactBytes)
	input := validatedReport{manifest: m, manifestHash: manifestHash, missingProvenance: pairManifestProvenance(root, m)}
	input.recordProvenance("missing manifest provenance: ")

	// Historical pair bundles load run evidence before target qualifications.
	cells, runIDs, err := loadPairRuns(root, m)
	if err != nil {
		return validatedReport{}, err
	}
	targets, failed, failure, err := loadPairTargetRecords(root, m, &input.missingProvenance)
	if err != nil {
		return validatedReport{}, err
	}
	if err := validatePairProcessEvidence(root, m, cells, targets); err != nil {
		return validatedReport{}, err
	}
	input.recordProvenance("missing manifest/target provenance: ")
	if err := validatePairRunSet(m, runIDs); err != nil {
		return validatedReport{}, err
	}
	input.cells, input.qualificationFailed, input.qualificationFailure = cells, failed, failure
	return input, nil
}

func loadThreeTargetReport(root string, m Manifest) (validatedReport, error) {
	if err := validateThreeTargetManifestShape(m); err != nil {
		return validatedReport{}, err
	}
	if err := validateSharedReportEvidence(root, m, len(threeTargetIDs)); err != nil {
		return validatedReport{}, err
	}
	// Preserve the historical empty diagnostic hash if the reread fails.
	manifestHash, _ := hashFileStringBounded(filepath.Join(root, "manifest.json"), SmallArtifactBytes)
	input := validatedReport{manifest: m, manifestHash: manifestHash, missingProvenance: threeTargetManifestProvenance(root, m)}
	input.recordProvenance("missing manifest provenance: ")

	// Three-target runs bind revisions and schedule blocks to loaded targets.
	targets, failed, failure, err := loadThreeTargetRecords(root, m, &input.missingProvenance)
	if err != nil {
		return validatedReport{}, err
	}
	input.recordProvenance("missing manifest/target provenance: ")
	cells, err := loadThreeTargetRuns(root, m, targets)
	if err != nil {
		return validatedReport{}, err
	}
	if err := validatePairProcessEvidence(root, m, cells, targets); err != nil {
		return validatedReport{}, err
	}
	input.cells, input.qualificationFailed, input.qualificationFailure = cells, failed, failure
	return input, nil
}
