package benchrun

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func validateRunEvidence(dir string, run Run) error {
	if run.WarmupMutations < 0 {
		return fmt.Errorf("run %s has negative warmup mutation count", run.ID)
	}
	for name, phase := range run.PhaseDurations {
		if err := ValidateMeasurement(phase); err != nil {
			return fmt.Errorf("run %s invalid phase duration %s: %w", run.ID, name, err)
		}
	}
	if run.PrimaryClass == Pass && (len(run.ProtocolErrors) > 0 || len(run.BehaviorErrors) > 0) {
		return fmt.Errorf("run %s contains protocol or behavior errors", run.ID)
	}
	ledgerPath, framePath := filepath.Join(dir, "ledger.jsonl"), filepath.Join(dir, "frames.jsonl")
	_, fileLimit, limitErr := bundleArtifactLimits(filepath.Dir(filepath.Dir(dir)))
	if limitErr != nil {
		// Unit tests may validate an isolated run directory without a manifest;
		// retain the standalone writer default in that diagnostic-only case.
		fileLimit = DefaultMaxArtifactBytes
	}
	ledger, err := os.Open(ledgerPath)
	if err != nil {
		return fmt.Errorf("run %s missing ledger: %w", run.ID, err)
	}
	defer ledger.Close()
	if info, e := os.Stat(ledgerPath); e != nil || info.Size() > fileLimit {
		return fmt.Errorf("run %s ledger exceeds artifact budget", run.ID)
	}
	frames, err := os.Open(framePath)
	if err != nil {
		return fmt.Errorf("run %s missing frames: %w", run.ID, err)
	}
	defer frames.Close()
	if info, e := os.Stat(framePath); e != nil || info.Size() > fileLimit {
		return fmt.Errorf("run %s frames exceeds artifact budget", run.ID)
	}
	ledgerCount, covered := 0, 0
	scan := bufio.NewScanner(ledger)
	scan.Buffer(make([]byte, 64<<10), MaxJSONLineBytes)
	for scan.Scan() {
		var row LedgerRow
		if err := json.Unmarshal(scan.Bytes(), &row); err != nil {
			return fmt.Errorf("run %s malformed ledger: %w", run.ID, err)
		}
		if row.SchemaVersion != SchemaVersion {
			return fmt.Errorf("run %s ledger schema mismatch", run.ID)
		}
		if row.RunID != run.ID {
			return fmt.Errorf("run %s ledger cross-link mismatch", run.ID)
		}
		if !ValidateCoverage(row.Coverage) {
			return fmt.Errorf("run %s invalid coverage %q", run.ID, row.Coverage)
		}
		if row.ClientEventID == "" || row.RecipientID == "" {
			return fmt.Errorf("run %s incomplete ledger identity", run.ID)
		}
		if len(row.ExpectedQuerySet) == 0 || len(row.ExpectedRecipientSet) == 0 {
			return fmt.Errorf("run %s ledger row lacks expected query/recipient sets", run.ID)
		}
		if row.SubmittedAt.IsZero() {
			return fmt.Errorf("run %s ledger row lacks submission timestamp", run.ID)
		}
		if row.AcknowledgementAt.IsZero() {
			if run.PrimaryClass == Pass {
				return fmt.Errorf("run %s passing ledger row lacks acknowledgement timestamp", run.ID)
			}
		}
		if row.ErrorClass != "" {
			if run.PrimaryClass == Pass {
				return fmt.Errorf("run %s contains ledger error evidence", run.ID)
			}
		}
		if row.Coverage != "none" {
			if row.ExpectedMaterializedDigest == "" || row.ObservedMaterializedDigest == "" {
				return fmt.Errorf("run %s semantic digest evidence is incomplete", run.ID)
			}
			if row.Coverage == "coalesced" {
				if row.CoveredExpectedMaterializedDigest == "" || row.CoveredExpectedMaterializedDigest != row.ObservedMaterializedDigest || row.CoveredExpectedStateVersion <= row.ExpectedStateVersion || row.ObservedStateVersion < row.CoveredExpectedStateVersion {
					return fmt.Errorf("run %s invalid coalesced prefix/digest evidence", run.ID)
				}
			} else if row.ExpectedMaterializedDigest != row.ObservedMaterializedDigest {
				return fmt.Errorf("run %s semantic digest mismatch", run.ID)
			}
			if row.CoverAt.IsZero() && row.ConvergedAt.IsZero() {
				return fmt.Errorf("run %s covered row has no convergence timestamp", run.ID)
			}
			covered++
		} else if run.PrimaryClass == Pass {
			return fmt.Errorf("run %s passing ledger row has no coverage", run.ID)
		}
		ledgerCount++
	}
	if err := scan.Err(); err != nil {
		return err
	}
	frameCount := 0
	scan = bufio.NewScanner(frames)
	scan.Buffer(make([]byte, 64<<10), MaxJSONLineBytes)
	for scan.Scan() {
		var frame Frame
		if err := json.Unmarshal(scan.Bytes(), &frame); err != nil {
			return fmt.Errorf("run %s malformed frame: %w", run.ID, err)
		}
		if frame.SchemaVersion != SchemaVersion || frame.RunID != run.ID || frame.RecipientID == "" {
			return fmt.Errorf("run %s frame cross-link/schema mismatch", run.ID)
		}
		if err := ValidateMeasurement(frame.PayloadBytes); err != nil {
			return fmt.Errorf("run %s invalid frame bytes: %w", run.ID, err)
		}
		if frame.ErrorClass != "" && run.PrimaryClass == Pass {
			return fmt.Errorf("run %s contains frame error evidence", run.ID)
		}
		frameCount++
	}
	if err := scan.Err(); err != nil {
		return err
	}
	if run.PrimaryClass == Pass && (run.ExpectedLedgerRows <= 0 || run.ExpectedMutationRecipients <= 0 || ledgerCount != run.ExpectedLedgerRows || ledgerCount != run.ExpectedMutationRecipients || frameCount == 0 || covered == 0 || covered != ledgerCount) {
		return fmt.Errorf("run %s passing result lacks semantic evidence", run.ID)
	}
	return nil
}

func validateRunEvidenceBudget(run Run, manifest Manifest) error {
	runLegacy := run.EvidenceMaxFrames == 0 && run.EvidenceMaxBytes == 0 && run.EvidenceMaxRetained == 0
	manifestLegacy := manifest.EvidenceMaxFrames == 0 && manifest.EvidenceMaxBytes == 0 && manifest.EvidenceMaxRetained == 0
	if runLegacy || manifestLegacy {
		if runLegacy && manifestLegacy {
			return nil
		}
		return fmt.Errorf("run %s evidence budget is not completely bound to the manifest", run.ID)
	}
	if run.EvidenceMaxFrames <= 0 || run.EvidenceMaxBytes <= 0 || run.EvidenceMaxRetained <= 0 {
		return fmt.Errorf("run %s evidence budget is incomplete", run.ID)
	}
	if manifest.EvidenceMaxFrames <= 0 || manifest.EvidenceMaxBytes <= 0 || manifest.EvidenceMaxRetained <= 0 {
		return fmt.Errorf("run %s carries evidence budget without manifest binding", run.ID)
	}
	if run.EvidenceMaxFrames != manifest.EvidenceMaxFrames || run.EvidenceMaxBytes != manifest.EvidenceMaxBytes || run.EvidenceMaxRetained != manifest.EvidenceMaxRetained {
		return fmt.Errorf("run %s evidence budget does not match manifest", run.ID)
	}
	return nil
}

func validateManifestEvidenceBudget(manifest Manifest, plan Plan) error {
	if manifest.EvidenceMaxFrames == 0 && manifest.EvidenceMaxBytes == 0 && manifest.EvidenceMaxRetained == 0 {
		return nil
	}
	if manifest.EvidenceMaxFrames == 0 || manifest.EvidenceMaxBytes == 0 || manifest.EvidenceMaxRetained == 0 {
		return errors.New("manifest evidence budget is incomplete")
	}
	budget, err := ContractEvidenceBudget(manifest.Family, manifest.SubscriberScale, plan)
	if err != nil {
		return fmt.Errorf("recompute evidence budget: %w", err)
	}
	if manifest.EvidenceMaxFrames != budget.MaxFrames || manifest.EvidenceMaxBytes != budget.MaxBytes || manifest.EvidenceMaxRetained != budget.MaxRetainedFrames {
		return errors.New("manifest evidence budget does not match contract")
	}
	return nil
}
