package main

import (
	"errors"
	"fmt"
	"github.com/instant-v2/instant-v2/internal/benchrun"
)

func qualificationChecksPass(checks map[string]bool) bool {
	if len(checks) == 0 {
		return false
	}
	for _, passed := range checks {
		if !passed {
			return false
		}
	}
	return true
}

func validateSmokeExecution(expected benchrun.Run, target benchrun.Target, execution benchrun.ExecutionResult, plan benchrun.Plan) error {
	run := execution.Run
	if run.SchemaVersion != benchrun.SchemaVersion || run.ID != expected.ID || run.PairID != expected.PairID {
		return errors.New("smoke run identity is incomplete")
	}
	if run.TargetID != target.ID || run.TargetRevision != target.Revision || run.Family != expected.Family || run.Scale != expected.Scale || run.Seed != expected.Seed {
		return errors.New("smoke run target/config identity mismatch")
	}
	if !benchrun.ValidateFailureClass(run.PrimaryClass) || run.PrimaryClass != benchrun.Pass {
		return fmt.Errorf("smoke run has non-passing execution class %q", run.PrimaryClass)
	}
	if len(run.ProtocolErrors) != 0 || len(run.BehaviorErrors) != 0 || run.Failure != "" {
		return errors.New("smoke run contains protocol, behavior, or failure evidence")
	}
	if run.StartedAt.IsZero() || run.EndedAt.IsZero() || run.MeasuredStartedAt.IsZero() || run.MeasuredFinishedAt.IsZero() ||
		!run.EndedAt.After(run.StartedAt) || !run.MeasuredFinishedAt.After(run.MeasuredStartedAt) ||
		run.MeasuredStartedAt.Before(run.StartedAt) || run.MeasuredFinishedAt.After(run.EndedAt) {
		return errors.New("smoke run timestamps are not strictly positive and ordered")
	}
	if plan.MeasureSeconds <= 0 || run.MeasuredFinishedAt.Sub(run.MeasuredStartedAt) <= 0 {
		return errors.New("smoke measured window is invalid")
	}
	if run.ExpectedLedgerRows <= 0 || run.ExpectedMutationRecipients != run.ExpectedLedgerRows || len(execution.Ledger) != run.ExpectedLedgerRows {
		return fmt.Errorf("smoke ledger cardinality is not exact: got %d expected %d recipients %d", len(execution.Ledger), run.ExpectedLedgerRows, run.ExpectedMutationRecipients)
	}

	rowKeys := make(map[string]bool, len(execution.Ledger))
	recipients := make(map[string]bool)
	for _, row := range execution.Ledger {
		if row.SchemaVersion != benchrun.SchemaVersion || row.RunID != run.ID || row.PairID != run.PairID || row.WriterID == "" || row.RecipientID == "" || row.ClientEventID == "" {
			return errors.New("smoke ledger row identity is incomplete")
		}
		if len(row.ExpectedQuerySet) == 0 || len(row.ExpectedRecipientSet) == 0 || !uniqueNonEmpty(row.ExpectedQuerySet) || !uniqueNonEmpty(row.ExpectedRecipientSet) || !contains(row.ExpectedRecipientSet, row.RecipientID) {
			return errors.New("smoke ledger row expected identity is incomplete")
		}
		key := row.ClientEventID + "\x00" + row.RecipientID
		if rowKeys[key] {
			return errors.New("smoke ledger contains duplicate event/recipient evidence")
		}
		rowKeys[key], recipients[row.RecipientID] = true, true
		if row.SubmittedAt.IsZero() || row.AcknowledgementAt.IsZero() || row.ConvergedAt.IsZero() ||
			(row.Coverage != "converged_without_intermediate" && row.CoverAt.IsZero()) ||
			row.SubmittedAt.Before(run.StartedAt) || row.ConvergedAt.After(run.EndedAt) ||
			row.AcknowledgementAt.Before(row.SubmittedAt) || row.CoverAt.Before(row.AcknowledgementAt) || row.ConvergedAt.Before(row.CoverAt) {
			return errors.New("smoke ledger timestamps are not strictly positive and ordered")
		}
		if !benchrun.ValidateCoverage(row.Coverage) || row.Coverage == "none" || row.ExpectedMaterializedDigest == "" || row.ObservedMaterializedDigest == "" {
			return errors.New("smoke ledger semantic evidence is incomplete")
		}
		if row.Coverage == "coalesced" {
			if row.CoveredExpectedMaterializedDigest == "" || row.CoveredExpectedMaterializedDigest != row.ObservedMaterializedDigest || row.CoveredExpectedStateVersion <= row.ExpectedStateVersion || row.ObservedStateVersion < row.CoveredExpectedStateVersion {
				return errors.New("smoke ledger coalesced evidence is invalid")
			}
		} else if row.ExpectedMaterializedDigest != row.ObservedMaterializedDigest {
			return errors.New("smoke ledger semantic digest evidence mismatches")
		}
	}
	if len(recipients) == 0 {
		return errors.New("smoke ledger has no recipient evidence")
	}
	if len(execution.Frames) == 0 {
		return errors.New("smoke frame evidence is empty")
	}
	for _, frame := range execution.Frames {
		if frame.SchemaVersion != benchrun.SchemaVersion || frame.RunID != run.ID || frame.RecipientID == "" || !recipients[frame.RecipientID] || frame.ReceivedAt.IsZero() || frame.ReceivedAt.Before(run.StartedAt) || frame.ReceivedAt.After(run.EndedAt) {
			return errors.New("smoke frame identity or timestamp evidence is invalid")
		}
		if err := benchrun.ValidateMeasurement(frame.PayloadBytes); err != nil {
			return fmt.Errorf("smoke frame payload evidence is invalid: %w", err)
		}
	}
	return nil
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func uniqueNonEmpty(values []string) bool {
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		if value == "" || seen[value] {
			return false
		}
		seen[value] = true
	}
	return true
}
