package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/instant-v2/instant-v2/internal/benchrun"
	"strings"
	"time"
)

func smokePlan(cfg benchrun.LiveConfig, measureSeconds int) benchrun.Plan {
	return benchrun.Plan{
		SchemaVersion: benchrun.SchemaVersion,
		Seed:          cfg.Seed, Families: []string{cfg.Family}, Scales: []int{cfg.Scale}, Pairs: 1,
		RampSeconds: 1, SettleSeconds: 1, WarmupSeconds: 1,
		MeasureSeconds: measureSeconds, GraceSeconds: 5,
	}
}

func selectSmokeTargets(cfg benchrun.LiveConfig, selection string) ([]benchrun.LiveTargetConfig, error) {
	selection = strings.TrimSpace(selection)
	if selection != "v1" && selection != "all" {
		return nil, errors.New("-targets must be v1 or all")
	}
	selected := make([]benchrun.LiveTargetConfig, 0, len(cfg.Targets))
	for _, target := range cfg.Targets {
		if selection == "all" || target.ID == "v1" {
			selected = append(selected, target)
		}
	}
	if selection == "v1" && (len(selected) != 1 || selected[0].ID != "v1") {
		return nil, errors.New("authorized config does not contain exactly one v1 target")
	}
	if selection == "all" && len(selected) != 3 {
		return nil, errors.New("all-target smoke requires v1, v2_reference, and v2_current")
	}
	if selection == "all" {
		required := map[string]bool{"v1": false, "v2_reference": false, "v2_current": false}
		for _, target := range selected {
			if _, exists := required[target.ID]; !exists || required[target.ID] {
				return nil, errors.New("all-target smoke requires exactly one each of v1, v2_reference, and v2_current")
			}
			required[target.ID] = true
		}
		for _, found := range required {
			if !found {
				return nil, errors.New("all-target smoke requires exactly one each of v1, v2_reference, and v2_current")
			}
		}
	}
	return selected, nil
}

func runSmoke(ctx context.Context, cfg benchrun.LiveConfig, executor benchrun.TargetExecutor, selected []benchrun.LiveTargetConfig, plan benchrun.Plan) ([]smokeResult, error) {
	results := make([]smokeResult, 0, len(selected))
	for index, spec := range selected {
		target := benchrun.Target{
			SchemaVersion: benchrun.SchemaVersion,
			ID:            spec.ID, Role: spec.Role, Kind: spec.Kind, Revision: spec.Revision,
			Protocol: spec.Transport, DatabaseName: spec.DatabaseName,
		}
		result := smokeResult{TargetID: target.ID, TargetKind: target.Kind, TargetRevision: target.Revision}
		qualification, err := executor.Qualify(ctx, target)
		result.QualificationOK = err == nil && qualification.Passed && qualificationChecksPass(qualification.Checks)
		if !result.QualificationOK {
			result.PrimaryClass = benchrun.SetupInvalid
			result.Failure = benchrun.Redact(qualification.Failure)
			if result.Failure == "" && err != nil {
				result.Failure = benchrun.Redact(err.Error())
			}
			results = append(results, result)
			return results, fmt.Errorf("target %s smoke qualification failed: %s", target.ID, result.Failure)
		}

		runID := fmt.Sprintf("%s-smoke-%s", cfg.PairID, target.ID)
		run := benchrun.Run{
			SchemaVersion: benchrun.SchemaVersion, ID: runID, PairID: cfg.PairID + "-smoke",
			TargetID: target.ID, TargetRevision: target.Revision,
			Family: cfg.Family, Scale: cfg.Scale, Seed: cfg.Seed,
			StartedAt: time.Now().UTC(), PrimaryClass: benchrun.Pass,
		}
		execution, err := executor.Execute(ctx, benchrun.RunSpec{
			Run: run, Target: target, Plan: plan, Attempt: index + 1, Order: target.ID,
		})
		result.PrimaryClass = execution.Run.PrimaryClass
		result.ExpectedLedgerRows = execution.Run.ExpectedLedgerRows
		result.LedgerRows = len(execution.Ledger)
		result.ProtocolErrors = len(execution.Run.ProtocolErrors)
		result.BehaviorErrors = len(execution.Run.BehaviorErrors)
		result.MeasuredWindowOK = !execution.Run.MeasuredStartedAt.IsZero() &&
			!execution.Run.MeasuredFinishedAt.IsZero() &&
			!execution.Run.MeasuredFinishedAt.Before(execution.Run.MeasuredStartedAt)
		if result.MeasuredWindowOK {
			result.MeasuredMillis = execution.Run.MeasuredFinishedAt.Sub(execution.Run.MeasuredStartedAt).Milliseconds()
		}
		if err != nil {
			result.Failure = benchrun.Redact(err.Error())
		} else {
			result.Failure = benchrun.Redact(execution.Run.Failure)
		}
		if err == nil {
			if validationErr := validateSmokeExecution(run, target, execution, plan); validationErr != nil {
				result.Failure = benchrun.Redact(validationErr.Error())
			} else {
				result.Passed = true
			}
		}
		results = append(results, result)
		if !result.Passed {
			return results, fmt.Errorf("target %s diagnostic smoke failed: class=%s ledger=%d/%d protocol=%d behavior=%d failure=%s",
				target.ID, result.PrimaryClass, result.LedgerRows, result.ExpectedLedgerRows,
				result.ProtocolErrors, result.BehaviorErrors, result.Failure)
		}
	}
	return results, nil
}
