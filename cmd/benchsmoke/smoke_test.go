package main

import (
	"context"
	"errors"
	"github.com/instant-v2/instant-v2/internal/benchrun"
	"testing"
	"time"
)

type fakeSmokeExecutor struct {
	failTarget      string
	missingMeasured bool
	badTimestamp    bool
	badIdentity     bool
	duplicateLedger bool
}

func (f fakeSmokeExecutor) Qualify(_ context.Context, target benchrun.Target) (benchrun.Qualification, error) {
	if target.ID == f.failTarget {
		return benchrun.Qualification{Passed: false, Failure: "qualification failed"}, errors.New("qualification failed")
	}
	return benchrun.Qualification{Passed: true, Checks: map[string]bool{"live_refresh": true}}, nil
}

func (f fakeSmokeExecutor) Execute(_ context.Context, spec benchrun.RunSpec) (benchrun.ExecutionResult, error) {
	if spec.Target.ID == f.failTarget {
		return benchrun.ExecutionResult{}, errors.New("execution failed")
	}
	now := time.Now().UTC()
	measuredStartedAt := now
	measuredFinishedAt := now.Add(time.Second)
	if f.missingMeasured {
		measuredStartedAt = time.Time{}
		measuredFinishedAt = time.Time{}
	}
	row := benchrun.LedgerRow{SchemaVersion: benchrun.SchemaVersion, PairID: spec.Run.PairID, RunID: spec.Run.ID,
		WriterID: "writer", RecipientID: "recipient", ClientEventID: spec.Run.ID + "/event",
		ExpectedQuerySet: []string{"query"}, ExpectedRecipientSet: []string{"recipient"},
		SubmittedAt: now, AcknowledgementAt: now, CoverAt: now, ConvergedAt: now,
		ExpectedMaterializedDigest: "digest", ObservedMaterializedDigest: "digest", Coverage: "exact",
	}
	if f.badTimestamp {
		row.AcknowledgementAt = time.Time{}
	}
	if f.badIdentity {
		row.RunID = "other-run"
	}
	rows := []benchrun.LedgerRow{row}
	if f.duplicateLedger {
		rows = append(rows, row)
	}
	frames := []benchrun.Frame{{SchemaVersion: benchrun.SchemaVersion, RunID: spec.Run.ID, RecipientID: "recipient", ReceivedAt: now, Kind: "refresh", MaterializedDigest: "digest", PayloadBytes: benchrun.Zero("bytes")}}
	return benchrun.ExecutionResult{Run: benchrun.Run{
		SchemaVersion:              benchrun.SchemaVersion,
		ID:                         spec.Run.ID,
		PairID:                     spec.Run.PairID,
		TargetID:                   spec.Target.ID,
		TargetRevision:             spec.Target.Revision,
		Family:                     spec.Run.Family,
		Scale:                      spec.Run.Scale,
		Seed:                       spec.Run.Seed,
		PrimaryClass:               benchrun.Pass,
		StartedAt:                  now.Add(-time.Second),
		EndedAt:                    now.Add(2 * time.Second),
		ExpectedLedgerRows:         1,
		MeasuredStartedAt:          measuredStartedAt,
		MeasuredFinishedAt:         measuredFinishedAt,
		ExpectedMutationRecipients: 1,
	}, Ledger: rows, Frames: frames}, nil
}

func smokeTestConfig() benchrun.LiveConfig {
	return benchrun.LiveConfig{
		PairID: "smoke", Seed: 17, Family: "H-append", Scale: 300,
		Targets: []benchrun.LiveTargetConfig{
			{ID: "v1", Role: "v1", Kind: "v1", Revision: "v1-sha"},
			{ID: "v2_reference", Role: "v2_reference", Kind: "v2", Revision: "ref-sha"},
			{ID: "v2_current", Role: "v2_current", Kind: "v2", Revision: "current-sha"},
		},
	}
}

func TestSelectSmokeTargets(t *testing.T) {
	cfg := smokeTestConfig()
	one, err := selectSmokeTargets(cfg, "v1")
	if err != nil || len(one) != 1 || one[0].ID != "v1" {
		t.Fatalf("v1 selection = %#v, %v", one, err)
	}
	all, err := selectSmokeTargets(cfg, "all")
	if err != nil || len(all) != 3 {
		t.Fatalf("all selection = %#v, %v", all, err)
	}
	if _, err := selectSmokeTargets(cfg, "v2"); err == nil {
		t.Fatal("noncanonical target selection was accepted")
	}
	invalid := cfg
	invalid.Targets = append([]benchrun.LiveTargetConfig(nil), cfg.Targets...)
	invalid.Targets[2].ID = "v2_other"
	if _, err := selectSmokeTargets(invalid, "all"); err == nil {
		t.Fatal("noncanonical triad was accepted")
	}
}

func TestRunSmokeExecutesEachSelectedTargetOnce(t *testing.T) {
	cfg := smokeTestConfig()
	results, err := runSmoke(context.Background(), cfg, fakeSmokeExecutor{}, cfg.Targets, smokePlan(cfg, 1))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatalf("results=%d, want 3", len(results))
	}
	for _, result := range results {
		if !result.Passed || result.LedgerRows != 1 || result.ExpectedLedgerRows != 1 || result.ProtocolErrors != 0 || !result.MeasuredWindowOK {
			t.Fatalf("incomplete smoke result: %+v", result)
		}
	}
}

func TestRunSmokeFailsClosedOnTargetFailure(t *testing.T) {
	cfg := smokeTestConfig()
	selected, err := selectSmokeTargets(cfg, "all")
	if err != nil {
		t.Fatal(err)
	}
	results, err := runSmoke(context.Background(), cfg, fakeSmokeExecutor{failTarget: "v1"}, selected, smokePlan(cfg, 1))
	if err == nil || len(results) != 1 || results[0].Passed {
		t.Fatalf("failure was not retained: results=%+v err=%v", results, err)
	}
}

func TestRunSmokeRejectsMissingMeasuredWindow(t *testing.T) {
	cfg := smokeTestConfig()
	results, err := runSmoke(context.Background(), cfg, fakeSmokeExecutor{missingMeasured: true}, cfg.Targets[:1], smokePlan(cfg, 1))
	if err == nil || len(results) != 1 || results[0].Passed || results[0].MeasuredWindowOK {
		t.Fatalf("missing measured window was accepted: results=%+v err=%v", results, err)
	}
}

func TestRunSmokeRejectsNonPositiveEvidenceTimestamps(t *testing.T) {
	cfg := smokeTestConfig()
	results, err := runSmoke(context.Background(), cfg, fakeSmokeExecutor{badTimestamp: true}, cfg.Targets[:1], smokePlan(cfg, 1))
	if err == nil || len(results) != 1 || results[0].Passed {
		t.Fatalf("non-positive evidence timestamp was accepted: results=%+v err=%v", results, err)
	}
}

func TestRunSmokeRejectsLedgerIdentityAndCardinalityMismatch(t *testing.T) {
	cfg := smokeTestConfig()
	for name, fake := range map[string]fakeSmokeExecutor{
		"identity":  {badIdentity: true},
		"duplicate": {duplicateLedger: true},
	} {
		t.Run(name, func(t *testing.T) {
			results, err := runSmoke(context.Background(), cfg, fake, cfg.Targets[:1], smokePlan(cfg, 1))
			if err == nil || len(results) != 1 || results[0].Passed {
				t.Fatalf("invalid %s evidence was accepted: results=%+v err=%v", name, results, err)
			}
		})
	}
}

func TestSmokePlanIsDiagnosticAndBounded(t *testing.T) {
	cfg := smokeTestConfig()
	plan := smokePlan(cfg, 2)
	if plan.Pairs != 1 || plan.MeasureSeconds != 2 || plan.RampSeconds != 1 || plan.SettleSeconds != 1 || plan.WarmupSeconds != 1 || plan.GraceSeconds != 5 {
		t.Fatalf("unexpected smoke plan: %+v", plan)
	}
}
