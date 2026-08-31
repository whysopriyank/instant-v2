package benchharness

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// ValidateBehaviorWindow prevents a direct RunPlan from silently extending
// the bounded S/R workload beyond the canonical duration used by the signed
// contract. Shorter windows remain valid and are reflected in their derived
// reconnect allowance; non-behavior families ignore the field.
func ValidateBehaviorWindow(w Workload, behaviorSeconds int) error {
	if behaviorSeconds <= 0 || (w.Family != FamilyS && w.Family != FamilyR) {
		return nil
	}
	if w.DurationSeconds <= 0 {
		return fmt.Errorf("behavior window %ds has no canonical workload duration", behaviorSeconds)
	}
	if behaviorSeconds > w.DurationSeconds {
		return fmt.Errorf("behavior window %ds exceeds canonical workload duration %ds", behaviorSeconds, w.DurationSeconds)
	}
	return nil
}

// Run validates the plan, qualifies a clean fixture, opens and warms sessions,
// then measures execution. Failed prerequisites never become measurements.
func (d *TargetDriver) Run(ctx context.Context, plan RunPlan) (TargetRunArtifacts, error) {
	if d == nil {
		return TargetRunArtifacts{}, fmt.Errorf("nil target driver")
	}
	base := TargetRunArtifacts{TargetID: d.cfg.ID, RunID: plan.RunID, PairID: plan.PairID, Family: plan.Workload.Family, Subscribers: plan.Workload.Subscribers, RampDuration: plan.RampDuration, SettleDuration: plan.SettleDuration, WarmupDuration: plan.WarmupDuration, WarmupMutations: plan.WarmupMutations}
	budget, err := d.validateRunPlan(&plan)
	if err != nil {
		return base, err
	}
	evidence := newEvidenceCollector(budget)
	if err := d.prepareRun(ctx, plan, evidence, &base); err != nil {
		return base, err
	}

	clients, writerSessions, err := d.openRunSessions(ctx, plan, evidence)
	if err != nil {
		return base, err
	}
	// Execute invokes these callbacks exactly when it records the measured
	// boundaries. Preserve caller callbacks (for example, process samplers)
	// while making the evidence window use the same timestamps.
	onMeasuredStart := plan.OnMeasuredStart
	onMeasuredEnd := plan.OnMeasuredEnd
	plan.OnMeasuredStart = func(at time.Time) error {
		evidence.beginMeasured(at)
		if onMeasuredStart != nil {
			return onMeasuredStart(at)
		}
		return nil
	}
	plan.OnMeasuredEnd = func(at time.Time) error {
		evidence.endMeasured(at)
		if onMeasuredEnd != nil {
			return onMeasuredEnd(at)
		}
		return nil
	}

	started := time.Now()
	receivers := newRunReceivers(ctx, d, plan, clients, writerSessions)
	result, runErr := Execute(ctx, plan, receivers.hooks(plan, evidence))
	receivers.stop()
	rawFrames, evidenceStats := evidence.snapshot()
	protocolCopy := receivers.protocolErrors()
	if runErr == nil && len(protocolCopy) > 0 {
		runErr = fmt.Errorf("target protocol errors: %s", strings.Join(protocolCopy, "; "))
	}
	base.Result = result
	base.RawFrames = rawFrames
	base.Evidence = evidenceStats
	base.ExpectedMutations = result.ExpectedMutations
	base.ExpectedRows = result.ExpectedRows
	base.MeasuredStartedAt = result.MeasuredStartedAt
	base.MeasuredFinishedAt = result.MeasuredFinishedAt
	base.ProtocolErrors = protocolCopy
	base.BehaviorErrors = append([]string(nil), result.BehaviorErrors...)
	base.InitialSnapshots = len(clients)
	base.StartedAt = started
	base.FinishedAt = time.Now()
	if evidenceStats.Overflow && runErr == nil {
		runErr = fmt.Errorf("target evidence budget exceeded: observed %d frames/%d bytes (limits %d/%d)", evidenceStats.ObservedFrames, evidenceStats.ObservedBytes, evidenceStats.MaxFrames, evidenceStats.MaxBytes)
	}
	return base, runErr
}

func (d *TargetDriver) finalSnapshotter(fixture Fixture, evidence *evidenceCollector) func(context.Context, string, string) (Materialized, error) {
	type cachedSnapshot struct {
		materialized Materialized
		err          error
	}
	queries := make(map[string]Query, len(fixture.Queries))
	for _, query := range fixture.Queries {
		queries[query.ID] = query
	}
	cache := make(map[string]cachedSnapshot)
	return func(ctx context.Context, recipient, queryID string) (Materialized, error) {
		query, ok := queries[queryID]
		if !ok {
			return Materialized{}, fmt.Errorf("unknown final-snapshot query %q", queryID)
		}
		wireQuery, err := d.cfg.QueryBuilder(query)
		if err != nil {
			return Materialized{}, fmt.Errorf("final snapshot query %q build: %w", queryID, err)
		}
		wireKey, err := json.Marshal(wireQuery)
		if err != nil {
			return Materialized{}, fmt.Errorf("final snapshot query %q encode: %w", queryID, err)
		}
		cached, ok := cache[string(wireKey)]
		if !ok {
			fresh, openErr := d.openSession(ctx, recipient+"-final", evidence)
			if openErr != nil {
				cached.err = openErr
			} else {
				cached.materialized, cached.err = fresh.FinalSnapshot(ctx, query)
				_ = fresh.Close()
			}
			cache[string(wireKey)] = cached
		}
		cached.materialized.QueryID = queryID
		return cached.materialized, cached.err
	}
}

// validateRunPlan applies the existing mutation limits before deriving evidence.
func (d *TargetDriver) validateRunPlan(plan *RunPlan) (EvidenceBudget, error) {
	if err := ValidateBehaviorWindow(plan.Workload, plan.BehaviorSeconds); err != nil {
		return EvidenceBudget{}, err
	}
	if d.cfg.QueryBuilder == nil || d.cfg.TransactionBuilder == nil {
		return EvidenceBudget{}, &UnsupportedTargetError{Check: "run", Reason: "query and transaction builders are required"}
	}
	if len(plan.Workload.Fixture.Queries) == 0 || plan.Workload.Subscribers != len(plan.Workload.Fixture.Queries) {
		return EvidenceBudget{}, &UnsupportedTargetError{Check: "run", Reason: "workload must contain one frozen query per subscriber"}
	}
	mutations := plan.Mutations
	if mutations <= 0 {
		mutations = plan.Workload.Measured
	}
	if plan.Workload.Family == FamilyT {
		if mutations <= 0 {
			mutations = 4096
		}
		if mutations > 4096 {
			mutations = 4096
		}
	}
	if mutations <= 0 {
		mutations = 1
	}
	plan.Mutations = mutations
	budget := d.cfg.EvidenceBudget
	if budget == (EvidenceBudget{}) {
		budgetWorkload := plan.Workload
		if (plan.Workload.Family == FamilyR || plan.Workload.Family == FamilyS) && plan.BehaviorSeconds > 0 {
			// R reconnect epochs are driven by the run plan's behavior window,
			// which may be shorter than the canonical workload duration.
			budgetWorkload.DurationSeconds = plan.BehaviorSeconds
		}
		var budgetErr error
		budget, budgetErr = EvidenceBudgetForWorkloadWithWarmup(budgetWorkload, mutations, plan.WarmupMutations)
		if budgetErr != nil {
			return EvidenceBudget{}, fmt.Errorf("derive live evidence budget: %w", budgetErr)
		}
	}
	return budget, nil
}

func (d *TargetDriver) prepareRun(ctx context.Context, plan RunPlan, evidence *evidenceCollector, base *TargetRunArtifacts) error {
	// Provision is deliberately per attempt. A caller may use it to create a
	// fresh database/process; the semantic barrier proves that exact attempt
	// has reached the clean fixture before qualification starts.
	if err := d.Provision(ctx); err != nil {
		return err
	}
	if err := d.waitForTargetReadiness(ctx, plan.Workload.Fixture, plan.RunID+"-pre-qualification-clean-check", evidence); err != nil {
		base.RawFrames, base.Evidence = evidence.snapshot()
		return fmt.Errorf("pre-qualification readiness: %w", err)
	}
	qualification, err := d.qualify(ctx, evidence)
	base.Qualification = qualification
	if err != nil {
		return err
	}
	// The four-client qualification probe intentionally mutates a probe row.
	// Restore the exact clean fixture before any measured subscriber opens, then
	// verify identity/metadata without issuing another mutation or probe.
	if err := d.Provision(ctx); err != nil {
		return fmt.Errorf("restore clean fixture after qualification: %w", err)
	}
	if err := d.waitForTargetReadiness(ctx, plan.Workload.Fixture, plan.RunID+"-post-qualification-clean-check", evidence); err != nil {
		base.RawFrames, base.Evidence = evidence.snapshot()
		return fmt.Errorf("post-qualification readiness: %w", err)
	}
	return nil
}
