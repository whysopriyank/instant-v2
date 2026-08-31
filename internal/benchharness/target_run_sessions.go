package benchharness

import (
	"context"
	"fmt"
	"time"
)

// openRunSessions transfers ownership only after ramp, settle and warm-up
// succeed. Every partial setup shares the same cleanup path.
func (d *TargetDriver) openRunSessions(ctx context.Context, plan RunPlan, evidence *evidenceCollector) (clients, writers []*TargetSession, err error) {
	clients = make([]*TargetSession, 0, len(plan.Workload.Fixture.Queries))
	writers = make([]*TargetSession, 0, 8)
	defer func() {
		if err != nil {
			closeTargetSessions(writers)
			closeTargetSessions(clients)
		}
	}()
	rampStarted := time.Now()
	for i, query := range plan.Workload.Fixture.Queries {
		if err = waitRamp(ctx, rampStarted, plan.RampDuration, i, len(plan.Workload.Fixture.Queries)); err != nil {
			return clients, writers, err
		}
		client, openErr := d.openSession(ctx, fmt.Sprintf("%s-client-%d", plan.RunID, i), evidence)
		if openErr != nil {
			return clients, writers, openErr
		}
		if _, err = client.Subscribe(ctx, query); err != nil {
			_ = client.Close()
			return clients, writers, err
		}
		clients = append(clients, client)
	}
	if err = waitPhase(ctx, plan.SettleDuration); err != nil {
		return clients, writers, err
	}
	if plan.Workload.Family == FamilyT {
		for i := 0; i < 8; i++ {
			writer, openErr := d.openSession(ctx, fmt.Sprintf("%s-writer-%d", plan.RunID, i), evidence)
			if openErr != nil {
				return clients, writers, openErr
			}
			writers = append(writers, writer)
		}
	}
	if err = d.runWarmup(ctx, plan, clients, writers); err != nil {
		return clients, writers, err
	}
	return clients, writers, nil
}

func waitPhase(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func waitRamp(ctx context.Context, started time.Time, duration time.Duration, index, total int) error {
	if duration <= 0 || total <= 1 || index <= 0 {
		return nil
	}
	// The final subscriber is admitted at the end of the declared ramp. Use
	// integer nanoseconds to avoid accumulating floating-point schedule drift.
	offset := time.Duration(int64(duration) * int64(index) / int64(total-1))
	wait := time.Until(started.Add(offset))
	if wait <= 0 {
		return nil
	}
	return waitPhase(ctx, wait)
}

func (d *TargetDriver) runWarmup(ctx context.Context, plan RunPlan, clients, writers []*TargetSession) error {
	count := plan.WarmupMutations
	if count <= 0 {
		return waitPhase(ctx, plan.WarmupDuration)
	}
	if len(clients) == 0 {
		return &UnsupportedTargetError{Check: "warmup", Reason: "no subscriber session is available for warm-up"}
	}
	start := plan.WarmupStart
	if start <= 0 {
		start = 1_000_000
	}
	rate := plan.WarmupRate
	if rate <= 0 {
		rate = plan.Rate
	}
	if rate <= 0 {
		rate = plan.Workload.TxRate
	}
	if rate <= 0 {
		rate = 8
	}
	scheduler, err := NewBlockingScheduler(rate, time.Now())
	if err != nil {
		return err
	}
	items := make([]Mutation, count)
	for i := range items {
		items[i] = plan.Workload.Mutation(start + int64(i))
	}
	writer := clients[0]
	if plan.Workload.Family == FamilyT && len(writers) > 0 {
		writer = writers[0]
	}
	warmCtx := ctx
	var cancel context.CancelFunc
	if plan.WarmupDuration > 0 {
		warmCtx, cancel = context.WithTimeout(ctx, plan.WarmupDuration)
		defer cancel()
	}
	started := time.Now()
	if err := scheduler.Run(warmCtx, items, func(callCtx context.Context, mutation Mutation) error {
		ack, err := writer.Transact(callCtx, mutation)
		if err != nil {
			return err
		}
		if !ack.Accepted {
			return fmt.Errorf("warm-up mutation %s was not accepted", mutation.EventID)
		}
		return nil
	}); err != nil {
		return err
	}
	if plan.WarmupDuration > 0 {
		if remaining := plan.WarmupDuration - time.Since(started); remaining > 0 {
			return waitPhase(ctx, remaining)
		}
	}
	return nil
}
