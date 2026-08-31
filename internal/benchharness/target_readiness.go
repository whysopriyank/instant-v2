package benchharness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

func (d *TargetDriver) Provision(ctx context.Context) error {
	if d == nil {
		return fmt.Errorf("nil target driver")
	}
	if d.cfg.Provisioner == nil {
		return &UnsupportedTargetError{Check: "provision", Reason: "no target provisioner/seed callback configured"}
	}
	return d.cfg.Provisioner(ctx)
}

const defaultSemanticReadinessTimeout = 20 * time.Second

// waitForSemanticReadiness retries only after an observed semantic failure.
// The bounded backoff avoids hammering a target that is still completing its
// provision/reset work, while the caller's context and timeout keep the
// readiness barrier finite. A successful check is the only completion signal;
// this is not a fixed sleep masquerading as readiness.
func waitForSemanticReadiness(ctx context.Context, timeout time.Duration, check func(context.Context) error) error {
	if check == nil {
		return errors.New("semantic readiness check is not configured")
	}
	if timeout <= 0 {
		timeout = defaultSemanticReadinessTimeout
	}
	checkCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var lastErr error
	for attempt := 0; ; attempt++ {
		lastErr = check(checkCtx)
		if lastErr == nil {
			return nil
		}
		if err := checkCtx.Err(); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("semantic readiness deadline after %s: %w", timeout, lastErr)
		}
		delay := 10 * time.Millisecond
		for i := 0; i < attempt && delay < 250*time.Millisecond; i++ {
			delay *= 2
		}
		if delay > 250*time.Millisecond {
			delay = 250 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-checkCtx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("semantic readiness deadline after %s: %w", timeout, lastErr)
		case <-timer.C:
		}
	}
}

// waitForTargetReadiness proves both independently observed metadata and the
// semantic fixture state before a run advances to qualification or ramp.
// verifyCleanFixture specifically rejects a lingering qualification probe and
// compares every frozen query against the prefix-zero oracle.
func (d *TargetDriver) waitForTargetReadiness(ctx context.Context, fixture Fixture, runID string, evidence *evidenceCollector) error {
	timeout := d.cfg.Probe.Timeout
	if timeout <= 0 {
		timeout = defaultSemanticReadinessTimeout
	}
	var attempt int
	return waitForSemanticReadiness(ctx, timeout, func(checkCtx context.Context) error {
		attempt++
		if err := d.VerifyMetadata(checkCtx); err != nil {
			return fmt.Errorf("metadata not ready: %w", err)
		}
		if err := d.verifyCleanFixture(checkCtx, fixture, fmt.Sprintf("%s-%d", runID, attempt), evidence); err != nil {
			return err
		}
		return nil
	})
}

// verifyCleanFixture proves that the second provisioning pass restored the
// frozen benchmark state. Qualification deliberately performs a mutation, so
// metadata identity alone is insufficient: each measured query gets a fresh
// full snapshot and is compared with the prefix-zero semantic oracle before a
// measured subscriber is opened.
func (d *TargetDriver) verifyCleanFixture(ctx context.Context, fixture Fixture, runID string, evidence *evidenceCollector) error {
	oracle := NewPrefixOracle(fixture)
	probeID := d.cfg.Probe.Mutation.EntityID
	client, err := d.openSession(ctx, runID+"-clean-check", evidence)
	if err != nil {
		return fmt.Errorf("clean fixture open session: %w", err)
	}
	defer func() { _ = client.Close() }() // Preserve snapshot/identity failures over cleanup status.
	snapshots := make(map[string]Materialized)
	for _, query := range fixture.Queries {
		expected, err := oracle.Materialize(query.ID, 0)
		if err != nil {
			return fmt.Errorf("clean fixture query %q oracle: %w", query.ID, err)
		}
		if probeID != "" {
			if _, present := expected.Entities[probeID]; present {
				return fmt.Errorf("clean fixture query %q includes reserved qualification probe entity %q", query.ID, probeID)
			}
		}
		wireQuery, err := d.cfg.QueryBuilder(query)
		if err != nil {
			return fmt.Errorf("clean fixture query %q build: %w", query.ID, err)
		}
		wireKey, err := json.Marshal(wireQuery)
		if err != nil {
			return fmt.Errorf("clean fixture query %q encode: %w", query.ID, err)
		}
		actual, ok := snapshots[string(wireKey)]
		if !ok {
			actualRefresh, snapshotErr := client.Subscribe(ctx, query)
			if snapshotErr != nil {
				return fmt.Errorf("clean fixture query %q snapshot: %w", query.ID, snapshotErr)
			}
			if actualRefresh.Full == nil {
				return fmt.Errorf("clean fixture query %q snapshot was not full", query.ID)
			}
			actual = *actualRefresh.Full
			snapshots[string(wireKey)] = actual
		}
		actual.QueryID = query.ID
		if probeID != "" {
			if _, present := actual.Entities[probeID]; present {
				return fmt.Errorf("clean fixture query %q still contains qualification probe entity %q", query.ID, probeID)
			}
		}
		expectedDigest, err := expected.Digest()
		if err != nil {
			return fmt.Errorf("clean fixture query %q expected digest: %w", query.ID, err)
		}
		actualDigest, err := actual.Digest()
		if err != nil {
			return fmt.Errorf("clean fixture query %q actual digest: %w", query.ID, err)
		}
		if expectedDigest != actualDigest {
			return fmt.Errorf("clean fixture query %q diverges after qualification cleanup: expected %s, observed %s", query.ID, expectedDigest, actualDigest)
		}
	}
	return nil
}
