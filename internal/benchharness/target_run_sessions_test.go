package benchharness

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTargetRunSessionSetupClosesEveryOpenedSessionOnFailure(t *testing.T) {
	for _, phase := range []string{"subscriber-dial", "subscribe", "writer-dial", "warmup"} {
		t.Run(phase, func(t *testing.T) {
			failure := errors.New("injected setup failure")
			dialer := &setupFailureDialer{failure: failure}
			switch phase {
			case "subscriber-dial":
				dialer.failAt = 2
			case "writer-dial":
				dialer.failAt = 4 // two subscribers and the first T writer opened
			}
			builds := 0
			driver := &TargetDriver{cfg: TargetConfig{
				Dialer: dialer,
				QueryBuilder: func(Query) (any, error) {
					builds++
					if phase == "subscribe" && builds == 2 {
						return nil, failure
					}
					return map[string]any{"todos": map[string]any{}}, nil
				},
				TransactionBuilder: func(Mutation) ([]any, error) { return nil, failure },
			}}
			plan := RunPlan{RunID: "setup", Workload: Workload{Family: FamilyT, Fixture: Fixture{Queries: []Query{{ID: "q-0"}, {ID: "q-1"}}}}}
			if phase == "warmup" {
				plan.WarmupMutations = 1
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, _, err := driver.openRunSessions(ctx, plan, newEvidenceCollector(EvidenceBudget{}))
			if !errors.Is(err, failure) {
				t.Fatalf("setup did not propagate injected %s failure: %v", phase, err)
			}
			if len(dialer.sessions) == 0 {
				t.Fatal("failure did not exercise partial session setup")
			}
			for i, session := range dialer.sessions {
				select {
				case <-session.closed:
				default:
					t.Errorf("session %d remained open after %s failure", i, phase)
				}
			}
		})
	}
}

type setupFailureDialer struct {
	failure  error
	failAt   int
	attempts int
	sessions []*scriptedTargetSession
}

func (d *setupFailureDialer) Dial(context.Context, SessionOptions) (Session, error) {
	d.attempts++
	if d.attempts == d.failAt {
		return nil, d.failure
	}
	session := newScriptedTargetSession()
	d.sessions = append(d.sessions, session)
	return session, nil
}
