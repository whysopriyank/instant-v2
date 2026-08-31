package benchharness

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Receipt is the adapter-normalized semantic event delivered by one recipient.
type Receipt struct {
	PairID                 string
	RunID                  string
	WriterID               string
	RecipientID            string
	QueryID                string
	ClientEventID          string
	Observed               Materialized
	ObservationDigest      string
	Prefix                 int
	ProvesIntermediate     bool
	ProcessedTransactionID string
	At                     time.Time
	EvidenceRef            string
	ProtocolError          string
}

// RunHooks is the only target-specific surface needed by Execute. Submit must
// issue one logical mutation and return when the target acknowledgement is
// available. Receipts can be delivered concurrently through the channel.
type RunHooks struct {
	Submit func(context.Context, Mutation) (Ack, error)
	// SubmitWriter is used by T-saturation. Each writer index must map to an
	// independent target session; Submit remains the compatibility hook for
	// non-saturation workloads and deterministic unit tests.
	SubmitWriter  func(context.Context, int, Mutation) (Ack, error)
	Receipts      <-chan Receipt
	FinalSnapshot func(context.Context, string, string) (Materialized, error)
	// StopReceipts is called once after the measured writer stops and before
	// convergence draining begins. A live target adapter uses it to stop
	// receipt producers and close Receipts deterministically; nil preserves the
	// original channel/grace behavior for synthetic callers.
	StopReceipts func()
	// OnCommitted receives the oracle prefix assigned to a successful
	// acknowledgement. Target adapters use the server transaction id to
	// correlate receipts, including T-saturation's concurrent writers, to the
	// actual acknowledged commit order.
	OnCommitted func(Mutation, Ack, int)
	// OnClientBehavior lets a target adapter execute deterministic slow-reader
	// and reconnect actions. It is called only for the affected 10% cohort.
	OnClientBehavior func(context.Context, int, int, ClientBehavior) error
	// OnBehaviorError makes pause/reconnect failures observable to the caller.
	OnBehaviorError func(int, int, ClientBehavior, error)
}

type RunPlan struct {
	PairID           string
	RunID            string
	WriterID         string
	Workload         Workload
	Start            time.Time
	Mutations        int
	Rate             float64
	StallTimeout     time.Duration
	ConvergenceGrace time.Duration
	BehaviorSeconds  int
	// Phase controls are explicit so setup/ramp/warm-up time never becomes a
	// hidden part of measured writer or ledger timings. Zero means disabled.
	RampDuration    time.Duration
	SettleDuration  time.Duration
	WarmupDuration  time.Duration
	WarmupMutations int
	WarmupStart     int64
	WarmupRate      float64
	// Boundary callbacks run synchronously at the exact timestamps recorded in
	// MeasuredStartedAt/MeasuredFinishedAt. Returning an error fails the run;
	// callers must not silently publish a result without both boundary samples.
	OnMeasuredStart func(time.Time) error
	OnMeasuredEnd   func(time.Time) error
}

type RunResult struct {
	Ledger          *Ledger
	Slips           []ScheduleSlip
	Submitted       int
	Acknowledged    int
	Errors          int
	BehaviorErrors  []string
	WarmupMutations int
	// ExpectedMutations/ExpectedRows are contract inputs, independent of what
	// the target happened to acknowledge or deliver.
	ExpectedMutations  int
	ExpectedRows       int
	StartedAt          time.Time
	MeasuredStartedAt  time.Time
	MeasuredFinishedAt time.Time
	FinishedAt         time.Time
}

// Execute runs one workload's dedicated writer and concurrently reconciles
// target receipts against the prefix oracle. It is deliberately not a pair
// runner: cross-target order, aggregation, and reporting belong to WP5-B.
func Execute(ctx context.Context, plan RunPlan, hooks RunHooks) (RunResult, error) {
	if err := ValidateBehaviorWindow(plan.Workload, plan.BehaviorSeconds); err != nil {
		return RunResult{}, err
	}
	if hooks.Submit == nil && hooks.SubmitWriter == nil {
		return RunResult{}, fmt.Errorf("run submit hook is nil")
	}
	if plan.Workload.Family != FamilyT && hooks.Submit == nil {
		return RunResult{}, fmt.Errorf("non-saturation run requires submit hook")
	}
	if plan.Workload.Family == FamilyT && plan.Mutations == 0 {
		plan.Mutations = plan.Workload.Measured
	}
	if plan.Mutations <= 0 {
		plan.Mutations = 1
	}
	if plan.Workload.Family == FamilyT && plan.Mutations > 4096 {
		plan.Mutations = 4096
	}
	if plan.WriterID == "" {
		plan.WriterID = "writer-0"
	}
	if plan.RunID == "" {
		plan.RunID = "run-0"
	}
	if plan.Rate == 0 {
		plan.Rate = plan.Workload.TxRate
	}
	if plan.Rate <= 0 {
		plan.Rate = 8
	}
	scheduler, err := NewBlockingScheduler(plan.Rate, plan.Start)
	if err != nil {
		return RunResult{}, err
	}
	ledger := NewLedger()
	oracle := NewPrefixOracle(plan.Workload.Fixture)
	result := RunResult{Ledger: ledger, StartedAt: time.Now(), WarmupMutations: plan.WarmupMutations, ExpectedMutations: plan.Mutations, ExpectedRows: expectedRowsForWorkload(plan.Workload, plan.Mutations)}
	if plan.WarmupMutations > 0 {
		start := plan.WarmupStart
		if start <= 0 {
			start = 1_000_000
		}
		for i := 0; i < plan.WarmupMutations; i++ {
			oracle.Append(plan.Workload.Mutation(start + int64(i)))
		}
	}
	behaviorCtx, stopBehavior := context.WithCancel(ctx)
	defer stopBehavior()
	var behaviorMu sync.Mutex
	var behaviorErrors []string
	var behaviorDone chan struct{}
	if hooks.OnClientBehavior != nil && (plan.Workload.Family == FamilyS || plan.Workload.Family == FamilyR) {
		seconds := plan.BehaviorSeconds
		if seconds <= 0 {
			seconds = plan.Workload.DurationSeconds
		}
		behaviorDone = make(chan struct{})
		go func() {
			defer close(behaviorDone)
			runClientBehaviors(behaviorCtx, plan.Workload, seconds, hooks.OnClientBehavior, func(clientID, elapsed int, behavior ClientBehavior, err error) {
				behaviorMu.Lock()
				behaviorErrors = append(behaviorErrors, fmt.Sprintf("client=%d elapsed=%d pause=%t reconnect=%t: %v", clientID, elapsed, behavior.PauseReads, behavior.Reconnect, err))
				behaviorMu.Unlock()
				if hooks.OnBehaviorError != nil {
					hooks.OnBehaviorError(clientID, elapsed, behavior, err)
				}
			})
		}()
	}
	receiptCtx, stopReceipts := context.WithCancel(ctx)
	var receiptsDone chan struct{}
	if hooks.Receipts != nil {
		receiptsDone = make(chan struct{})
		go func() {
			defer close(receiptsDone)
			reconcileReceipts(receiptCtx, ledger, oracle, hooks.Receipts, plan.PairID, plan.RunID, plan.WriterID)
		}()
	}
	var runErr error
	result.MeasuredStartedAt = time.Now()
	if plan.OnMeasuredStart != nil {
		if err := plan.OnMeasuredStart(result.MeasuredStartedAt); err != nil {
			runErr = fmt.Errorf("measured-start callback: %w", err)
		}
	}
	if runErr == nil {
		if plan.Workload.Family == FamilyT {
			result, runErr = executeSaturation(ctx, plan, hooks, ledger, oracle, result)
		} else {
			items := make([]Mutation, plan.Mutations)
			for i := range items {
				items[i] = plan.Workload.Mutation(int64(i + 1))
			}
			runErr = scheduler.Run(ctx, items, func(ctx context.Context, m Mutation) error {
				keyPrefix := LedgerKey{PairID: plan.PairID, RunID: plan.RunID, WriterID: plan.WriterID, ClientEventID: m.EventID}
				oracle.Append(m)
				prefix := oracle.Len()
				assignments := assignmentsFor(plan.Workload, m)
				for _, a := range assignments {
					key := keyPrefix
					key.RecipientID = a.RecipientID
					digest, e := oracle.ExpectedDigest(a.QueryID, prefix)
					if e != nil {
						oracle.RemoveLast(m)
						return e
					}
					if e = ledger.AddExpected(key, m); e != nil {
						oracle.RemoveLast(m)
						return e
					}
					ledger.SetExpectation(key, []string{a.QueryID}, []string{a.RecipientID}, digest, prefix)
					ledger.Submitted(key, time.Now())
				}
				ack, e := hooks.Submit(ctx, m)
				for _, a := range assignments {
					key := keyPrefix
					key.RecipientID = a.RecipientID
					ledger.Acknowledged(key, ack, time.Now(), e)
				}
				if e != nil {
					oracle.RemoveLast(m)
					result.Errors++
					return e
				}
				if !ack.Accepted {
					oracle.RemoveLast(m)
					for _, a := range assignments {
						key := keyPrefix
						key.RecipientID = a.RecipientID
						ledger.Error(key, ErrorTarget, "write acknowledgement was not accepted")
					}
					result.Errors++
					return fmt.Errorf("mutation %s was not accepted", m.EventID)
				}
				result.Submitted++
				if hooks.OnCommitted != nil {
					hooks.OnCommitted(m, ack, prefix)
				}
				result.Acknowledged++
				return nil
			})
		}
	}
	result.MeasuredFinishedAt = time.Now()
	if plan.OnMeasuredEnd != nil {
		if err := plan.OnMeasuredEnd(result.MeasuredFinishedAt); err != nil && runErr == nil {
			runErr = fmt.Errorf("measured-end callback: %w", err)
		}
	}
	// Stop behavior producers and wait for any reconnect hook to finish before
	// closing the receipt readers. This keeps WaitGroup Add/Wait ordering safe.
	stopBehavior()
	if behaviorDone != nil {
		<-behaviorDone
	}
	if hooks.StopReceipts != nil {
		hooks.StopReceipts()
	}
	if receiptsDone != nil {
		grace := plan.ConvergenceGrace
		if grace <= 0 {
			grace = 30 * time.Second
		}
		drainCtx, cancel := context.WithTimeout(ctx, grace)
		select {
		case <-receiptsDone:
		case <-drainCtx.Done():
		}
		cancel()
		stopReceipts()
		<-receiptsDone
	} else {
		stopReceipts()
	}
	if hooks.FinalSnapshot != nil {
		finalizeSnapshots(ctx, plan, ledger, oracle, hooks.FinalSnapshot)
	}
	behaviorMu.Lock()
	result.BehaviorErrors = append([]string(nil), behaviorErrors...)
	behaviorMu.Unlock()
	if runErr == nil && len(result.BehaviorErrors) > 0 {
		runErr = fmt.Errorf("client behavior failures: %s", strings.Join(result.BehaviorErrors, "; "))
	}
	if runErr != nil {
		result.Errors++
	}
	ledger.Finalize()
	result.Slips = scheduler.Slips()
	result.FinishedAt = time.Now()
	return result, runErr
}

func runClientBehaviors(ctx context.Context, workload Workload, seconds int, hook func(context.Context, int, int, ClientBehavior) error, onError func(int, int, ClientBehavior, error)) {
	runClientBehaviorsAt(ctx, workload, seconds, time.Second, hook, onError)
}

// runClientBehaviorsAt is split out for deterministic timing tests. Behavior
// callbacks for the affected cohort are launched together and joined before
// the next tick, bounding concurrency by the cohort rather than serializing
// reconnect backoffs across clients.
func runClientBehaviorsAt(ctx context.Context, workload Workload, seconds int, interval time.Duration, hook func(context.Context, int, int, ClientBehavior) error, onError func(int, int, ClientBehavior, error)) {
	if seconds <= 0 {
		return
	}
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	elapsed := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			elapsed++
			var wg sync.WaitGroup
			for id := 0; id < workload.Subscribers; id++ {
				behavior := workload.Behavior(id, elapsed)
				if behavior.PauseReads || behavior.Reconnect {
					wg.Add(1)
					go func(clientID, at int, requested ClientBehavior) {
						defer wg.Done()
						if err := hook(ctx, clientID, at, requested); err != nil && onError != nil {
							onError(clientID, at, requested, err)
						}
					}(id, elapsed, behavior)
				}
			}
			wg.Wait()
			if elapsed >= seconds {
				return
			}
		}
	}
}

func executeSaturation(ctx context.Context, plan RunPlan, hooks RunHooks, ledger *Ledger, oracle *PrefixOracle, result RunResult) (RunResult, error) {
	const writerCount = 8
	operations := plan.Mutations
	if operations <= 0 {
		operations = 4096
	}
	if operations > 4096 {
		operations = 4096
	}
	type committed struct {
		writer   int
		mutation Mutation
		ack      Ack
		keys     []LedgerKey
		ackAt    time.Time
		order    int64
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	var orderingErr error
	var commits []committed
	for writer := 0; writer < writerCount; writer++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			for n := writer; n < operations; n += writerCount {
				select {
				case <-ctx.Done():
					return
				default:
				}
				m := plan.Workload.Mutation(int64(n + 1))
				assignments := assignmentsFor(plan.Workload, m)
				keys := make([]LedgerKey, 0, len(assignments))
				// Create every attempted row and record submission before the
				// network call. The oracle prefix is assigned only after all
				// acknowledgements can be sorted by an observable server id.
				for _, a := range assignments {
					key := LedgerKey{PairID: plan.PairID, RunID: plan.RunID, WriterID: fmt.Sprintf("writer-%d", writer), RecipientID: a.RecipientID, ClientEventID: m.EventID}
					if err := ledger.AddExpected(key, m); err != nil {
						mu.Lock()
						if firstErr == nil {
							firstErr = err
						}
						mu.Unlock()
						continue
					}
					ledger.SetExpectation(key, []string{a.QueryID}, []string{a.RecipientID}, "", 0)
					ledger.Submitted(key, time.Now())
					keys = append(keys, key)
				}
				failed := len(keys) != len(assignments)
				if failed {
					return
				}
				var ack Ack
				var e error
				if hooks.SubmitWriter != nil {
					ack, e = hooks.SubmitWriter(ctx, writer, m)
				} else {
					ack, e = hooks.Submit(ctx, m)
				}
				ackAt := time.Now()
				if e == nil && ack.Accepted {
					order, orderable := saturationOrder(ack)
					mu.Lock()
					if !orderable && orderingErr == nil {
						orderingErr = &UnsupportedTargetError{Check: "t_tx_order", Reason: "T-saturation requires a numeric server tx-id or processed-tx-id"}
					}
					commits = append(commits, committed{writer: writer, mutation: m, ack: ack, keys: keys, ackAt: ackAt, order: order})
					mu.Unlock()
				} else {
					for _, key := range keys {
						ledger.Acknowledged(key, ack, ackAt, e)
						message := "write acknowledgement was not accepted"
						if e != nil {
							message = e.Error()
						}
						ledger.Error(key, ErrorTarget, message)
					}
					mu.Lock()
					if firstErr == nil {
						if e != nil {
							firstErr = e
						} else {
							firstErr = fmt.Errorf("mutation %s was not accepted", m.EventID)
						}
					}
					mu.Unlock()
				}
				if e != nil || !ack.Accepted {
					return
				}
			}
		}(writer)
	}
	wg.Wait()
	mu.Lock()
	err := firstErr
	orderErr := orderingErr
	ordered := append([]committed(nil), commits...)
	mu.Unlock()
	expectedRows := expectedRowsForWorkload(plan.Workload, operations)
	acceptedRows := 0
	for _, commit := range ordered {
		acceptedRows += len(commit.keys)
	}
	if err == nil && (len(ordered) != operations || acceptedRows != expectedRows) {
		if ctx.Err() != nil {
			err = fmt.Errorf("T saturation stopped before contract completion: %w (accepted %d/%d mutations, %d/%d rows)", ctx.Err(), len(ordered), operations, acceptedRows, expectedRows) //nolint:staticcheck // ST1005: T is the frozen workload family identifier.
		} else {
			err = fmt.Errorf("T saturation incomplete: accepted %d/%d mutations and %d/%d rows", len(ordered), operations, acceptedRows, expectedRows) //nolint:staticcheck // ST1005: T is the frozen workload family identifier.
		}
	}
	if orderErr != nil {
		for _, commit := range ordered {
			for _, key := range commit.keys {
				ledger.Acknowledged(key, commit.ack, commit.ackAt, orderErr)
				ledger.Error(key, ErrorInfrastructure, orderErr.Error())
			}
		}
		return result, orderErr
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].order < ordered[j].order })
	for i := 1; i < len(ordered); i++ {
		if ordered[i-1].order == ordered[i].order {
			err = &UnsupportedTargetError{Check: "t_tx_order", Reason: fmt.Sprintf("duplicate numeric transaction identity %d", ordered[i].order)}
			break
		}
	}
	if err != nil && len(ordered) > 0 {
		// A rejected write does not establish a total commit order for the
		// accepted rows. Preserve attempted rows but do not claim coverage.
		if _, unsupported := err.(*UnsupportedTargetError); unsupported {
			for _, commit := range ordered {
				for _, key := range commit.keys {
					ledger.Acknowledged(key, commit.ack, commit.ackAt, err)
					ledger.Error(key, ErrorInfrastructure, err.Error())
				}
			}
			return result, err
		}
	}
	committedRows := 0
	for _, commit := range ordered {
		oracle.Append(commit.mutation)
		prefix := oracle.Len()
		assignments := assignmentsFor(plan.Workload, commit.mutation)
		for i, a := range assignments {
			key := commit.keys[i]
			digest, derr := oracle.ExpectedDigest(a.QueryID, prefix)
			if derr != nil {
				if err == nil {
					err = derr
				}
				ledger.Error(key, ErrorInfrastructure, derr.Error())
				continue
			}
			ledger.SetExpectation(key, []string{a.QueryID}, []string{a.RecipientID}, digest, prefix)
			ledger.Acknowledged(key, commit.ack, commit.ackAt, nil)
			committedRows++
		}
		if hooks.OnCommitted != nil {
			hooks.OnCommitted(commit.mutation, commit.ack, prefix)
		}
		result.Submitted++
		result.Acknowledged++
	}
	if err == nil && committedRows != expectedRows {
		err = fmt.Errorf("T saturation produced %d/%d expected rows", committedRows, expectedRows) //nolint:staticcheck // ST1005: T is the frozen workload family identifier.
	}
	return result, err
}

func saturationOrder(ack Ack) (int64, bool) {
	for _, raw := range []string{ack.ServerTransactionID, ack.ProcessedTransactionID} {
		if raw == "" {
			continue
		}
		n, err := strconv.ParseInt(raw, 10, 64)
		if err == nil && n >= 0 {
			return n, true
		}
	}
	return 0, false
}

type assignment struct{ QueryID, RecipientID string }

func expectedRowsForWorkload(w Workload, mutations int) int {
	rows := 0
	for i := 1; i <= mutations; i++ {
		m := w.Mutation(int64(i))
		for _, q := range w.Fixture.Queries {
			if q.MatchAll || q.Bucket == m.Bucket || m.Kind == MutationRetract || m.Kind == MutationReorder {
				rows++
			}
		}
	}
	return rows
}

func assignmentsFor(w Workload, m Mutation) []assignment {
	out := make([]assignment, 0, len(w.Fixture.Queries))
	for _, q := range w.Fixture.Queries {
		applicable := q.MatchAll || q.Bucket == m.Bucket || m.Kind == MutationRetract || m.Kind == MutationReorder
		if applicable {
			out = append(out, assignment{q.ID, "client-" + q.ID})
		}
	}
	return out
}

func reconcileReceipts(ctx context.Context, ledger *Ledger, oracle *PrefixOracle, receipts <-chan Receipt, pairID, runID, writerID string) {
	for {
		select {
		case <-ctx.Done():
			return
		case r, ok := <-receipts:
			if !ok {
				return
			}
			if r.PairID == "" {
				r.PairID = pairID
			}
			if r.RunID == "" {
				r.RunID = runID
			}
			if r.WriterID == "" {
				r.WriterID = writerID
			}
			if r.ClientEventID == "" || r.RecipientID == "" || r.QueryID == "" {
				continue
			}
			key := LedgerKey{PairID: r.PairID, RunID: r.RunID, WriterID: r.WriterID, RecipientID: r.RecipientID, ClientEventID: r.ClientEventID}
			row, ok := ledger.Expected(key)
			if !ok {
				key, row, ok = ledger.FindByEvent(r.PairID, r.RunID, r.RecipientID, r.ClientEventID)
				if !ok {
					continue
				}
			}
			if r.ProtocolError != "" {
				ledger.Error(key, ErrorProtocol, r.ProtocolError)
				continue
			}
			if r.Prefix <= 0 {
				ledger.Error(key, ErrorProtocol, "receipt has no explicit state prefix")
				continue
			}
			latest := ""
			var err error
			if r.Prefix > int(row.ExpectedStateVersion) {
				latest, err = oracle.ExpectedDigest(r.QueryID, r.Prefix)
			}
			if err != nil {
				continue
			}
			at := r.At
			if at.IsZero() {
				at = time.Now()
			}
			_ = ledger.Observe(key, Observation{ObservedDigest: mustDigest(r.Observed), ExpectedDigest: row.ExpectedMaterializedDigest, LatestExpectedDigest: latest, Prefix: r.Prefix, ExpectedPrefix: int(row.ExpectedStateVersion), ProvesIntermediate: r.ProvesIntermediate, Applicable: true, ProcessedTransactionID: r.ProcessedTransactionID, At: at, EvidenceRef: r.EvidenceRef})
		}
	}
}

func finalizeSnapshots(ctx context.Context, plan RunPlan, ledger *Ledger, oracle *PrefixOracle, snapshot func(context.Context, string, string) (Materialized, error)) {
	prefix := oracle.Len()
	for _, q := range plan.Workload.Fixture.Queries {
		recipient := "client-" + q.ID
		observed, err := snapshot(ctx, recipient, q.ID)
		rows := ledger.Rows()
		for _, row := range rows {
			if row.RecipientID != recipient || len(row.ExpectedQuerySet) == 0 || row.ExpectedQuerySet[0] != q.ID {
				continue
			}
			key := row.LedgerKey
			if err != nil {
				ledger.Invalidate(key, ErrorTarget, "final snapshot failed: "+err.Error())
				ledger.Error(key, ErrorTarget, err.Error())
				continue
			}
			latest, e := oracle.ExpectedDigest(q.ID, prefix)
			if e != nil {
				ledger.Invalidate(key, ErrorInfrastructure, "final snapshot oracle divergence: "+e.Error())
				ledger.Error(key, ErrorInfrastructure, e.Error())
				continue
			}
			observedDigest := mustDigest(observed)
			_ = ledger.Observe(key, Observation{ObservedDigest: observedDigest, ExpectedDigest: row.ExpectedMaterializedDigest, LatestExpectedDigest: latest, Prefix: prefix, ExpectedPrefix: int(row.ExpectedStateVersion), Applicable: true, At: time.Now(), EvidenceRef: "final-snapshot"})
			if observedDigest != latest {
				ledger.Invalidate(key, ErrorTarget, "final snapshot digest does not match prefix oracle")
			}
		}
	}
}
func mustDigest(m Materialized) string { d, _ := m.Digest(); return d }

// SetExpectation records query/recipient and expected-prefix evidence. It is
// separate from AddExpected so callers can create the row before expensive
// oracle canonicalization and still retain a complete attempted ledger.
func (l *Ledger) SetExpectation(key LedgerKey, queries, recipients []string, digest string, prefix int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if r := l.rows[key]; r != nil {
		r.ExpectedQuerySet = append([]string(nil), queries...)
		r.ExpectedRecipientSet = append([]string(nil), recipients...)
		r.ExpectedMaterializedDigest = digest
		r.ExpectedStateVersion = int64(prefix)
		if pending, ok := l.buffered[key]; ok {
			delete(l.buffered, key)
			if pending.ExpectedDigest == "" {
				pending.ExpectedDigest = digest
			}
			if pending.ExpectedPrefix == 0 {
				pending.ExpectedPrefix = prefix
			}
			_ = l.observeLocked(key, pending)
		}
	}
}
