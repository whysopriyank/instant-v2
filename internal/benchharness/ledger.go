package benchharness

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// LedgerKey is the stable identity of a writer/recipient event observation.
type LedgerKey struct {
	PairID        string
	RunID         string
	WriterID      string
	RecipientID   string
	ClientEventID string
}

type Coverage string

const (
	CoverageExact                        Coverage = "exact"
	CoverageCoalesced                    Coverage = "coalesced"
	CoverageConvergedWithoutIntermediate Coverage = "converged_without_intermediate"
	CoverageIrrelevant                   Coverage = "irrelevant"
	CoverageMissing                      Coverage = "missing"
)

type ErrorClass string

const (
	ErrorNone           ErrorClass = ""
	ErrorProtocol       ErrorClass = "protocol_error"
	ErrorInfrastructure ErrorClass = "infrastructure_error"
	ErrorTarget         ErrorClass = "target_error"
)

// Ack is the target-neutral acknowledgement returned by a write adapter.
type Ack struct {
	ServerTransactionID    string
	ProcessedTransactionID string
	Accepted               bool
	Error                  string
}

// Observation is a parsed receipt or final materialized snapshot. Prefix is
// the latest ordered writer prefix proven by the observation.
type Observation struct {
	ObservedDigest         string
	ExpectedDigest         string
	LatestExpectedDigest   string
	Prefix                 int
	ExpectedPrefix         int
	ObservedStateVersion   int64
	ProvesIntermediate     bool
	Applicable             bool
	ProcessedTransactionID string
	At                     time.Time
	EvidenceRef            string
}

// LedgerRow contains the contract's raw semantic and timing evidence.
type LedgerRow struct {
	LedgerKey
	ServerTransactionID        string
	ExpectedQuerySet           []string
	ExpectedRecipientSet       []string
	SubmittedAt                time.Time
	AcknowledgementAt          time.Time
	CoverAt                    time.Time
	ProcessedTransactionID     string
	ExpectedMaterializedDigest string
	ObservedMaterializedDigest string
	ExpectedStateVersion       int64
	// CoveredExpected* preserve the later prefix/digest used to validate a
	// coalesced observation. The original Expected* fields are immutable proof
	// of the mutation's own prefix and are never overwritten.
	CoveredExpectedMaterializedDigest string
	CoveredExpectedStateVersion       int64
	ObservedStateVersion              int64
	Coverage                          Coverage
	RefreshBeforeAck                  bool
	BufferedSnapshotAhead             bool
	ConvergedAt                       time.Time
	ErrorClass                        ErrorClass
	EvidenceRef                       string
	Mutation                          Mutation
}

// Ledger is safe for a reader goroutine and the dedicated writer to update
// concurrently. It retains unresolved rows as missing at Finalize time.
type Ledger struct {
	mu       sync.Mutex
	rows     map[LedgerKey]*LedgerRow
	buffered map[LedgerKey]Observation
}

func NewLedger() *Ledger {
	return &Ledger{rows: make(map[LedgerKey]*LedgerRow), buffered: make(map[LedgerKey]Observation)}
}

func (l *Ledger) AddExpected(key LedgerKey, mutation Mutation) error {
	if l == nil {
		return fmt.Errorf("nil ledger")
	}
	if key.ClientEventID == "" {
		return fmt.Errorf("ledger event id is empty")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.rows[key]; ok {
		return fmt.Errorf("duplicate ledger key %q", key.ClientEventID)
	}
	l.rows[key] = &LedgerRow{LedgerKey: key, Mutation: mutation, Coverage: CoverageMissing, ErrorClass: ErrorNone}
	return nil
}

func (l *Ledger) Submitted(key LedgerKey, at time.Time) {
	l.update(key, func(r *LedgerRow) { r.SubmittedAt = at })
}

func (l *Ledger) Acknowledged(key LedgerKey, ack Ack, at time.Time, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r := l.rows[key]
	if r == nil {
		return
	}
	r.AcknowledgementAt, r.ServerTransactionID, r.ProcessedTransactionID = at, ack.ServerTransactionID, ack.ProcessedTransactionID
	if !r.CoverAt.IsZero() && r.CoverAt.Before(at) {
		r.RefreshBeforeAck = true
	}
	if err != nil || !ack.Accepted {
		r.ErrorClass = ErrorTarget
		delete(l.buffered, key)
		// A receipt may race the write acknowledgement. It is not evidence
		// for a rejected/errored mutation, even if its digest happened to
		// match an earlier prefix.
		if err != nil || !ack.Accepted {
			r.Coverage = CoverageMissing
		}
		return
	}
	if pending, ok := l.buffered[key]; ok {
		delete(l.buffered, key)
		_ = l.observeLocked(key, pending)
	}
}

// Observe applies a receipt/snapshot to a recipient row. Observations ahead of
// acknowledgement retain both timestamps and are never re-ordered.
func (l *Ledger) Observe(key LedgerKey, observation Observation) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.rows[key]; !ok {
		return fmt.Errorf("unknown ledger key %q", key.ClientEventID)
	}
	return l.observeLocked(key, observation)
}

func (l *Ledger) observeLocked(key LedgerKey, observation Observation) error {
	r := l.rows[key]
	if r == nil {
		return fmt.Errorf("unknown ledger key %q", key.ClientEventID)
	}
	if r.Coverage != CoverageMissing && r.Coverage != "" {
		return nil
	}
	if !observation.Applicable {
		r.Coverage = CoverageIrrelevant
		return nil
	}
	if observation.Prefix <= 0 {
		return nil
	}
	// The saturation writer installs the expected prefix only after the
	// acknowledgement establishes actual commit order. Retain an early
	// receipt until that order is known, but do not let it cover a row yet.
	if observation.ExpectedPrefix <= 0 {
		l.buffered[key] = observation
		r.BufferedSnapshotAhead = true
		return nil
	}
	if observation.Prefix < observation.ExpectedPrefix {
		return nil
	}
	valid := observation.ObservedDigest != "" && observation.ExpectedDigest != "" && observation.ObservedDigest == observation.ExpectedDigest
	if observation.Prefix > observation.ExpectedPrefix && observation.LatestExpectedDigest != "" {
		valid = observation.ObservedDigest == observation.LatestExpectedDigest
	}
	if !valid {
		return nil
	}
	if observation.Prefix > observation.ExpectedPrefix && r.AcknowledgementAt.IsZero() {
		l.buffered[key] = observation
		r.BufferedSnapshotAhead = true
		return nil
	}
	if !observation.At.IsZero() {
		r.CoverAt = observation.At
		r.ConvergedAt = observation.At
	}
	r.ObservedMaterializedDigest = observation.ObservedDigest
	r.ObservedStateVersion = observation.ObservedStateVersion
	if r.ObservedStateVersion == 0 && observation.Prefix > 0 {
		r.ObservedStateVersion = int64(observation.Prefix)
	}
	r.ProcessedTransactionID = observation.ProcessedTransactionID
	r.EvidenceRef = observation.EvidenceRef
	if observation.Prefix > observation.ExpectedPrefix {
		r.CoveredExpectedMaterializedDigest = observation.LatestExpectedDigest
		r.CoveredExpectedStateVersion = int64(observation.Prefix)
	}
	if !r.AcknowledgementAt.IsZero() && !observation.At.IsZero() && observation.At.Before(r.AcknowledgementAt) {
		r.RefreshBeforeAck = true
	}
	if observation.Prefix > observation.ExpectedPrefix {
		r.Coverage = CoverageCoalesced
	} else if observation.ProvesIntermediate {
		r.Coverage = CoverageExact
	} else {
		r.Coverage = CoverageConvergedWithoutIntermediate
	}
	return nil
}

func (l *Ledger) Error(key LedgerKey, class ErrorClass, evidence string) {
	l.update(key, func(r *LedgerRow) {
		if r.Coverage == CoverageMissing || r.Coverage == "" {
			r.ErrorClass = class
			r.EvidenceRef = evidence
		}
	})
}

// Invalidate removes a previously accepted coverage proof when a later
// authoritative snapshot contradicts it. This is intentionally separate from
// Error: ordinary late diagnostics must not rewrite first-valid-proof state,
// while final convergence is an authoritative correctness gate.
func (l *Ledger) Invalidate(key LedgerKey, class ErrorClass, evidence string) {
	l.update(key, func(r *LedgerRow) {
		r.Coverage = CoverageMissing
		r.ErrorClass = class
		r.EvidenceRef = evidence
	})
}

func (l *Ledger) Finalize() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, r := range l.rows {
		if r.Coverage == "" {
			r.Coverage = CoverageMissing
		}
	}
}

func (l *Ledger) update(key LedgerKey, fn func(*LedgerRow)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if r := l.rows[key]; r != nil {
		fn(r)
	}
}

// Expected returns a stable row snapshot for receipt reconciliation.
func (l *Ledger) Expected(key LedgerKey) (LedgerRow, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r := l.rows[key]
	if r == nil {
		return LedgerRow{}, false
	}
	return *r, true
}

// FindByEvent resolves a receipt when a transport adapter cannot carry the
// logical writer id. Client event ids are unique within a run, so this keeps
// T-saturation receipts joinable without guessing a writer from local arrival
// order.
func (l *Ledger) FindByEvent(pairID, runID, recipientID, eventID string) (LedgerKey, LedgerRow, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for key, row := range l.rows {
		if key.PairID == pairID && key.RunID == runID && key.RecipientID == recipientID && key.ClientEventID == eventID {
			return key, *row, true
		}
	}
	return LedgerKey{}, LedgerRow{}, false
}

func (l *Ledger) Rows() []LedgerRow {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]LedgerRow, 0, len(l.rows))
	for _, r := range l.rows {
		c := *r
		c.ExpectedQuerySet = append([]string(nil), r.ExpectedQuerySet...)
		c.ExpectedRecipientSet = append([]string(nil), r.ExpectedRecipientSet...)
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ClientEventID == out[j].ClientEventID {
			return out[i].RecipientID < out[j].RecipientID
		}
		return out[i].ClientEventID < out[j].ClientEventID
	})
	return out
}

// CoverageCounts keeps exact/coalesced/irrelevant/missing visible separately.
func (l *Ledger) CoverageCounts() map[Coverage]int {
	out := map[Coverage]int{}
	for _, r := range l.Rows() {
		out[r.Coverage]++
	}
	return out
}

// PrefixOracle replays the ordered writer mutation log for every assigned query.
type PrefixOracle struct {
	mu      sync.RWMutex
	initial map[string]Entity
	queries map[string]Query
	events  []Mutation
}

func NewPrefixOracle(fixture Fixture) *PrefixOracle {
	qs := make(map[string]Query, len(fixture.Queries))
	for _, q := range fixture.Queries {
		qs[q.ID] = q
	}
	return &PrefixOracle{initial: cloneEntities(fixture.Entities), queries: qs}
}

func (o *PrefixOracle) Append(m Mutation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, m)
}

// RemoveLast rolls back an unacknowledged planned mutation. It is only valid
// for the writer's latest event, preserving the committed prefix oracle.
func (o *PrefixOracle) RemoveLast(m Mutation) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.events) == 0 || o.events[len(o.events)-1].EventID != m.EventID {
		return false
	}
	o.events = o.events[:len(o.events)-1]
	return true
}

func (o *PrefixOracle) Len() int { o.mu.RLock(); defer o.mu.RUnlock(); return len(o.events) }
func (o *PrefixOracle) query(id string) (Query, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	q, ok := o.queries[id]
	return q, ok
}

func (o *PrefixOracle) Events() []Mutation {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return append([]Mutation(nil), o.events...)
}

// ExpectedDigest returns a digest for a query at an ordered mutation prefix.
func (o *PrefixOracle) ExpectedDigest(queryID string, prefix int) (string, error) {
	m, err := o.Materialize(queryID, prefix)
	if err != nil {
		return "", err
	}
	return m.Digest()
}
