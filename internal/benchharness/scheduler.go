package benchharness

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ScheduleSlip records how late a blocking writer started relative to its
// exact scheduled instant. A missed instant is never dropped or replaced.
type ScheduleSlip struct {
	Sequence    int64
	ScheduledAt time.Time
	StartedAt   time.Time
	Slip        time.Duration
}

// BlockingScheduler schedules every item at a fixed global rate. It uses one
// timer per item rather than a buffered ticker, so backpressure is visible as
// schedule slip and cannot silently discard tokens.
type BlockingScheduler struct {
	interval time.Duration
	start    time.Time
	mu       sync.Mutex
	slips    []ScheduleSlip
}

// NewBlockingScheduler validates a positive rate and records the start instant
// for reproducibility. A start in the past is accepted so callers can resume a
// predeclared schedule and observe the resulting slip.
func NewBlockingScheduler(rate float64, start time.Time) (*BlockingScheduler, error) {
	if rate <= 0 {
		return nil, fmt.Errorf("scheduler rate must be positive")
	}
	if start.IsZero() {
		start = time.Now()
	}
	interval := time.Duration(float64(time.Second) / rate)
	if interval <= 0 {
		return nil, fmt.Errorf("scheduler rate is too high for nanosecond clock")
	}
	return &BlockingScheduler{interval: interval, start: start}, nil
}

// Run calls submit exactly once per item, in sequence order, waiting until its
// scheduled instant. If submit blocks, later items remain pending and their
// measured slips capture that pressure.
func (s *BlockingScheduler) Run(ctx context.Context, items []Mutation, submit func(context.Context, Mutation) error) error {
	if submit == nil {
		return fmt.Errorf("scheduler submit function is nil")
	}
	for i, item := range items {
		seq := int64(i + 1)
		if item.Sequence > 0 {
			seq = item.Sequence
		}
		due := s.start.Add(time.Duration(i) * s.interval)
		if err := waitUntil(ctx, due); err != nil {
			return err
		}
		started := time.Now()
		slip := started.Sub(due)
		if slip < 0 {
			slip = 0
		}
		s.mu.Lock()
		s.slips = append(s.slips, ScheduleSlip{Sequence: seq, ScheduledAt: due, StartedAt: started, Slip: slip})
		s.mu.Unlock()
		if err := submit(ctx, item); err != nil {
			return err
		}
	}
	return nil
}

func waitUntil(ctx context.Context, at time.Time) error {
	d := time.Until(at)
	if d <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Slips returns an immutable, sequence-ordered copy.
func (s *BlockingScheduler) Slips() []ScheduleSlip {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ScheduleSlip, len(s.slips))
	copy(out, s.slips)
	return out
}

// Writer submits all mutations through one dedicated scheduler and records
// acknowledgement timestamps into the supplied ledger.
type Writer struct {
	ID        string
	Scheduler *BlockingScheduler
	Ledger    *Ledger
}

// Write runs the dedicated writer. Acknowledgements may expose server tx ids;
// they remain nullable when the target does not provide one.
func (w *Writer) Write(ctx context.Context, items []Mutation, submit func(context.Context, Mutation) (Ack, error)) error {
	if w == nil || w.Scheduler == nil || submit == nil {
		return fmt.Errorf("writer requires scheduler and submit function")
	}
	if w.ID == "" {
		w.ID = "writer-0"
	}
	return w.Scheduler.Run(ctx, items, func(ctx context.Context, m Mutation) error {
		key := LedgerKey{WriterID: w.ID, ClientEventID: m.EventID}
		if w.Ledger != nil {
			if err := w.Ledger.AddExpected(key, m); err != nil {
				return err
			}
			w.Ledger.Submitted(key, time.Now())
		}
		ack, err := submit(ctx, m)
		if w.Ledger != nil {
			w.Ledger.Acknowledged(key, ack, time.Now(), err)
		}
		return err
	})
}
