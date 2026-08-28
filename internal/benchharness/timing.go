package benchharness

import "time"

// Interval is an acknowledgement-bounded commit-to-cover observation. Lower
// is the earliest defensible value (assuming acknowledgement is the lower
// bound), upper is submit-to-cover. It intentionally does not claim a commit
// timestamp.
type Interval struct {
	Lower time.Duration
	Upper time.Duration
}

type TimingObservation struct {
	SubmittedAt    time.Time
	AcknowledgedAt time.Time
	CoveredAt      time.Time
}

func (t TimingObservation) SubmitToCover() time.Duration {
	if t.SubmittedAt.IsZero() || t.CoveredAt.IsZero() || t.CoveredAt.Before(t.SubmittedAt) {
		return 0
	}
	return t.CoveredAt.Sub(t.SubmittedAt)
}

func (t TimingObservation) AcknowledgementBoundedCommitToCover() Interval {
	upper := t.SubmitToCover()
	if upper == 0 {
		return Interval{}
	}
	lower := upper
	if !t.AcknowledgedAt.IsZero() {
		if t.CoveredAt.After(t.AcknowledgedAt) {
			lower = t.CoveredAt.Sub(t.AcknowledgedAt)
		} else {
			lower = 0
		}
	}
	return Interval{Lower: lower, Upper: upper}
}

func (t TimingObservation) RefreshBeforeAck() bool {
	return !t.AcknowledgedAt.IsZero() && !t.CoveredAt.IsZero() && t.CoveredAt.Before(t.AcknowledgedAt)
}
