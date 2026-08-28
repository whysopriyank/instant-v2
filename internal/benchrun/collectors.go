package benchrun

import "context"

type CollectorState string

const (
	CollectorSupported   CollectorState = "supported"
	CollectorUnsupported CollectorState = "unsupported"
	CollectorFailed      CollectorState = "failed"
)

type CollectorResult[T any] struct {
	State  CollectorState `json:"state"`
	Value  T              `json:"value,omitempty"`
	Reason string         `json:"reason,omitempty"`
}

type ProcessCollector interface {
	Sample(context.Context) (ProcessSample, error)
	Close() error
}
type RuntimeCollector interface {
	Sample(context.Context) (RuntimeSample, error)
	Close() error
}
type DatabaseCollector interface {
	Before(context.Context) (DBSnapshot, error)
	After(context.Context) (DBSnapshot, error)
	Close() error
}
type NetworkCollector interface {
	Sample(context.Context) (Measurement, error)
	Close() error
}

type UnsupportedCollector struct{ Reason string }

func (u UnsupportedCollector) Sample(context.Context) (Measurement, error) {
	return Unsupported("bytes", u.Reason), nil
}
func (u UnsupportedCollector) Close() error { return nil }

type UnsupportedProcessCollector struct{ Reason string }

func (u UnsupportedProcessCollector) Sample(context.Context) (ProcessSample, error) {
	return ProcessSample{UserCPU: Unsupported("cpu_seconds", u.Reason), SystemCPU: Unsupported("cpu_seconds", u.Reason), RSS: Unsupported("bytes", u.Reason), PeakRSS: Unsupported("bytes", u.Reason), Threads: Unsupported("threads", u.Reason), FDs: Unsupported("fds", u.Reason)}, nil
}
func (u UnsupportedProcessCollector) Close() error { return nil }

type UnsupportedRuntimeCollector struct{ Reason string }

func (u UnsupportedRuntimeCollector) Sample(context.Context) (RuntimeSample, error) {
	return RuntimeSample{AllocBytes: Unsupported("bytes", u.Reason), LiveHeap: Unsupported("bytes", u.Reason), HeapGoal: Unsupported("bytes", u.Reason), GCCycles: Unsupported("cycles", u.Reason), GCPause: Unsupported("seconds", u.Reason), Goroutines: Unsupported("count", u.Reason)}, nil
}
func (u UnsupportedRuntimeCollector) Close() error { return nil }

type UnsupportedDatabaseCollector struct{ Reason string }

func (u UnsupportedDatabaseCollector) Before(context.Context) (DBSnapshot, error) {
	return unsupportedDB(u.Reason), nil
}
func (u UnsupportedDatabaseCollector) After(context.Context) (DBSnapshot, error) {
	return unsupportedDB(u.Reason), nil
}
func (u UnsupportedDatabaseCollector) Close() error { return nil }
func unsupportedDB(reason string) DBSnapshot {
	return DBSnapshot{Connections: Unsupported("count", reason), BlockHits: Unsupported("count", reason), BlockReads: Unsupported("count", reason), TempBytes: Unsupported("bytes", reason), TempFiles: Unsupported("count", reason), Commits: Unsupported("count", reason), Rollbacks: Unsupported("count", reason), TupleReads: Unsupported("count", reason), TupleWrites: Unsupported("count", reason), WALBytes: Unsupported("bytes", reason), SlotLag: Unsupported("bytes", reason), PoolActive: Unsupported("count", reason), PoolIdle: Unsupported("count", reason)}
}

func CollectorMeasurement(state CollectorState, value float64, unit, reason string) Measurement {
	switch state {
	case CollectorSupported:
		if value == 0 {
			return Zero(unit)
		}
		return Measurement{Status: StatusValue, Value: value, Unit: unit}
	case CollectorUnsupported:
		return Unsupported(unit, reason)
	case CollectorFailed:
		return Measurement{Status: StatusFailed, Unit: unit, Error: reason}
	default:
		return Missing(unit)
	}
}
