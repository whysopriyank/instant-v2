package benchharness

import (
	"encoding/json"
	"runtime"
	"sync"
	"time"
)

// ResourceSample is intentionally portable. Target-specific collectors can
// enrich it, while missing optional fields remain absent rather than zero.
type ResourceSample struct {
	At           time.Time `json:"at"`
	PID          int       `json:"pid,omitempty"`
	AllocBytes   uint64    `json:"alloc_bytes,omitempty"`
	HeapInUse    uint64    `json:"heap_inuse_bytes,omitempty"`
	HeapObjects  uint64    `json:"heap_objects,omitempty"`
	NumGC        uint32    `json:"num_gc,omitempty"`
	PauseTotalNs uint64    `json:"pause_total_ns,omitempty"`
	Goroutines   int       `json:"goroutines,omitempty"`
	Error        string    `json:"error,omitempty"`
}

func SampleRuntime() ResourceSample {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return ResourceSample{At: time.Now(), AllocBytes: m.TotalAlloc, HeapInUse: m.HeapInuse, HeapObjects: m.HeapObjects, NumGC: m.NumGC, PauseTotalNs: m.PauseTotalNs, Goroutines: runtime.NumGoroutine()}
}

// EventWriter emits bounded, newline-delimited structured events for the soak
// CLI. The callback can write a file, pipe to a collector, or be nil.
type EventWriter struct {
	mu       sync.Mutex
	write    func([]byte) error
	maxBytes int
}

func NewEventWriter(write func([]byte) error, maxBytes int) *EventWriter {
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	return &EventWriter{write: write, maxBytes: maxBytes}
}
func (w *EventWriter) Emit(name string, fields map[string]any) error {
	if w == nil || w.write == nil {
		return nil
	}
	event := map[string]any{"event": name, "at": time.Now().UTC()}
	for k, v := range fields {
		event[k] = v
	}
	b, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if len(b) > w.maxBytes {
		return &eventTooLarge{len(b), w.maxBytes}
	}
	b = append(b, '\n')
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.write(b)
}

type eventTooLarge struct{ got, want int }

func (e *eventTooLarge) Error() string { return "structured event exceeds configured byte limit" }
