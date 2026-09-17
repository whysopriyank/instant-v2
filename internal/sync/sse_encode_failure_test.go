package sync

// RT-002b/RT-002e SSE writer encode-failure coverage.
//
// writeSSEEvent must fail before the first network write/flush when
// Frame.Encode rejects an invalid payload: zero bytes, no empty frame,
// no snapshot/watermark mutation (the writer owns none), and the caller
// observes an error so the stream ends and the same transaction is
// retried/replayed — never silently certified.

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/instant-v2/instant-v2/internal/reactive"
)

func TestWriteSSEEventEncodeFailureWritesNothing(t *testing.T) {
	sub := &reactive.Subscription{}
	sub.Gen.Store(1)
	w := newSSEDeliveryWriter()

	bad := Frame{
		"op":              json.RawMessage(`"refresh-ok"`),
		"computations":    json.RawMessage(`not json{{{`),
		"processed-tx-id": json.RawMessage(`7`),
	}
	if err := writeSSEEvent(w, sseEvent{frame: bad, hasGen: true, gen: 1, sub: sub}); err == nil {
		t.Fatal("invalid frame must fail Encode before any write")
	} else if !strings.Contains(err.Error(), "invalid JSON") {
		t.Fatalf("unexpected encode error %v; want invalid-JSON failure", err)
	}
	if got := w.bytes(); len(got) != 0 {
		t.Fatalf("encode failure wrote %q before failing; want zero bytes", got)
	}
	// No empty SSE data frame may reach the wire.
	if got := string(w.bytes()); strings.Contains(got, "data: \n") || strings.Contains(got, "data: {}\n") {
		t.Fatalf("empty frame emitted on encode failure: %q", got)
	}
	// The writer owns no snapshot/watermark; a failed write must not have
	// fabricated any.
	if snap := sub.Snapshot(); snap != nil {
		t.Fatalf("writer mutated snapshot on encode failure: %s", snap)
	}
	if tx := sub.TxID.Load(); tx != 0 {
		t.Fatalf("writer advanced watermark to %d on encode failure", tx)
	}
}

// TestWriteSSEEventEncodeFailureUnguardedControl proves the same pre-write
// guarantee for unguarded control replies (no generation lease).
func TestWriteSSEEventEncodeFailureUnguardedControl(t *testing.T) {
	w := newSSEDeliveryWriter()
	bad := Frame{"op": json.RawMessage(`{invalid}`)}
	if err := writeSSEEvent(w, sseEvent{frame: bad}); err == nil {
		t.Fatal("invalid control frame must fail Encode")
	}
	if got := w.bytes(); len(got) != 0 {
		t.Fatalf("invalid control wrote %q; want zero bytes", got)
	}
}
