package benchharness

import (
	"testing"
)

func TestEventWriterStructuredOutput(t *testing.T) {
	var got []byte
	w := NewEventWriter(func(b []byte) error { got = append(got, b...); return nil }, 1000)
	if err := w.Emit("begin", map[string]any{"run": "r"}); err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 {
		t.Fatal("event not emitted")
	}
}
