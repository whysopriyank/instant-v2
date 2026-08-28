package triple

// EncodeValues must be byte-identical to per-value EncodeValue — the batched
// write path swapped per-value Buffer+Encoder churn for one shared encoder
// (audit backlog), and the storage contract (HTML escaping off, newline
// trimmed, nil → 'null') rides on that parity.

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestEncodeValuesParity(t *testing.T) {
	values := []any{
		nil,
		"plain",
		"html <script> & \"quotes\"",
		int64(42),
		3.14,
		true,
		[]any{"a", int64(1), nil},
		map[string]any{"b": int64(2), "a": "x"},
		map[string]any{"nested": map[string]any{"deep": []any{map[string]any{"k": false}}}},
		"",
	}
	got, err := EncodeValues(values)
	if err != nil {
		t.Fatalf("EncodeValues: %v", err)
	}
	for i, v := range values {
		want, err := EncodeValue(v)
		if err != nil {
			t.Fatalf("EncodeValue(%v): %v", v, err)
		}
		if !bytes.Equal(got[i], want) {
			t.Fatalf("value %d (%v): EncodeValues=%s EncodeValue=%s", i, v, got[i], want)
		}
	}
	// nil renders as the JSON 'null' literal — storage binds value_md5 to
	// md5('null'::text) == JSONNullMD5.
	if string(got[0]) != "null" {
		t.Fatalf("nil encoded as %q, want null", got[0])
	}
	// HTML escaping stays off (cheshire parity).
	if bytes.Contains(got[2], []byte(`\u003c`)) {
		t.Fatalf("HTML escaping leaked into %s", got[2])
	}
}

func TestEncodeValuesRejectsUnsupported(t *testing.T) {
	if _, err := EncodeValues([]any{"ok", struct{ X int }{1}}); err == nil {
		t.Fatal("unsupported type admitted")
	}
}

// EncodeValues output must remain valid, re-decodable JSON of the same shape.
func TestEncodeValuesRoundTrip(t *testing.T) {
	in := map[string]any{"k": []any{int64(1), "two", nil}}
	b, err := EncodeValues([]any{in})
	if err != nil {
		t.Fatal(err)
	}
	var back map[string]any
	if err := json.Unmarshal(b[0], &back); err != nil {
		t.Fatalf("round trip: %v (%s)", err, b[0])
	}
	arr, ok := back["k"].([]any)
	if !ok || len(arr) != 3 {
		t.Fatalf("shape lost: %v", back)
	}
}
