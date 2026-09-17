package sync

import (
	"encoding/json"
	"fmt"
	"sort"
)

// Frame is the wire envelope (map of raw JSON fields) with typed accessors.
type Frame map[string]json.RawMessage

// GetOp implements the op accessor.
func (f Frame) GetOp() (string, error) {
	raw, ok := f["op"]
	if !ok {
		return "", fmt.Errorf("frame missing op")
	}
	var op string
	if err := json.Unmarshal(raw, &op); err != nil {
		return "", err
	}
	return op, nil
}

// String reads a string field.
func (f Frame) String(key string) (string, bool) {
	raw, ok := f[key]
	if !ok {
		return "", false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return s, true
}

// ParseFrame decodes wire bytes.
func ParseFrame(b []byte) (Frame, error) {
	var f Frame
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, err
	}
	if _, err := f.GetOp(); err != nil {
		return nil, err
	}
	return f, nil
}

// Encode serializes the frame without re-walking values. json.Marshal on a
// map[string]RawMessage runs compact() over every value — an O(payload) scan
// per refresh generation on multi-hundred-KB envelopes — even though group
// dispatch already guarantees every value is compact, valid JSON produced by
// json.Marshal or literal RawMessages. Keys are emitted in the same sorted
// order as stdlib, so output is byte-identical for all frames this package
// builds (asserted by TestFrameEncodeParity).
//
// RT-002b/RT-002e: values are validated with json.Valid so an invalid or
// unencodable payload fails here — matching stdlib's Marshal error on bad
// RawMessage — instead of emitting invalid or empty bytes to the wire.
// Callers treat any error as a failed generation: nothing is sent, nothing
// is committed, and the same transaction is retried.
func (f Frame) Encode() ([]byte, error) {
	if f == nil {
		return []byte("null"), nil
	}
	for k, v := range f {
		if !json.Valid(v) {
			return nil, fmt.Errorf("frame: invalid JSON value for key %q", k)
		}
	}
	keys := make([]string, 0, len(f))
	size := 2
	for k, v := range f {
		keys = append(keys, k)
		size += len(k) + len(v) + 4 // quotes, colon, comma slack
	}
	sort.Strings(keys)
	buf := make([]byte, 0, size)
	buf = append(buf, '{')
	for i, k := range keys {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = appendJSONKey(buf, k)
		buf = append(buf, ':')
		buf = append(buf, f[k]...)
	}
	return append(buf, '}'), nil
}

// appendJSONKey quotes a key; plain protocol identifiers skip the escaper.
func appendJSONKey(buf []byte, k string) []byte {
	for i := range len(k) {
		c := k[i]
		if c < 0x20 || c > '~' || c == '"' || c == '\\' || c == '<' || c == '>' || c == '&' {
			b, _ := json.Marshal(k)
			return append(buf, b...)
		}
	}
	buf = append(buf, '"')
	buf = append(buf, k...)
	return append(buf, '"')
}

// computationEntry renders the one-element computations array of a refresh
// frame without re-scanning values: pairs must already be in sorted key
// order (stdlib parity, asserted by TestFrameEncodeParity). All values are
// pre-marshaled by the callers.
func computationEntry(pairs ...[2]json.RawMessage) []byte {
	size := 4
	for _, p := range pairs {
		size += len(p[0]) + len(p[1]) + 8
	}
	buf := make([]byte, 0, size)
	buf = append(buf, '[', '{')
	for i, p := range pairs {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, '"')
		buf = append(buf, p[0]...)
		buf = append(buf, '"', ':')
		buf = append(buf, p[1]...)
	}
	return append(buf, '}', ']')
}

// Sorted computation-entry keys (stdlib map-marshal order).
var (
	keyDelta         = json.RawMessage(`delta`)
	keyInstaqlQuery  = json.RawMessage(`instaql-query`)
	keyInstaqlResult = json.RawMessage(`instaql-result`)
	keyResultMeta    = json.RawMessage(`result-meta`)
)

// ErrFrame builds an error envelope (docs/03 §5).
func ErrFrame(status int, typ, msg string) Frame {
	return Frame{
		"op":      json.RawMessage(`"error"`),
		"status":  json.RawMessage(mustJSON(status)),
		"type":    json.RawMessage(mustJSON(typ)),
		"message": json.RawMessage(mustJSON(msg)),
	}
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func mustRaw(s string) json.RawMessage { return json.RawMessage(s) }
