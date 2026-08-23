package sync

import (
	"encoding/json"
	"fmt"
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

// Encode serializes the frame.
func (f Frame) Encode() ([]byte, error) { return json.Marshal(map[string]json.RawMessage(f)) }

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
