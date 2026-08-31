// Package httpjson contains shared HTTP JSON mechanics, not response envelopes.
package httpjson

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// DecodeObject reads exactly one non-null JSON object. Numbers retain the
// encoding/json float64 representation used by the HTTP handlers. Callers own
// request limits, field validation, and the error response.
func DecodeObject(r io.Reader) (map[string]any, error) {
	if r == nil {
		return nil, io.EOF
	}
	dec := json.NewDecoder(r)
	var object map[string]any
	if err := dec.Decode(&object); err != nil {
		return nil, err
	}
	if object == nil {
		return nil, errors.New("JSON body must be an object")
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("JSON body must contain exactly one object")
	}
	return object, nil
}

// Write preserves Encoder.Encode's trailing newline. escapeHTML is explicit
// because existing HTTP surfaces have different escaping contracts. Once the
// status is written, callers must not try to replace a failed write response.
func Write(w http.ResponseWriter, status int, value any, escapeHTML bool) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(escapeHTML)
	return enc.Encode(value)
}
