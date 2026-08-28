// Package triple owns the value encoding and triple-row semantics that mirror
// v1's db/model/triple.clj. The physical encoding contract:
//
//   - blob values: native JSON (string/number/bool/object/array); JSON null is
//     stored as SQL NULL with the fixed JSONNullMD5 value_md5.
//   - ref values: a JSON *string* containing the uuid text (verified against PG 17:
//     '(value->>0)::uuid' accepts scalar strings, which is what v1's CHECK uses).
//   - value_md5: computed by Postgres as md5(value::text) — never client-side —
//     except for SQL-NULL values which use JSONNullMD5.
package triple

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// JSONNullMD5 is md5("null") — v1 instant.util.crypt/json-null-md5. Used as
// value_md5 for rows whose value column is SQL NULL.
const JSONNullMD5 = "37a6259cc0c1dae299a7866489dff0bd"

// Triple is [entity, attr, value] with uuid identity. Value may be nil (JSON null).
type Triple struct {
	E [16]byte // entity id
	A [16]byte // attr id
	V any      // nil | string | int64/float64 | bool | map[string]any | []any; refs are uuid strings
}

// EncodeValue renders v the way cheshire's generate-string does for the value
// types Instant allows (strings, numbers, booleans, null, arrays, maps,
// uuid-as-string, date-as-string). Go's encoder matches once HTML escaping is
// disabled and map keys are plain strings.
func EncodeValue(v any) ([]byte, error) {
	if !IsValue(v) {
		return nil, fmt.Errorf("triple: unsupported value type %T", v)
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	out := buf.Bytes()
	if n := len(out); n > 0 && out[n-1] == '\n' {
		out = out[:n-1]
	}
	return out, nil
}

// EncodeValues renders many values with ONE shared encoder and buffer —
// byte-identical to calling EncodeValue per value, but the batched write
// path pays two allocations total instead of two per value (audit backlog:
// large-tx writes allocated a fresh Buffer+Encoder for every triple).
func EncodeValues(vs []any) ([][]byte, error) {
	out := make([][]byte, len(vs))
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	for i, v := range vs {
		if !IsValue(v) {
			return nil, fmt.Errorf("triple: unsupported value type %T", v)
		}
		buf.Reset()
		if err := enc.Encode(v); err != nil {
			return nil, err
		}
		b := buf.Bytes()
		if n := len(b); n > 0 && b[n-1] == '\n' {
			b = b[:n-1]
		}
		out[i] = append(make([]byte, 0, len(b)), b...)
	}
	return out, nil
}

// IsValue mirrors db.model.triple/value?: string, uuid(string), number, nil,
// boolean, sequential, associative. UUIDs and dates arrive as strings from the
// wire, so they are covered by the string case.
func IsValue(v any) bool {
	switch x := v.(type) {
	case nil, string, bool:
		return true
	case int, int32, int64, float32, float64:
		return true
	case []any:
		for _, e := range x {
			if !IsValue(e) {
				return false
			}
		}
		return true
	case map[string]any:
		for _, e := range x {
			if !IsValue(e) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// IsRefValue reports whether s is a plausible stored ref (uuid text). The DB
// CHECK is authoritative at write time; this is the cheap pre-check.
func IsRefValue(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
		} else if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}
