package benchharness

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
)

func numericInt(v any) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), n == float64(int(n))
	case float32:
		return int(n), n == float32(int(n))
	case int:
		return n, true
	case int64:
		return int(n), int64(int(n)) == n
	case json.Number:
		i, err := strconv.ParseInt(string(n), 10, 64)
		return int(i), err == nil && int64(int(i)) == i
	default:
		return 0, false
	}
}

func rawValue(raw json.RawMessage) (any, error) {
	var out any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&out); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("expected one JSON value")
	}
	return normalizeJSONNumber(out), nil
}

func normalizeJSONNumber(v any) any {
	switch x := v.(type) {
	case json.Number:
		return normalizeNumber(x)
	case []any:
		for i := range x {
			x[i] = normalizeJSONNumber(x[i])
		}
	case map[string]any:
		for k := range x {
			x[k] = normalizeJSONNumber(x[k])
		}
	}
	return v
}

// normalizeNumber retains the established float64 semantics for safe integers
// and fractions. Large integers use exact JSON numbers instead, including
// equivalent integral decimal/exponent spellings within float64's finite range.
// Signed/unsigned 64-bit integers are covered; extreme overflow such as 1e400
// retains its old fallback and is not equivalent to an expanded integer token.
// Exact numbers never pass through float64 again during hashing/query matching.
func normalizeNumber(number json.Number) any {
	const maxExactInteger = 1 << 53
	text := string(number)
	if integer, err := strconv.ParseInt(text, 10, 64); err == nil {
		if integer >= -maxExactInteger && integer <= maxExactInteger {
			return float64(integer)
		}
		return number
	}
	if !strings.ContainsAny(text, ".eE") {
		// Integer tokens outside int64 still have an exact JSON representation.
		return number
	}
	value, err := strconv.ParseFloat(text, 64)
	if err != nil {
		// Preserve the existing fallback for out-of-range fractional/exponent
		// values; this boundary does not introduce a new noninteger policy.
		return text
	}
	if value >= maxExactInteger || value <= -maxExactInteger {
		if exact, ok := new(big.Rat).SetString(text); ok && exact.IsInt() {
			return json.Number(exact.Num().String())
		}
	}
	return value
}
