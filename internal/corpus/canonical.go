// Package corpus implements scenario replay and explicit wire comparisons.
// Only a successful live differential run establishes agreement with v1.
package corpus

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strings"
)

// CanonicalOptions tunes canonicalization per comparison mode.
type CanonicalOptions struct {
	// Differential masks fields the two servers legitimately differ on:
	// attrs arrays (v1 always sends them; v2 skips for skip-attrs clients)
	// and the global tx watermark sequences.
	Differential bool
}

// CanonicalBytes sorts JSON object keys recursively and lowercases any string
// that looks like a UUID so that client- and server-produced hex compare stable.
func CanonicalBytes(b []byte) ([]byte, error) {
	return CanonicalBytesOpts(b, CanonicalOptions{})
}

// CanonicalBytesOpts is CanonicalBytes with mode-dependent masking.
func CanonicalBytesOpts(b []byte, opts CanonicalOptions) ([]byte, error) {
	var v any
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("canonical: %w", err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("canonical: expected exactly one JSON value")
	}
	norm := canonicalizeValue(v, opts, false)
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(norm); err != nil {
		return nil, err
	}
	out := buf.Bytes()
	// json.Encoder appends a trailing newline.
	if len(out) > 0 && out[len(out)-1] == '\n' {
		out = out[:len(out)-1]
	}
	return out, nil
}

// NormalizedSessionID replaces per-session volatile identifiers so goldens
// compare stable across runs (v1 and v2 both mint random session ids).
const NormalizedSessionID = "<session-id>"

// NormalizedTxID masks the global tx watermark (v1 and v2 count independently).
const NormalizedTxID = "<tx-id>"

// Traversal allocates each output container once; field policy is independent
// of traversal. authObject marks only a map reached through an "auth" field.
func canonicalizeValue(v any, opts CanonicalOptions, authObject bool) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, value := range x {
			value, keep := canonicalField(k, value, opts, authObject)
			if keep {
				out[k] = canonicalizeValue(value, opts, k == "auth")
			}
		}
		// encoding/json emits object keys in sorted order.
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = canonicalizeValue(e, opts, false)
		}
		return out
	case string:
		if isUUIDish(x) {
			return strings.ToLower(x)
		}
		return x
	case json.Number:
		return canonicalNumber(x)
	default:
		return v
	}
}

// These are the existing exclusions, not permission to ignore new differences.
// Their recursive scope is retained for compatibility; see corpus/README.md.
func canonicalField(key string, value any, opts CanonicalOptions, authObject bool) (any, bool) {
	switch key {
	case "tx-id", "processed-tx-id":
		return NormalizedTxID, true
	case "session-id":
		if _, ok := value.(string); ok {
			return NormalizedSessionID, true
		}
	}
	if !opts.Differential {
		return value, true
	}
	switch key {
	case "attrs":
		return "<attrs>", true
	case "processed-isn", "isn", "trace-id", "server-hostname", "server-port":
		return nil, false
	case "result-meta":
		if value == nil {
			return map[string]any{}, true
		}
	case "admin?":
		if authObject && value == nil {
			return false, true
		}
	case "app":
		if app, ok := value.(map[string]any); authObject && ok {
			return map[string]any{"id": app["id"]}, true
		}
	}
	return value, true
}

// Exact decimal semantic equality: 1, 1.0 and 1e0 compare equal, while integers
// above 2^53 and arbitrary fractional digits remain distinct. The exponent is
// manipulated symbolically so a huge exponent never allocates a huge decimal.
func canonicalNumber(n json.Number) json.Number {
	s := string(n)
	sign := ""
	if strings.HasPrefix(s, "-") {
		sign, s = "-", s[1:]
	}
	exponent := new(big.Int)
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		exponent.SetString(s[i+1:], 10) // decoder already validated the number
		s = s[:i]
	}
	if i := strings.IndexByte(s, '.'); i >= 0 {
		exponent.Sub(exponent, big.NewInt(int64(len(s)-i-1)))
		s = s[:i] + s[i+1:]
	}
	s = strings.TrimLeft(s, "0")
	if s == "" {
		return "0"
	}
	digits := strings.TrimRight(s, "0")
	exponent.Add(exponent, big.NewInt(int64(len(s)-len(digits))))
	point := new(big.Int).Add(exponent, big.NewInt(int64(len(digits))))
	if point.IsInt64() && point.Int64() > -6 && point.Int64() <= 21 {
		p := int(point.Int64())
		switch {
		case p <= 0:
			s = "0." + strings.Repeat("0", -p) + digits
		case p >= len(digits):
			s = digits + strings.Repeat("0", p-len(digits))
		default:
			s = digits[:p] + "." + digits[p:]
		}
	} else {
		s = digits[:1]
		if len(digits) > 1 {
			s += "." + digits[1:]
		}
		s += "e" + point.Sub(point, big.NewInt(1)).String()
	}
	return json.Number(sign + s)
}

func isUUIDish(s string) bool {
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

// Diff produces a human-readable delta between two canonical frame sequences.
// Returns "" when every expected frame byte-equals its actual counterpart.
func Diff(expected, actual [][]byte) string {
	if len(expected) == 0 && len(actual) == 0 {
		return ""
	}
	if len(expected) == len(actual) {
		allEqual := true
		for i := range expected {
			if !bytes.Equal(expected[i], actual[i]) {
				allEqual = false
				break
			}
		}
		if allEqual {
			return ""
		}
	}
	var b bytes.Buffer
	ne, na := len(expected), len(actual)
	fmt.Fprintf(&b, "frames: expected %d, actual %d\n", ne, na)
	n := ne
	if na < n {
		n = na
	}
	for i := 0; i < n; i++ {
		if !bytes.Equal(expected[i], actual[i]) {
			fmt.Fprintf(&b, "[%d] expected %s\n", i, expected[i])
			fmt.Fprintf(&b, "[%d] actual   %s\n", i, actual[i])
		}
	}
	if ne != na {
		for i := n; i < ne; i++ {
			fmt.Fprintf(&b, "[%d] missing  %s\n", i, expected[i])
		}
		for i := n; i < na; i++ {
			fmt.Fprintf(&b, "[%d] extra    %s\n", i, actual[i])
		}
	}
	return b.String()
}

// RandomClientEventID is the UUIDv4 helper used by scenario authors and by the
// driver when a frame is missing one.
func RandomClientEventID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	hexb := make([]byte, 32)
	hex.Encode(hexb, b[:])
	// dash insertion to make canonical UUID print comparable with v1's hex form.
	return string(hexb[0:8]) + "-" + string(hexb[8:12]) + "-" + string(hexb[12:16]) + "-" + string(hexb[16:20]) + "-" + string(hexb[20:32])
}
