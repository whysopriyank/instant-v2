// Package corpus implements the deterministic scenario replay engine that proves
// v2 is byte-compatible with v1. See docs/05-conformance.md.
package corpus

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Step is one line of a corpus NDJSON file.
type Step struct {
	Dir string          `json:"dir"` // "c2s" | "s2c" | "meta"
	Raw json.RawMessage `json:"raw"` // bound by Dir (frame bytes or meta envelope)
}

// Meta is the header line (dir=="meta") of a scenario.
type Meta struct {
	Suite       string            `json:"suite"`
	SDKVersion  string            `json:"sdkVersion,omitempty"`
	SeedFixture string            `json:"seedFixture,omitempty"`
	FeatureBits map[string]bool   `json:"featureGates,omitempty"`
	Expect      map[string]string `json:"expect,omitempty"`
}

// Scenario is the decoded form of a single corpus *.ndjson file.
type Scenario struct {
	Meta    Meta
	Steps   []Step
	File    string // source path, for diagnostics only
}

// CanonicalBytes sorts JSON object keys recursively and lowercases any string
// that looks like a UUID so that client- and server-produced hex compare stable.
func CanonicalBytes(b []byte) ([]byte, error) {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, fmt.Errorf("canonical: %w", err)
	}
	norm := canonicalizeValue(v)
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

func canonicalizeValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make(map[string]any, len(x))
		for _, k := range keys {
			out[k] = canonicalizeValue(x[k])
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = canonicalizeValue(e)
		}
		return out
	case string:
		if isUUIDish(x) {
			return strings.ToLower(x)
		}
		return x
	default:
		return v
	}
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
