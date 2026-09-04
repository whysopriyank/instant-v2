package corpus

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	Meta  Meta
	Steps []Step
	File  string // source path, for diagnostics only
}

// LoadScenario decodes a single NDJSON scenario file.
// Each non-empty line must be a JSON object with a "dir" field ("meta"|"c2s"|"s2c")
// and a "raw" rawMessage already unmarshaled by the scanner's secondary decode.
func LoadScenario(path string) (*Scenario, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	sc := &Scenario{File: path}
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	lineno := 0
	for s.Scan() {
		lineno++
		raw := s.Bytes()
		if len(bytes.TrimSpace(raw)) == 0 {
			continue
		}
		var outer struct {
			Dir string          `json:"dir"`
			Raw json.RawMessage `json:"raw"`
		}
		if err := json.Unmarshal(raw, &outer); err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, lineno, err)
		}
		switch outer.Dir {
		case "meta":
			if len(sc.Steps) != 0 {
				return nil, fmt.Errorf("%s:%d: meta must occur exactly once, before frames", path, lineno)
			}
			var m Meta
			if err := json.Unmarshal(outer.Raw, &m); err != nil {
				return nil, fmt.Errorf("%s:%d meta: %w", path, lineno, err)
			}
			sc.Meta = m
			if strings.TrimSpace(m.Suite) == "" {
				return nil, fmt.Errorf("%s:%d: meta requires a suite", path, lineno)
			}
			sc.Steps = append(sc.Steps, Step{Dir: "meta", Raw: outer.Raw})
		case "c2s", "s2c":
			if len(sc.Steps) == 0 {
				return nil, fmt.Errorf("%s:%d: missing initial meta", path, lineno)
			}
			// Validate it is JSON without converting numbers through float64. The
			// raw message is retained verbatim in the scenario for evidence.
			v, err := decodeJSONValue(outer.Raw)
			if err != nil {
				return nil, fmt.Errorf("%s:%d %s: %w", path, lineno, outer.Dir, err)
			}
			if outer.Dir == "s2c" {
				frame, ok := v.(map[string]any)
				op, _ := frame["op"].(string)
				if !ok || op == "" {
					return nil, fmt.Errorf("%s:%d: expected server frame requires an op", path, lineno)
				}
			}
			sc.Steps = append(sc.Steps, Step{Dir: outer.Dir, Raw: outer.Raw})
		default:
			return nil, fmt.Errorf("%s:%d: dir %q must be one of meta/c2s/s2c", path, lineno, outer.Dir)
		}
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	if len(sc.C2S()) == 0 || len(sc.ExpectedS2C()) == 0 {
		return nil, fmt.Errorf("%s: scenario requires client and expected server frames", path)
	}
	return sc, nil
}

// decodeJSONValue validates one JSON value while preserving json.Number for
// the exact numeric domain used by canonicalization. It also rejects trailing
// values so a frame cannot hide a second JSON value after a valid one.
func decodeJSONValue(raw []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var value any
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("expected exactly one JSON value")
		}
		return nil, err
	}
	return value, nil
}

// LoadCorpus loads every *.ndjson under dir (recursively), optionally filtering by
// `suite` substring (empty means all). Files are returned sorted.
func LoadCorpus(dir, suiteFilter string) ([]*Scenario, error) {
	var paths []string
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".ndjson" {
			return nil
		}
		paths = append(paths, path)
		return nil
	}); err != nil {
		return nil, err
	}
	var out []*Scenario
	for _, p := range paths {
		sc, err := LoadScenario(p)
		if err != nil {
			return nil, err
		}
		if suiteFilter != "" && !strings.Contains(sc.Meta.Suite, suiteFilter) && !strings.Contains(filepath.Base(p), suiteFilter) {
			continue
		}
		out = append(out, sc)
	}
	return out, nil
}

// C2S returns all client-to-server frames in wire order.
func (sc *Scenario) C2S() []json.RawMessage {
	var out []json.RawMessage
	for _, s := range sc.Steps {
		if s.Dir == "c2s" {
			out = append(out, s.Raw)
		}
	}
	return out
}

// ExpectedS2C returns all server-to-client golden frames.
func (sc *Scenario) ExpectedS2C() []json.RawMessage {
	var out []json.RawMessage
	for _, s := range sc.Steps {
		if s.Dir == "s2c" {
			out = append(out, s.Raw)
		}
	}
	return out
}
