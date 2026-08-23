package corpus

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

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
		if len(bytesTrimSpace(raw)) == 0 {
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
			var m Meta
			if err := json.Unmarshal(outer.Raw, &m); err != nil {
				return nil, fmt.Errorf("%s:%d meta: %w", path, lineno, err)
			}
			sc.Meta = m
			sc.Steps = append(sc.Steps, Step{Dir: "meta", Raw: outer.Raw})
		case "c2s", "s2c":
			// Validate it is JSON.
			var v any
			if err := json.Unmarshal(outer.Raw, &v); err != nil {
				return nil, fmt.Errorf("%s:%d %s: %w", path, lineno, outer.Dir, err)
			}
			sc.Steps = append(sc.Steps, Step{Dir: outer.Dir, Raw: outer.Raw})
		default:
			return nil, fmt.Errorf("%s:%d: dir %q must be one of meta/c2s/s2c", path, lineno, outer.Dir)
		}
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return sc, nil
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
		if suiteFilter != "" && !containsSuite(path, suiteFilter) {
			// also check Meta.Suite post-load, but cheap path filter prunes walk
			_ = suiteFilter
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

func containsSuite(path, filter string) bool {
	base := filepath.Base(path)
	return strings.Contains(base, filter)
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

func bytesTrimSpace(b []byte) []byte {
	start, end := 0, len(b)
	for start < end && (b[start] == ' ' || b[start] == '\n' || b[start] == '\r' || b[start] == '\t') {
		start++
	}
	for end > start && (b[end-1] == ' ' || b[end-1] == '\n' || b[end-1] == '\r' || b[end-1] == '\t') {
		end--
	}
	return b[start:end]
}
