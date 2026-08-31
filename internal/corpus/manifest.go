package corpus

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Manifest inventories coverage without turning authored expectations into a
// v1 oracle claim. Version 1 describes WS NDJSON only, not HTTP/SDK recordings.
type Manifest struct {
	Version   int             `json:"version"`
	V1Ref     string          `json:"v1Ref"`
	Fixtures  []FixtureEntry  `json:"fixtures"`
	Surfaces  []Surface       `json:"surfaces"`
	Scenarios []ScenarioEntry `json:"scenarios"`
}

type FixtureEntry struct {
	ID   string `json:"id"`
	Path string `json:"path"`
}

// Fixture describes an isolated app plus actual transactor bootstrap inputs.
// These steps are executed by TestCorpusReplayIntegration, not by LoadCorpus.
type Fixture struct {
	AppID      string            `json:"appId"`
	CreatorID  string            `json:"creatorId"`
	AdminToken string            `json:"adminToken,omitempty"`
	Steps      []json.RawMessage `json:"txSteps"`
	Rules      json.RawMessage   `json:"rules,omitempty"`
}

type Surface struct {
	ID     string `json:"id"`
	Status string `json:"status"` // covered | gap | unsupported
	Note   string `json:"note"`
}

type ScenarioEntry struct {
	ID            string   `json:"id"`
	Path          string   `json:"path"`
	Owner         string   `json:"owner"`
	Fixture       string   `json:"fixture"`
	Normalization string   `json:"normalization"`
	Oracle        Oracle   `json:"oracle"`
	Surfaces      []string `json:"surfaces"`
	Ordering      string   `json:"ordering"`
}

type Oracle struct {
	Kind     string `json:"kind"` // spec | regression | v1-capture
	Source   string `json:"source"`
	Ref      string `json:"ref,omitempty"`
	Evidence string `json:"evidence,omitempty"`
}

// ValidateCorpus requires a bijection between manifest entries and NDJSON
// files, registered fixture profiles, and truthful explicit coverage gaps.
// The report is sorted and contains no timestamps or machine-local paths.
func ValidateCorpus(dir string) (string, error) {
	m, err := LoadManifest(dir)
	if err != nil {
		return "", err
	}
	scenarios, err := LoadCorpus(dir, "")
	if err != nil {
		return "", err
	}
	if len(scenarios) == 0 {
		return "", fmt.Errorf("empty corpus")
	}
	byPath := make(map[string]*Scenario, len(scenarios))
	for _, sc := range scenarios {
		rel, err := filepath.Rel(dir, sc.File)
		if err != nil {
			return "", err
		}
		byPath[filepath.ToSlash(rel)] = sc
	}
	fixtures := map[string]bool{}
	for _, entry := range m.Fixtures {
		if entry.ID == "" || fixtures[entry.ID] {
			return "", fmt.Errorf("empty or duplicate fixture %q", entry.ID)
		}
		if _, err := LoadFixture(dir, entry.Path); err != nil {
			return "", fmt.Errorf("fixture %s: %w", entry.ID, err)
		}
		fixtures[entry.ID] = true
	}
	surfaces := map[string]Surface{}
	for _, surface := range m.Surfaces {
		if surface.ID == "" || surfaces[surface.ID].ID != "" || surface.Note == "" {
			return "", fmt.Errorf("invalid or duplicate surface %q", surface.ID)
		}
		if surface.Status != "covered" && surface.Status != "gap" && surface.Status != "unsupported" {
			return "", fmt.Errorf("surface %s: invalid status", surface.ID)
		}
		surfaces[surface.ID] = surface
	}
	ids, paths, coverage := map[string]bool{}, map[string]bool{}, map[string]int{}
	oracles := map[string]int{}
	var lines []string
	for _, entry := range m.Scenarios {
		if ids[entry.ID] || entry.ID == "" || paths[entry.Path] {
			return "", fmt.Errorf("empty or duplicate scenario %q (%s)", entry.ID, entry.Path)
		}
		ids[entry.ID], paths[entry.Path] = true, true
		if _, err := containedPath(dir, entry.Path); err != nil {
			return "", err
		}
		sc := byPath[entry.Path]
		if sc == nil {
			return "", fmt.Errorf("scenario %s: unlisted or missing file %s", entry.ID, entry.Path)
		}
		if sc.Meta.Suite != entry.ID || entry.Owner == "" || entry.Ordering != "ordered-frames" || entry.Normalization != "canonical-v1" {
			return "", fmt.Errorf("scenario %s: inconsistent suite/owner/ordering/normalization", entry.ID)
		}
		if !fixtures[entry.Fixture] || sc.Meta.SeedFixture != entry.Fixture {
			return "", fmt.Errorf("scenario %s: missing or inconsistent fixture %q", entry.ID, entry.Fixture)
		}
		if err := validateOracle(dir, m.V1Ref, entry.Oracle, sc.ExpectedS2C()); err != nil {
			return "", fmt.Errorf("scenario %s: %w", entry.ID, err)
		}
		if len(entry.Surfaces) == 0 {
			return "", fmt.Errorf("scenario %s: no coverage tags", entry.ID)
		}
		seen := map[string]bool{}
		for _, id := range entry.Surfaces {
			if surfaces[id].Status != "covered" || seen[id] {
				return "", fmt.Errorf("scenario %s: invalid/duplicate coverage tag %q", entry.ID, id)
			}
			seen[id] = true
			coverage[id]++
		}
		oracles[entry.Oracle.Kind]++
		lines = append(lines, fmt.Sprintf("scenario %s: %s; fixture=%s; c2s=%d s2c=%d", entry.ID, entry.Oracle.Kind, entry.Fixture, len(sc.C2S()), len(sc.ExpectedS2C())))
	}
	for path := range byPath {
		if !paths[path] {
			return "", fmt.Errorf("unregistered scenario %s", path)
		}
	}
	for id, surface := range surfaces {
		if surface.Status == "covered" && coverage[id] == 0 {
			return "", fmt.Errorf("covered surface %s has no scenarios", id)
		}
		lines = append(lines, fmt.Sprintf("surface %s: %s (%d scenarios); %s", id, surface.Status, coverage[id], surface.Note))
	}
	sort.Strings(lines)
	return fmt.Sprintf("validated %d scenarios; spec=%d regression=%d v1-capture=%d\n%s\n", len(scenarios), oracles["spec"], oracles["regression"], oracles["v1-capture"], strings.Join(lines, "\n")), nil
}

func LoadManifest(dir string) (*Manifest, error) {
	var m Manifest
	if err := readJSON(filepath.Join(dir, "manifest.json"), &m); err != nil {
		return nil, err
	}
	if m.Version != 1 || !isCommit(m.V1Ref) {
		return nil, fmt.Errorf("manifest requires version 1 and full pinned v1Ref")
	}
	return &m, nil
}

func LoadFixture(dir, path string) (*Fixture, error) {
	full, err := containedPath(dir, path)
	if err != nil {
		return nil, err
	}
	var f Fixture
	if err := readJSON(full, &f); err != nil {
		return nil, err
	}
	if !isUUIDish(f.AppID) || !isUUIDish(f.CreatorID) {
		return nil, fmt.Errorf("fixture requires appId and creatorId UUIDs")
	}
	if f.AdminToken != "" && !isUUIDish(f.AdminToken) {
		return nil, fmt.Errorf("fixture adminToken must be a UUID")
	}
	for i, step := range f.Steps {
		var parts []json.RawMessage
		if err := json.Unmarshal(step, &parts); err != nil || len(parts) == 0 {
			return nil, fmt.Errorf("fixture step %d must be a nonempty array", i)
		}
	}
	return &f, nil
}

func readJSON(path string, dst any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("%s: trailing JSON", path)
	}
	return nil
}

func containedPath(dir, path string) (string, error) {
	if !filepath.IsLocal(path) || filepath.ToSlash(filepath.Clean(path)) != path {
		return "", fmt.Errorf("non-local or noncanonical corpus path %q", path)
	}
	full := filepath.Join(dir, filepath.FromSlash(path))
	resolved, err := filepath.EvalSymlinks(full)
	if err != nil {
		return "", err
	}
	root, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(root, resolved)
	if err != nil || !filepath.IsLocal(rel) {
		return "", fmt.Errorf("corpus path escapes root: %q", path)
	}
	return full, nil
}

func isCommit(ref string) bool {
	b, err := hex.DecodeString(ref)
	return err == nil && len(b) == 20
}

func validateOracle(dir, pin string, o Oracle, expected []json.RawMessage) error {
	if strings.TrimSpace(o.Source) == "" {
		return fmt.Errorf("oracle source is required")
	}
	switch o.Kind {
	case "spec", "regression":
		if o.Ref != "" || o.Evidence != "" {
			return fmt.Errorf("authored oracle must not claim v1 capture evidence")
		}
	case "v1-capture":
		if o.Ref != pin {
			return fmt.Errorf("v1 capture ref must match pinned v1Ref")
		}
		path, err := containedPath(dir, o.Evidence)
		if err != nil {
			return fmt.Errorf("v1 capture evidence: %w", err)
		}
		var capture struct {
			V1Ref string            `json:"v1Ref"`
			Raw   []json.RawMessage `json:"raw"`
		}
		if err := readJSON(path, &capture); err != nil {
			return err
		}
		if capture.V1Ref != pin || len(capture.Raw) == 0 {
			return fmt.Errorf("v1 capture requires pinned raw output")
		}
		if len(capture.Raw) != len(expected) {
			return fmt.Errorf("v1 capture frame count differs from expected outputs")
		}
		for i, raw := range capture.Raw {
			got, err := CanonicalBytes(raw)
			if err != nil {
				return fmt.Errorf("v1 capture frame %d: %w", i, err)
			}
			want, err := CanonicalBytes(expected[i])
			if err != nil {
				return err
			}
			if !bytes.Equal(got, want) {
				return fmt.Errorf("v1 capture frame %d differs from expected output", i)
			}
		}
	default:
		return fmt.Errorf("invalid oracle kind %q", o.Kind)
	}
	return nil
}
