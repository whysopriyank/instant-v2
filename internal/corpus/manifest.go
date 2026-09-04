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
	Coverage  []CoverageEntry `json:"coverage,omitempty"`
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

// CoverageEntry is the claim ledger for one behavior case. Unlike a surface,
// which is a coarse inventory label, an entry records the case shape, actual
// transport, fixture owner, expected state, and evidence/oracle status.
type CoverageEntry struct {
	ID                 string   `json:"id"`
	Family             string   `json:"family"`
	Case               string   `json:"case"`
	Surface            string   `json:"surface"`
	Transport          string   `json:"transport"`
	Scenario           string   `json:"scenario,omitempty"`
	Fixture            string   `json:"fixture,omitempty"`
	Owner              string   `json:"owner"`
	ExpectedState      string   `json:"expectedState"`
	Status             string   `json:"status"` // covered | gap | unsupported
	Oracle             Oracle   `json:"oracle"`
	Evidence           []string `json:"evidence,omitempty"`
	AcceptedDifference string   `json:"acceptedDifference,omitempty"`
	Note               string   `json:"note"`
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
	if len(m.Coverage) != 0 {
		if err := validateCoverage(dir, m, surfaces); err != nil {
			return "", err
		}
		lines = append(lines, fmt.Sprintf("coverage matrix: %d entries", len(m.Coverage)))
	}
	sort.Strings(lines)
	return fmt.Sprintf("validated %d scenarios; coverage=%d; spec=%d regression=%d v1-capture=%d\n%s\n", len(scenarios), len(m.Coverage), oracles["spec"], oracles["regression"], oracles["v1-capture"], strings.Join(lines, "\n")), nil
}

var coverageFamilies = map[string]bool{
	"auth": true, "permissions": true, "query": true, "refresh": true,
	"rooms": true, "transactions": true,
}

var coverageCases = map[string]bool{
	"positive": true, "denied-error": true, "boundary": true,
	"lifecycle": true, "concurrency": true,
}

var coverageTransports = map[string]bool{"http": true, "sse": true, "ws": true}

func validateCoverage(dir string, m *Manifest, surfaces map[string]Surface) error {
	ids := make(map[string]bool, len(m.Coverage))
	fixtures := make(map[string]bool, len(m.Fixtures))
	scenarios := make(map[string]ScenarioEntry, len(m.Scenarios))
	for _, fixture := range m.Fixtures {
		fixtures[fixture.ID] = true
	}
	for _, scenario := range m.Scenarios {
		scenarios[scenario.ID] = scenario
	}
	for _, entry := range m.Coverage {
		if entry.ID == "" || ids[entry.ID] {
			return fmt.Errorf("coverage: empty or duplicate id %q", entry.ID)
		}
		ids[entry.ID] = true
		if !coverageFamilies[entry.Family] || !coverageCases[entry.Case] || !coverageTransports[entry.Transport] {
			return fmt.Errorf("coverage %s: invalid family/case/transport", entry.ID)
		}
		surface, ok := surfaces[entry.Surface]
		if !ok {
			return fmt.Errorf("coverage %s: unknown surface %q", entry.ID, entry.Surface)
		}
		if entry.Owner == "" || entry.ExpectedState == "" || entry.Note == "" {
			return fmt.Errorf("coverage %s: owner, expectedState and note are required", entry.ID)
		}
		if entry.Status != "covered" && entry.Status != "gap" && entry.Status != "unsupported" {
			return fmt.Errorf("coverage %s: invalid status %q", entry.ID, entry.Status)
		}
		if entry.Fixture != "" && !fixtures[entry.Fixture] {
			return fmt.Errorf("coverage %s: unknown fixture %q", entry.ID, entry.Fixture)
		}
		if entry.Scenario != "" {
			if _, ok := scenarios[entry.Scenario]; !ok {
				return fmt.Errorf("coverage %s: unknown scenario %q", entry.ID, entry.Scenario)
			}
		}
		if entry.Status == "covered" && len(entry.Evidence) == 0 {
			return fmt.Errorf("coverage %s: covered entry requires evidence", entry.ID)
		}
		if len(entry.Evidence) != 0 && entry.Scenario == "" {
			return fmt.Errorf("coverage %s: evidence requires a declared scenario", entry.ID)
		}
		if entry.Status == "unsupported" && surface.Status != "unsupported" {
			return fmt.Errorf("coverage %s: unsupported entry must use unsupported surface", entry.ID)
		}
		if entry.Oracle.Kind != "spec" && entry.Oracle.Kind != "regression" && entry.Oracle.Kind != "v1-capture" {
			return fmt.Errorf("coverage %s: invalid oracle kind %q", entry.ID, entry.Oracle.Kind)
		}
		if entry.Oracle.Source == "" {
			return fmt.Errorf("coverage %s: oracle source is required", entry.ID)
		}
		for _, evidence := range entry.Evidence {
			path, err := containedPath(dir, evidence)
			if err != nil {
				return fmt.Errorf("coverage %s evidence: %w", entry.ID, err)
			}
			if err := validateCoverageEvidence(path, evidence, entry, scenarios[entry.Scenario]); err != nil {
				return fmt.Errorf("coverage %s evidence %s: %w", entry.ID, evidence, err)
			}
		}
	}
	return nil
}

// validateCoverageEvidence binds an evidence file to the coverage row that
// claims it. Legacy WS NDJSON evidence is bound by its declared scenario path;
// transport evidence uses the small metadata envelope written by corpusctl.
func validateCoverageEvidence(path, evidence string, entry CoverageEntry, scenario ScenarioEntry) error {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".ndjson":
		if entry.Transport != "ws" {
			return fmt.Errorf("NDJSON evidence requires ws transport")
		}
		if scenario.Path != evidence {
			return fmt.Errorf("path is not the declared scenario %q", scenario.Path)
		}
		return nil
	case ".json":
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var metadata struct {
			Scenario  string `json:"scenario"`
			ID        string `json:"id"`
			Transport string `json:"transport"`
			Status    string `json:"status"`
		}
		if err := json.Unmarshal(b, &metadata); err != nil {
			return fmt.Errorf("invalid metadata: %w", err)
		}
		declaredScenario := metadata.Scenario
		if declaredScenario == "" {
			declaredScenario = metadata.ID
		}
		if declaredScenario != entry.Scenario {
			return fmt.Errorf("declared scenario %q does not match %q", declaredScenario, entry.Scenario)
		}
		if metadata.Transport != entry.Transport || metadata.Status != entry.Status {
			return fmt.Errorf("declared transport/status %q/%q does not match %q/%q", metadata.Transport, metadata.Status, entry.Transport, entry.Status)
		}
		return nil
	default:
		return fmt.Errorf("unsupported evidence format %q", filepath.Ext(path))
	}
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
