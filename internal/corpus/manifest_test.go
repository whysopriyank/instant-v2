package corpus

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCorpusManifest(t *testing.T) {
	first, err := ValidateCorpus("../../corpus")
	if err != nil {
		t.Fatal(err)
	}
	second, err := ValidateCorpus("../../corpus")
	if err != nil || first != second {
		t.Fatal("validation report is not deterministic")
	}
	if !strings.Contains(first, "v1-capture=0") {
		t.Fatal("authored corpus must not claim recorded v1 oracle evidence")
	}
	if !strings.Contains(first, "coverage=26") {
		t.Fatalf("coverage matrix missing from validation report: %s", first)
	}
}

func manifestFixture(t *testing.T) (string, *Manifest) {
	t.Helper()
	dir := t.TempDir()
	writeScenario(t, dir, "test.ndjson", strings.Replace(validScenario, `"suite":"test"`, `"suite":"test","seedFixture":"empty"`, 1))
	writeScenario(t, dir, "fixtures/empty.json", `{"appId":"22222222-2222-4222-8222-222222222222","creatorId":"11111111-1111-4111-8111-111111111111","txSteps":[]}`)
	m := &Manifest{Version: 1, V1Ref: "a4d2ef33b60f281a437191006e4541d4780f9e4a", Fixtures: []FixtureEntry{{ID: "empty", Path: "fixtures/empty.json"}}, Surfaces: []Surface{{ID: "init", Status: "covered", Note: "initialization"}}, Scenarios: []ScenarioEntry{{ID: "test", Path: "test.ndjson", Owner: "corpus", Fixture: "empty", Normalization: "canonical-v1", Ordering: "ordered-frames", Oracle: Oracle{Kind: "regression", Source: "authored"}, Surfaces: []string{"init"}}}}
	return dir, m
}

func saveManifest(t *testing.T, dir string, m *Manifest) {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestManifestRejectsInvalidInventory(t *testing.T) {
	for name, mutate := range map[string]func(string, *Manifest){
		"duplicate-id":      func(_ string, m *Manifest) { m.Scenarios = append(m.Scenarios, m.Scenarios[0]) },
		"missing-file":      func(_ string, m *Manifest) { m.Scenarios[0].Path = "missing.ndjson" },
		"unregistered-file": func(dir string, _ *Manifest) { writeScenario(t, dir, "extra.ndjson", validScenario) },
		"unknown-fixture":   func(_ string, m *Manifest) { m.Scenarios[0].Fixture = "missing" },
		"unknown-coverage":  func(_ string, m *Manifest) { m.Scenarios[0].Surfaces = []string{"missing"} },
		"false-coverage": func(_ string, m *Manifest) {
			m.Surfaces = append(m.Surfaces, Surface{ID: "uncovered", Status: "covered", Note: "must fail"})
		},
		"unknown-oracle": func(_ string, m *Manifest) { m.Scenarios[0].Oracle.Kind = "v1" },
		"fabricated-v1": func(_ string, m *Manifest) {
			m.Scenarios[0].Oracle = Oracle{Kind: "v1-capture", Source: "claimed capture", Ref: m.V1Ref, Evidence: "missing.json"}
		},
		"escape":        func(_ string, m *Manifest) { m.Fixtures[0].Path = "../outside.json" },
		"wrong-pin":     func(_ string, m *Manifest) { m.V1Ref = "main" },
		"normalization": func(_ string, m *Manifest) { m.Scenarios[0].Normalization = "ignore-everything" },
	} {
		t.Run(name, func(t *testing.T) {
			dir, m := manifestFixture(t)
			mutate(dir, m)
			saveManifest(t, dir, m)
			if _, err := ValidateCorpus(dir); err == nil {
				t.Fatal("invalid manifest accepted")
			}
		})
	}
}

func TestManifestCaptureMatchesActualExpectedFrames(t *testing.T) {
	dir, m := manifestFixture(t)
	m.Scenarios[0].Oracle = Oracle{Kind: "v1-capture", Source: "test-only synthetic evidence; not repository v1 provenance", Ref: m.V1Ref, Evidence: "capture.json"}
	writeScenario(t, dir, "capture.json", `{"v1Ref":"`+m.V1Ref+`","raw":[{"op":"wrong"}]}`)
	saveManifest(t, dir, m)
	if _, err := ValidateCorpus(dir); err == nil || !strings.Contains(err.Error(), "differs from expected") {
		t.Fatalf("mismatched capture: %v", err)
	}
	writeScenario(t, dir, "capture.json", `{"v1Ref":"`+m.V1Ref+`","raw":[{"op":"init-ok"}]}`)
	if _, err := ValidateCorpus(dir); err != nil {
		t.Fatal(err)
	}
}

func TestCoverageMatrixRequiresEvidenceForCoveredRows(t *testing.T) {
	dir, m := manifestFixture(t)
	m.Coverage = []CoverageEntry{{
		ID:            "init-positive",
		Family:        "auth",
		Case:          "positive",
		Surface:       "init",
		Transport:     "ws",
		Scenario:      "test",
		Fixture:       "empty",
		Owner:         "corpus",
		ExpectedState: "session initialized",
		Status:        "covered",
		Oracle:        Oracle{Kind: "regression", Source: "authored"},
		Evidence:      []string{"test.ndjson"},
		Note:          "test row",
	}}
	saveManifest(t, dir, m)
	if _, err := ValidateCorpus(dir); err != nil {
		t.Fatal(err)
	}
	m.Coverage[0].Evidence = nil
	saveManifest(t, dir, m)
	if _, err := ValidateCorpus(dir); err == nil || !strings.Contains(err.Error(), "requires evidence") {
		t.Fatalf("covered row without evidence accepted: %v", err)
	}
}

func TestCoverageEvidenceBindsScenarioTransportAndStatus(t *testing.T) {
	dir, m := manifestFixture(t)
	writeScenario(t, dir, "capture.json", `{"scenario":"init-positive","id":"init-positive","transport":"http","status":"covered","fixture":"empty"}`)
	m.Coverage = []CoverageEntry{{
		ID:            "init-positive",
		Family:        "auth",
		Case:          "positive",
		Surface:       "init",
		Transport:     "http",
		Scenario:      "init-positive",
		Fixture:       "empty",
		Owner:         "corpus",
		ExpectedState: "session initialized",
		Status:        "covered",
		Oracle:        Oracle{Kind: "regression", Source: "authored"},
		Evidence:      []string{"capture.json"},
		Note:          "test row",
	}}
	saveManifest(t, dir, m)
	if _, err := ValidateCorpus(dir); err != nil {
		t.Fatal(err)
	}
	writeScenario(t, dir, "capture.json", `{"scenario":"other","id":"init-positive","transport":"http","status":"covered","fixture":"empty"}`)
	if _, err := ValidateCorpus(dir); err == nil || !strings.Contains(err.Error(), "declared scenario") {
		t.Fatalf("mismatched evidence scenario accepted: %v", err)
	}
	writeScenario(t, dir, "capture.json", `{"scenario":"init-positive","id":"init-positive","transport":"http","status":"gap","fixture":"empty"}`)
	if _, err := ValidateCorpus(dir); err == nil || !strings.Contains(err.Error(), "transport/status") {
		t.Fatalf("mismatched evidence status accepted: %v", err)
	}
	writeScenario(t, dir, "capture.json", `{"scenario":"init-positive","id":"init-positive","transport":"http","status":"covered","fixture":"empty"}`)
	m.Coverage[0].Transport = "sse"
	saveManifest(t, dir, m)
	if _, err := ValidateCorpus(dir); err == nil || !strings.Contains(err.Error(), "transport/status") {
		t.Fatalf("mismatched evidence transport accepted: %v", err)
	}
}

// TestCoverageHTTPRequiresSelfBinding proves transport (.json) evidence is
// honestly self-bound to its claiming coverage entry: a wrong envelope id, a
// wrong envelope fixture, a borrowed WS scenario id, and a WS row self-binding
// a .json envelope are each rejected, while the self-bound identity validates.
func TestCoverageHTTPRequiresSelfBinding(t *testing.T) {
	selfBound := func(t *testing.T) (string, *Manifest) {
		t.Helper()
		dir, m := manifestFixture(t)
		writeScenario(t, dir, "capture.json", `{"scenario":"http-case","id":"http-case","transport":"http","status":"covered","fixture":"empty"}`)
		m.Coverage = []CoverageEntry{{
			ID:            "http-case",
			Family:        "auth",
			Case:          "denied-error",
			Surface:       "init",
			Transport:     "http",
			Scenario:      "http-case",
			Fixture:       "empty",
			Owner:         "corpus",
			ExpectedState: "denial is client-safe",
			Status:        "covered",
			Oracle:        Oracle{Kind: "regression", Source: "authored"},
			Evidence:      []string{"capture.json"},
			Note:          "test row",
		}}
		saveManifest(t, dir, m)
		return dir, m
	}
	t.Run("self-bound-accepts", func(t *testing.T) {
		dir, _ := selfBound(t)
		if _, err := ValidateCorpus(dir); err != nil {
			t.Fatalf("self-bound http evidence rejected: %v", err)
		}
	})
	for name, tc := range map[string]struct {
		mutate func(t *testing.T, dir string)
		want   string
	}{
		"wrong-id": {
			mutate: func(t *testing.T, dir string) {
				t.Helper()
				writeScenario(t, dir, "capture.json", `{"scenario":"http-case","id":"other-case","transport":"http","status":"covered","fixture":"empty"}`)
			},
			want: "declared id",
		},
		"wrong-fixture": {
			mutate: func(t *testing.T, dir string) {
				t.Helper()
				writeScenario(t, dir, "capture.json", `{"scenario":"http-case","id":"http-case","transport":"http","status":"covered","fixture":"other"}`)
			},
			want: "declared fixture",
		},
		"borrowed-ws-scenario": {
			mutate: func(t *testing.T, dir string) {
				t.Helper()
				// "test" is the registered WS NDJSON scenario in the
				// fixture; an http row borrowing it (with an otherwise
				// correct id/fixture envelope) is false attribution.
				b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
				if err != nil {
					t.Fatal(err)
				}
				var m Manifest
				if err := json.Unmarshal(b, &m); err != nil {
					t.Fatal(err)
				}
				m.Coverage[0].Scenario = "test"
				saveManifest(t, dir, &m)
				writeScenario(t, dir, "capture.json", `{"scenario":"test","id":"http-case","transport":"http","status":"covered","fixture":"empty"}`)
			},
			want: "self-bind",
		},
		"ws-self-bind-rejected": {
			mutate: func(t *testing.T, dir string) {
				t.Helper()
				// A covered WS row must name a registered ScenarioEntry;
				// self-binding a handcrafted .json envelope with
				// transport ws bypasses the WS scenario/NDJSON path.
				b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
				if err != nil {
					t.Fatal(err)
				}
				var m Manifest
				if err := json.Unmarshal(b, &m); err != nil {
					t.Fatal(err)
				}
				m.Coverage[0].Transport = "ws"
				saveManifest(t, dir, &m)
				writeScenario(t, dir, "capture.json", `{"scenario":"http-case","id":"http-case","transport":"ws","status":"covered","fixture":"empty"}`)
			},
			want: "unknown scenario",
		},
	} {
		t.Run(name, func(t *testing.T) {
			dir, _ := selfBound(t)
			tc.mutate(t, dir)
			if _, err := ValidateCorpus(dir); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("%s accepted: %v", name, err)
			}
		})
	}
}
