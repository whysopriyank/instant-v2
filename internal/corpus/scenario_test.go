package corpus

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeScenario(t *testing.T, dir, name, data string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

const validScenario = `{"dir":"meta","raw":{"suite":"test"}}
{"dir":"c2s","raw":{"op":"init"}}
{"dir":"s2c","raw":{"op":"init-ok"}}
`

func TestScenarioRejectsInvalidStructure(t *testing.T) {
	for name, data := range map[string]string{
		"missing-meta":     strings.SplitN(validScenario, "\n", 2)[1],
		"duplicate-meta":   validScenario + `{"dir":"meta","raw":{"suite":"again"}}`,
		"empty-suite":      strings.Replace(validScenario, `"suite":"test"`, `"suite":""`, 1),
		"non-frame-output": strings.Replace(validScenario, `{"op":"init-ok"}`, `null`, 1),
		"empty":            "",
	} {
		t.Run(name, func(t *testing.T) {
			path := writeScenario(t, t.TempDir(), "test.ndjson", data)
			if _, err := LoadScenario(path); err == nil {
				t.Fatal("invalid scenario accepted")
			}
		})
	}
}

func TestCorpusRecursiveDeterministicAndSuiteFilter(t *testing.T) {
	dir := t.TempDir()
	writeScenario(t, dir, "z.ndjson", validScenario)
	writeScenario(t, dir, "family/a.ndjson", strings.Replace(validScenario, `"test"`, `"nested"`, 1))
	scenarios, err := LoadCorpus(dir, "")
	if err != nil || len(scenarios) != 2 {
		t.Fatalf("load: %v %v", scenarios, err)
	}
	if scenarios[0].Meta.Suite != "nested" {
		t.Fatal("not path sorted")
	}
	filtered, err := LoadCorpus(dir, "nested")
	if err != nil || len(filtered) != 1 || filtered[0].File != scenarios[0].File {
		t.Fatalf("filter: %v %v", filtered, err)
	}
}

func TestScenarioLoadPreservesExactNumberDomain(t *testing.T) {
	const raw = `{"dir":"c2s","raw":{"large":9007199254740993,"fraction":0.123456789012345678901,"huge":1e999999999999999999999,"tiny":-1e-999999999999999999999}}`
	path := writeScenario(t, t.TempDir(), "exact.ndjson", `{"dir":"meta","raw":{"suite":"exact"}}
`+raw+`
{"dir":"s2c","raw":{"op":"ok"}}
`)
	sc, err := LoadScenario(path)
	if err != nil {
		t.Fatalf("exact-number scenario rejected: %v", err)
	}
	if got := string(sc.C2S()[0]); got != `{"large":9007199254740993,"fraction":0.123456789012345678901,"huge":1e999999999999999999999,"tiny":-1e-999999999999999999999}` {
		t.Fatalf("loader changed raw frame: %s", got)
	}
	canonical, err := CanonicalBytes(sc.C2S()[0])
	if err != nil {
		t.Fatalf("canonical exact frame: %v", err)
	}
	if !bytes.Contains(canonical, []byte(`9007199254740993`)) || !bytes.Contains(canonical, []byte(`1e999999999999999999999`)) || !bytes.Contains(canonical, []byte(`-1e-999999999999999999999`)) {
		t.Fatalf("canonicalizer lost exact values: %s", canonical)
	}
}
