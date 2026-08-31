package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/instant-v2/instant-v2/internal/corpus"
)

func TestValidateCommand(t *testing.T) {
	var out, stderr bytes.Buffer
	if code := run([]string{"--mode", "validate", "--corpus", "../../corpus"}, &out, &stderr); code != 0 {
		t.Fatalf("code=%d stderr=%s", code, &stderr)
	}
	if !strings.Contains(out.String(), "v1-capture=0") {
		t.Fatalf("missing oracle inventory: %s", &out)
	}
}

func TestCommandFailuresAreNonzero(t *testing.T) {
	for name, args := range map[string][]string{
		"missing-manifest":              {"--mode", "validate", "--corpus", t.TempDir()},
		"filtered-validation":           {"--mode", "validate", "--corpus", "../../corpus", "--suite", "smoke"},
		"empty-selection":               {"--mode", "replay", "--target", ":invalid", "--corpus", "../../corpus", "--suite", "not-a-suite"},
		"failed-replay":                 {"--mode", "replay", "--target", ":invalid", "--corpus", "../../corpus/00-smoke.ndjson"},
		"missing-differential-evidence": {"--mode", "differential", "--target", ":invalid", "--other", ":invalid"},
		"unknown-mode":                  {"--mode", "unknown"},
		"record-unavailable":            {"--mode", "record"},
	} {
		t.Run(name, func(t *testing.T) {
			var out, stderr bytes.Buffer
			if code := run(args, &out, &stderr); code == 0 {
				t.Fatalf("failure reported success: %s %s", &out, &stderr)
			}
		})
	}
}

func TestEvidenceRetainsRawAndRefusesOverwrite(t *testing.T) {
	dir := t.TempDir()
	sc := &corpus.Scenario{File: "test.ndjson", Meta: corpus.Meta{Suite: "test", SeedFixture: "smoke"}}
	raw := json.RawMessage(`{"tx-id":9007199254740993}`)
	canonical, err := corpus.CanonicalBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	result := corpus.ReplayResult{RawCollected: []json.RawMessage{raw}, Collected: [][]byte{canonical}}
	o := options{mode: "replay", outputDir: dir}
	if err := writeEvidence(o, sc, result, corpus.ReplayResult{}, ""); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "test.ndjson.evidence.json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var record struct {
		Target frameEvidence `json:"target"`
	}
	if err := json.Unmarshal(b, &record); err != nil {
		t.Fatal(err)
	}
	if len(record.Target.Raw) != 1 || record.Target.Raw[0] != string(raw) || len(record.Target.Normalized) != 1 {
		t.Fatalf("missing raw/normalized evidence: %s", b)
	}
	got, err := corpus.CanonicalBytes(record.Target.Normalized[0])
	if err != nil || !bytes.Equal(got, canonical) {
		t.Fatalf("wrong normalized evidence: %s (%v)", got, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("private permissions: %v %v", info, err)
	}
	if err := writeEvidence(o, sc, result, corpus.ReplayResult{}, ""); err == nil {
		t.Fatal("evidence overwritten")
	}
}

func TestDifferentialCommandFailsForTwoFailedConnections(t *testing.T) {
	// The current Git checkout serves only as a pin-validation test fixture;
	// this test never claims that it is a v1 checkout or a live oracle.
	ref, err := revision(".")
	if err != nil {
		t.Fatal(err)
	}
	dir, outputDir := t.TempDir(), t.TempDir()
	scenario, err := os.ReadFile("../../corpus/00-smoke.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "smoke.ndjson"), scenario, 0600); err != nil {
		t.Fatal(err)
	}
	manifest := []byte(`{"version":1,"v1Ref":"` + ref + `"}`)
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), manifest, 0600); err != nil {
		t.Fatal(err)
	}
	var out, stderr bytes.Buffer
	args := []string{"--mode", "differential", "--corpus", dir, "--target", ":invalid", "--other", ":invalid", "--v1-path", ".", "--output-dir", outputDir}
	if code := run(args, &out, &stderr); code != 1 || !strings.Contains(out.String(), "replay incomplete") || strings.Contains(out.String(), "PASS") {
		t.Fatalf("code=%d out=%s err=%s", code, &out, &stderr)
	}
	if _, err := os.Stat(filepath.Join(outputDir, "smoke.ndjson.evidence.json")); err != nil {
		t.Fatal(err)
	}
}
