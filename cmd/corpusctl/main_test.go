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

func TestSSEEvidenceIncludesHTTPExchanges(t *testing.T) {
	dir := t.TempDir()
	scenario := corpus.SSEScenario{
		ID: "evidence-sse",
		Connect: corpus.HTTPExchange{
			Request:  corpus.HTTPRequest{Method: "GET", Target: "/stream", Headers: map[string][]string{"Authorization": {"Bearer secret"}}},
			Response: corpus.HTTPResponse{Status: 200, Headers: map[string][]string{"Set-Cookie": {"session=secret"}}},
		},
		Posts: []corpus.HTTPExchange{{
			Request:  corpus.HTTPRequest{Method: "POST", Target: "/refresh", Headers: map[string][]string{"Cookie": {"session=secret"}}},
			Response: corpus.HTTPResponse{Status: 200, Headers: map[string][]string{"X-Trace": {"secret"}}, Body: []byte(`{"ok":true}`)},
		}},
		Expected: []corpus.SSERecord{{Kind: "data", Data: []byte(`{"ok":true}`)}},
	}
	result := corpus.SSEReplayResult{
		Connect: corpus.HTTPExchange{
			Request:  corpus.HTTPRequest{Method: "GET", Target: "/stream", Headers: map[string][]string{"Authorization": {"Bearer actual"}}},
			Response: corpus.HTTPResponse{Status: 200, Headers: map[string][]string{"Set-Cookie": {"session=actual"}}},
		},
		Posts: []corpus.HTTPExchange{{
			Request:  corpus.HTTPRequest{Method: "POST", Target: "/refresh", Headers: map[string][]string{"Cookie": {"session=actual"}}},
			Response: corpus.HTTPResponse{Status: 200, Headers: map[string][]string{"X-Trace": {"actual"}}, Body: []byte(`{"ok":true}`)},
		}},
		Collected: scenario.Expected,
	}
	if err := writeSSEEvidence(options{outputDir: dir, redactHeaders: headerList{"X-Trace"}}, scenario, result); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "evidence-sse.sse.evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]json.RawMessage
	if err := json.Unmarshal(b, &record); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"expectedConnect", "expectedPosts", "actualConnect", "actualPosts"} {
		if len(record[key]) == 0 {
			t.Errorf("SSE evidence omitted %s: %s", key, b)
		}
	}
	var evidence struct {
		ExpectedConnect corpus.HTTPExchange   `json:"expectedConnect"`
		ExpectedPosts   []corpus.HTTPExchange `json:"expectedPosts"`
		ActualConnect   corpus.HTTPExchange   `json:"actualConnect"`
		ActualPosts     []corpus.HTTPExchange `json:"actualPosts"`
		Expected        []corpus.SSERecord    `json:"expected"`
		RedactedHeaders []string              `json:"redactedHeaders"`
	}
	if err := json.Unmarshal(b, &evidence); err != nil {
		t.Fatal(err)
	}
	if got := evidence.ExpectedConnect.Request.Headers.Get("Authorization"); got != "<redacted>" {
		t.Fatalf("default Authorization redaction missing: %q", got)
	}
	if got := evidence.ExpectedConnect.Response.Headers.Get("Set-Cookie"); got != "<redacted>" {
		t.Fatalf("default Set-Cookie redaction missing: %q", got)
	}
	if got := evidence.ExpectedPosts[0].Request.Headers.Get("Cookie"); got != "<redacted>" {
		t.Fatalf("default Cookie redaction missing: %q", got)
	}
	if got := evidence.ExpectedPosts[0].Response.Headers.Get("X-Trace"); got != "<redacted>" {
		t.Fatalf("repeatable header redaction missing: %q", got)
	}
	if got := string(evidence.Expected[0].Normalized); got != `{"ok":true}` {
		t.Fatalf("normalized SSE evidence missing: %q", got)
	}
}

func TestHTTPEvidenceRedactsDefaultAndOverrideHeaders(t *testing.T) {
	dir := t.TempDir()
	exchange := corpus.HTTPExchange{
		Request:  corpus.HTTPRequest{Method: "GET", Target: "/health", Headers: map[string][]string{"Authorization": {"Bearer secret"}, "X-Trace": {"trace-secret"}}},
		Response: corpus.HTTPResponse{Status: 200, Headers: map[string][]string{"Authorization": {"Bearer response-secret"}, "Set-Cookie": {"session=secret"}, "X-Trace": {"trace-secret"}}},
	}
	result := corpus.HTTPReplayResult{Actual: exchange, ExpectedHeader: exchange.Response.Headers, ActualHeader: exchange.Response.Headers, Passed: true}
	if err := writeHTTPEvidence(options{corpusDir: "health.json", outputDir: dir, redactHeaders: headerList{"X-Trace"}}, exchange, result); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "health.json.http.evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	var evidence struct {
		Expected   corpus.HTTPExchange `json:"expected"`
		Normalized struct {
			ExpectedHeaders map[string][]string `json:"expectedHeaders"`
		} `json:"normalized"`
	}
	if err := json.Unmarshal(b, &evidence); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"Authorization", "X-Trace"} {
		if got := evidence.Expected.Request.Headers.Get(name); got != "<redacted>" {
			t.Errorf("request %s was not redacted: %q", name, got)
		}
		if got := evidence.Normalized.ExpectedHeaders[name]; len(got) != 1 || got[0] != "<redacted>" {
			t.Errorf("normalized %s was not redacted: %v", name, got)
		}
	}
	if got := evidence.Expected.Response.Headers.Get("Set-Cookie"); got != "<redacted>" {
		t.Fatalf("response Set-Cookie was not redacted: %q", got)
	}
}

func TestRedactionHeaderConfigIsRepeatable(t *testing.T) {
	var configured headerList
	if err := configured.Set("X-Trace"); err != nil {
		t.Fatal(err)
	}
	if err := configured.Set("X-Debug"); err != nil {
		t.Fatal(err)
	}
	policy := redactionPolicy(options{redactHeaders: configured})
	if len(policy.HeaderNames) != 5 || policy.HeaderNames[3] != "X-Trace" || policy.HeaderNames[4] != "X-Debug" {
		t.Fatalf("repeatable redaction config was not retained: %v", policy.HeaderNames)
	}
}

func TestSSEEvidenceRejectsUnsafeScenarioID(t *testing.T) {
	for _, id := range []string{"../escape", ""} {
		err := writeSSEEvidence(options{outputDir: t.TempDir()}, corpus.SSEScenario{ID: id}, corpus.SSEReplayResult{})
		if err == nil || !strings.Contains(err.Error(), "invalid scenario id") {
			t.Fatalf("unsafe SSE evidence path %q was accepted: %v", id, err)
		}
	}
}
