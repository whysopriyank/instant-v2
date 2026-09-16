package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
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

func TestValidateReleaseCommand(t *testing.T) {
	var out, stderr bytes.Buffer
	if code := run([]string{
		"--mode", "validate-release",
		"--corpus", "../../corpus",
		"--release-envelope", "../../docs/reference/release-envelope.md",
	}, &out, &stderr); code != 0 {
		t.Fatalf("code=%d stderr=%s", code, &stderr)
	}
	if !strings.Contains(out.String(), "coverage=26") {
		t.Fatalf("missing composed validation report: %s", &out)
	}
}

func TestCommandFailuresAreNonzero(t *testing.T) {
	for name, args := range map[string][]string{
		"missing-manifest":              {"--mode", "validate", "--corpus", t.TempDir()},
		"filtered-validation":           {"--mode", "validate", "--corpus", "../../corpus", "--suite", "smoke"},
		"missing-release-envelope":      {"--mode", "validate-release", "--corpus", "../../corpus"},
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

func TestRecordHTTPSuccessAndRetention(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/query" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer secret-auth" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Set-Cookie", "auth-session=secret-session")
		w.Header().Set("X-Custom-Secret", "secret-custom")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"session-id":"11111111-1111-4111-8111-111111111111","tx-id":100,"n":1.0,"ok":true}`)
	}))
	defer srv.Close()

	tempDir := t.TempDir()
	scenarioPath := filepath.Join(tempDir, "query.json")
	scenarioContent := []byte(`{
		"request": {
			"method": "POST",
			"target": "/v1/query",
			"headers": {"Authorization": ["Bearer secret-auth"]},
			"body": "eyJtZXNzYWdlIjoiaGVsbG8ifQ=="
		}
	}`)
	if err := os.WriteFile(scenarioPath, scenarioContent, 0600); err != nil {
		t.Fatal(err)
	}

	outDir := filepath.Join(tempDir, "evidence-out")
	var out, stderr bytes.Buffer
	args := []string{
		"--mode", "record",
		"--transport", "http",
		"--corpus", scenarioPath,
		"--target", srv.URL,
		"--output-dir", outDir,
		"--endpoint-id", "test-endpoint-v2",
		"--source-id", "test-source-rev",
		"--fixture-id", "smoke",
		"--redact-header", "X-Custom-Secret",
	}

	code := run(args, &out, &stderr)
	if code != 0 {
		t.Fatalf("record failed code=%d stderr=%s", code, &stderr)
	}
	if !strings.Contains(out.String(), "PASS query.json (http record)") {
		t.Fatalf("unexpected stdout: %s", &out)
	}

	evidencePath := filepath.Join(outDir, "query.json.http.evidence.json")
	info, err := os.Stat(evidencePath)
	if err != nil {
		t.Fatalf("missing evidence file: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("expected 0600 permissions, got %v", info.Mode().Perm())
	}

	b, err := os.ReadFile(evidencePath)
	if err != nil {
		t.Fatal(err)
	}
	var record struct {
		Mode            string                `json:"mode"`
		Transport       string                `json:"transport"`
		Status          string                `json:"status"`
		Metadata        corpus.RecordMetadata `json:"metadata"`
		TargetURL       string                `json:"targetUrl"`
		RedactedHeaders []string              `json:"redactedHeaders"`
		Raw             corpus.HTTPExchange   `json:"raw"`
		Normalized      struct {
			Body    []byte              `json:"body"`
			Headers map[string][]string `json:"headers"`
		} `json:"normalized"`
	}
	if err := json.Unmarshal(b, &record); err != nil {
		t.Fatalf("unmarshal evidence JSON: %v", err)
	}

	if record.Mode != "record" || record.Transport != "http" || record.Status != "recorded" {
		t.Fatalf("unexpected record header: %#v", record)
	}
	if record.Metadata.EndpointID != "test-endpoint-v2" || record.Metadata.SourceID != "test-source-rev" || record.Metadata.FixtureID != "smoke" || record.Metadata.FixtureReset != "caller-owned" {
		t.Fatalf("unexpected metadata: %#v", record.Metadata)
	}
	if record.TargetURL != srv.URL {
		t.Fatalf("unexpected target url: %s", record.TargetURL)
	}

	// Verify path-scoped redaction
	if got := record.Raw.Request.Headers.Get("Authorization"); got != "<redacted>" {
		t.Fatalf("Authorization not redacted: %q", got)
	}
	if got := record.Raw.Response.Headers.Get("Set-Cookie"); got != "<redacted>" {
		t.Fatalf("Set-Cookie not redacted: %q", got)
	}
	if got := record.Raw.Response.Headers.Get("X-Custom-Secret"); got != "<redacted>" {
		t.Fatalf("X-Custom-Secret not redacted: %q", got)
	}

	// Verify raw and canonical normalized forms
	if !strings.Contains(string(record.Raw.Response.Body), `"tx-id":100`) {
		t.Fatalf("raw body missing unmasked tx-id: %s", record.Raw.Response.Body)
	}
	// In canonical bytes, root tx-id is masked as <tx-id>
	if !strings.Contains(string(record.Normalized.Body), corpus.NormalizedTxID) {
		t.Fatalf("normalized body missing canonical tx-id mask: %s", record.Normalized.Body)
	}
}

func TestRecordSSESuccessAndRetention(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			flusher, _ := w.(http.Flusher)
			_, _ = io.WriteString(w, "data: {\"op\":\"init-ok\",\"session-id\":\"11111111-1111-4111-8111-111111111111\"}\n\n")
			if flusher != nil {
				flusher.Flush()
			}
			_, _ = io.WriteString(w, "data: {\"op\":\"refresh-ok\",\"n\":1.0}\n\n")
			if flusher != nil {
				flusher.Flush()
			}
		case http.MethodPost:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"status":"posted"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	tempDir := t.TempDir()
	scenarioPath := filepath.Join(tempDir, "sse-scenario.json")
	scenarioContent := []byte(`{
		"id": "test-sse-record",
		"fixture": "smoke",
		"connect": {
			"request": {
				"method": "GET",
				"target": "/stream",
				"headers": {"Authorization": ["Bearer sse-secret"]}
			}
		},
		"posts": [
			{
				"request": {
					"method": "POST",
					"target": "/post",
					"headers": {"Authorization": ["Bearer post-secret"]},
					"body": "e30="
				}
			}
		],
		"expected": [
			{"kind": "data"},
			{"kind": "data"}
		]
	}`)
	if err := os.WriteFile(scenarioPath, scenarioContent, 0600); err != nil {
		t.Fatal(err)
	}

	outDir := filepath.Join(tempDir, "sse-out")
	var out, stderr bytes.Buffer
	args := []string{
		"--mode", "record",
		"--transport", "sse",
		"--corpus", scenarioPath,
		"--target", srv.URL,
		"--output-dir", outDir,
		"--endpoint-id", "test-endpoint-v2",
		"--source-id", "test-source-rev",
		"--fixture-id", "smoke",
		"--record-limit", "2",
	}

	code := run(args, &out, &stderr)
	if code != 0 {
		t.Fatalf("sse record failed code=%d stderr=%s", code, &stderr)
	}
	if !strings.Contains(out.String(), "PASS test-sse-record (sse record)") {
		t.Fatalf("unexpected stdout: %s", &out)
	}

	evidencePath := filepath.Join(outDir, "test-sse-record.sse.evidence.json")
	info, err := os.Stat(evidencePath)
	if err != nil {
		t.Fatalf("missing sse evidence file: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("expected 0600 permissions, got %v", info.Mode().Perm())
	}

	b, err := os.ReadFile(evidencePath)
	if err != nil {
		t.Fatal(err)
	}
	var record struct {
		Mode       string                `json:"mode"`
		Transport  string                `json:"transport"`
		Status     string                `json:"status"`
		ID         string                `json:"id"`
		Metadata   corpus.RecordMetadata `json:"metadata"`
		RawConnect corpus.HTTPExchange   `json:"rawConnect"`
		RawPosts   []corpus.HTTPExchange `json:"rawPosts"`
		RawRecords []corpus.SSERecord    `json:"rawRecords"`
		Normalized struct {
			Records []corpus.SSERecord `json:"records"`
		} `json:"normalized"`
	}
	if err := json.Unmarshal(b, &record); err != nil {
		t.Fatalf("unmarshal sse evidence JSON: %v", err)
	}

	if record.Mode != "record" || record.Transport != "sse" || record.Status != "recorded" || record.ID != "test-sse-record" {
		t.Fatalf("unexpected sse record header: %#v", record)
	}
	if record.Metadata.FixtureReset != "caller-owned" {
		t.Fatalf("expected fixtureReset caller-owned: %v", record.Metadata.FixtureReset)
	}
	if got := record.RawConnect.Request.Headers.Get("Authorization"); got != "<redacted>" {
		t.Fatalf("connect Authorization not redacted: %q", got)
	}
	if len(record.RawPosts) != 1 || record.RawPosts[0].Request.Headers.Get("Authorization") != "<redacted>" {
		t.Fatalf("post Authorization not redacted: %#v", record.RawPosts)
	}
	if len(record.RawRecords) != 2 || len(record.Normalized.Records) != 2 {
		t.Fatalf("unexpected record counts: raw=%d norm=%d", len(record.RawRecords), len(record.Normalized.Records))
	}

	// Verify raw and canonical normalized forms for SSE
	if !strings.Contains(string(record.RawRecords[0].Raw), "session-id") {
		t.Fatalf("raw record omitted framing: %s", record.RawRecords[0].Raw)
	}
	if !strings.Contains(string(record.Normalized.Records[0].Normalized), corpus.NormalizedSessionID) {
		t.Fatalf("normalized sse record missing canonical session-id: %s", record.Normalized.Records[0].Normalized)
	}
}

func TestRecordMissingRequiredMetadataFails(t *testing.T) {
	tempDir := t.TempDir()
	scenarioPath := filepath.Join(tempDir, "req.json")
	if err := os.WriteFile(scenarioPath, []byte(`{"request":{"method":"GET","target":"/test"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	freshDir := filepath.Join(tempDir, "fresh")

	cases := map[string][]string{
		"missing-target": {
			"--mode", "record", "--transport", "http", "--corpus", scenarioPath,
			"--output-dir", freshDir, "--endpoint-id", "ep", "--source-id", "src", "--fixture-id", "fix",
		},
		"missing-output-dir": {
			"--mode", "record", "--transport", "http", "--corpus", scenarioPath,
			"--target", "http://localhost", "--endpoint-id", "ep", "--source-id", "src", "--fixture-id", "fix",
		},
		"missing-endpoint-id": {
			"--mode", "record", "--transport", "http", "--corpus", scenarioPath,
			"--target", "http://localhost", "--output-dir", freshDir, "--source-id", "src", "--fixture-id", "fix",
		},
		"missing-source-id": {
			"--mode", "record", "--transport", "http", "--corpus", scenarioPath,
			"--target", "http://localhost", "--output-dir", freshDir, "--endpoint-id", "ep", "--fixture-id", "fix",
		},
		"missing-fixture-id": {
			"--mode", "record", "--transport", "http", "--corpus", scenarioPath,
			"--target", "http://localhost", "--output-dir", freshDir, "--endpoint-id", "ep", "--source-id", "src",
		},
	}

	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			var out, stderr bytes.Buffer
			code := run(args, &out, &stderr)
			if code == 0 {
				t.Fatalf("expected failure for %s, got 0", name)
			}
			if stderr.Len() == 0 {
				t.Fatalf("expected diagnostic message for %s", name)
			}
		})
	}
}

func TestRecordWSUnsupported(t *testing.T) {
	cases := map[string][]string{
		"explicit-ws":   {"--mode", "record", "--transport", "ws"},
		"default-is-ws": {"--mode", "record"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			var out, stderr bytes.Buffer
			code := run(args, &out, &stderr)
			if code == 0 {
				t.Fatal("expected nonzero exit for unsupported WS record")
			}
			if !strings.Contains(stderr.String(), "record mode is unsupported for ws") {
				t.Fatalf("expected unsupported WS message, got: %s", stderr.String())
			}
		})
	}
}

func TestRecordMissingMetadataMakesZeroRequests(t *testing.T) {
	var requestCount int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&requestCount, 1)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer srv.Close()

	tempDir := t.TempDir()
	scenarioPath := filepath.Join(tempDir, "req.json")
	if err := os.WriteFile(scenarioPath, []byte(`{"request":{"method":"GET","target":"/test"}}`), 0600); err != nil {
		t.Fatal(err)
	}

	cases := map[string][]string{
		"missing-endpoint-id": {
			"--mode", "record", "--transport", "http", "--corpus", scenarioPath,
			"--target", srv.URL, "--output-dir", filepath.Join(tempDir, "out1"), "--source-id", "src", "--fixture-id", "fix",
		},
		"missing-source-id": {
			"--mode", "record", "--transport", "http", "--corpus", scenarioPath,
			"--target", srv.URL, "--output-dir", filepath.Join(tempDir, "out2"), "--endpoint-id", "ep", "--fixture-id", "fix",
		},
		"missing-fixture-id": {
			"--mode", "record", "--transport", "http", "--corpus", scenarioPath,
			"--target", srv.URL, "--output-dir", filepath.Join(tempDir, "out3"), "--endpoint-id", "ep", "--source-id", "src",
		},
		"whitespace-endpoint-id": {
			"--mode", "record", "--transport", "http", "--corpus", scenarioPath,
			"--target", srv.URL, "--output-dir", filepath.Join(tempDir, "out4"), "--endpoint-id", "   ", "--source-id", "src", "--fixture-id", "fix",
		},
		"missing-output-dir": {
			"--mode", "record", "--transport", "http", "--corpus", scenarioPath,
			"--target", srv.URL, "--endpoint-id", "ep", "--source-id", "src", "--fixture-id", "fix",
		},
		"existing-output-dir": {
			"--mode", "record", "--transport", "http", "--corpus", scenarioPath,
			"--target", srv.URL, "--output-dir", tempDir, "--endpoint-id", "ep", "--source-id", "src", "--fixture-id", "fix",
		},
	}

	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			atomic.StoreInt64(&requestCount, 0)
			var out, stderr bytes.Buffer
			code := run(args, &out, &stderr)
			if code == 0 {
				t.Fatalf("expected failure for %s, got 0", name)
			}
			if got := atomic.LoadInt64(&requestCount); got != 0 {
				t.Fatalf("expected 0 HTTP requests for %s, got %d", name, got)
			}
		})
	}
}

func TestRecordOutputFreshnessAndWriteOnce(t *testing.T) {
	tempDir := t.TempDir()
	scenarioPath := filepath.Join(tempDir, "req.json")
	if err := os.WriteFile(scenarioPath, []byte(`{"request":{"method":"GET","target":"/test"}}`), 0600); err != nil {
		t.Fatal(err)
	}

	// 1. Output directory contains existing files: must fail closed as not fresh
	nonFreshDir := filepath.Join(tempDir, "not-fresh")
	if err := os.Mkdir(nonFreshDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nonFreshDir, "existing.txt"), []byte("exists"), 0600); err != nil {
		t.Fatal(err)
	}

	var out, stderr bytes.Buffer
	args := []string{
		"--mode", "record", "--transport", "http", "--corpus", scenarioPath,
		"--target", "http://127.0.0.1:1", "--output-dir", nonFreshDir,
		"--endpoint-id", "ep", "--source-id", "src", "--fixture-id", "fix",
	}
	code := run(args, &out, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "already exists") {
		t.Fatalf("non-fresh output dir was accepted: code=%d stderr=%s", code, &stderr)
	}

	// 2. Existing empty directory: must fail closed
	emptyDir := filepath.Join(tempDir, "empty-dir")
	if err := os.Mkdir(emptyDir, 0700); err != nil {
		t.Fatal(err)
	}
	stderr.Reset()
	args = []string{
		"--mode", "record", "--transport", "http", "--corpus", scenarioPath,
		"--target", "http://127.0.0.1:1", "--output-dir", emptyDir,
		"--endpoint-id", "ep", "--source-id", "src", "--fixture-id", "fix",
	}
	code = run(args, &out, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "already exists") {
		t.Fatalf("empty output dir was accepted: code=%d stderr=%s", code, &stderr)
	}

	// 3. Symlink output directory: must fail closed
	symlinkDir := filepath.Join(tempDir, "symlink-dir")
	if err := os.Symlink(emptyDir, symlinkDir); err != nil {
		t.Fatal(err)
	}
	stderr.Reset()
	args = []string{
		"--mode", "record", "--transport", "http", "--corpus", scenarioPath,
		"--target", "http://127.0.0.1:1", "--output-dir", symlinkDir,
		"--endpoint-id", "ep", "--source-id", "src", "--fixture-id", "fix",
	}
	code = run(args, &out, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "symlink") {
		t.Fatalf("symlink output dir was accepted: code=%d stderr=%s", code, &stderr)
	}

	// 4. Output directory with symlink parent: must fail closed
	childUnderSymlink := filepath.Join(symlinkDir, "child")
	stderr.Reset()
	args = []string{
		"--mode", "record", "--transport", "http", "--corpus", scenarioPath,
		"--target", "http://127.0.0.1:1", "--output-dir", childUnderSymlink,
		"--endpoint-id", "ep", "--source-id", "src", "--fixture-id", "fix",
	}
	code = run(args, &out, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "symlink") {
		t.Fatalf("child under symlink parent was accepted: code=%d stderr=%s", code, &stderr)
	}

	// 5. Unsafe output directory: must fail closed
	stderr.Reset()
	args = []string{
		"--mode", "record", "--transport", "http", "--corpus", scenarioPath,
		"--target", "http://127.0.0.1:1", "--output-dir", "../escape",
		"--endpoint-id", "ep", "--source-id", "src", "--fixture-id", "fix",
	}
	code = run(args, &out, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "output directory escapes") {
		t.Fatalf("unsafe output dir was accepted: code=%d stderr=%s", code, &stderr)
	}

	// 6. Successful record creates reserved dir mode 0700 and writes mode 0600 evidence
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer srv.Close()

	freshDir := filepath.Join(tempDir, "fresh-run")
	out.Reset()
	stderr.Reset()
	args = []string{
		"--mode", "record", "--transport", "http", "--corpus", scenarioPath,
		"--target", srv.URL, "--output-dir", freshDir,
		"--endpoint-id", "ep", "--source-id", "src", "--fixture-id", "fix",
	}
	code = run(args, &out, &stderr)
	if code != 0 {
		t.Fatalf("record failed on fresh dir: code=%d stderr=%s", code, &stderr)
	}

	dirFI, err := os.Stat(freshDir)
	if err != nil {
		t.Fatal(err)
	}
	if dirFI.Mode().Perm() != 0700 {
		t.Fatalf("expected dir mode 0700, got %#o", dirFI.Mode().Perm())
	}

	evFI, err := os.Stat(filepath.Join(freshDir, "req.json.http.evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	if evFI.Mode().Perm() != 0600 {
		t.Fatalf("expected file mode 0600, got %#o", evFI.Mode().Perm())
	}

	// Duplicate run on the now-existing dir must fail closed
	out.Reset()
	stderr.Reset()
	code = run(args, &out, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "already exists") {
		t.Fatalf("duplicate record run on existing dir was accepted: code=%d stderr=%s", code, &stderr)
	}
}

func TestRecordFailurePropagation(t *testing.T) {
	tempDir := t.TempDir()
	scenarioPath := filepath.Join(tempDir, "req.json")
	if err := os.WriteFile(scenarioPath, []byte(`{"request":{"method":"GET","target":"/unreachable"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	outDir := filepath.Join(tempDir, "out")

	// HTTP failure propagation: unreachable target
	var out, stderr bytes.Buffer
	args := []string{
		"--mode", "record", "--transport", "http", "--corpus", scenarioPath,
		"--target", "http://127.0.0.1:1", "--output-dir", outDir,
		"--endpoint-id", "ep", "--source-id", "src", "--fixture-id", "fix",
		"--timeout", "100ms",
	}
	code := run(args, &out, &stderr)
	if code == 0 {
		t.Fatal("unreachable HTTP target reported success")
	}
	if !strings.Contains(stderr.String(), "HTTP capture failed") {
		t.Fatalf("unexpected error message: %s", &stderr)
	}
	// Ensure no evidence file was emitted
	if _, err := os.Stat(filepath.Join(outDir, "req.json.http.evidence.json")); err == nil {
		t.Fatal("evidence file was emitted despite failed capture")
	}

	// SSE failure propagation: incomplete stream
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"op\":\"first\"}\n\n")
		// closes immediately after 1 event
	}))
	defer srv.Close()

	sseScenarioPath := filepath.Join(tempDir, "sse-req.json")
	if err := os.WriteFile(sseScenarioPath, []byte(`{
		"id": "sse-incomplete",
		"connect": {"request": {"method": "GET", "target": "/stream"}},
		"expected": [{"kind": "data"}, {"kind": "data"}]
	}`), 0600); err != nil {
		t.Fatal(err)
	}
	sseOutDir := filepath.Join(tempDir, "sse-out")

	out.Reset()
	stderr.Reset()
	sseArgs := []string{
		"--mode", "record", "--transport", "sse", "--corpus", sseScenarioPath,
		"--target", srv.URL, "--output-dir", sseOutDir,
		"--endpoint-id", "ep", "--source-id", "src", "--fixture-id", "fix",
		"--record-limit", "2",
	}
	code = run(sseArgs, &out, &stderr)
	if code == 0 {
		t.Fatal("incomplete SSE stream reported success")
	}
	if !strings.Contains(stderr.String(), "incomplete SSE stream") {
		t.Fatalf("expected incomplete stream error, got: %s", &stderr)
	}
	// Ensure no evidence file was emitted
	if _, err := os.Stat(filepath.Join(sseOutDir, "sse-incomplete.sse.evidence.json")); err == nil {
		t.Fatal("evidence file was emitted despite incomplete stream")
	}
}
