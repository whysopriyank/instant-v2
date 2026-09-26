// Command corpusctl validates and replays the WS corpus. See corpus/README.md.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/instant-v2/instant-v2/internal/corpus"
)

type options struct {
	mode, transport, corpusDir, releaseEnvelope, suite, target, other, v1Path, v1Ref, outputDir string
	endpointID, sourceID, fixtureID                                                             string
	repo, databaseURL                                                                           string
	recordLimit                                                                                 int
	redactHeaders                                                                               headerList
	timeout                                                                                     time.Duration
}

// headerList permits repeatable --redact-header flags while keeping the
// built-in credential-bearing headers enabled by default.
type headerList []string

func (h *headerList) String() string { return strings.Join(*h, ",") }

func (h *headerList) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("redact-header cannot be empty")
	}
	*h = append(*h, value)
	return nil
}

var defaultRedactionHeaders = []string{"Authorization", "Cookie", "Set-Cookie"}

func redactionPolicy(o options) corpus.RedactionPolicy {
	names := append([]string(nil), defaultRedactionHeaders...)
	names = append(names, o.redactHeaders...)
	return corpus.RedactionPolicy{HeaderNames: names}
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, out, diagnostic io.Writer) int {
	var o options
	var endpointFlag, endpointIDFlag string
	var sourceFlag, sourceIDFlag string
	var fixtureFlag, fixtureIDFlag string
	flags := flag.NewFlagSet("corpusctl", flag.ContinueOnError)
	flags.SetOutput(diagnostic)
	flags.StringVar(&o.mode, "mode", "replay", "validate, validate-release, replay, differential, record (http/sse only; ws unsupported), managed-record (candidate-bound local HTTP+SSE capture), or managed-record-multiclient (candidate-bound FU-01 multi-client SSE capture)")
	flags.StringVar(&o.transport, "transport", "ws", "ws, http, or sse (replay or record mode; ws record unsupported; ignored by managed-record and managed-record-multiclient)")
	flags.StringVar(&o.corpusDir, "corpus", "corpus", "corpus directory or one .ndjson file")
	flags.StringVar(&o.releaseEnvelope, "release-envelope", "", "release-envelope document (required for validate-release)")
	flags.StringVar(&o.suite, "suite", "", "suite/filename substring")
	flags.StringVar(&o.target, "target", "", "target WebSocket or HTTP/SSE URL")
	flags.StringVar(&o.other, "other", "", "pinned v1 WebSocket URL for differential")
	flags.StringVar(&o.v1Path, "v1-path", "../instant", "local pinned v1 checkout")
	flags.StringVar(&o.v1Ref, "v1-ref", "", "expected v1 commit (defaults to corpus manifest)")
	flags.StringVar(&o.outputDir, "output-dir", "", "new private evidence files (required for differential, record, managed-record, and managed-record-multiclient)")
	flags.StringVar(&o.repo, "repo", ".", "repository path for managed-record and managed-record-multiclient (candidate SHA and binary build; must be clean)")
	flags.StringVar(&o.databaseURL, "database-url", "", "admin PostgreSQL URL for managed-record and managed-record-multiclient (defaults to DATABASE_URL; never recorded)")
	flags.StringVar(&endpointIDFlag, "endpoint-id", "", "caller-supplied target endpoint identity for record mode")
	flags.StringVar(&endpointFlag, "endpoint", "", "alias for --endpoint-id")
	flags.StringVar(&sourceIDFlag, "source-id", "", "caller-supplied source revision/deployment identity for record mode")
	flags.StringVar(&sourceFlag, "source", "", "alias for --source-id")
	flags.StringVar(&fixtureIDFlag, "fixture-id", "", "caller-supplied fixture identity for record mode; fixture reset is caller-owned")
	flags.StringVar(&fixtureFlag, "fixture", "", "alias for --fixture-id")
	flags.IntVar(&o.recordLimit, "record-limit", 0, "maximum SSE records to capture during record mode")
	flags.Var(&o.redactHeaders, "redact-header", "additional HTTP header to redact in private evidence (repeatable)")
	flags.DurationVar(&o.timeout, "timeout", 12*time.Second, "per-scenario timeout")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if endpointIDFlag != "" {
		o.endpointID = endpointIDFlag
	} else {
		o.endpointID = endpointFlag
	}
	if sourceIDFlag != "" {
		o.sourceID = sourceIDFlag
	} else {
		o.sourceID = sourceFlag
	}
	if fixtureIDFlag != "" {
		o.fixtureID = fixtureIDFlag
	} else {
		o.fixtureID = fixtureFlag
	}
	fail := func(err error) int { _, _ = fmt.Fprintf(diagnostic, "corpusctl: %v\n", err); return 1 }
	if flags.NArg() != 0 {
		return fail(fmt.Errorf("unexpected positional arguments"))
	}
	switch o.mode {
	case "managed-record":
		return runManagedRecord(o, out, diagnostic)
	case "managed-record-multiclient":
		return runManagedMulticlient(o, out, diagnostic)
	case "validate", "validate-release":
		if o.suite != "" {
			return fail(fmt.Errorf("validate checks the entire corpus; --suite is not supported"))
		}
		report, err := corpus.ValidateCorpus(o.corpusDir)
		if err != nil {
			return fail(err)
		}
		if o.mode == "validate-release" {
			if o.releaseEnvelope == "" {
				return fail(fmt.Errorf("validate-release requires --release-envelope"))
			}
			if err := corpus.ValidateDA004VExclusion(o.corpusDir, o.releaseEnvelope); err != nil {
				return fail(err)
			}
			if err := ValidateCF003Exceptions(o.corpusDir, o.releaseEnvelope); err != nil {
				return fail(err)
			}
		}
		if _, err := fmt.Fprint(out, report); err != nil {
			return fail(err)
		}
		return 0
	case "record":
		if o.transport == "ws" {
			return fail(fmt.Errorf("record mode is unsupported for ws; record is implemented only for http and sse transports"))
		}
		if o.transport != "http" && o.transport != "sse" {
			return fail(fmt.Errorf("unknown transport %q", o.transport))
		}
		return runRecordHTTPOrSSE(o, out, diagnostic)
	case "replay", "differential":
	default:
		return fail(fmt.Errorf("unknown mode %q", o.mode))
	}
	if o.transport != "ws" && o.transport != "http" && o.transport != "sse" {
		return fail(fmt.Errorf("unknown transport %q", o.transport))
	}
	if o.transport != "ws" {
		if o.mode != "replay" {
			return fail(fmt.Errorf("transport %s supports replay only", o.transport))
		}
		return runHTTPOrSSE(o, out, diagnostic)
	}
	if o.target == "" {
		return fail(fmt.Errorf("--target is required"))
	}
	if o.mode == "differential" {
		if o.other == "" || o.outputDir == "" {
			return fail(fmt.Errorf("differential requires --other and --output-dir; independently provision equivalent isolated fixtures first"))
		}
		manifestDir := o.corpusDir
		if filepath.Ext(manifestDir) == ".ndjson" {
			manifestDir = filepath.Dir(manifestDir)
		}
		manifest, err := corpus.LoadManifest(manifestDir)
		if err != nil {
			return fail(err)
		}
		if o.v1Ref == "" {
			o.v1Ref = manifest.V1Ref
		}
		if o.v1Ref != manifest.V1Ref {
			return fail(fmt.Errorf("--v1-ref must equal the full manifest pin"))
		}
		ref, err := revision(o.v1Path)
		if err != nil {
			return fail(fmt.Errorf("read v1 revision: %w", err))
		}
		if ref != o.v1Ref {
			return fail(fmt.Errorf("v1 checkout revision does not match manifest pin"))
		}
	}
	scenarios, err := resolveCorpus(o.corpusDir, o.suite)
	if err != nil {
		return fail(err)
	}
	if len(scenarios) == 0 {
		return fail(fmt.Errorf("no scenarios matched"))
	}
	failures := 0
	for _, sc := range scenarios {
		var a, b corpus.ReplayResult
		var delta string
		if o.mode == "replay" {
			a = corpus.Replay(context.Background(), o.target, sc, o.timeout)
			delta = a.Delta
		} else {
			a, b, delta = corpus.Differential(context.Background(), sc, o.target, o.other, o.timeout)
		}
		if o.outputDir != "" {
			if err := writeEvidence(o, sc, a, b, delta); err != nil {
				return fail(err)
			}
		}
		if a.Err != nil || b.Err != nil || delta != "" {
			if _, err := fmt.Fprintf(out, "FAIL %s: target=%v other=%v\n%s\n", sc.Meta.Suite, a.Err, b.Err, delta); err != nil {
				return fail(err)
			}
			failures++
		} else {
			if _, err := fmt.Fprintf(out, "PASS %s (%s)\n", sc.Meta.Suite, o.mode); err != nil {
				return fail(err)
			}
		}
	}
	if _, err := fmt.Fprintf(out, "%d/%d scenarios passed\n", len(scenarios)-failures, len(scenarios)); err != nil {
		return fail(err)
	}
	if failures > 0 {
		return 1
	}
	return 0
}

func runHTTPOrSSE(o options, out, diagnostic io.Writer) int {
	if o.target == "" {
		_, _ = fmt.Fprintln(diagnostic, "corpusctl: --target is required")
		return 1
	}
	if o.other != "" || o.v1Ref != "" {
		_, _ = fmt.Fprintln(diagnostic, "corpusctl: --other/--v1-ref are only valid for WebSocket differential replay")
		return 1
	}
	if o.suite != "" && !strings.Contains(filepath.Base(o.corpusDir), o.suite) {
		_, _ = fmt.Fprintln(diagnostic, "corpusctl: --suite for HTTP/SSE requires the scenario filename to match")
		return 1
	}
	info, err := os.Stat(o.corpusDir)
	if err != nil {
		_, _ = fmt.Fprintln(diagnostic, "corpusctl:", err)
		return 1
	}
	if info.IsDir() {
		_, _ = fmt.Fprintln(diagnostic, "corpusctl: HTTP/SSE replay requires one scenario JSON file")
		return 1
	}
	b, err := os.ReadFile(o.corpusDir)
	if err != nil {
		_, _ = fmt.Fprintln(diagnostic, "corpusctl:", err)
		return 1
	}
	ctx := context.Background()
	client := http.DefaultClient
	if o.transport == "http" {
		var expected corpus.HTTPExchange
		if err := json.Unmarshal(b, &expected); err != nil {
			_, _ = fmt.Fprintln(diagnostic, "corpusctl: HTTP scenario:", err)
			return 1
		}
		result := corpus.ReplayHTTP(ctx, client, o.target, expected, o.timeout, nil)
		if o.outputDir != "" {
			if err := writeHTTPEvidence(o, expected, result); err != nil {
				_, _ = fmt.Fprintln(diagnostic, "corpusctl:", err)
				return 1
			}
		}
		if result.Err != nil || result.Delta != "" {
			_, _ = fmt.Fprintf(out, "FAIL %s: %v\n%s\n", filepath.Base(o.corpusDir), result.Err, result.Delta)
			return 1
		}
		_, _ = fmt.Fprintf(out, "PASS %s (http replay)\n", filepath.Base(o.corpusDir))
		return 0
	}
	var scenario corpus.SSEScenario
	if err := json.Unmarshal(b, &scenario); err != nil {
		_, _ = fmt.Fprintln(diagnostic, "corpusctl: SSE scenario:", err)
		return 1
	}
	result := corpus.ReplaySSE(ctx, client, o.target, scenario, o.timeout)
	if o.outputDir != "" {
		if err := writeSSEEvidence(o, scenario, result); err != nil {
			_, _ = fmt.Fprintln(diagnostic, "corpusctl:", err)
			return 1
		}
	}
	if result.Err != nil || result.Delta != "" {
		_, _ = fmt.Fprintf(out, "FAIL %s: %v\n%s\n", filepath.Base(o.corpusDir), result.Err, result.Delta)
		return 1
	}
	_, _ = fmt.Fprintf(out, "PASS %s (sse replay)\n", filepath.Base(o.corpusDir))
	return 0
}

func resolveCorpus(path, suite string) ([]*corpus.Scenario, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return corpus.LoadCorpus(path, suite)
	}
	sc, err := corpus.LoadScenario(path)
	if err != nil {
		return nil, err
	}
	if suite != "" && !strings.Contains(sc.Meta.Suite, suite) && !strings.Contains(filepath.Base(path), suite) {
		return nil, nil
	}
	return []*corpus.Scenario{sc}, nil
}

func revision(path string) (string, error) {
	b, err := exec.Command("git", "-C", path, "rev-parse", "HEAD").Output()
	return strings.TrimSpace(string(b)), err
}

type frameEvidence struct {
	Raw        []string          `json:"raw"`
	Normalized []json.RawMessage `json:"normalized"`
	Error      string            `json:"error,omitempty"`
}

func evidenceOf(result corpus.ReplayResult) frameEvidence {
	out := frameEvidence{Raw: []string{}, Normalized: []json.RawMessage{}}
	for _, raw := range result.RawCollected {
		out.Raw = append(out.Raw, string(raw))
	}
	for _, raw := range result.Collected {
		out.Normalized = append(out.Normalized, json.RawMessage(raw))
	}
	if result.Err != nil {
		out.Error = result.Err.Error()
	}
	return out
}

// Evidence contains full raw frames, which can carry auth/application data.
// Files are private, write-once, and never automatically promoted to v1 oracles.
func writeEvidence(o options, sc *corpus.Scenario, a, b corpus.ReplayResult, delta string) error {
	v2Ref, err := revision(".")
	if err != nil {
		return err
	}
	dirty, err := exec.Command("git", "status", "--porcelain").Output()
	if err != nil {
		return err
	}
	record := struct {
		Scenario      string        `json:"scenario"`
		Fixture       string        `json:"fixture"`
		Mode          string        `json:"mode"`
		V1Ref         string        `json:"v1Ref,omitempty"`
		V2Ref         string        `json:"v2Ref"`
		V2Dirty       bool          `json:"v2Dirty"`
		Normalization string        `json:"normalization"`
		Target        frameEvidence `json:"target"`
		Other         frameEvidence `json:"other"`
		Delta         string        `json:"delta"`
	}{sc.Meta.Suite, sc.Meta.SeedFixture, o.mode, o.v1Ref, v2Ref, len(dirty) > 0, "canonical-v1", evidenceOf(a), evidenceOf(b), delta}
	name := filepath.Base(sc.File) + ".evidence.json"
	return corpus.WriteEvidence(filepath.Join(o.outputDir, name), record)
}

func writeHTTPEvidence(o options, expected corpus.HTTPExchange, result corpus.HTTPReplayResult) error {
	policy := redactionPolicy(o)
	v2Ref, err := revision(".")
	if err != nil {
		return err
	}
	dirty, err := exec.Command("git", "status", "--porcelain").Output()
	if err != nil {
		return err
	}
	record := struct {
		Transport       string              `json:"transport"`
		Scenario        string              `json:"scenario"`
		Status          string              `json:"status"`
		RedactedHeaders []string            `json:"redactedHeaders"`
		V2Ref           string              `json:"v2Ref"`
		V2Dirty         bool                `json:"v2Dirty"`
		Expected        corpus.HTTPExchange `json:"expected"`
		Actual          corpus.HTTPExchange `json:"actual"`
		Normalized      struct {
			ExpectedBody []byte      `json:"expectedBody,omitempty"`
			ActualBody   []byte      `json:"actualBody,omitempty"`
			ExpectedHead http.Header `json:"expectedHeaders,omitempty"`
			ActualHead   http.Header `json:"actualHeaders,omitempty"`
		} `json:"normalized"`
		Delta string `json:"delta"`
		Error string `json:"error,omitempty"`
	}{
		Transport:       "http",
		Scenario:        filepath.Base(o.corpusDir),
		Status:          replayStatus(result.Passed),
		RedactedHeaders: append([]string(nil), policy.HeaderNames...),
		V2Ref:           v2Ref,
		V2Dirty:         len(dirty) > 0,
		Expected:        corpus.RedactExchange(expected, policy),
		Actual:          corpus.RedactExchange(result.Actual, policy),
		Delta:           result.Delta,
	}
	record.Normalized.ExpectedBody = result.ExpectedBody
	record.Normalized.ActualBody = result.ActualBody
	record.Normalized.ExpectedHead = corpus.RedactExchange(corpus.HTTPExchange{Response: corpus.HTTPResponse{Headers: result.ExpectedHeader}}, policy).Response.Headers
	record.Normalized.ActualHead = corpus.RedactExchange(corpus.HTTPExchange{Response: corpus.HTTPResponse{Headers: result.ActualHeader}}, policy).Response.Headers
	if result.Err != nil {
		record.Error = result.Err.Error()
	}
	path, err := corpus.EvidencePath(o.outputDir, filepath.Base(o.corpusDir), ".http.evidence.json")
	if err != nil {
		return err
	}
	return corpus.WriteEvidence(path, record)
}

func writeSSEEvidence(o options, scenario corpus.SSEScenario, result corpus.SSEReplayResult) error {
	policy := redactionPolicy(o)
	path, err := corpus.EvidencePath(o.outputDir, scenario.ID, ".sse.evidence.json")
	if err != nil {
		return err
	}
	v2Ref, err := revision(".")
	if err != nil {
		return err
	}
	dirty, err := exec.Command("git", "status", "--porcelain").Output()
	if err != nil {
		return err
	}
	record := struct {
		Transport       string                `json:"transport"`
		Scenario        string                `json:"scenario"`
		ID              string                `json:"id"`
		Status          string                `json:"status"`
		Fixture         string                `json:"fixture,omitempty"`
		RedactedHeaders []string              `json:"redactedHeaders"`
		V2Ref           string                `json:"v2Ref"`
		V2Dirty         bool                  `json:"v2Dirty"`
		ExpectedConnect corpus.HTTPExchange   `json:"expectedConnect"`
		ExpectedPosts   []corpus.HTTPExchange `json:"expectedPosts,omitempty"`
		ActualConnect   corpus.HTTPExchange   `json:"actualConnect"`
		ActualPosts     []corpus.HTTPExchange `json:"actualPosts,omitempty"`
		Expected        []corpus.SSERecord    `json:"expected"`
		Collected       []corpus.SSERecord    `json:"collected"`
		Delta           string                `json:"delta"`
		Error           string                `json:"error,omitempty"`
	}{
		Transport:       "sse",
		Scenario:        scenario.ID,
		ID:              scenario.ID,
		Status:          replayStatus(result.Passed),
		Fixture:         scenario.Fixture,
		RedactedHeaders: append([]string(nil), policy.HeaderNames...),
		V2Ref:           v2Ref,
		V2Dirty:         len(dirty) > 0,
		ExpectedConnect: corpus.RedactExchange(scenario.Connect, policy),
		ExpectedPosts:   redactExchanges(scenario.Posts, policy),
		ActualConnect:   corpus.RedactExchange(result.Connect, policy),
		ActualPosts:     redactExchanges(result.Posts, policy),
		Expected:        corpus.NormalizeSSERecords(scenario.Expected),
		Collected:       corpus.NormalizeSSERecords(result.Collected),
		Delta:           result.Delta,
	}
	if result.Err != nil {
		record.Error = result.Err.Error()
	}
	return corpus.WriteEvidence(path, record)
}

func redactExchanges(exchanges []corpus.HTTPExchange, policy corpus.RedactionPolicy) []corpus.HTTPExchange {
	if exchanges == nil {
		return nil
	}
	out := make([]corpus.HTTPExchange, len(exchanges))
	for i, exchange := range exchanges {
		out[i] = corpus.RedactExchange(exchange, policy)
	}
	return out
}

func replayStatus(passed bool) string {
	if passed {
		return "covered"
	}
	return "gap"
}

func runRecordHTTPOrSSE(o options, out, diagnostic io.Writer) int {
	fail := func(err error) int { _, _ = fmt.Fprintf(diagnostic, "corpusctl: %v\n", err); return 1 }

	metadata := corpus.RecordMetadata{
		EndpointID:   strings.TrimSpace(o.endpointID),
		SourceID:     strings.TrimSpace(o.sourceID),
		FixtureID:    strings.TrimSpace(o.fixtureID),
		FixtureReset: "caller-owned",
	}
	if err := metadata.Validate(); err != nil {
		return fail(err)
	}

	if o.target == "" {
		return fail(fmt.Errorf("--target is required for record mode"))
	}
	if o.outputDir == "" {
		return fail(fmt.Errorf("--output-dir is required for record mode"))
	}

	reservedDir, err := corpus.ReserveOutputDir(o.outputDir)
	if err != nil {
		return fail(err)
	}
	defer func() { _ = reservedDir.Close() }()

	if o.suite != "" && !strings.Contains(filepath.Base(o.corpusDir), o.suite) {
		return fail(fmt.Errorf("--suite for HTTP/SSE requires the scenario filename to match"))
	}

	info, err := os.Stat(o.corpusDir)
	if err != nil {
		return fail(err)
	}
	if info.IsDir() {
		return fail(fmt.Errorf("record mode requires one scenario JSON file, not a directory"))
	}
	b, err := os.ReadFile(o.corpusDir)
	if err != nil {
		return fail(err)
	}

	ctx := context.Background()
	client := http.DefaultClient

	if o.transport == "http" {
		var expected corpus.HTTPExchange
		if err := json.Unmarshal(b, &expected); err != nil || (expected.Request.Target == "" && expected.Request.Method == "") {
			var req corpus.HTTPRequest
			if rerr := json.Unmarshal(b, &req); rerr != nil || req.Target == "" {
				return fail(fmt.Errorf("HTTP scenario must declare request target"))
			}
			expected.Request = req
		}
		if expected.Request.Target == "" {
			return fail(fmt.Errorf("HTTP scenario must declare request target"))
		}

		cctx := ctx
		if o.timeout > 0 {
			var cancel context.CancelFunc
			cctx, cancel = context.WithTimeout(ctx, o.timeout)
			defer cancel()
		}

		captured, err := corpus.CaptureHTTP(cctx, client, o.target, expected.Request, corpus.CaptureOptions{})
		if err != nil {
			return fail(fmt.Errorf("HTTP capture failed: %w", err))
		}

		if err := writeHTTPRecordEvidence(reservedDir, o, metadata, captured); err != nil {
			return fail(err)
		}
		_, _ = fmt.Fprintf(out, "PASS %s (http record)\n", filepath.Base(o.corpusDir))
		return 0
	}

	// Transport == "sse"
	var scenario corpus.SSEScenario
	if err := json.Unmarshal(b, &scenario); err != nil {
		return fail(fmt.Errorf("SSE scenario: %w", err))
	}
	if scenario.ID == "" {
		scenario.ID = strings.TrimSuffix(filepath.Base(o.corpusDir), filepath.Ext(o.corpusDir))
	}
	if err := corpus.ValidateScenarioID(scenario.ID); err != nil {
		return fail(fmt.Errorf("invalid SSE scenario ID %q: %w", scenario.ID, err))
	}
	if scenario.Connect.Request.Target == "" {
		return fail(fmt.Errorf("SSE scenario requires connect request target"))
	}
	if scenario.Fixture != "" && metadata.FixtureID != "" && scenario.Fixture != metadata.FixtureID {
		return fail(fmt.Errorf("fixture flag %q does not match scenario fixture %q", metadata.FixtureID, scenario.Fixture))
	}

	recordLimit := o.recordLimit
	if recordLimit <= 0 {
		recordLimit = len(scenario.Expected)
	}
	if recordLimit <= 0 {
		return fail(fmt.Errorf("SSE record requires a positive record limit (via --record-limit or scenario expected records)"))
	}

	postReqs := make([]corpus.HTTPRequest, len(scenario.Posts))
	for i, post := range scenario.Posts {
		postReqs[i] = post.Request
	}

	cctx := ctx
	if o.timeout > 0 {
		var cancel context.CancelFunc
		cctx, cancel = context.WithTimeout(ctx, o.timeout)
		defer cancel()
	}

	result := corpus.CaptureSSEWithOptions(cctx, client, o.target, scenario.Connect.Request, postReqs, recordLimit, o.timeout, corpus.SSECaptureOptions{
		Limits:     scenario.Limits,
		Quiescence: scenario.Quiescence,
	})
	if result.Err != nil {
		return fail(fmt.Errorf("SSE capture failed: %w", result.Err))
	}
	if len(result.Records) < recordLimit {
		return fail(fmt.Errorf("SSE capture incomplete: captured %d records, expected %d", len(result.Records), recordLimit))
	}

	if err := writeSSERecordEvidence(reservedDir, o, metadata, scenario.ID, result, recordLimit); err != nil {
		return fail(err)
	}
	_, _ = fmt.Fprintf(out, "PASS %s (sse record)\n", scenario.ID)
	return 0
}

func writeHTTPRecordEvidence(reservedDir *corpus.ReservedDir, o options, meta corpus.RecordMetadata, captured corpus.HTTPExchange) error {
	policy := redactionPolicy(o)
	v2Ref, err := revision(".")
	if err != nil {
		return err
	}
	dirty, err := exec.Command("git", "status", "--porcelain").Output()
	if err != nil {
		return err
	}

	redactedRaw := corpus.RedactExchange(captured, policy)

	var normalizedBody []byte
	if strings.Contains(strings.ToLower(captured.Response.Headers.Get("Content-Type")), "json") {
		if b, err := corpus.CanonicalBytesOpts(captured.Response.Body, corpus.CanonicalOptions{}); err == nil {
			normalizedBody = b
		} else {
			normalizedBody = append([]byte(nil), captured.Response.Body...)
		}
	} else {
		normalizedBody = append([]byte(nil), captured.Response.Body...)
	}

	normalizedHeaders := corpus.ComparableHeaders(redactedRaw.Response.Headers, nil)

	record := struct {
		Mode            string                `json:"mode"`
		Transport       string                `json:"transport"`
		Scenario        string                `json:"scenario"`
		Status          string                `json:"status"`
		Metadata        corpus.RecordMetadata `json:"metadata"`
		TargetURL       string                `json:"targetUrl"`
		RedactedHeaders []string              `json:"redactedHeaders"`
		V2Ref           string                `json:"v2Ref"`
		V2Dirty         bool                  `json:"v2Dirty"`
		Raw             corpus.HTTPExchange   `json:"raw"`
		Normalized      struct {
			Body    []byte      `json:"body,omitempty"`
			Headers http.Header `json:"headers,omitempty"`
		} `json:"normalized"`
		DurationMS int64 `json:"durationMs"`
	}{
		Mode:            "record",
		Transport:       "http",
		Scenario:        filepath.Base(o.corpusDir),
		Status:          "recorded",
		Metadata:        meta,
		TargetURL:       o.target,
		RedactedHeaders: append([]string(nil), policy.HeaderNames...),
		V2Ref:           v2Ref,
		V2Dirty:         len(dirty) > 0,
		Raw:             redactedRaw,
		DurationMS:      captured.DurationMS,
	}
	record.Normalized.Body = normalizedBody
	record.Normalized.Headers = normalizedHeaders

	filename := filepath.Base(o.corpusDir) + ".http.evidence.json"
	return reservedDir.WriteEvidence(filename, record)
}

func writeSSERecordEvidence(reservedDir *corpus.ReservedDir, o options, meta corpus.RecordMetadata, scenarioID string, result corpus.SSECaptureResult, recordLimit int) error {
	policy := redactionPolicy(o)
	v2Ref, err := revision(".")
	if err != nil {
		return err
	}
	dirty, err := exec.Command("git", "status", "--porcelain").Output()
	if err != nil {
		return err
	}

	redactedConnect := corpus.RedactExchange(result.Connect, policy)
	redactedPosts := redactExchanges(result.Posts, policy)
	normalizedRecords := corpus.NormalizeSSERecords(result.Records)

	record := struct {
		Mode            string                `json:"mode"`
		Transport       string                `json:"transport"`
		Scenario        string                `json:"scenario"`
		ID              string                `json:"id"`
		Status          string                `json:"status"`
		Metadata        corpus.RecordMetadata `json:"metadata"`
		TargetURL       string                `json:"targetUrl"`
		RedactedHeaders []string              `json:"redactedHeaders"`
		V2Ref           string                `json:"v2Ref"`
		V2Dirty         bool                  `json:"v2Dirty"`
		RecordLimit     int                   `json:"recordLimit"`
		RawConnect      corpus.HTTPExchange   `json:"rawConnect"`
		RawPosts        []corpus.HTTPExchange `json:"rawPosts,omitempty"`
		RawRecords      []corpus.SSERecord    `json:"rawRecords"`
		Normalized      struct {
			ConnectHeaders http.Header        `json:"connectHeaders,omitempty"`
			Records        []corpus.SSERecord `json:"records"`
		} `json:"normalized"`
		DurationMS int64 `json:"durationMs"`
	}{
		Mode:            "record",
		Transport:       "sse",
		Scenario:        scenarioID,
		ID:              scenarioID,
		Status:          "recorded",
		Metadata:        meta,
		TargetURL:       o.target,
		RedactedHeaders: append([]string(nil), policy.HeaderNames...),
		V2Ref:           v2Ref,
		V2Dirty:         len(dirty) > 0,
		RecordLimit:     recordLimit,
		RawConnect:      redactedConnect,
		RawPosts:        redactedPosts,
		RawRecords:      result.Records,
		DurationMS:      result.DurationMS,
	}
	record.Normalized.ConnectHeaders = corpus.ComparableHeaders(redactedConnect.Response.Headers, nil)
	record.Normalized.Records = normalizedRecords

	filename := scenarioID + ".sse.evidence.json"
	return reservedDir.WriteEvidence(filename, record)
}
