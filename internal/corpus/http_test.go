package corpus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCaptureAndReplayHTTPCanonicalizesJSONAndRetainsRaw(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.String() != "/runtime/framework/query?app_id=app" {
			t.Errorf("request = %s %s", r.Method, r.URL)
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != `{"query":{"posts":{}}}` {
			t.Errorf("request body = %q", body)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Request-ID", "new-id")
		_, _ = io.WriteString(w, `{"n":1,"ok":true}`)
	}))
	defer srv.Close()

	expected := HTTPExchange{
		Request: HTTPRequest{
			Method:  http.MethodPost,
			Target:  "/runtime/framework/query?app_id=app",
			Headers: http.Header{"Authorization": {"Bearer secret"}, "Content-Type": {"application/json"}},
			Body:    []byte(`{"query":{"posts":{}}}`),
		},
		Response: HTTPResponse{
			Status:  200,
			Headers: http.Header{"Content-Type": {"application/json"}, "X-Request-ID": {"old-id"}},
			Body:    []byte(`{"n":1.0,"ok":true}`),
		},
	}
	res := ReplayHTTP(context.Background(), srv.Client(), srv.URL, expected, time.Second, []string{"X-Request-ID"})
	if !res.Passed || res.Delta != "" {
		t.Fatalf("replay failed: passed=%v delta=%q err=%v", res.Passed, res.Delta, res.Err)
	}
	if string(res.Actual.Response.Body) != `{"n":1,"ok":true}` {
		t.Fatalf("raw response was not retained: %q", res.Actual.Response.Body)
	}
	if string(res.ExpectedBody) != string(res.ActualBody) {
		t.Fatalf("normalized bodies differ: %q vs %q", res.ExpectedBody, res.ActualBody)
	}
	redacted := RedactExchange(res.Actual, RedactionPolicy{HeaderNames: []string{"authorization", "x-request-id"}})
	if got := redacted.Request.Headers.Get("Authorization"); got != "<redacted>" {
		t.Fatalf("authorization not redacted: %q", got)
	}
	if got := redacted.Response.Headers.Get("X-Request-ID"); got != "<redacted>" {
		t.Fatalf("request id not redacted: %q", got)
	}
}

func TestReplayHTTPTransportFailureIsNotPass(t *testing.T) {
	expected := HTTPExchange{Request: HTTPRequest{Method: http.MethodGet, Target: "/health"}, Response: HTTPResponse{Status: 200}}
	res := ReplayHTTP(context.Background(), http.DefaultClient, "http://127.0.0.1:1", expected, 100*time.Millisecond, nil)
	if res.Passed || res.Delta == "" || res.Err == nil {
		t.Fatalf("transport failure was accepted: %#v", res)
	}
}

func TestParseSSEPreservesRecordsAndMultilineData(t *testing.T) {
	input := ": ping\n\ndata: {\"n\":1}\ndata: {\"ok\":true}\nevent: update\nid: 7\n\n"
	records, err := ParseSSE(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].Kind != "comment" || records[1].Kind != "data" {
		t.Fatalf("records = %#v", records)
	}
	if string(records[1].Data) != "{\"n\":1}\n{\"ok\":true}" || records[1].Event != "update" || records[1].ID != "7" {
		t.Fatalf("data record = %#v", records[1])
	}
	if !strings.Contains(string(records[1].Raw), "event: update") {
		t.Fatalf("raw framing was not retained: %q", records[1].Raw)
	}
}

func TestReplaySSEChecksHandshakeEventsAndPostStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, "data: {\"op\":\"init-ok\",\"session-id\":\"wire\"}\n\n")
			if fl, ok := w.(http.Flusher); ok {
				fl.Flush()
			}
			_, _ = fmt.Fprint(w, "data: {\"op\":\"refresh-ok\",\"n\":1}\n\n")
		case http.MethodPost:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	scenario := SSEScenario{
		ID: "sse-test",
		Connect: HTTPExchange{
			Request:  HTTPRequest{Method: http.MethodGet, Target: "/runtime/sse?app_id=app"},
			Response: HTTPResponse{Status: http.StatusOK},
		},
		Posts: []HTTPExchange{{Request: HTTPRequest{Method: http.MethodPost, Target: "/runtime/sse", Body: []byte(`{"messages":[]}`)}, Response: HTTPResponse{Status: http.StatusOK, Headers: http.Header{"Content-Type": {"application/json"}}, Body: []byte(`{}`)}}},
		Expected: []SSERecord{
			{Kind: "data", Data: []byte(`{"op":"init-ok","session-id":"wire"}`)},
			{Kind: "data", Data: []byte(`{"op":"refresh-ok","n":1}`)},
		},
		Quiescence: 5 * time.Millisecond,
	}
	res := ReplaySSE(context.Background(), srv.Client(), srv.URL, scenario, time.Second)
	if !res.Passed || res.Delta != "" || res.Err != nil {
		t.Fatalf("SSE replay failed: passed=%v delta=%q err=%v records=%#v", res.Passed, res.Delta, res.Err, res.Collected)
	}
}

func TestReplaySSECanonicalizesPOSTResponseBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"op\":\"ready\"}\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"n":1}`)
	}))
	defer srv.Close()

	scenario := SSEScenario{
		ID:      "sse-post-body",
		Connect: HTTPExchange{Request: HTTPRequest{Method: http.MethodGet, Target: "/stream"}, Response: HTTPResponse{Status: http.StatusOK}},
		Posts: []HTTPExchange{{
			Request:  HTTPRequest{Method: http.MethodPost, Target: "/post"},
			Response: HTTPResponse{Status: http.StatusOK, Headers: http.Header{"Content-Type": {"application/json"}}, Body: []byte(`{"n":1.0}`)},
		}},
		Expected:   []SSERecord{{Kind: "data", Data: []byte(`{"op":"ready"}`)}},
		Quiescence: time.Millisecond,
	}
	res := ReplaySSE(context.Background(), srv.Client(), srv.URL, scenario, time.Second)
	if !res.Passed || res.Err != nil {
		t.Fatalf("canonical SSE POST body was rejected: passed=%v delta=%q err=%v", res.Passed, res.Delta, res.Err)
	}
	if got := string(res.Collected[0].Normalized); got != `{"op":"ready"}` {
		t.Fatalf("SSE record did not retain canonical data: %q", got)
	}
}

func TestReplaySSERejectsPOSTResponseBodyMismatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"op\":\"ready\"}\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"n":2}`)
	}))
	defer srv.Close()

	scenario := SSEScenario{
		ID:      "sse-post-mismatch",
		Connect: HTTPExchange{Request: HTTPRequest{Method: http.MethodGet, Target: "/stream"}, Response: HTTPResponse{Status: http.StatusOK}},
		Posts: []HTTPExchange{{
			Request:  HTTPRequest{Method: http.MethodPost, Target: "/post"},
			Response: HTTPResponse{Status: http.StatusOK, Headers: http.Header{"Content-Type": {"application/json"}}, Body: []byte(`{"n":1}`)},
		}},
		Expected:   []SSERecord{{Kind: "data", Data: []byte(`{"op":"ready"}`)}},
		Quiescence: time.Millisecond,
	}
	res := ReplaySSE(context.Background(), srv.Client(), srv.URL, scenario, time.Second)
	if res.Err == nil || !strings.Contains(res.Err.Error(), "SSE POST body differs") || res.Passed {
		t.Fatalf("mismatched SSE POST body was accepted: passed=%v delta=%q err=%v", res.Passed, res.Delta, res.Err)
	}
}

func TestCaptureSSERequiresBoundedRecordCount(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"op\":\"init-ok\"}\n\ndata: {\"op\":\"refresh-ok\"}\n\n")
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	res := CaptureSSE(context.Background(), srv.Client(), srv.URL, HTTPRequest{Method: http.MethodGet, Target: "/runtime/sse"}, []HTTPRequest{{Method: http.MethodPost, Target: "/runtime/sse", Body: []byte(`{"messages":[]}`)}}, 2, time.Second)
	if res.Err != nil || len(res.Records) != 2 || res.Connect.Response.Status != http.StatusOK || len(res.Posts) != 1 {
		t.Fatalf("capture = %#v", res)
	}
	if res.Records[0].ElapsedMS < 0 || len(res.Records[0].Raw) == 0 {
		t.Fatalf("capture omitted timing/raw framing: %#v", res.Records[0])
	}
	if got := CaptureSSE(context.Background(), srv.Client(), srv.URL, HTTPRequest{Method: http.MethodGet, Target: "/runtime/sse"}, nil, 0, time.Second); got.Err == nil {
		t.Fatal("unbounded SSE capture was accepted")
	}
}

func TestWriteEvidenceIsPrivateAndWriteOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "evidence.json")
	if err := WriteEvidence(path, map[string]any{"raw": json.RawMessage(`{"ok":true}`)}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("evidence mode = %v err=%v", info, err)
	}
	if err := WriteEvidence(path, map[string]any{"overwrite": true}); err == nil {
		t.Fatal("evidence overwrite was accepted")
	}
	if err := WriteEvidence(filepath.Join("..", "escape.json"), map[string]any{}); err == nil {
		t.Fatal("relative traversal evidence path was accepted")
	}
}

func TestComparableHeadersMergesCaseVariantValues(t *testing.T) {
	got := comparableHeaders(http.Header{
		"x-trace": {"b"},
		"X-Trace": {"a", "c"},
	}, nil)
	want := http.Header{"X-Trace": {"a", "b", "c"}}
	if !headersEqual(got, want) {
		t.Fatalf("case-variant headers were not merged deterministically: got=%v want=%v", got, want)
	}
}

func TestValidateScenarioIDAndEvidencePath(t *testing.T) {
	for _, id := range []string{"", ".", "..", "../escape", `..\\escape`, "a..b", "/absolute", "a/b", "-leading", "bad\x00id"} {
		if err := ValidateScenarioID(id); err == nil {
			t.Errorf("scenario ID %q was accepted", id)
		}
	}
	for _, id := range []string{"scenario-01", "sse_test.json", "A1"} {
		if err := ValidateScenarioID(id); err != nil {
			t.Errorf("valid scenario ID %q rejected: %v", id, err)
		}
	}
	root := t.TempDir()
	path, err := EvidencePath(root, "scenario-01", ".sse.evidence.json")
	if err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(root, path)
	if err != nil || strings.HasPrefix(rel, "..") {
		t.Fatalf("evidence escaped root: path=%q rel=%q err=%v", path, rel, err)
	}
	if _, err := EvidencePath(root, "../escape", ".json"); err == nil {
		t.Fatal("path traversal scenario ID was accepted")
	}
	if _, err := EvidencePath(root, "safe", "/escape"); err == nil {
		t.Fatal("path separator in evidence suffix was accepted")
	}
}

func TestReplaySSERejectsUnsafeScenarioIDBeforeNetwork(t *testing.T) {
	res := ReplaySSE(context.Background(), http.DefaultClient, "http://127.0.0.1:1", SSEScenario{
		ID:       "../escape",
		Connect:  HTTPExchange{Request: HTTPRequest{Method: http.MethodGet, Target: "/stream"}},
		Expected: []SSERecord{{Kind: "data", Data: []byte(`{"ready":true}`)}},
	}, time.Second)
	if res.Err == nil || !strings.Contains(res.Err.Error(), "invalid scenario id") {
		t.Fatalf("unsafe scenario ID was not rejected before network: %#v", res)
	}
}

func TestValidateSSEScenarioRequiresResponseStatuses(t *testing.T) {
	base := SSEScenario{
		ID:       "status-check",
		Connect:  HTTPExchange{Request: HTTPRequest{Method: http.MethodGet, Target: "/stream"}, Response: HTTPResponse{Status: http.StatusOK}},
		Expected: []SSERecord{{Kind: "data", Data: []byte(`{"ready":true}`)}},
	}
	if err := ValidateSSEScenario(base); err != nil {
		t.Fatal(err)
	}
	base.Connect.Response.Status = 0
	if err := ValidateSSEScenario(base); err == nil || !strings.Contains(err.Error(), "connect response status") {
		t.Fatalf("zero connect status accepted: %v", err)
	}
	base.Connect.Response.Status = http.StatusOK
	base.Posts = []HTTPExchange{{Request: HTTPRequest{Method: http.MethodPost, Target: "/post"}}}
	if err := ValidateSSEScenario(base); err == nil || !strings.Contains(err.Error(), "POST response status") {
		t.Fatalf("zero POST status accepted: %v", err)
	}
}

type sseRoundTripper func(*http.Request) (*http.Response, error)

func (f sseRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

type blockingSSEBody struct {
	first   []byte
	sent    chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingSSEBody) Read(p []byte) (int, error) {
	if len(b.first) != 0 {
		n := copy(p, b.first)
		b.first = b.first[n:]
		if len(b.first) == 0 {
			close(b.sent)
		}
		return n, nil
	}
	<-b.release
	return 0, io.EOF
}

func (b *blockingSSEBody) Close() error {
	b.once.Do(func() { close(b.release) })
	return nil
}

func TestReplaySSEDoesNotAcceptEOFAfterParentCancellation(t *testing.T) {
	sent, release := make(chan struct{}), make(chan struct{})
	body := &blockingSSEBody{first: []byte("data: {\"ready\":true}\n\n"), sent: sent, release: release}
	client := &http.Client{Transport: sseRoundTripper(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     http.Header{"Content-Type": {"text/event-stream"}},
			Body:       body,
			Request:    req,
		}, nil
	})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-sent
		cancel()
		_ = body.Close()
	}()
	scenario := SSEScenario{
		ID:         "parent-cancel-eof",
		Connect:    HTTPExchange{Request: HTTPRequest{Method: http.MethodGet, Target: "/stream"}, Response: HTTPResponse{Status: http.StatusOK}},
		Expected:   []SSERecord{{Kind: "data", Data: []byte(`{"ready":true}`)}},
		Quiescence: time.Second,
	}
	res := ReplaySSE(ctx, client, "http://sse.test", scenario, time.Second)
	if res.Err == nil || res.Passed {
		t.Fatalf("parent cancellation was certified as clean EOF: passed=%v delta=%q err=%v", res.Passed, res.Delta, res.Err)
	}
}

func TestParseSSERejectsTruncatedRecord(t *testing.T) {
	for _, input := range []string{"data: value\n", "data: value"} {
		_, err := ParseSSE(strings.NewReader(input))
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("truncated input %q returned %v, want unexpected EOF", input, err)
		}
	}
}

func TestSSEReaderBoundsLineRecordAndTotalBytes(t *testing.T) {
	lineReader := newSSEReader(strings.NewReader("12345\n\n"), SSELimits{LineBytes: 4})
	if _, err := lineReader.read(); err == nil || !strings.Contains(err.Error(), "line exceeds") {
		t.Fatalf("line limit was not enforced: %v", err)
	}

	recordReader := newSSEReader(strings.NewReader("data: x\n\n"), SSELimits{RecordBytes: 5})
	if _, err := recordReader.read(); err == nil || !strings.Contains(err.Error(), "record exceeds") {
		t.Fatalf("record limit was not enforced: %v", err)
	}

	totalReader := newSSEReader(strings.NewReader(":\n\n:\n\n"), SSELimits{TotalBytes: 3})
	if _, err := totalReader.read(); err != nil {
		t.Fatalf("first bounded record failed: %v", err)
	}
	if _, err := totalReader.read(); err == nil || !strings.Contains(err.Error(), "stream exceeds") {
		t.Fatalf("total limit was not enforced: %v", err)
	}
}

func TestQuiescenceExpiredDistinguishesParentAndChildDeadline(t *testing.T) {
	parent := context.Background()
	child, cancelChild := context.WithDeadline(parent, time.Now().Add(-time.Second))
	defer cancelChild()
	if !quiescenceExpired(parent, child, context.DeadlineExceeded) {
		t.Fatal("child deadline was not classified as quiescence")
	}

	parentDeadline, cancelParent := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelParent()
	childOfParent, cancelChildOfParent := context.WithTimeout(parentDeadline, time.Hour)
	defer cancelChildOfParent()
	if quiescenceExpired(parentDeadline, childOfParent, context.DeadlineExceeded) {
		t.Fatal("parent deadline was incorrectly classified as quiescence")
	}

	parentCancel, cancel := context.WithCancel(context.Background())
	cancel()
	childCanceled, cancelChildCanceled := context.WithTimeout(parentCancel, time.Hour)
	defer cancelChildCanceled()
	if quiescenceExpired(parentCancel, childCanceled, context.Canceled) {
		t.Fatal("parent cancellation was incorrectly classified as quiescence")
	}
}
