package corpus

// HTTP and SSE replay support deliberately stays transport-focused.  It does
// not model route behavior: callers provide the request and expected response
// captured from a real handler or an independently provisioned endpoint.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

const defaultCaptureBodyLimit int64 = 16 << 20

const (
	defaultSSELineLimit   int64 = 64 << 10
	defaultSSERecordLimit int64 = 16 << 20
	defaultSSETotalLimit  int64 = 64 << 20
)

// HTTPRequest is a target-relative request. Target must be an absolute path
// (including query) or an absolute URL whose authority is replaced by the
// replay target. Headers and Body are retained exactly for evidence.
type HTTPRequest struct {
	Method  string      `json:"method"`
	Target  string      `json:"target"`
	Headers http.Header `json:"headers,omitempty"`
	Body    []byte      `json:"body,omitempty"`
}

// HTTPResponse is the observed HTTP result. Body is raw bytes, not a decoded
// interface, so malformed JSON and exact binary payloads remain testable.
type HTTPResponse struct {
	Status  int         `json:"status"`
	Headers http.Header `json:"headers,omitempty"`
	Body    []byte      `json:"body,omitempty"`
}

// HTTPExchange is one request/response pair. Duration is diagnostic only and
// is never used as an equality oracle.
type HTTPExchange struct {
	Request    HTTPRequest  `json:"request"`
	Response   HTTPResponse `json:"response"`
	DurationMS int64        `json:"durationMs,omitempty"`
}

// HTTPReplayResult keeps actual raw bytes separate from normalized comparison
// bytes. A failed transport always yields a non-empty Delta.
type HTTPReplayResult struct {
	Expected       HTTPExchange
	Actual         HTTPExchange
	ExpectedBody   []byte
	ActualBody     []byte
	ExpectedHeader http.Header
	ActualHeader   http.Header
	DurationMS     int64
	Err            error
	Delta          string
	Passed         bool
}

// CaptureOptions bounds a capture. Replay normalization always uses the
// package's existing canonicalization policy.
type CaptureOptions struct {
	MaxBody int64
}

func (o CaptureOptions) maxBody() int64 {
	if o.MaxBody > 0 {
		return o.MaxBody
	}
	return defaultCaptureBodyLimit
}

// CaptureHTTP executes one request against baseURL and retains the raw
// request/response. It is suitable for httptest handlers and explicitly
// provisioned local endpoints; it does not create or reset fixtures.
func CaptureHTTP(ctx context.Context, client *http.Client, baseURL string, req HTTPRequest, opts CaptureOptions) (HTTPExchange, error) {
	if client == nil {
		client = http.DefaultClient
	}
	target, err := resolveTarget(baseURL, req.Target)
	if err != nil {
		return HTTPExchange{}, err
	}
	method := req.Method
	if method == "" {
		method = http.MethodGet
	}
	hreq, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(req.Body))
	if err != nil {
		return HTTPExchange{}, err
	}
	hreq.Header = req.Headers.Clone()
	start := time.Now()
	resp, err := client.Do(hreq)
	if err != nil {
		return HTTPExchange{}, err
	}
	defer resp.Body.Close()
	body, err := readBounded(resp.Body, opts.maxBody())
	if err != nil {
		return HTTPExchange{}, fmt.Errorf("capture response body: %w", err)
	}
	return HTTPExchange{
		Request:    HTTPRequest{Method: method, Target: relativeTarget(target, baseURL), Headers: hreq.Header.Clone(), Body: append([]byte(nil), req.Body...)},
		Response:   HTTPResponse{Status: resp.StatusCode, Headers: resp.Header.Clone(), Body: body},
		DurationMS: time.Since(start).Milliseconds(),
	}, nil
}

// ReplayHTTP sends the declared request and compares status, headers, and
// body. Header names listed in ignoreHeaders are intentionally excluded from
// comparison (for example Date or a server-generated request id).
func ReplayHTTP(ctx context.Context, client *http.Client, baseURL string, expected HTTPExchange, timeout time.Duration, ignoreHeaders []string) (res HTTPReplayResult) {
	start := time.Now()
	res.Expected = expected
	defer func() {
		res.DurationMS = time.Since(start).Milliseconds()
		if res.Err != nil {
			res.Delta = fmt.Sprintf("http replay incomplete: %v", res.Err)
			res.Passed = false
			return
		}
		res.ExpectedBody, res.ActualBody = normalizedHTTPBodyPair(expected.Response, res.Actual.Response)
		res.ExpectedHeader = comparableHeaders(expected.Response.Headers, ignoreHeaders)
		res.ActualHeader = comparableHeaders(res.Actual.Response.Headers, ignoreHeaders)
		res.Delta = diffHTTP(expected.Response, res.Actual.Response, res.ExpectedBody, res.ActualBody, res.ExpectedHeader, res.ActualHeader)
		res.Passed = res.Delta == ""
	}()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	actual, err := CaptureHTTP(ctx, client, baseURL, expected.Request, CaptureOptions{})
	if err != nil {
		res.Err = err
		return
	}
	res.Actual = actual
	return
}

func diffHTTP(expected, actual HTTPResponse, expectedBody, actualBody []byte, expectedHeader, actualHeader http.Header) string {
	var b strings.Builder
	if expected.Status != actual.Status {
		fmt.Fprintf(&b, "status: expected %d, actual %d\n", expected.Status, actual.Status)
	}
	if !headersEqual(expectedHeader, actualHeader) {
		fmt.Fprintf(&b, "headers: expected %v, actual %v\n", expectedHeader, actualHeader)
	}
	if !bytes.Equal(expectedBody, actualBody) {
		fmt.Fprintf(&b, "body: expected %s, actual %s\n", expectedBody, actualBody)
	}
	return b.String()
}

func normalizedHTTPBodyPair(expected, actual HTTPResponse) ([]byte, []byte) {
	if len(expected.Body) == 0 && len(actual.Body) == 0 {
		return nil, nil
	}
	jsonBody := func(resp HTTPResponse) bool {
		return strings.Contains(strings.ToLower(resp.Headers.Get("Content-Type")), "json")
	}
	if jsonBody(expected) || jsonBody(actual) {
		expectedCanonical, expectedErr := CanonicalBytesOpts(expected.Body, CanonicalOptions{})
		actualCanonical, actualErr := CanonicalBytesOpts(actual.Body, CanonicalOptions{})
		if expectedErr == nil && actualErr == nil {
			return expectedCanonical, actualCanonical
		}
	}
	return append([]byte(nil), expected.Body...), append([]byte(nil), actual.Body...)
}

func comparableHeaders(h http.Header, ignored []string) http.Header {
	return ComparableHeaders(h, ignored)
}

// ComparableHeaders sorts and canonicalizes header keys, merging values and
// stripping volatile transport headers like Date and Content-Length.
func ComparableHeaders(h http.Header, ignored []string) http.Header {
	out := make(http.Header)
	ignore := make(map[string]bool, len(ignored)+3)
	for _, name := range []string{"Date", "Content-Length", "Transfer-Encoding"} {
		ignore[strings.ToLower(name)] = true
	}
	for _, name := range ignored {
		ignore[strings.ToLower(name)] = true
	}
	for name, values := range h {
		if ignore[strings.ToLower(name)] {
			continue
		}
		canonicalName := http.CanonicalHeaderKey(name)
		for _, value := range values {
			out[canonicalName] = append(out[canonicalName], strings.TrimSpace(value))
		}
	}
	for name, values := range out {
		sort.Strings(values)
		out[name] = values
	}
	return out
}

func headersEqual(a, b http.Header) bool {
	return len(a) == len(b) && func() bool {
		for k, av := range a {
			bv, ok := b[k]
			if !ok || len(av) != len(bv) {
				return false
			}
			for i := range av {
				if av[i] != bv[i] {
					return false
				}
			}
		}
		return true
	}()
}

func readBounded(r io.Reader, max int64) ([]byte, error) {
	if max <= 0 {
		max = defaultCaptureBodyLimit
	}
	b, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > max {
		return nil, fmt.Errorf("body exceeds %d bytes", max)
	}
	return b, nil
}

func resolveTarget(baseURL, target string) (string, error) {
	base, err := url.Parse(baseURL)
	if err != nil || base.Scheme == "" || base.Host == "" {
		return "", fmt.Errorf("invalid replay base URL %q", baseURL)
	}
	u, err := url.Parse(target)
	if err != nil || u.Path == "" && u.RawQuery == "" {
		return "", fmt.Errorf("invalid request target %q", target)
	}
	if u.IsAbs() {
		u.Scheme, u.Host = base.Scheme, base.Host
	} else {
		if !strings.HasPrefix(u.Path, "/") {
			return "", fmt.Errorf("request target must be absolute path: %q", target)
		}
		u.Scheme, u.Host = base.Scheme, base.Host
	}
	return u.String(), nil
}

func relativeTarget(target, baseURL string) string {
	u, err := url.Parse(target)
	if err != nil {
		return target
	}
	if q := u.RawQuery; q != "" {
		return u.EscapedPath() + "?" + q
	}
	return u.EscapedPath()
}

// SSERecord is one ordered SSE block. Kind is data or comment; comments are
// retained so heartbeat and disconnect behavior can be asserted explicitly.
type SSERecord struct {
	Kind       string `json:"kind"`
	Event      string `json:"event,omitempty"`
	ID         string `json:"id,omitempty"`
	Retry      string `json:"retry,omitempty"`
	Data       []byte `json:"data,omitempty"`
	Raw        []byte `json:"raw,omitempty"`
	Normalized []byte `json:"normalized,omitempty"`
	ElapsedMS  int64  `json:"elapsedMs,omitempty"`
}

// SSELimits bounds each input line, one event block, and the complete stream.
// Zero values use conservative defaults suitable for the runtime SSE contract.
type SSELimits struct {
	LineBytes   int64 `json:"lineBytes,omitempty"`
	RecordBytes int64 `json:"recordBytes,omitempty"`
	TotalBytes  int64 `json:"totalBytes,omitempty"`
}

func (l SSELimits) withDefaults() SSELimits {
	if l.LineBytes <= 0 {
		l.LineBytes = defaultSSELineLimit
	}
	if l.RecordBytes <= 0 {
		l.RecordBytes = defaultSSERecordLimit
	}
	if l.TotalBytes <= 0 {
		l.TotalBytes = defaultSSETotalLimit
	}
	return l
}

// SSEScenario is a GET stream plus ordered POST exchanges. Expected includes
// the initial handshake and any comments/data frames selected by the capture.
type SSEScenario struct {
	ID         string         `json:"id"`
	Fixture    string         `json:"fixture,omitempty"`
	Connect    HTTPExchange   `json:"connect"`
	Posts      []HTTPExchange `json:"posts,omitempty"`
	Expected   []SSERecord    `json:"expected"`
	Limits     SSELimits      `json:"limits,omitempty"`
	Quiescence time.Duration  `json:"-"`
}

type SSEReplayResult struct {
	Scenario   SSEScenario
	Connect    HTTPExchange
	Posts      []HTTPExchange
	Collected  []SSERecord
	DurationMS int64
	Err        error
	Delta      string
	Passed     bool
}

// ValidateSSEScenario checks the fields required to make an SSE replay
// meaningful. A zero expected status is an omitted assertion, not a valid
// protocol expectation, so it is rejected for both the handshake and posts.
func ValidateSSEScenario(scenario SSEScenario) error {
	if err := ValidateScenarioID(scenario.ID); err != nil {
		return err
	}
	if scenario.Connect.Response.Status == 0 {
		return errors.New("SSE replay requires a nonzero connect response status")
	}
	for i, post := range scenario.Posts {
		if post.Response.Status == 0 {
			return fmt.Errorf("SSE replay requires a nonzero POST response status at index %d", i)
		}
	}
	if len(scenario.Expected) == 0 {
		return errors.New("SSE replay requires expected records")
	}
	return nil
}

// SSECaptureResult is the raw result of driving one SSE GET plus its ordered
// POST requests. RecordLimit is mandatory so a capture cannot wait forever on
// a live stream; a caller may then persist the result as an SSEScenario.
type SSECaptureResult struct {
	Connect    HTTPExchange
	Posts      []HTTPExchange
	Records    []SSERecord
	DurationMS int64
	Err        error
}

// SSECaptureOptions tunes bounds and quiescence during SSE capture.
type SSECaptureOptions struct {
	Limits     SSELimits
	Quiescence time.Duration
}

// CaptureSSE executes the real GET/POST pair and captures exactly recordLimit
// stream records. It does not seed/reset fixtures. The records retain raw SSE
// blocks and relative arrival timing; timing is diagnostic, not an oracle.
func CaptureSSE(ctx context.Context, client *http.Client, baseURL string, connect HTTPRequest, posts []HTTPRequest, recordLimit int, timeout time.Duration) (res SSECaptureResult) {
	return CaptureSSEWithOptions(ctx, client, baseURL, connect, posts, recordLimit, timeout, SSECaptureOptions{})
}

// CaptureSSEWithOptions executes SSE capture with caller-specified bounds and quiescence.
func CaptureSSEWithOptions(ctx context.Context, client *http.Client, baseURL string, connect HTTPRequest, posts []HTTPRequest, recordLimit int, timeout time.Duration, opts SSECaptureOptions) (res SSECaptureResult) {
	start := time.Now()
	defer func() {
		res.DurationMS = time.Since(start).Milliseconds()
		if res.Err == nil && len(res.Records) < recordLimit {
			res.Err = fmt.Errorf("incomplete SSE stream: captured %d records, want %d", len(res.Records), recordLimit)
		}
	}()
	if recordLimit <= 0 {
		res.Err = errors.New("SSE capture requires a positive record limit")
		return
	}
	if client == nil {
		client = http.DefaultClient
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	getURL, err := resolveTarget(baseURL, connect.Target)
	if err != nil {
		res.Err = err
		return
	}
	getReq, err := http.NewRequestWithContext(ctx, http.MethodGet, getURL, bytes.NewReader(connect.Body))
	if err != nil {
		res.Err = err
		return
	}
	getReq.Header = connect.Headers.Clone()
	getStart := time.Now()
	resp, err := client.Do(getReq)
	if err != nil {
		res.Err = fmt.Errorf("SSE GET: %w", err)
		return
	}
	defer resp.Body.Close()
	res.Connect = HTTPExchange{
		Request:    HTTPRequest{Method: http.MethodGet, Target: relativeTarget(getURL, baseURL), Headers: getReq.Header.Clone(), Body: append([]byte(nil), connect.Body...)},
		Response:   HTTPResponse{Status: resp.StatusCode, Headers: resp.Header.Clone()},
		DurationMS: time.Since(getStart).Milliseconds(),
	}
	if resp.StatusCode/100 != 2 {
		res.Err = fmt.Errorf("SSE GET status %s", resp.Status)
		return
	}
	if !strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream") {
		res.Err = fmt.Errorf("SSE GET content type %q is not text/event-stream", resp.Header.Get("Content-Type"))
		return
	}
	readResult := make(chan sseReadResult, 1)
	go func() {
		reader := newSSEReader(resp.Body, opts.Limits)
		got := make([]SSERecord, 0, recordLimit)
		for len(got) < recordLimit {
			record, readErr := reader.read()
			if readErr != nil {
				if errors.Is(readErr, io.EOF) {
					readResult <- sseReadResult{records: got, err: fmt.Errorf("incomplete SSE stream: server closed stream after %d records, expected %d: %w", len(got), recordLimit, readErr)}
				} else {
					readResult <- sseReadResult{records: got, err: readErr}
				}
				return
			}
			record.ElapsedMS = time.Since(start).Milliseconds()
			got = append(got, record)
		}
		window := opts.Quiescence
		if window <= 0 {
			window = 25 * time.Millisecond
		}
		qctx, cancel := context.WithTimeout(ctx, window)
		defer cancel()
		rec, qerr := readSSERecordWithContext(qctx, reader)
		if qerr == nil {
			rec.ElapsedMS = time.Since(start).Milliseconds()
			got = append(got, rec)
			readResult <- sseReadResult{records: got, err: fmt.Errorf("extra SSE record received during quiescence window (%d expected)", recordLimit)}
		} else if errors.Is(qerr, io.EOF) {
			if ctx.Err() != nil {
				readResult <- sseReadResult{records: got, err: ctx.Err()}
			} else {
				readResult <- sseReadResult{records: got, err: nil}
			}
		} else if quiescenceExpired(ctx, qctx, qerr) {
			readResult <- sseReadResult{records: got, err: nil}
		} else {
			readResult <- sseReadResult{records: got, err: qerr}
		}
	}()
	for _, post := range posts {
		captured, postErr := CaptureHTTP(ctx, client, baseURL, post, CaptureOptions{})
		if postErr != nil {
			res.Err = fmt.Errorf("SSE POST: %w", postErr)
			return
		}
		res.Posts = append(res.Posts, captured)
	}
	select {
	case got := <-readResult:
		res.Records = got.records
		res.Err = got.err
	case <-ctx.Done():
		res.Err = ctx.Err()
	}
	return
}

// ReplaySSE drives the real GET/POST SSE pair. It stops after the expected
// record count and observes a bounded quiescence window for an extra record.
// The finite window is deliberately not a claim that no later event exists.
func ReplaySSE(ctx context.Context, client *http.Client, baseURL string, scenario SSEScenario, timeout time.Duration) (res SSEReplayResult) {
	start := time.Now()
	res.Scenario = scenario
	defer func() {
		res.DurationMS = time.Since(start).Milliseconds()
		if res.Err != nil {
			res.Delta = fmt.Sprintf("SSE replay incomplete: %v", res.Err)
			return
		}
		res.Delta = diffSSE(scenario.Expected, res.Collected)
		res.Passed = res.Delta == ""
	}()
	if err := ValidateSSEScenario(scenario); err != nil {
		res.Err = err
		return
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	if client == nil {
		client = http.DefaultClient
	}
	getURL, err := resolveTarget(baseURL, scenario.Connect.Request.Target)
	if err != nil {
		res.Err = err
		return
	}
	getReq, err := http.NewRequestWithContext(ctx, http.MethodGet, getURL, bytes.NewReader(scenario.Connect.Request.Body))
	if err != nil {
		res.Err = err
		return
	}
	getReq.Header = scenario.Connect.Request.Headers.Clone()
	resp, err := client.Do(getReq)
	if err != nil {
		res.Err = fmt.Errorf("SSE GET: %w", err)
		return
	}
	defer resp.Body.Close()
	res.Connect = HTTPExchange{
		Request: HTTPRequest{
			Method:  http.MethodGet,
			Target:  relativeTarget(getURL, baseURL),
			Headers: getReq.Header.Clone(),
			Body:    append([]byte(nil), scenario.Connect.Request.Body...),
		},
		Response: HTTPResponse{Status: resp.StatusCode, Headers: resp.Header.Clone()},
	}
	if scenario.Connect.Response.Status != 0 && resp.StatusCode != scenario.Connect.Response.Status {
		res.Err = fmt.Errorf("SSE GET status: expected %d, got %d", scenario.Connect.Response.Status, resp.StatusCode)
		return
	}
	if !strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream") {
		res.Err = fmt.Errorf("SSE GET content type %q is not text/event-stream", resp.Header.Get("Content-Type"))
		return
	}
	if len(scenario.Connect.Response.Headers) != 0 && !headersEqual(comparableHeaders(scenario.Connect.Response.Headers, nil), comparableHeaders(resp.Header, nil)) {
		res.Err = fmt.Errorf("SSE GET headers differ: expected %v, got %v", scenario.Connect.Response.Headers, resp.Header)
		return
	}
	records := make(chan sseReadResult, 1)
	go func() {
		var got []SSERecord
		reader := newSSEReader(resp.Body, scenario.Limits)
		for {
			rec, readErr := reader.read()
			if readErr != nil {
				records <- sseReadResult{records: got, err: readErr}
				return
			}
			got = append(got, rec)
			if len(got) >= len(scenario.Expected) {
				window := scenario.Quiescence
				if window <= 0 {
					window = 25 * time.Millisecond
				}
				qctx, cancel := context.WithTimeout(ctx, window)
				defer cancel()
				rec, qerr := readSSERecordWithContext(qctx, reader)
				if qerr == nil {
					got = append(got, rec)
					records <- sseReadResult{records: got, err: nil}
				} else if errors.Is(qerr, io.EOF) {
					if ctx.Err() != nil {
						records <- sseReadResult{records: got, err: ctx.Err()}
					} else {
						records <- sseReadResult{records: got, err: nil}
					}
				} else if quiescenceExpired(ctx, qctx, qerr) {
					records <- sseReadResult{records: got, err: nil}
				} else {
					records <- sseReadResult{records: got, err: qerr}
				}
				return
			}
		}
	}()
	for _, post := range scenario.Posts {
		postURL, perr := resolveTarget(baseURL, post.Request.Target)
		if perr != nil {
			res.Err = perr
			return
		}
		method := post.Request.Method
		if method == "" {
			method = http.MethodGet
		}
		preq, perr := http.NewRequestWithContext(ctx, method, postURL, bytes.NewReader(post.Request.Body))
		if perr != nil {
			res.Err = perr
			return
		}
		preq.Header = post.Request.Headers.Clone()
		presp, perr := client.Do(preq)
		if perr != nil {
			res.Err = fmt.Errorf("SSE POST: %w", perr)
			return
		}
		postStart := time.Now()
		postBody, readErr := readBounded(presp.Body, defaultCaptureBodyLimit)
		_ = presp.Body.Close()
		postResult := HTTPExchange{
			Request: HTTPRequest{
				Method:  method,
				Target:  relativeTarget(postURL, baseURL),
				Headers: preq.Header.Clone(),
				Body:    append([]byte(nil), post.Request.Body...),
			},
			Response:   HTTPResponse{Status: presp.StatusCode, Headers: presp.Header.Clone(), Body: postBody},
			DurationMS: time.Since(postStart).Milliseconds(),
		}
		res.Posts = append(res.Posts, postResult)
		if readErr != nil {
			res.Err = fmt.Errorf("SSE POST body: %w", readErr)
			return
		}
		if post.Response.Status != 0 && presp.StatusCode != post.Response.Status {
			res.Err = fmt.Errorf("SSE POST status: expected %d, got %d", post.Response.Status, presp.StatusCode)
			return
		}
		if len(post.Response.Headers) != 0 && !headersEqual(comparableHeaders(post.Response.Headers, nil), comparableHeaders(presp.Header, nil)) {
			res.Err = fmt.Errorf("SSE POST headers differ: expected %v, got %v", post.Response.Headers, presp.Header)
			return
		}
		expectedBody, actualBody := normalizedHTTPBodyPair(post.Response, postResult.Response)
		if !bytes.Equal(expectedBody, actualBody) {
			res.Err = fmt.Errorf("SSE POST body differs: expected %s, got %s", expectedBody, actualBody)
			return
		}
	}
	select {
	case got := <-records:
		res.Collected = got.records
		res.Err = got.err
	case <-ctx.Done():
		res.Err = ctx.Err()
	}
	return
}

func quiescenceExpired(parent, child context.Context, err error) bool {
	if parent.Err() != nil || child.Err() != context.DeadlineExceeded {
		return false
	}
	return errors.Is(err, context.DeadlineExceeded)
}

type sseReadResult struct {
	records []SSERecord
	err     error
}

// ParseSSE reads all complete records from an SSE body. It preserves comments,
// data-line joining, event metadata, and exact block bytes for evidence.
func ParseSSE(r io.Reader) ([]SSERecord, error) {
	reader := newSSEReader(r, SSELimits{})
	var records []SSERecord
	for {
		rec, err := reader.read()
		if errors.Is(err, io.EOF) {
			return records, nil
		}
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
}

func readSSERecordWithContext(ctx context.Context, r io.Reader) (SSERecord, error) {
	if reader, ok := r.(*sseReader); ok {
		return readSSERecordFromReader(ctx, reader)
	}
	return readSSERecordFromReader(ctx, newSSEReader(r, SSELimits{}))
}

func readSSERecordFromReader(ctx context.Context, reader *sseReader) (SSERecord, error) {
	// A goroutine is used only for the bounded read operation; callers cancel
	// the request body when the context expires, which unblocks network reads.
	result := make(chan sseReadResult, 1)
	go func() {
		rec, err := reader.read()
		result <- sseReadResult{records: []SSERecord{rec}, err: err}
	}()
	select {
	case <-ctx.Done():
		return SSERecord{}, ctx.Err()
	case got := <-result:
		if got.err != nil {
			return SSERecord{}, got.err
		}
		return got.records[0], nil
	}
}

type sseReader struct {
	r     *bufio.Reader
	limit SSELimits
	total int64
}

func (r *sseReader) Read(p []byte) (int, error) { return r.r.Read(p) }

func newSSEReader(input io.Reader, limits SSELimits) *sseReader {
	limits = limits.withDefaults()
	bufferSize := limits.LineBytes
	if bufferSize > defaultSSELineLimit {
		bufferSize = defaultSSELineLimit
	}
	return &sseReader{r: bufio.NewReaderSize(input, int(bufferSize)), limit: limits}
}

func (r *sseReader) readLine() ([]byte, error) {
	var line []byte
	for {
		part, err := r.r.ReadSlice('\n')
		if len(part) != 0 {
			if r.total+int64(len(part)) > r.limit.TotalBytes {
				return nil, fmt.Errorf("SSE stream exceeds %d bytes", r.limit.TotalBytes)
			}
			if int64(len(line)+len(part)) > r.limit.LineBytes {
				return nil, fmt.Errorf("SSE line exceeds %d bytes", r.limit.LineBytes)
			}
			line = append(line, part...)
			r.total += int64(len(part))
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		if err != nil && len(line) == 0 {
			return nil, err
		}
		return line, err
	}
}

func (r *sseReader) read() (SSERecord, error) {
	var raw bytes.Buffer
	var data []string
	var event, id, retry string
	kind := ""
	for {
		lineBytes, err := r.readLine()
		line := string(lineBytes)
		if len(lineBytes) != 0 {
			if int64(raw.Len())+int64(len(lineBytes)) > r.limit.RecordBytes {
				return SSERecord{}, fmt.Errorf("SSE record exceeds %d bytes", r.limit.RecordBytes)
			}
			raw.Write(lineBytes)
			line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		}
		if line == "" {
			if kind != "" || event != "" || id != "" || retry != "" || len(data) != 0 {
				if err != nil {
					return SSERecord{}, io.ErrUnexpectedEOF
				}
				return NormalizeSSERecord(SSERecord{Kind: kind, Event: event, ID: id, Retry: retry, Data: []byte(strings.Join(data, "\n")), Raw: raw.Bytes()}), nil
			}
			if err != nil {
				if errors.Is(err, io.EOF) && raw.Len() == 0 {
					return SSERecord{}, io.EOF
				}
				return SSERecord{}, err
			}
			continue
		}
		switch {
		case strings.HasPrefix(line, ":"):
			if kind == "" {
				kind = "comment"
			}
		case strings.HasPrefix(line, "data:"):
			if kind == "" {
				kind = "data"
			}
			data = append(data, sseValue(strings.TrimPrefix(line, "data:")))
		case strings.HasPrefix(line, "event:"):
			event = sseValue(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "id:"):
			id = sseValue(strings.TrimPrefix(line, "id:"))
		case strings.HasPrefix(line, "retry:"):
			retry = sseValue(strings.TrimPrefix(line, "retry:"))
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return SSERecord{}, io.ErrUnexpectedEOF
			}
			return SSERecord{}, err
		}
	}
}

func sseValue(v string) string {
	if strings.HasPrefix(v, " ") {
		return v[1:]
	}
	return v
}

func diffSSE(expected, actual []SSERecord) string {
	if len(expected) != len(actual) {
		return fmt.Sprintf("records: expected %d, actual %d", len(expected), len(actual))
	}
	for i := range expected {
		if expected[i].Kind != actual[i].Kind || expected[i].Event != actual[i].Event || expected[i].ID != actual[i].ID || expected[i].Retry != actual[i].Retry || !bytes.Equal(normalizedSSEData(expected[i]), normalizedSSEData(actual[i])) {
			return fmt.Sprintf("record %d: expected kind=%q event=%q data=%s, actual kind=%q event=%q data=%s", i, expected[i].Kind, expected[i].Event, normalizedSSEData(expected[i]), actual[i].Kind, actual[i].Event, normalizedSSEData(actual[i]))
		}
	}
	return ""
}

func normalizedSSEData(rec SSERecord) []byte {
	if rec.Kind == "data" && len(rec.Data) != 0 {
		if b, err := CanonicalBytesOpts(rec.Data, CanonicalOptions{}); err == nil {
			return b
		}
	}
	return append([]byte(nil), rec.Data...)
}

// NormalizeSSERecord returns an evidence-ready copy with canonical data next
// to the raw SSE block. Replay comparisons still derive normalization from
// Data, so a supplied Normalized field cannot alter the oracle.
func NormalizeSSERecord(record SSERecord) SSERecord {
	record.Data = append([]byte(nil), record.Data...)
	record.Raw = append([]byte(nil), record.Raw...)
	record.Normalized = normalizedSSEData(record)
	return record
}

// NormalizeSSERecords returns independent, evidence-ready record copies.
func NormalizeSSERecords(records []SSERecord) []SSERecord {
	if records == nil {
		return nil
	}
	out := make([]SSERecord, len(records))
	for i, record := range records {
		out[i] = NormalizeSSERecord(record)
	}
	return out
}

// ReservedDir represents a securely reserved, private (mode 0700) output directory
// with a pinned identity (file descriptor and dev/ino). It prevents path repointing
// or symlink swap races between reservation and publication.
type ReservedDir struct {
	path       string
	dirFile    *os.File
	parentFile *os.File
	dev        uint64
	ino        uint64
	parentDev  uint64
	parentIno  uint64
	baseName   string
	closed     bool
	mu         sync.Mutex
}

// Path returns the cleaned filesystem path of the reserved directory.
func (d *ReservedDir) Path() string {
	return d.path
}

// Close releases the directory file descriptors held by the reservation.
func (d *ReservedDir) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	var errs []error
	if d.dirFile != nil {
		if err := d.dirFile.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if d.parentFile != nil {
		if err := d.parentFile.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// EvidencePath joins a validated scenario ID to this reserved directory.
func (d *ReservedDir) EvidencePath(scenarioID, suffix string) (string, error) {
	return EvidencePath(d.path, scenarioID, suffix)
}

// WriteEvidence writes one private, write-once JSON artifact into this reserved
// directory using atomic publication (temporary file mode 0600, fsync, atomic
// rename with no-replace semantics, and directory sync). Failed publication does
// not attempt ambiguous named-file cleanup.
func (d *ReservedDir) WriteEvidence(filenameOrPath string, value any) error {
	return writeEvidenceInDir(d, filenameOrPath, value, writeHooks{})
}

type reserveHooks struct {
	afterStatBeforeMkdir func() error
}

type writeHooks struct {
	beforeTempCreate func() error
	beforeWrite      func() error
	beforeSync       func() error
	beforeClose      func() error
	beforeDirSync    func() error
}

func validateFreshOutputDirPath(dir string) (string, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return "", errors.New("output directory is required")
	}
	cleaned := filepath.Clean(dir)
	if cleaned == "." || cleaned == string(filepath.Separator) {
		return "", fmt.Errorf("invalid output directory %q", dir)
	}
	if !filepath.IsAbs(cleaned) && (cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator))) {
		return "", fmt.Errorf("output directory escapes current path: %q", dir)
	}
	parts := strings.Split(cleaned, string(filepath.Separator))
	for _, p := range parts {
		if p == ".." {
			return "", fmt.Errorf("output directory escapes current path: %q", dir)
		}
	}

	evalPath := cleaned
	if runtime.GOOS == "darwin" && filepath.IsAbs(cleaned) {
		for _, sysPrefix := range []string{"/var", "/tmp", "/etc"} {
			if evalPath == sysPrefix || strings.HasPrefix(evalPath, sysPrefix+"/") {
				evalPath = "/private" + evalPath
				break
			}
		}
	}
	return evalPath, nil
}

// ReserveOutputDir securely reserves a fresh private output directory with mode 0700
// and pins its file descriptor and (dev, ino) identity. It rejects existing paths
// (including empty dirs and regular files), symlink components, parent symlink
// redirection, and path traversal.
func ReserveOutputDir(dir string) (*ReservedDir, error) {
	return reserveOutputDirPlatform(dir)
}

// CheckFreshOutputDir ensures the output directory path is safe, fresh, and does not already
// exist. It rejects existing output paths (including empty dirs and regular files),
// symlink components, parent symlink redirection, and path traversal.
func CheckFreshOutputDir(dir string) error {
	return checkFreshOutputDirPlatform(dir)
}

// WriteEvidence writes one private, write-once JSON artifact. The caller may
// use RedactEvidence before passing a value here; no overwrite is permitted.
// Publication is atomic (temporary file mode 0600, fsync, atomic no-replace publication,
// directory sync). Failed publication may leave private untrusted residue.
func WriteEvidence(path string, value any) error {
	return writeEvidenceWithHooks(path, value, writeHooks{})
}

// ValidateScenarioID accepts only a filename-safe identifier. IDs are used in
// evidence names, so separators, dot components, and control characters are
// rejected before any output path is constructed.
func ValidateScenarioID(id string) error {
	if id == "" || len(id) > 128 || id == "." || id == ".." || strings.Contains(id, "..") {
		return fmt.Errorf("invalid scenario id %q", id)
	}
	for i, c := range id {
		if i == 0 && !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
			return fmt.Errorf("invalid scenario id %q", id)
		}
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.') {
			return fmt.Errorf("invalid scenario id %q", id)
		}
	}
	return nil
}

// EvidencePath joins a validated scenario ID to an output directory and
// proves the resulting path remains contained by that directory.
func EvidencePath(dir, scenarioID, suffix string) (string, error) {
	if err := ValidateScenarioID(scenarioID); err != nil {
		return "", err
	}
	if suffix == "" || strings.ContainsAny(suffix, `/\\`) || strings.ContainsRune(suffix, 0) {
		return "", fmt.Errorf("invalid evidence suffix %q", suffix)
	}
	root, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	path := filepath.Join(root, scenarioID+suffix)
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("evidence path escapes output directory")
	}
	return path, nil
}

// RedactionPolicy only redacts explicitly named HTTP headers. Bodies remain
// raw evidence and therefore must be kept in the private evidence directory.
// This narrow policy avoids recursively hiding application fields by name.
type RedactionPolicy struct {
	HeaderNames []string `json:"headerNames,omitempty"`
}

func RedactExchange(e HTTPExchange, policy RedactionPolicy) HTTPExchange {
	out := e
	out.Request.Headers = redactHeaders(e.Request.Headers, policy.HeaderNames)
	out.Response.Headers = redactHeaders(e.Response.Headers, policy.HeaderNames)
	out.Request.Body = append([]byte(nil), e.Request.Body...)
	out.Response.Body = append([]byte(nil), e.Response.Body...)
	return out
}

func redactHeaders(in http.Header, names []string) http.Header {
	out := in.Clone()
	for _, name := range names {
		for key := range out {
			if strings.EqualFold(key, name) {
				out[key] = []string{"<redacted>"}
			}
		}
	}
	return out
}

// MarshalSSERecords produces deterministic JSON evidence for stream records.
// It is a small helper for callers that want normalized records without
// copying transport framing behavior into the command.
func MarshalSSERecords(records []SSERecord) ([]byte, error) {
	return json.Marshal(NormalizeSSERecords(records))
}

// RecordMetadata records caller-supplied provenance and fixture identity.
// The CLI does not provision or reset external fixtures; reset is caller-owned.
type RecordMetadata struct {
	EndpointID   string `json:"endpointId"`
	SourceID     string `json:"sourceId"`
	FixtureID    string `json:"fixtureId"`
	FixtureReset string `json:"fixtureReset,omitempty"`
}

// Validate ensures all required identity metadata fields are present and non-empty.
func (m RecordMetadata) Validate() error {
	if strings.TrimSpace(m.EndpointID) == "" {
		return errors.New("missing required endpoint identity metadata")
	}
	if strings.TrimSpace(m.SourceID) == "" {
		return errors.New("missing required source identity metadata")
	}
	if strings.TrimSpace(m.FixtureID) == "" {
		return errors.New("missing required fixture identity metadata; fixture provisioning and reset are caller-owned")
	}
	return nil
}
