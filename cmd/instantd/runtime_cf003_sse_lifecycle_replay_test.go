package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// sseLifecycleCapture is the checked-in raw SSE capture
// (corpus/refresh-sse-lifecycle.json) bound to the refresh-sse-lifecycle
// coverage row by its self-bound metadata envelope (scenario == id ==
// refresh-sse-lifecycle, dedicated fixture refresh-sse-lifecycle,
// transport sse). Connects and POST exchanges carry exact status/body bytes;
// stream records carry parsed data-payload objects with session-bearing
// values replaced by the documented sentinel placeholders (see the capture's
// redaction rule). Heartbeat comment records are excluded by that rule.
type sseLifecycleCapture struct {
	Scenario  string `json:"scenario"`
	ID        string `json:"id"`
	Fixture   string `json:"fixture"`
	Transport string `json:"transport"`
	Status    string `json:"status"`
	Connects  []struct {
		Request struct {
			Method string `json:"method"`
			Target string `json:"target"`
		} `json:"request"`
		Response struct {
			Status  int                 `json:"status"`
			Headers map[string][]string `json:"headers"`
		} `json:"response"`
	} `json:"connects"`
	Posts []struct {
		Request struct {
			Method  string              `json:"method"`
			Target  string              `json:"target"`
			Headers map[string][]string `json:"headers"`
			Body    string              `json:"body"`
		} `json:"request"`
		Response struct {
			Status  int                 `json:"status"`
			Headers map[string][]string `json:"headers"`
			Body    string              `json:"body"`
		} `json:"response"`
	} `json:"posts"`
	Stream []struct {
		Connection int            `json:"connection"`
		Data       map[string]any `json:"data"`
	} `json:"stream"`
}

// Sentinel placeholders for session-bearing values. None is UUID-shaped and
// none carries the live "sess-" prefix, so a checked-in real session value
// fails the mechanical pre-checks below before any byte is replayed.
const (
	sseLifecycleTransport1 = "<sse-transport-session-1>"
	sseLifecycleProtocol1  = "<sse-protocol-session-1>"
	sseLifecycleToken1     = "<sse-token-1>"
	sseLifecycleMachine    = "<sse-machine-id>"
	sseLifecycleTransport2 = "<sse-transport-session-2>"
	sseLifecycleProtocol2  = "<sse-protocol-session-2>"
	sseLifecycleToken2     = "<sse-token-2>"
	sseLifecycleAttrID     = "<sse-attr-id-todos-id>"
	sseLifecycleAttrTitle  = "<sse-attr-id-todos-title>"
)

// cf003AssertSSEQuiet proves terminal quiescence by a bounded window: any
// data frame arriving inside the window fails the capture, and the wait
// never extends past the bound. Heartbeat comment records (":"-prefixed,
// e.g. ": ping") and blank separators are skipped, never treated as data;
// premature stream termination (EOF/close) inside the window is a failure —
// the stream must stay open and quiet. The 250ms bound matches the FU-01
// quiescence idiom: well under the 20s production heartbeat cadence and
// the 15s test context, well over expected intra-test propagation. On the
// quiet path the abandoned reader stays blocked only until the caller
// closes the stream body (which unblocks Scanner.Scan), then exits via the
// buffered channel; no further reads may follow on that scanner.
func cf003AssertSSEQuiet(t *testing.T, scanner *bufio.Scanner, name string, window time.Duration) {
	t.Helper()
	type outcome struct {
		frame map[string]any
		err   error
		ended bool
	}
	ch := make(chan outcome, 1)
	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var frame map[string]any
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &frame); err != nil {
				ch <- outcome{err: err}
				return
			}
			ch <- outcome{frame: frame}
			return
		}
		ch <- outcome{ended: true}
	}()
	select {
	case out := <-ch:
		switch {
		case out.err != nil:
			t.Fatalf("SSE quiet %s: decode error: %v", name, out.err)
		case out.ended:
			t.Fatalf("SSE quiet %s: stream ended inside quiet window", name)
		default:
			t.Fatalf("SSE quiet %s: unexpected frame inside quiet window: %#v", name, out.frame)
		}
	case <-time.After(window):
	}
}

// TestCF003SSERefreshLifecycleCaptureReplay replays the checked-in
// refresh-sse-lifecycle raw capture against the production-mounted
// GET /runtime/sse + POST /runtime/sse routes on the owned isolated
// refresh-sse-lifecycle fixture (the deterministic cf003 seed triple: app
// 00000000-0000-4000-8000-000000000003). One subscriber only: two strictly
// sequential connections; the second dials only after the first session's
// terminal 401 teardown is observed. Every connect asserts exact status and
// exact Content-Type/Cache-Control; every POST asserts exact status, exact
// whole-body bytes, and exact Content-Type in file order; all 9 stream data
// frames assert whole-result reflect.DeepEqual under the masking rule, with
// exact float64 processed-tx-id comparison (initial trees exactly 0, refresh
// exactly the live trigger tx-id, reconnect tree exactly 0 with the refresh
// title). Terminal quiescence is proven by a bounded 250ms quiet window on
// each connection immediately after its final expected frame (see
// cf003AssertSSEQuiet): the stream must stay open with no further data
// frame. The seed/trigger admin transacts run live (tx-id watermarks and
// X-admin-token never checked in) with the same shape as the accepted
// assembled leg. FU-02 Option A report-only; no corpusctl behavior change, no
// second subscriber, no WS leg, no v1 claim.
func TestCF003SSERefreshLifecycleCaptureReplay(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "corpus", "refresh-sse-lifecycle.json"))
	if err != nil {
		t.Fatal(err)
	}
	var capture sseLifecycleCapture
	if err := json.Unmarshal(raw, &capture); err != nil {
		t.Fatalf("capture decode: %v", err)
	}
	if capture.Scenario != "refresh-sse-lifecycle" || capture.ID != "refresh-sse-lifecycle" ||
		capture.Fixture != "refresh-sse-lifecycle" || capture.Transport != "sse" || capture.Status != "covered" {
		t.Fatalf("capture envelope = %q/%q/%q/%q/%q; want refresh-sse-lifecycle/refresh-sse-lifecycle/refresh-sse-lifecycle/sse/covered",
			capture.Scenario, capture.ID, capture.Fixture, capture.Transport, capture.Status)
	}
	if len(capture.Connects) != 2 {
		t.Fatalf("capture connects = %d; want exactly 2 (initial + reconnect, strictly sequential)", len(capture.Connects))
	}
	if len(capture.Posts) != 5 {
		t.Fatalf("capture posts = %d; want exactly 5 (init, add-query, terminal teardown 401, reconnect init, reconnect add-query)", len(capture.Posts))
	}
	if len(capture.Stream) != 9 {
		t.Fatalf("capture stream frames = %d; want exactly 9 (5 on connection 1, 4 on connection 2)", len(capture.Stream))
	}
	// Single-subscriber shape: connection tags must be exactly five 1s
	// followed by four 2s — any second concurrent subscriber would need a
	// third tag or interleaved frames.
	for i, record := range capture.Stream {
		wantConn := 1
		if i >= 5 {
			wantConn = 2
		}
		if record.Connection != wantConn {
			t.Fatalf("stream frame %d connection = %d; want %d (single sequential subscriber)", i, record.Connection, wantConn)
		}
	}

	// Mechanical masking-rule pre-checks: every checked-in template must
	// carry its sentinels and no real session value (any sess- substring)
	// may appear in the checked-in bytes.
	mustMasked := func(where, template string, sentinels ...string) {
		t.Helper()
		if strings.Contains(template, "sess-") {
			t.Fatalf("%s carries a real session value: %q", where, template)
		}
		for _, sentinel := range sentinels {
			if !strings.Contains(template, sentinel) {
				t.Fatalf("%s lacks masking sentinel %q", where, sentinel)
			}
		}
	}
	// unescapedJSON marshals without Go's default HTML escaping so sentinel
	// angle brackets survive string pre-checks exactly as checked in.
	unescapedJSON := func(value any) string {
		t.Helper()
		var sb strings.Builder
		encoder := json.NewEncoder(&sb)
		encoder.SetEscapeHTML(false)
		if err := encoder.Encode(value); err != nil {
			t.Fatalf("JSON encode: %v", err)
		}
		return sb.String()
	}
	for i, post := range capture.Posts {
		mustMasked(
			"post body",
			post.Request.Body,
			map[int][]string{
				0: {sseLifecycleTransport1, sseLifecycleToken1},
				1: {sseLifecycleTransport1, sseLifecycleToken1},
				2: {sseLifecycleTransport1, sseLifecycleToken1},
				3: {sseLifecycleTransport2, sseLifecycleToken2},
				4: {sseLifecycleTransport2, sseLifecycleToken2},
			}[i]...,
		)
		if post.Request.Method != http.MethodPost || post.Request.Target != "/runtime/sse" {
			t.Fatalf("post %d method/target = %q %q; want POST /runtime/sse", i, post.Request.Method, post.Request.Target)
		}
		if got := post.Request.Headers["Content-Type"]; !reflect.DeepEqual(got, []string{"application/json"}) {
			t.Fatalf("post %d request Content-Type = %q; want exactly [application/json]", i, got)
		}
	}
	for i, record := range capture.Stream {
		encoded := unescapedJSON(record.Data)
		switch i {
		case 0:
			mustMasked("handshake 1", string(encoded), sseLifecycleMachine, sseLifecycleTransport1, sseLifecycleToken1)
		case 1:
			mustMasked("protocol-init 1", string(encoded), sseLifecycleProtocol1, sseLifecycleAttrID, sseLifecycleAttrTitle)
		case 4:
			mustMasked("refresh", string(encoded), sseLifecycleAttrID, sseLifecycleAttrTitle)
		case 5:
			mustMasked("handshake 2", string(encoded), sseLifecycleMachine, sseLifecycleTransport2, sseLifecycleToken2)
		case 6:
			mustMasked("protocol-init 2", string(encoded), sseLifecycleProtocol2, sseLifecycleAttrID, sseLifecycleAttrTitle)
		}
	}

	// The capture's id and fixture must match the manifest coverage row that
	// claims it: a borrowed scenario or a mismatched fixture is rejected here
	// before any byte is replayed.
	manifestRaw, err := os.ReadFile(filepath.Join("..", "..", "corpus", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Coverage []struct {
			ID        string `json:"id"`
			Scenario  string `json:"scenario"`
			Fixture   string `json:"fixture"`
			Transport string `json:"transport"`
			Status    string `json:"status"`
		} `json:"coverage"`
	}
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		t.Fatalf("manifest decode: %v", err)
	}
	var row *struct {
		ID        string `json:"id"`
		Scenario  string `json:"scenario"`
		Fixture   string `json:"fixture"`
		Transport string `json:"transport"`
		Status    string `json:"status"`
	}
	for i := range manifest.Coverage {
		if manifest.Coverage[i].ID == capture.ID {
			row = &manifest.Coverage[i]
			break
		}
	}
	if row == nil {
		t.Fatalf("manifest has no coverage row for capture id %q", capture.ID)
	}
	if capture.Scenario != row.Scenario || capture.Fixture != row.Fixture ||
		capture.Transport != row.Transport || capture.Status != row.Status {
		t.Fatalf("capture envelope %q/%q/%q/%q does not match manifest row %q/%q/%q/%q",
			capture.Scenario, capture.Fixture, capture.Transport, capture.Status,
			row.Scenario, row.Fixture, row.Transport, row.Status)
	}

	// The SSE leg provisions the identical deterministic cf003 seed triple
	// declared by corpus/fixtures/refresh-sse-lifecycle.json, so
	// cf003PostgresMux is reused here rather than duplicated.
	mux, appID, adminToken := cf003PostgresMux(t)
	appStr := platform.UUIDToStr(appID)

	// The declared fixture must name the actual capture/replay identity: the
	// fixture file's appId, creatorId, adminToken, and txSteps must equal the
	// live replay seed (deterministic triple app
	// 00000000-0000-4000-8000-000000000003, creator
	// 00000000-0000-4000-8000-000000000001, admin
	// 00000000-0000-4000-8000-000000000002, plus exactly-empty txSteps)
	// declared by the seed helper. A fixture edited to a foreign identity
	// fails here before any byte is replayed.
	fixtureRaw, err := os.ReadFile(filepath.Join("..", "..", "corpus", "fixtures", "refresh-sse-lifecycle.json"))
	if err != nil {
		t.Fatal(err)
	}
	var declaredFixture struct {
		AppID      string `json:"appId"`
		CreatorID  string `json:"creatorId"`
		AdminToken string `json:"adminToken"`
		TxSteps    []any  `json:"txSteps"`
	}
	if err := json.Unmarshal(fixtureRaw, &declaredFixture); err != nil {
		t.Fatalf("fixture decode: %v", err)
	}
	if declaredFixture.AppID != appStr {
		t.Fatalf("declared fixture appId = %q; want replay app %q", declaredFixture.AppID, appStr)
	}
	if declaredFixture.CreatorID != cf003CreatorID {
		t.Fatalf("declared fixture creatorId = %q; want seed creator %q", declaredFixture.CreatorID, cf003CreatorID)
	}
	if declaredFixture.AdminToken != cf003AdminToken || declaredFixture.AdminToken != adminToken {
		t.Fatalf("declared fixture adminToken = %q; want seed admin %q", declaredFixture.AdminToken, cf003AdminToken)
	}
	if declaredFixture.TxSteps == nil || len(declaredFixture.TxSteps) != 0 {
		t.Fatalf("declared fixture txSteps = %#v; want exactly empty", declaredFixture.TxSteps)
	}
	for i, connect := range capture.Connects {
		wantTarget := "/runtime/sse?app_id=" + appStr
		if connect.Request.Method != http.MethodGet || connect.Request.Target != wantTarget {
			t.Fatalf("connect %d method/target = %q %q; want GET %q", i, connect.Request.Method, connect.Request.Target, wantTarget)
		}
	}

	// Live substitution maps, filled as each connection's credentials and
	// attr IDs are observed. POST templates render through them; live
	// frames are masked back through them before DeepEqual.
	live := map[string]string{}
	render := func(template string) string {
		t.Helper()
		out := template
		for sentinel, value := range live {
			out = strings.ReplaceAll(out, sentinel, value)
		}
		if strings.Contains(out, "<sse-") {
			t.Fatalf("unsubstituted sentinel remains in rendered body: %q", out)
		}
		return out
	}
	maskFrame := func(frame map[string]any) map[string]any {
		t.Helper()
		encoded, err := json.Marshal(frame)
		if err != nil {
			t.Fatalf("live frame is not JSON: %v", err)
		}
		masked := string(encoded)
		for sentinel, value := range live {
			// live maps sentinel->value; masking inverts it, and only
			// for values actually observed (non-empty).
			if value == "" {
				t.Fatalf("masking with unobserved value for %q", sentinel)
			}
			masked = strings.ReplaceAll(masked, value, sentinel)
		}
		var out map[string]any
		if err := json.Unmarshal([]byte(masked), &out); err != nil {
			t.Fatalf("masked frame is not JSON: %v", err)
		}
		return out
	}
	assertStreamFrame := func(index int, frame map[string]any) {
		t.Helper()
		masked := maskFrame(frame)
		if !reflect.DeepEqual(masked, capture.Stream[index].Data) {
			want, _ := json.Marshal(capture.Stream[index].Data)
			got, _ := json.Marshal(masked)
			t.Fatalf("stream frame %d masked = %s; want exactly %s", index, got, want)
		}
	}

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	// The seed transact is excluded from the checked-in bytes (tx-id
	// watermark, credential-bearing admin header), so it runs live with the
	// same shape as the accepted assembled leg: one fixed-UUID todo over
	// /admin/transact, 200 with a positive tx-id asserted live.
	seedStatus, seedBody, _ := cf003Serve(mux, http.MethodPost, "/admin/transact", cf003JSON(t, map[string]any{
		"app-id": appStr,
		"steps":  []any{[]any{"update", "todos", cf003EntityID, map[string]any{"title": "sse-initial"}}},
	}), map[string]string{"X-admin-token": adminToken})
	if seedStatus != http.StatusOK {
		t.Fatalf("SSE seed transact = %d %q; want 200", seedStatus, seedBody)
	}
	var seedTx struct {
		TxID int64 `json:"tx-id"`
	}
	if err := json.Unmarshal([]byte(seedBody), &seedTx); err != nil || seedTx.TxID <= 0 {
		t.Fatalf("SSE seed body = %q; want positive tx-id", seedBody)
	}

	connect := func(tag string, index int) (*http.Response, *bufio.Scanner) {
		t.Helper()
		target := server.URL + capture.Connects[index].Request.Target
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		want := capture.Connects[index].Response
		if resp.StatusCode != want.Status {
			_ = resp.Body.Close()
			t.Fatalf("%s GET status = %d; want %d", tag, resp.StatusCode, want.Status)
		}
		for key, values := range want.Headers {
			if got := resp.Header.Values(key); !reflect.DeepEqual(got, values) {
				_ = resp.Body.Close()
				t.Fatalf("%s GET header %q = %q; want exactly %q", tag, key, got, values)
			}
		}
		return resp, bufio.NewScanner(resp.Body)
	}
	post := func(tag string, index int, body string) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+capture.Posts[index].Request.Target, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		for key, values := range capture.Posts[index].Request.Headers {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}
		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		payload, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		want := capture.Posts[index].Response
		if resp.StatusCode != want.Status || string(payload) != want.Body {
			t.Fatalf("%s POST status/body = %d %q; want %d %q", tag, resp.StatusCode, payload, want.Status, want.Body)
		}
		for key, values := range want.Headers {
			if got := resp.Header.Values(key); !reflect.DeepEqual(got, values) {
				t.Fatalf("%s POST header %q = %q; want exactly %q", tag, key, got, values)
			}
		}
	}

	// Connection 1: exact handshake, protocol-init attrs, add-query ack,
	// initial full tree.
	resp, scanner := connect("conn1", 0)
	hello := cf003ReadSSE(t, scanner)
	transport1, _ := hello["session-id"].(string)
	token1, _ := hello["sse-token"].(string)
	machine1, _ := hello["machine-id"].(string)
	if len(hello) != 4 || hello["op"] != "init-ok" || machine1 == "" || transport1 == "" || token1 == "" {
		_ = resp.Body.Close()
		t.Fatalf("SSE handshake 1 = %#v", hello)
	}
	for _, value := range []string{transport1, token1, machine1} {
		if strings.Contains(value, "<sse-") {
			_ = resp.Body.Close()
			t.Fatalf("live handshake value collides with sentinel space: %q", value)
		}
	}
	live[sseLifecycleTransport1] = transport1
	live[sseLifecycleToken1] = token1
	live[sseLifecycleMachine] = machine1
	assertStreamFrame(0, hello)

	post("conn1-init", 0, render(capture.Posts[0].Request.Body))
	initFrame := cf003ReadSSE(t, scanner)
	protocol1, _ := initFrame["session-id"].(string)
	if protocol1 == "" || protocol1 == transport1 {
		_ = resp.Body.Close()
		t.Fatalf("SSE protocol session 1 = %q (transport %q); want distinct non-empty protocol session", protocol1, transport1)
	}
	live[sseLifecycleProtocol1] = protocol1
	idAttr, titleAttr := cf003AssertSSEInit(t, initFrame, appStr)
	live[sseLifecycleAttrID] = idAttr
	live[sseLifecycleAttrTitle] = titleAttr
	assertStreamFrame(1, initFrame)

	post("conn1-add-query", 1, render(capture.Posts[1].Request.Body))
	addQueryOK := cf003ReadSSE(t, scanner)
	cf003AssertSSEAddQuery(t, addQueryOK, "cf003-add-query")
	assertStreamFrame(2, addQueryOK)
	tree := cf003ReadSSE(t, scanner)
	cf003AssertSSETodos(t, tree, "sse-initial", true)
	assertStreamFrame(3, tree)

	// Admin-transaction-driven refresh: the trigger transact runs live (same
	// exclusion as the seed) and the refresh frame must carry exactly the
	// triggering transaction ID as a float64 watermark plus exact attr IDs.
	triggerStatus, triggerBody, _ := cf003Serve(mux, http.MethodPost, "/admin/transact", cf003JSON(t, map[string]any{
		"app-id": appStr,
		"steps":  []any{[]any{"update", "todos", cf003EntityID, map[string]any{"title": "sse-refresh"}}},
	}), map[string]string{"X-admin-token": adminToken})
	if triggerStatus != http.StatusOK {
		t.Fatalf("SSE trigger transact = %d %q; want 200", triggerStatus, triggerBody)
	}
	var triggerTx struct {
		TxID int64 `json:"tx-id"`
	}
	if err := json.Unmarshal([]byte(triggerBody), &triggerTx); err != nil || triggerTx.TxID <= 0 {
		t.Fatalf("SSE trigger body = %q; want positive tx-id", triggerBody)
	}
	refresh := cf003ReadSSE(t, scanner)
	cf003AssertSSERefreshTodo(t, refresh, idAttr, titleAttr, "sse-refresh", triggerTx.TxID)
	if txID, ok := refresh["processed-tx-id"].(float64); !ok || txID != float64(triggerTx.TxID) || int64(txID) != triggerTx.TxID {
		t.Fatalf("SSE refresh watermark = %#v; want exactly float64(%d)", refresh["processed-tx-id"], triggerTx.TxID)
	}
	if txID, ok := capture.Stream[4].Data["processed-tx-id"].(float64); !ok || txID != float64(triggerTx.TxID) {
		t.Fatalf("checked-in refresh watermark = %#v; want exactly float64(%d) (deterministic trigger tx-id)", capture.Stream[4].Data["processed-tx-id"], triggerTx.TxID)
	}
	assertStreamFrame(4, refresh)

	// Terminal quiescence on connection 1: the bounded reader outcome is
	// decided before the body is closed below, so a duplicate/spurious
	// data frame right after the refresh fails here instead of passing
	// silently on a closed-unread stream.
	cf003AssertSSEQuiet(t, scanner, "conn1", 250*time.Millisecond)

	// Old-session teardown: close the stream, then poll the exact
	// checked-in empty-message body until the terminal 401 lands.
	// Intermediate 200s are timing-dependent in count and are excluded from
	// the capture; any other shape fails here.
	_ = resp.Body.Close()
	teardownBody := render(capture.Posts[2].Request.Body)
	deadline := time.Now().Add(2 * time.Second)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/runtime/sse", strings.NewReader(teardownBody))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		teardownResp, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		payload, readErr := io.ReadAll(teardownResp.Body)
		_ = teardownResp.Body.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if teardownResp.StatusCode == http.StatusUnauthorized {
			want := capture.Posts[2].Response
			if string(payload) != want.Body {
				t.Fatalf("old SSE rejection body = %q; want exactly %q", payload, want.Body)
			}
			if got := teardownResp.Header.Values("Content-Type"); !reflect.DeepEqual(got, want.Headers["Content-Type"]) {
				t.Fatalf("old SSE rejection Content-Type = %q; want %q", got, want.Headers["Content-Type"])
			}
			break
		}
		if teardownResp.StatusCode != http.StatusOK || string(payload) != "{}\n" || time.Now().After(deadline) {
			t.Fatalf("old SSE session remained live: %d %q", teardownResp.StatusCode, payload)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Connection 2 dials only after the teardown 401 above: single
	// subscriber, strictly sequential. Fresh credentials are required and
	// the converged state must carry the refresh title with stable attrs.
	resp2, scanner2 := connect("conn2", 1)
	defer func() { _ = resp2.Body.Close() }()
	hello2 := cf003ReadSSE(t, scanner2)
	transport2, _ := hello2["session-id"].(string)
	token2, _ := hello2["sse-token"].(string)
	machine2, _ := hello2["machine-id"].(string)
	if len(hello2) != 4 || hello2["op"] != "init-ok" || machine2 == "" || transport2 == "" || token2 == "" {
		t.Fatalf("SSE handshake 2 = %#v", hello2)
	}
	if transport2 == transport1 || token2 == token1 {
		t.Fatalf("reconnect reused credentials: session=%q token=%q", transport2, token2)
	}
	if machine2 != machine1 {
		t.Fatalf("reconnect machine changed: %q vs %q (same process must share boot id)", machine2, machine1)
	}
	live[sseLifecycleTransport2] = transport2
	live[sseLifecycleToken2] = token2
	assertStreamFrame(5, hello2)

	post("conn2-init", 3, render(capture.Posts[3].Request.Body))
	initFrame2 := cf003ReadSSE(t, scanner2)
	protocol2, _ := initFrame2["session-id"].(string)
	if protocol2 == "" || protocol2 == transport2 {
		t.Fatalf("SSE protocol session 2 = %q (transport %q); want distinct non-empty protocol session", protocol2, transport2)
	}
	live[sseLifecycleProtocol2] = protocol2
	reconnectedIDAttr, reconnectedTitleAttr := cf003AssertSSEInit(t, initFrame2, appStr)
	if reconnectedIDAttr != idAttr || reconnectedTitleAttr != titleAttr {
		t.Fatalf("reconnect attr identity drift: id=%q/%q title=%q/%q", idAttr, reconnectedIDAttr, titleAttr, reconnectedTitleAttr)
	}
	assertStreamFrame(6, initFrame2)

	post("conn2-add-query", 4, render(capture.Posts[4].Request.Body))
	reconnectQueryOK := cf003ReadSSE(t, scanner2)
	cf003AssertSSEAddQuery(t, reconnectQueryOK, "cf003-reconnect-query")
	assertStreamFrame(7, reconnectQueryOK)
	retree := cf003ReadSSE(t, scanner2)
	cf003AssertSSETodos(t, retree, "sse-refresh", false)
	if txID, ok := retree["processed-tx-id"].(float64); !ok || txID != 0 {
		t.Fatalf("reconnect tree watermark = %#v; want exactly 0", retree["processed-tx-id"])
	}
	if txID, ok := capture.Stream[8].Data["processed-tx-id"].(float64); !ok || txID != 0 {
		t.Fatalf("checked-in reconnect watermark = %#v; want exactly 0", capture.Stream[8].Data["processed-tx-id"])
	}
	assertStreamFrame(8, retree)

	// Terminal quiescence on connection 2: the stream must stay open and
	// quiet for the bounded window (the deferred body close runs only
	// after this outcome is decided), so a spurious data frame right
	// after the reconnect tree fails here instead of passing unread.
	cf003AssertSSEQuiet(t, scanner2, "conn2", 250*time.Millisecond)

	// No unclaimed mutations: the store holds exactly the refresh title on
	// the single seeded entity, the old session is still rejected, and the
	// reconnected session is still live.
	queryStatus, queryBody, _ := cf003Serve(mux, http.MethodPost, "/runtime/framework/query",
		`{"query":{"todos":{}}}`, map[string]string{"app-id": appStr})
	wantQuery := `{"data":{"todos":[{"id":"` + cf003EntityID + `","title":"sse-refresh"}]}}` + "\n"
	if queryStatus != http.StatusOK || queryBody != wantQuery {
		t.Fatalf("final query status/body = %d %q; want %d %q", queryStatus, queryBody, http.StatusOK, wantQuery)
	}
	post("old-session-still-dead", 2, teardownBody)
	// The reconnected session is still live: the same empty-message shape
	// with connection-2 credentials returns the exact empty envelope.
	stillLiveBody := render(strings.ReplaceAll(strings.ReplaceAll(
		capture.Posts[2].Request.Body, sseLifecycleTransport1, sseLifecycleTransport2),
		sseLifecycleToken1, sseLifecycleToken2))
	stillLiveReq, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/runtime/sse", strings.NewReader(stillLiveBody))
	if err != nil {
		t.Fatal(err)
	}
	stillLiveReq.Header.Set("Content-Type", "application/json")
	stillLiveResp, err := server.Client().Do(stillLiveReq)
	if err != nil {
		t.Fatal(err)
	}
	stillLivePayload, readErr := io.ReadAll(stillLiveResp.Body)
	_ = stillLiveResp.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if stillLiveResp.StatusCode != http.StatusOK || string(stillLivePayload) != "{}\n" ||
		stillLiveResp.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("reconnected session liveness = %d %q %q; want 200 {}\\n application/json",
			stillLiveResp.StatusCode, stillLivePayload, stillLiveResp.Header.Get("Content-Type"))
	}
}
