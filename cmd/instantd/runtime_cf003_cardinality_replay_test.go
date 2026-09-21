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

// cardinalityCapture is the checked-in raw SSE capture
// (corpus/transactions-cardinality-boundary.json) bound to the
// transactions-cardinality-boundary coverage row by its self-bound metadata
// envelope (scenario == id == transactions-cardinality-boundary, dedicated
// fixture transactions-cardinality-boundary, transport sse). The single
// connect and every POST exchange carry exact status/body bytes; stream
// records carry parsed data-payload objects with session-bearing values
// replaced by the documented sentinel placeholders (see the capture's
// redaction rule). Heartbeat comment records are excluded by that rule.
// HTTP query convergence bodies are not checked in: they are the live
// convergence oracle and are asserted whole-body with whole-result DeepEqual.
type cardinalityCapture struct {
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
	cardinalityTransport = "<card-transport-session>"
	cardinalityToken     = "<card-token>"
	cardinalityMachine   = "<card-machine-id>"
	cardinalityProtocol  = "<card-protocol-session>"
	cardinalityAttrID    = "<card-attr-id-todos-id>"
	cardinalityAttrTitle = "<card-attr-id-todos-title>"
)

// cardinalityProbeEntityID is the throwaway entity of the terminal
// exactly-once watermark probe. It is disjoint from the seeded entity
// (...0004) so the probe mints exactly one transaction without touching the
// asserted final state; each replay runs on a fresh isolated DB, so the
// fixed UUID replays deterministically.
const cardinalityProbeEntityID = "00000000-0000-4000-8000-000000000044"

// TestCF003CardinalityCaptureReplay replays the checked-in
// transactions-cardinality-boundary raw capture against the
// production-mounted GET /runtime/sse + POST /runtime/sse routes on the owned
// isolated transactions-cardinality-boundary fixture (the deterministic cf003
// seed triple: app 00000000-0000-4000-8000-000000000003). One subscriber only:
// a single SSE connection reproducing the accepted assembled SSE leg step for
// step (seed, init, positive transact, exact denial, same-batch
// cardinality-one batch). The connect asserts exact status and exact
// Content-Type/Cache-Control; every POST asserts exact status, exact
// whole-body bytes, and exact Content-Type in file order; all 5 stream data
// frames assert whole-result reflect.DeepEqual under the masking rule, with
// exact float64 transact-ok watermarks (checked-in 2 and 3 asserted exactly
// equal to the live tx-ids, strictly advancing past the live seed tx).
// Convergence is proven live over the production-mounted POST
// /runtime/framework/query path with exact whole-body bytes and whole-result
// DeepEqual lengths-before-elements: baseline, positive write, post-denial
// byte-identical (mutates nothing), and the cardinality final value winning
// exactly once. Terminal quiescence is proven by a bounded 250ms quiet window
// after the final frame (see cf003AssertSSEQuiet), and the dead-reader
// teardown lands the exact checked-in 401. Exactly-once is proven by a
// terminal watermark probe: final-state DeepEqual alone cannot distinguish
// exactly-once application from duplicate-committed-same-value, so an
// isolated probe transact must return exactly tx-id 4 (seed 1, sse-t1 2,
// sse-card 3, denial minting nothing). The seed admin transact runs live
// (tx-id watermark and X-admin-token never checked in) with the same shape as
// the accepted assembled leg. FU-02 Option A report-only; no corpusctl
// behavior change, no second subscriber, no WS leg, no v1 claim. The WS
// variant of this row remains excluded by enforcement: no WS evidence is
// checked in and no WS claim is made. Merge/cascade/required legs remain
// unflipped as future work (see the manifest row residual): no accepted
// assembled SSE-capturable leg proves deep-merge preservation convergence,
// cascade-delete convergence, or required-field denial over the
// single-subscriber SSE path.
func TestCF003CardinalityCaptureReplay(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "corpus", "transactions-cardinality-boundary.json"))
	if err != nil {
		t.Fatal(err)
	}
	var capture cardinalityCapture
	if err := json.Unmarshal(raw, &capture); err != nil {
		t.Fatalf("capture decode: %v", err)
	}
	if capture.Scenario != "transactions-cardinality-boundary" || capture.ID != "transactions-cardinality-boundary" ||
		capture.Fixture != "transactions-cardinality-boundary" || capture.Transport != "sse" || capture.Status != "covered" {
		t.Fatalf("capture envelope = %q/%q/%q/%q/%q; want transactions-cardinality-boundary/transactions-cardinality-boundary/transactions-cardinality-boundary/sse/covered",
			capture.Scenario, capture.ID, capture.Fixture, capture.Transport, capture.Status)
	}
	if len(capture.Connects) != 1 {
		t.Fatalf("capture connects = %d; want exactly 1 (single subscriber, single connection)", len(capture.Connects))
	}
	if len(capture.Posts) != 5 {
		t.Fatalf("capture posts = %d; want exactly 5 (init, positive transact, denial transact, cardinality batch, terminal teardown 401)", len(capture.Posts))
	}
	if len(capture.Stream) != 5 {
		t.Fatalf("capture stream frames = %d; want exactly 5 (handshake, protocol-init, transact-ok, denial, cardinality transact-ok)", len(capture.Stream))
	}
	// Single-subscriber shape: every frame rides connection 1 — any second
	// concurrent subscriber would need a second tag or interleaved frames.
	for i, record := range capture.Stream {
		if record.Connection != 1 {
			t.Fatalf("stream frame %d connection = %d; want 1 (single subscriber)", i, record.Connection)
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
				0: {cardinalityTransport, cardinalityToken},
				1: {cardinalityTransport, cardinalityToken, cardinalityAttrTitle},
				2: {cardinalityTransport, cardinalityToken},
				3: {cardinalityTransport, cardinalityToken, cardinalityAttrTitle},
				4: {cardinalityTransport, cardinalityToken},
			}[i]...,
		)
		if post.Request.Method != http.MethodPost || post.Request.Target != "/runtime/sse" {
			t.Fatalf("post %d method/target = %q %q; want POST /runtime/sse", i, post.Request.Method, post.Request.Target)
		}
		if got := post.Request.Headers["Content-Type"]; !reflect.DeepEqual(got, []string{"application/json"}) {
			t.Fatalf("post %d request Content-Type = %q; want exactly [application/json]", i, got)
		}
	}
	// The denial step carries a single bare entity arg: it must not smuggle
	// an attr UUID past the masking rule, and the positive/cardinality steps
	// must mask exactly their title-attr positions (1 and 2 occurrences).
	if strings.Contains(capture.Posts[2].Request.Body, cardinalityAttrID) ||
		strings.Contains(capture.Posts[2].Request.Body, cardinalityAttrTitle) {
		t.Fatalf("denial post body must carry no attr sentinel: %q", capture.Posts[2].Request.Body)
	}
	if got := strings.Count(capture.Posts[1].Request.Body, cardinalityAttrTitle); got != 1 {
		t.Fatalf("positive post title sentinel occurs %d times; want exactly 1", got)
	}
	if got := strings.Count(capture.Posts[3].Request.Body, cardinalityAttrTitle); got != 2 {
		t.Fatalf("cardinality post title sentinel occurs %d times; want exactly 2", got)
	}
	if strings.Contains(capture.Posts[1].Request.Body, cardinalityAttrID) ||
		strings.Contains(capture.Posts[3].Request.Body, cardinalityAttrID) {
		t.Fatalf("transact posts must mask only the title attr, never the id attr")
	}
	for i, record := range capture.Stream {
		encoded := unescapedJSON(record.Data)
		switch i {
		case 0:
			mustMasked("handshake", encoded, cardinalityMachine, cardinalityTransport, cardinalityToken)
		case 1:
			mustMasked("protocol-init", encoded, cardinalityProtocol, cardinalityAttrID, cardinalityAttrTitle)
		}
		if strings.Contains(encoded, "sess-") {
			t.Fatalf("stream frame %d carries a real session value", i)
		}
	}
	// The transact-ok watermarks are deterministic across fresh isolated DBs
	// (seed tx 1, denial minting nothing): the capture pins exactly 2 and 3
	// and the live run must equal them as float64 below.
	for i, wantTx := range map[int]float64{2: 2, 4: 3} {
		txID, ok := capture.Stream[i].Data["tx-id"].(float64)
		if !ok || txID != wantTx {
			t.Fatalf("checked-in stream frame %d tx-id = %#v; want exactly float64(%v)", i, capture.Stream[i].Data["tx-id"], wantTx)
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
	if row.Transport != "sse" {
		t.Fatalf("manifest row transport = %q; this SSE-variant flip requires exactly sse", row.Transport)
	}

	// The cardinality leg provisions the identical deterministic cf003 seed
	// triple declared by corpus/fixtures/transactions-cardinality-boundary.json,
	// so cf003PostgresMux is reused here rather than duplicated.
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
	fixtureRaw, err := os.ReadFile(filepath.Join("..", "..", "corpus", "fixtures", "transactions-cardinality-boundary.json"))
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
	wantTarget := "/runtime/sse?app_id=" + appStr
	if connect := capture.Connects[0]; connect.Request.Method != http.MethodGet || connect.Request.Target != wantTarget {
		t.Fatalf("connect method/target = %q %q; want GET %q", connect.Request.Method, connect.Request.Target, wantTarget)
	}

	// Live substitution maps, filled as the connection's credentials and
	// attr IDs are observed. POST templates render through them; live
	// frames are masked back through them before DeepEqual.
	live := map[string]string{}
	render := func(template string) string {
		t.Helper()
		out := template
		for sentinel, value := range live {
			out = strings.ReplaceAll(out, sentinel, value)
		}
		if strings.Contains(out, "<card-") {
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

	// The SSE connection opens before seeding, mirroring the accepted
	// assembled leg: the transport handshake carries no catalog, so the
	// protocol-init below observes the seeded identity.
	target := server.URL + capture.Connects[0].Request.Target
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	wantConnect := capture.Connects[0].Response
	if resp.StatusCode != wantConnect.Status {
		_ = resp.Body.Close()
		t.Fatalf("GET status = %d; want %d", resp.StatusCode, wantConnect.Status)
	}
	for key, values := range wantConnect.Headers {
		if got := resp.Header.Values(key); !reflect.DeepEqual(got, values) {
			_ = resp.Body.Close()
			t.Fatalf("GET header %q = %q; want exactly %q", key, got, values)
		}
	}
	scanner := bufio.NewScanner(resp.Body)
	hello := cf003ReadSSE(t, scanner)
	transport, _ := hello["session-id"].(string)
	token, _ := hello["sse-token"].(string)
	machine, _ := hello["machine-id"].(string)
	if len(hello) != 4 || hello["op"] != "init-ok" || machine == "" || transport == "" || token == "" {
		_ = resp.Body.Close()
		t.Fatalf("SSE handshake = %#v", hello)
	}
	for _, value := range []string{transport, token, machine} {
		if strings.Contains(value, "<card-") {
			_ = resp.Body.Close()
			t.Fatalf("live handshake value collides with sentinel space: %q", value)
		}
	}
	live[cardinalityTransport] = transport
	live[cardinalityToken] = token
	live[cardinalityMachine] = machine
	assertStreamFrame(0, hello)

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
		postResp, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		payload, readErr := io.ReadAll(postResp.Body)
		_ = postResp.Body.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		want := capture.Posts[index].Response
		if postResp.StatusCode != want.Status || string(payload) != want.Body {
			t.Fatalf("%s POST status/body = %d %q; want %d %q", tag, postResp.StatusCode, payload, want.Status, want.Body)
		}
		for key, values := range want.Headers {
			if got := postResp.Header.Values(key); !reflect.DeepEqual(got, values) {
				t.Fatalf("%s POST header %q = %q; want exactly %q", tag, key, got, values)
			}
		}
	}

	// The seed transact is excluded from the checked-in bytes (tx-id
	// watermark, credential-bearing admin header), so it runs live with the
	// same shape as the accepted assembled leg: one fixed-UUID todo over
	// /admin/transact, 200 with a positive tx-id asserted live.
	seedStatus, seedBody, _ := cf003Serve(mux, http.MethodPost, "/admin/transact", cf003JSON(t, map[string]any{
		"app-id": appStr,
		"steps":  []any{[]any{"update", "todos", cf003EntityID, map[string]any{"title": "transport-baseline"}}},
	}), map[string]string{"X-admin-token": adminToken})
	if seedStatus != http.StatusOK {
		_ = resp.Body.Close()
		t.Fatalf("cardinality seed transact = %d %q; want 200", seedStatus, seedBody)
	}
	var seedTx struct {
		TxID int64 `json:"tx-id"`
	}
	if err := json.Unmarshal([]byte(seedBody), &seedTx); err != nil || seedTx.TxID <= 0 {
		_ = resp.Body.Close()
		t.Fatalf("cardinality seed body = %q; want positive tx-id", seedBody)
	}

	// SSE init after seeding observes the exact live catalog identity, and
	// that identity must equal the HTTP-schema resolution: any drift fails
	// here before any transact is replayed.
	post("init", 0, render(capture.Posts[0].Request.Body))
	initFrame := cf003ReadSSE(t, scanner)
	protocol, _ := initFrame["session-id"].(string)
	if protocol == "" || protocol == transport {
		_ = resp.Body.Close()
		t.Fatalf("SSE protocol session = %q (transport %q); want distinct non-empty protocol session", protocol, transport)
	}
	live[cardinalityProtocol] = protocol
	idAttr, titleAttr := cf003AssertSSEInit(t, initFrame, appStr)
	live[cardinalityAttrID] = idAttr
	live[cardinalityAttrTitle] = titleAttr
	assertStreamFrame(1, initFrame)
	schemaIDAttr, schemaTitleAttr := cf003TransactionAttrs(t, mux, appStr, adminToken)
	if idAttr != schemaIDAttr || titleAttr != schemaTitleAttr {
		_ = resp.Body.Close()
		t.Fatalf("SSE attr identity drift: sse id=%q title=%q vs schema id=%q title=%q", idAttr, titleAttr, schemaIDAttr, schemaTitleAttr)
	}

	// No checked-in byte may leak a live value: the templates must carry
	// sentinels only where the live server emits randomness.
	for _, value := range []string{transport, token, machine, protocol, idAttr, titleAttr} {
		if value == "" {
			_ = resp.Body.Close()
			t.Fatalf("unobserved live value for leak check")
		}
		if strings.Contains(string(raw), value) {
			_ = resp.Body.Close()
			t.Fatalf("checked-in capture leaks live value %q", value)
		}
	}

	// Convergence oracle over the production-mounted query path: exact
	// whole-body bytes plus whole-result DeepEqual lengths-before-elements,
	// so a partial write cannot hide behind a subset check.
	queryExact := func(tag, wantBody string) []any {
		t.Helper()
		status, body, headers := cf003Serve(mux, http.MethodPost, "/runtime/framework/query",
			`{"query":{"todos":{}}}`, map[string]string{"app-id": appStr})
		if status != http.StatusOK || body != wantBody {
			t.Fatalf("%s query status/body = %d %q; want %d %q", tag, status, body, http.StatusOK, wantBody)
		}
		if got := headers.Values("Content-Type"); !reflect.DeepEqual(got, []string{"application/json"}) {
			t.Fatalf("%s query Content-Type = %q; want exactly [application/json]", tag, got)
		}
		var envelope struct {
			Data struct {
				Todos []any `json:"todos"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(body), &envelope); err != nil {
			t.Fatalf("%s query body = %q: %v", tag, body, err)
		}
		if envelope.Data.Todos == nil {
			envelope.Data.Todos = []any{}
		}
		return envelope.Data.Todos
	}
	assertSingleTitle := func(tag string, todos []any, want string) {
		t.Helper()
		if len(todos) != 1 {
			t.Fatalf("%s todos length = %d; want exactly 1 (%#v)", tag, len(todos), todos)
		}
		cf003ExactTodo(t, cf003TodoByID(t, todos, cf003EntityID), map[string]any{
			"id": cf003EntityID, "title": want,
		})
	}
	exactTransactOK := func(tag string, index int, frame map[string]any, eventID string, pastTx float64) float64 {
		t.Helper()
		txID, ok := frame["tx-id"].(float64)
		if !ok || txID <= 0 {
			t.Fatalf("%s transact-ok tx id = %#v", tag, frame["tx-id"])
		}
		if wantTx, _ := capture.Stream[index].Data["tx-id"].(float64); !ok || txID != wantTx || int64(txID) != int64(wantTx) {
			t.Fatalf("%s transact-ok watermark = %#v; want exactly float64(%v) (checked-in deterministic watermark)", tag, frame["tx-id"], capture.Stream[index].Data["tx-id"])
		}
		if txID <= pastTx {
			t.Fatalf("%s cardinality tx %v did not advance past watermark %v", tag, txID, pastTx)
		}
		want := map[string]any{"op": "transact-ok", "tx-id": txID, "client-event-id": eventID}
		if !reflect.DeepEqual(frame, want) {
			t.Fatalf("%s transact-ok = %#v; want %#v", tag, frame, want)
		}
		assertStreamFrame(index, frame)
		return txID
	}

	baselineBody := `{"data":{"todos":[{"id":"` + cf003EntityID + `","title":"transport-baseline"}]}}` + "\n"
	assertSingleTitle("baseline", queryExact("baseline", baselineBody), "transport-baseline")

	// SSE positive low-level write converges exactly past the seed watermark.
	post("sse-t1", 1, render(capture.Posts[1].Request.Body))
	sseTx := exactTransactOK("sse-t1", 2, cf003ReadSSE(t, scanner), "sse-t1", float64(seedTx.TxID))
	oneBody := `{"data":{"todos":[{"id":"` + cf003EntityID + `","title":"sse-one"}]}}` + "\n"
	assertSingleTitle("sse-one", queryExact("sse-one", oneBody), "sse-one")

	// SSE validation failure mutates nothing.
	post("sse-bad", 2, render(capture.Posts[2].Request.Body))
	denial := cf003ReadSSE(t, scanner)
	wantDenial := map[string]any{
		"op": "error", "status": float64(400), "type": "tx-step-validation",
		"message": "transact: add-triple: want 3 or 4 args, got 1",
	}
	if !reflect.DeepEqual(denial, wantDenial) {
		_ = resp.Body.Close()
		t.Fatalf("transport denial = %#v; want %#v", denial, wantDenial)
	}
	assertStreamFrame(3, denial)
	if body := queryExact("post-denial", oneBody); !reflect.DeepEqual(body, []any{map[string]any{"id": cf003EntityID, "title": "sse-one"}}) {
		_ = resp.Body.Close()
		t.Fatalf("denial mutated state: %#v", body)
	}

	// SSE same-batch cardinality-one boundary: the final value wins exactly
	// once through the mounted SSE POST path.
	post("sse-card", 3, render(capture.Posts[3].Request.Body))
	sseTx = exactTransactOK("sse-card", 4, cf003ReadSSE(t, scanner), "sse-card", sseTx)
	cardBody := `{"data":{"todos":[{"id":"` + cf003EntityID + `","title":"sse-card-last"}]}}` + "\n"
	assertSingleTitle("sse-card", queryExact("sse-card", cardBody), "sse-card-last")

	// Terminal quiescence: the stream must stay open and quiet for the
	// bounded window, so a spurious data frame right after the cardinality
	// transact-ok fails here instead of passing unread.
	cf003AssertSSEQuiet(t, scanner, "card", 250*time.Millisecond)

	// Old-session teardown: close the stream, then poll the exact
	// checked-in empty-message body until the terminal 401 lands.
	// Intermediate 200s are timing-dependent in count and are excluded from
	// the capture; any other shape fails here.
	_ = resp.Body.Close()
	teardownBody := render(capture.Posts[4].Request.Body)
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
			want := capture.Posts[4].Response
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

	// No unclaimed mutations: the store holds exactly the cardinality final
	// value on the single seeded entity and the old session is still dead.
	assertSingleTitle("final", queryExact("final", cardBody), "sse-card-last")
	post("old-session-still-dead", 4, teardownBody)

	// Exactly-once watermark proof: final-state DeepEqual above cannot
	// distinguish exactly-once application from duplicate-committed-same-value
	// (a duplicate commit of the same title would converge identically while
	// minting one more transaction). The durable sequence must therefore hold
	// exactly 3 committed transactions — seed tx 1, sse-t1 tx 2, sse-card tx 3
	// — with the denial minting nothing. An isolated probe transact on a
	// throwaway entity must return exactly tx-id 4; any duplicate or extra
	// post-ack commit would advance the watermark and fail this assert. The
	// probe is the terminal claimed mutation: no state is asserted after it.
	probeStatus, probeBody, _ := cf003Serve(mux, http.MethodPost, "/admin/transact", cf003JSON(t, map[string]any{
		"app-id": appStr,
		"steps":  []any{[]any{"update", "todos", cardinalityProbeEntityID, map[string]any{"title": "watermark-probe"}}},
	}), map[string]string{"X-admin-token": adminToken})
	if probeStatus != http.StatusOK {
		t.Fatalf("watermark probe transact = %d %q; want 200", probeStatus, probeBody)
	}
	var probeTx struct {
		TxID int64 `json:"tx-id"`
	}
	if err := json.Unmarshal([]byte(probeBody), &probeTx); err != nil || probeTx.TxID != 4 {
		t.Fatalf("watermark probe tx-id = %q; want exactly tx-id 4 (seed 1, sse-t1 2, sse-card 3, denial minting nothing)", probeBody)
	}
	if float64(probeTx.TxID) <= sseTx {
		t.Fatalf("watermark probe tx %d did not advance past cardinality watermark %v", probeTx.TxID, sseTx)
	}
}
