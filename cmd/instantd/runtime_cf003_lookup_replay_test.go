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

// lookupCapture is the checked-in raw SSE capture
// (corpus/transactions-lookup-lifecycle.json) bound to the
// transactions-lookup-lifecycle coverage row by its self-bound metadata
// envelope (scenario == id == transactions-lookup-lifecycle, dedicated
// fixture transactions-lookup-lifecycle, transport sse). The single
// connect and every POST exchange carry exact status/body bytes; stream
// records carry parsed data-payload objects with session-bearing values
// replaced by the documented sentinel placeholders (see the capture's
// redaction rule). Heartbeat comment records are excluded by that rule.
// HTTP query convergence bodies are not checked in: they are the live
// convergence oracle and are asserted whole-body with whole-result DeepEqual.
type lookupCapture struct {
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
	lookupTransport = "<lookup-transport-session>"
	lookupToken     = "<lookup-token>"
	lookupMachine   = "<lookup-machine-id>"
	lookupProtocol  = "<lookup-protocol-session>"
	lookupAttrID    = "<lookup-attr-id-todos-id>"
	lookupAttrTitle = "<lookup-attr-id-todos-title>"
	lookupFreshEID  = "<lookup-fresh-eid>"
)

// TestCF003LookupCaptureReplay replays the checked-in
// transactions-lookup-lifecycle raw capture against the
// production-mounted GET /runtime/sse + POST /runtime/sse routes on the owned
// isolated transactions-lookup-lifecycle fixture (the deterministic cf003
// seed triple: app 00000000-0000-4000-8000-000000000003). One subscriber only:
// a single SSE connection reproducing the accepted assembled SSE leg step for
// step (seed, init, add-query, lookup-eid update retargeting the seeded
// entity, absent-id lookup minting a fresh entity). The connect asserts exact
// status and exact Content-Type/Cache-Control; every POST asserts exact
// status, exact whole-body bytes, and exact Content-Type in file order; all 6
// stream data frames assert whole-result reflect.DeepEqual under the masking
// rule, with exact float64 refresh-ok watermarks (initial tree exactly 0,
// lookup update exactly 2, lookup mint exactly 3, each asserted exactly equal
// to the live trigger tx-id). Convergence is proven live over the
// production-mounted POST /runtime/framework/query path with exact
// whole-body bytes and whole-result DeepEqual lengths-before-elements:
// baseline lookup-one, post-update lookup-two, and the two-entity
// lookup-two-plus-fresh whole result in id order with a strict child-nodes
// oracle on every node-list refresh. Terminal quiescence is proven by a
// bounded 250ms quiet window after the final frame (see
// cf003AssertSSEQuiet), and the dead-reader teardown lands the exact
// checked-in 401. Exactly-once is proven by a terminal watermark probe:
// final-state DeepEqual alone cannot distinguish exactly-once application
// from duplicate-committed-same-value, so an isolated probe transact must
// return exactly tx-id 4 (seed 1, lookup-update 2, lookup-mint 3). The seed
// and both lookup admin transacts run live (tx-id watermarks and
// X-admin-token never checked in) with the same shape as the accepted
// assembled leg. FU-02 Option A report-only; no corpusctl behavior change, no
// second subscriber, no WS leg, no v1 claim.
func TestCF003LookupCaptureReplay(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "corpus", "transactions-lookup-lifecycle.json"))
	if err != nil {
		t.Fatal(err)
	}
	var capture lookupCapture
	if err := json.Unmarshal(raw, &capture); err != nil {
		t.Fatalf("capture decode: %v", err)
	}
	if capture.Scenario != "transactions-lookup-lifecycle" || capture.ID != "transactions-lookup-lifecycle" ||
		capture.Fixture != "transactions-lookup-lifecycle" || capture.Transport != "sse" || capture.Status != "covered" {
		t.Fatalf("capture envelope = %q/%q/%q/%q/%q; want transactions-lookup-lifecycle/transactions-lookup-lifecycle/transactions-lookup-lifecycle/sse/covered",
			capture.Scenario, capture.ID, capture.Fixture, capture.Transport, capture.Status)
	}
	if len(capture.Connects) != 1 {
		t.Fatalf("capture connects = %d; want exactly 1 (single subscriber, single connection)", len(capture.Connects))
	}
	if len(capture.Posts) != 3 {
		t.Fatalf("capture posts = %d; want exactly 3 (init, add-query, terminal teardown 401)", len(capture.Posts))
	}
	if len(capture.Stream) != 6 {
		t.Fatalf("capture stream frames = %d; want exactly 6 (handshake, protocol-init, add-query-ok, initial tree, update refresh, mint refresh)", len(capture.Stream))
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
		mustMasked("post body", post.Request.Body, lookupTransport, lookupToken)
		if post.Request.Method != http.MethodPost || post.Request.Target != "/runtime/sse" {
			t.Fatalf("post %d method/target = %q %q; want POST /runtime/sse", i, post.Request.Method, post.Request.Target)
		}
		if got := post.Request.Headers["Content-Type"]; !reflect.DeepEqual(got, []string{"application/json"}) {
			t.Fatalf("post %d request Content-Type = %q; want exactly [application/json]", i, got)
		}
	}
	// The mutating lookup transacts ride the live /admin/transact path (see
	// the capture's excluded rule), so no SSE POST template may smuggle an
	// attr or fresh-entity sentinel.
	for i, post := range capture.Posts {
		if strings.Contains(post.Request.Body, lookupAttrID) ||
			strings.Contains(post.Request.Body, lookupAttrTitle) ||
			strings.Contains(post.Request.Body, lookupFreshEID) {
			t.Fatalf("SSE post %d body must carry only session sentinels: %q", i, post.Request.Body)
		}
	}
	for i, record := range capture.Stream {
		encoded := unescapedJSON(record.Data)
		switch i {
		case 0:
			mustMasked("handshake", encoded, lookupMachine, lookupTransport, lookupToken)
		case 1:
			mustMasked("protocol-init", encoded, lookupProtocol, lookupAttrID, lookupAttrTitle)
		case 4:
			mustMasked("update refresh", encoded, lookupAttrID, lookupAttrTitle)
		case 5:
			mustMasked("mint refresh", encoded, lookupAttrID, lookupAttrTitle, lookupFreshEID)
		}
		if strings.Contains(encoded, "sess-") {
			t.Fatalf("stream frame %d carries a real session value", i)
		}
	}
	// The add-query-ok frame carries no volatile value; pin its exact shape.
	if want := map[string]any{"client-event-id": "cf003-lookup-query", "op": "add-query-ok"}; !reflect.DeepEqual(capture.Stream[2].Data, want) {
		t.Fatalf("checked-in add-query-ok = %#v; want %#v", capture.Stream[2].Data, want)
	}
	// The refresh-ok watermarks are deterministic across fresh isolated DBs
	// (seed tx 1, initial tree 0): the capture pins exactly 0, 2, and 3,
	// and the live run must equal them as float64 below.
	for i, wantTx := range map[int]float64{3: 0, 4: 2, 5: 3} {
		txID, ok := capture.Stream[i].Data["processed-tx-id"].(float64)
		if !ok || txID != wantTx {
			t.Fatalf("checked-in stream frame %d processed-tx-id = %#v; want exactly float64(%v)", i, capture.Stream[i].Data["processed-tx-id"], wantTx)
		}
	}
	// Sentinel occurrence counts pin the exact masked shape: the update
	// refresh carries one triple pair on the seeded entity; the mint refresh
	// carries one pair per entity with the fresh id in exactly three
	// positions (id-triple entity, id-triple value, title-triple entity).
	if got := strings.Count(unescapedJSON(capture.Stream[4].Data), lookupAttrTitle); got != 1 {
		t.Fatalf("update refresh title sentinel occurs %d times; want exactly 1", got)
	}
	if got := strings.Count(unescapedJSON(capture.Stream[5].Data), lookupFreshEID); got != 3 {
		t.Fatalf("mint refresh fresh-eid sentinel occurs %d times; want exactly 3", got)
	}
	if got := strings.Count(unescapedJSON(capture.Stream[5].Data), lookupAttrTitle); got != 2 {
		t.Fatalf("mint refresh title sentinel occurs %d times; want exactly 2", got)
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

	// The lookup leg provisions the identical deterministic cf003 seed
	// triple declared by corpus/fixtures/transactions-lookup-lifecycle.json,
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
	fixtureRaw, err := os.ReadFile(filepath.Join("..", "..", "corpus", "fixtures", "transactions-lookup-lifecycle.json"))
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
		if strings.Contains(out, "<lookup-") {
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
		if strings.Contains(value, "<lookup-") {
			_ = resp.Body.Close()
			t.Fatalf("live handshake value collides with sentinel space: %q", value)
		}
	}
	live[lookupTransport] = transport
	live[lookupToken] = token
	live[lookupMachine] = machine
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
	transact := func(tag string, steps ...any) int64 {
		t.Helper()
		seedStatus, seedBody, _ := cf003Serve(mux, http.MethodPost, "/admin/transact", cf003JSON(t, map[string]any{
			"app-id": appStr,
			"steps":  steps,
		}), map[string]string{"X-admin-token": adminToken})
		if seedStatus != http.StatusOK {
			t.Fatalf("%s transact = %d %q; want 200", tag, seedStatus, seedBody)
		}
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal([]byte(seedBody), &envelope); err != nil || len(envelope) != 1 {
			t.Fatalf("%s transact envelope = %q: %v", tag, seedBody, err)
		}
		var txID int64
		if raw, ok := envelope["tx-id"]; !ok || json.Unmarshal(raw, &txID) != nil || txID <= 0 {
			t.Fatalf("%s transact tx-id = %q", tag, seedBody)
		}
		return txID
	}
	seedTx := transact("lookup seed", []any{"update", "todos", cf003EntityID, map[string]any{"title": "lookup-one"}})

	// SSE init after seeding observes the exact live catalog identity, and
	// that identity must equal the HTTP-schema resolution: any drift fails
	// here before any lookup transact is replayed.
	post("init", 0, render(capture.Posts[0].Request.Body))
	initFrame := cf003ReadSSE(t, scanner)
	protocol, _ := initFrame["session-id"].(string)
	if protocol == "" || protocol == transport {
		_ = resp.Body.Close()
		t.Fatalf("SSE protocol session = %q (transport %q); want distinct non-empty protocol session", protocol, transport)
	}
	live[lookupProtocol] = protocol
	idAttr, titleAttr := cf003AssertSSEInit(t, initFrame, appStr)
	live[lookupAttrID] = idAttr
	live[lookupAttrTitle] = titleAttr
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

	// Strict node-list oracle for refresh-ok frames: lengths before
	// elements, exact float watermark, exact single computation, exactly one
	// node carrying exactly data plus empty child-nodes, one flat join-row,
	// and the exact entity→title map. Returns the observed entity set.
	assertLookupRefresh := func(tag string, index int, frame map[string]any, wantTx int64, wantTriples int, wantTitles map[string]string) map[string]bool {
		t.Helper()
		txID, ok := frame["processed-tx-id"].(float64)
		if !ok || txID != float64(wantTx) || int64(txID) != wantTx {
			t.Fatalf("%s refresh watermark = %#v; want exactly float64(%d)", tag, frame["processed-tx-id"], wantTx)
		}
		if wantTx, _ := capture.Stream[index].Data["processed-tx-id"].(float64); !ok || txID != wantTx || int64(txID) != int64(wantTx) {
			t.Fatalf("%s refresh watermark = %#v; want exactly float64(%v) (checked-in deterministic watermark)", tag, frame["processed-tx-id"], capture.Stream[index].Data["processed-tx-id"])
		}
		if len(frame) != 3 || frame["op"] != "refresh-ok" {
			t.Fatalf("%s refresh envelope = %#v; want refresh-ok at tx %d", tag, frame, wantTx)
		}
		comps, ok := frame["computations"].([]any)
		if !ok || len(comps) != 1 {
			t.Fatalf("%s refresh computations = %#v; want exactly one", tag, frame["computations"])
		}
		comp, _ := comps[0].(map[string]any)
		if len(comp) != 2 || !reflect.DeepEqual(comp["instaql-query"], map[string]any{"todos": map[string]any{}}) {
			t.Fatalf("%s refresh computation = %#v", tag, comp)
		}
		if _, hasDelta := comp["delta"]; hasDelta {
			t.Fatalf("%s refresh carried delta: %#v", tag, comp)
		}
		results, ok := comp["instaql-result"].([]any)
		if !ok || len(results) != 1 {
			t.Fatalf("%s refresh nodes = %#v; want exactly one node", tag, comp["instaql-result"])
		}
		node, _ := results[0].(map[string]any)
		if len(node) != 2 {
			t.Fatalf("%s refresh node = %#v; want exactly data plus child-nodes", tag, results[0])
		}
		if _, ok := node["data"]; !ok {
			t.Fatalf("%s refresh node = %#v; want exactly data plus child-nodes", tag, results[0])
		}
		children, ok := node["child-nodes"].([]any)
		if !ok {
			t.Fatalf("%s refresh child nodes = %#v; want []any", tag, node["child-nodes"])
		}
		if len(children) != 0 {
			t.Fatalf("%s refresh child nodes = %#v; want none", tag, node["child-nodes"])
		}
		rows, _ := node["data"].(map[string]any)
		datalog, _ := rows["datalog-result"].(map[string]any)
		// BuildNodeList wraps all deduped triples as one flat row; pin that
		// shape so a regrouped plan cannot pass silently.
		joinRows, _ := datalog["join-rows"].([]any)
		if len(joinRows) != 1 {
			t.Fatalf("%s refresh join-rows groups = %#v; want exactly one flat row", tag, datalog["join-rows"])
		}
		row, _ := joinRows[0].([]any)
		if len(row) != wantTriples {
			t.Fatalf("%s refresh triples = %d; want exactly %d", tag, len(row), wantTriples)
		}
		eids := map[string]bool{}
		titles := map[string]string{}
		for _, rawTriple := range row {
			triple, _ := rawTriple.([]any)
			if len(triple) != 3 {
				t.Fatalf("%s refresh triple = %#v; want exactly three parts", tag, rawTriple)
			}
			eid, _ := triple[0].(string)
			attr, _ := triple[1].(string)
			value, _ := triple[2].(string)
			if eid == "" || value == "" {
				t.Fatalf("%s refresh triple = %#v; want string entity and value", tag, rawTriple)
			}
			switch attr {
			case idAttr:
				if eid != value {
					t.Fatalf("%s refresh id triple = %#v", tag, triple)
				}
				if eids[eid] {
					t.Fatalf("%s refresh duplicate id triple for %q", tag, eid)
				}
				eids[eid] = true
			case titleAttr:
				if _, dup := titles[eid]; dup {
					t.Fatalf("%s refresh duplicate title triple for %q", tag, eid)
				}
				titles[eid] = value
			default:
				t.Fatalf("%s refresh triple has unexpected attr: %#v", tag, triple)
			}
		}
		if len(eids) != len(titles) {
			t.Fatalf("%s refresh id/title coverage = ids %d titles %d", tag, len(eids), len(titles))
		}
		if !reflect.DeepEqual(titles, wantTitles) {
			t.Fatalf("%s refresh titles = %#v; want %#v", tag, titles, wantTitles)
		}
		assertStreamFrame(index, frame)
		return eids
	}

	post("add-query", 1, render(capture.Posts[1].Request.Body))
	addQueryOK := cf003ReadSSE(t, scanner)
	cf003AssertSSEAddQuery(t, addQueryOK, "cf003-lookup-query")
	assertStreamFrame(2, addQueryOK)
	tree := cf003ReadSSE(t, scanner)
	cf003AssertSSETodos(t, tree, "lookup-one", true)
	assertStreamFrame(3, tree)

	baselineBody := `{"data":{"todos":[{"id":"` + cf003EntityID + `","title":"lookup-one"}]}}` + "\n"
	if todos := queryExact("baseline", baselineBody); !reflect.DeepEqual(todos, []any{map[string]any{"id": cf003EntityID, "title": "lookup-one"}}) {
		_ = resp.Body.Close()
		t.Fatalf("baseline todos = %#v; want exactly one lookup-one row", todos)
	}

	// A lookup-eid update retargets the seeded entity without naming its
	// storage id directly; the stream must carry the exact new title at the
	// exact triggering watermark and the HTTP path must converge exactly.
	lookupSeed := `lookup__id__"` + cf003EntityID + `"`
	firstTx := transact("lookup update", []any{"update", "todos", lookupSeed, map[string]any{"title": "lookup-two"}})
	if firstTx <= seedTx {
		t.Fatalf("lookup update tx %d did not advance past seed tx %d", firstTx, seedTx)
	}
	firstRefresh := cf003ReadSSE(t, scanner)
	assertLookupRefresh("lookup update", 4, firstRefresh, firstTx, 2, map[string]string{cf003EntityID: "lookup-two"})
	oneBody := `{"data":{"todos":[{"id":"` + cf003EntityID + `","title":"lookup-two"}]}}` + "\n"
	if todos := queryExact("lookup-two", oneBody); !reflect.DeepEqual(todos, []any{map[string]any{"id": cf003EntityID, "title": "lookup-two"}}) {
		_ = resp.Body.Close()
		t.Fatalf("lookup update todos = %#v; want exactly one lookup-two row", todos)
	}

	// A lookup-eid update against an absent id mints a fresh entity; both
	// rows must appear on the stream at the exact new watermark and in the
	// exact HTTP whole result in id order.
	absentLookup := `lookup__id__"00000000-0000-4000-8000-000000000046"`
	secondTx := transact("lookup mint", []any{"update", "todos", absentLookup, map[string]any{"title": "lookup-fresh"}})
	if secondTx <= firstTx {
		t.Fatalf("lookup mint tx %d did not advance past update tx %d", secondTx, firstTx)
	}
	refresh := cf003ReadSSE(t, scanner)
	// The absent-id lookup mints a fresh storage id rather than adopting
	// the requested string, so discover it exactly from the refresh triples
	// before masking: two entities, the seeded one retitled and one fresh
	// row carrying the lookup title.
	preEIDs := map[string]bool{}
	preTitles := map[string]string{}
	{
		comps, ok := refresh["computations"].([]any)
		if !ok || len(comps) != 1 {
			t.Fatalf("lookup mint computations = %#v; want exactly one", refresh["computations"])
		}
		comp, _ := comps[0].(map[string]any)
		results, ok := comp["instaql-result"].([]any)
		if !ok || len(results) != 1 {
			t.Fatalf("lookup mint nodes = %#v; want exactly one node", comp["instaql-result"])
		}
		node, _ := results[0].(map[string]any)
		rows, _ := node["data"].(map[string]any)
		datalog, _ := rows["datalog-result"].(map[string]any)
		joinRows, _ := datalog["join-rows"].([]any)
		for _, rawRow := range joinRows {
			row, _ := rawRow.([]any)
			for _, rawTriple := range row {
				triple, _ := rawTriple.([]any)
				if len(triple) != 3 {
					continue
				}
				eid, _ := triple[0].(string)
				attr, _ := triple[1].(string)
				value, _ := triple[2].(string)
				switch attr {
				case idAttr:
					preEIDs[eid] = true
				case titleAttr:
					preTitles[eid] = value
				}
			}
		}
	}
	if len(preEIDs) != 2 || !preEIDs[cf003EntityID] {
		_ = resp.Body.Close()
		t.Fatalf("lookup mint entities = %#v; want seeded plus one fresh", preEIDs)
	}
	var freshID string
	for eid := range preEIDs {
		if eid != cf003EntityID {
			freshID = eid
		}
	}
	if _, err := platform.ScanUUIDErr(freshID); err != nil {
		_ = resp.Body.Close()
		t.Fatalf("lookup fresh id = %q: %v", freshID, err)
	}
	// Probe disjointness: the terminal watermark probe must stay isolated
	// on cardinalityProbeEntityID (...0044). A broken mint returning the
	// probe UUID would otherwise pass the mint frame, HTTP convergence,
	// and probe checks while mutating the minted entity.
	if freshID == cardinalityProbeEntityID {
		_ = resp.Body.Close()
		t.Fatalf("lookup fresh id %q collides with watermark probe entity %q; probe must stay isolated", freshID, cardinalityProbeEntityID)
	}
	if strings.Contains(freshID, "<lookup-") {
		_ = resp.Body.Close()
		t.Fatalf("live fresh id collides with sentinel space: %q", freshID)
	}
	live[lookupFreshEID] = freshID
	if strings.Contains(string(raw), freshID) {
		_ = resp.Body.Close()
		t.Fatalf("checked-in capture leaks live fresh id %q", freshID)
	}
	assertLookupRefresh("lookup mint", 5, refresh, secondTx, 4, map[string]string{cf003EntityID: "lookup-two", freshID: "lookup-fresh"})
	twoBody := `{"data":{"todos":[{"id":"` + cf003EntityID + `","title":"lookup-two"},{"id":"` + freshID + `","title":"lookup-fresh"}]}}` + "\n"
	if todos := queryExact("lookup-fresh", twoBody); !reflect.DeepEqual(todos, []any{
		map[string]any{"id": cf003EntityID, "title": "lookup-two"},
		map[string]any{"id": freshID, "title": "lookup-fresh"},
	}) {
		_ = resp.Body.Close()
		t.Fatalf("lookup mint todos = %#v; want exactly two rows in id order", todos)
	}
	cf003ExactTodo(t, cf003TodoByID(t, queryExact("lookup-fresh-by-id", twoBody), cf003EntityID), map[string]any{
		"id": cf003EntityID, "title": "lookup-two",
	})
	cf003ExactTodo(t, cf003TodoByID(t, queryExact("lookup-fresh-by-id", twoBody), freshID), map[string]any{
		"id": freshID, "title": "lookup-fresh",
	})

	// Terminal quiescence: the stream must stay open and quiet for the
	// bounded window, so a spurious data frame right after the mint
	// refresh-ok fails here instead of passing unread.
	cf003AssertSSEQuiet(t, scanner, "lookup", 250*time.Millisecond)

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

	// No unclaimed mutations: the store holds exactly the two lookup rows
	// in id order and the old session is still dead.
	if todos := queryExact("final", twoBody); !reflect.DeepEqual(todos, []any{
		map[string]any{"id": cf003EntityID, "title": "lookup-two"},
		map[string]any{"id": freshID, "title": "lookup-fresh"},
	}) {
		t.Fatalf("final todos = %#v; want exactly two rows in id order", todos)
	}
	post("old-session-still-dead", 2, teardownBody)

	// Exactly-once watermark proof: final-state DeepEqual above cannot
	// distinguish exactly-once application from duplicate-committed-same-value
	// (a duplicate commit of the same title would converge identically while
	// minting one more transaction). The durable sequence must therefore hold
	// exactly 3 committed transactions — seed tx 1, lookup-update tx 2,
	// lookup-mint tx 3. An isolated probe transact on a throwaway entity
	// must return exactly tx-id 4; any duplicate or extra post-ack commit
	// would advance the watermark and fail this assert. The probe is the
	// terminal claimed mutation: no state is asserted after it.
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
		t.Fatalf("watermark probe tx-id = %q; want exactly tx-id 4 (seed 1, lookup-update 2, lookup-mint 3)", probeBody)
	}
	if float64(probeTx.TxID) <= float64(secondTx) {
		t.Fatalf("watermark probe tx %d did not advance past mint watermark %d", probeTx.TxID, secondTx)
	}
}
