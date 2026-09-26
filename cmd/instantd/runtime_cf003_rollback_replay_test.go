package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// rollbackCapture is the checked-in raw HTTP capture
// (corpus/transactions-rollback-error.json) bound to the
// transactions-rollback-error coverage row by its self-bound metadata envelope
// (scenario == id == transactions-rollback-error, dedicated fixture
// transactions-rollback-error). Query requests carry the load-bearing app-id
// header pinned exactly (the query path reads app identity from the header,
// not the body). The one checked-in failing transact request carries no
// headers: the credential-bearing X-admin-token is attached live and never
// checked in, and the run-minted todos/title and todos/id catalog attr UUIDs
// are sentinel-masked (__CF003_TITLE_ATTR__/__CF003_ID_ATTR__) per the capture
// redaction rule and substituted with the live schema-resolved values.
type rollbackCapture struct {
	Scenario  string `json:"scenario"`
	ID        string `json:"id"`
	Fixture   string `json:"fixture"`
	Transport string `json:"transport"`
	Status    string `json:"status"`
	Exchanges []struct {
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
	} `json:"exchanges"`
}

// rollbackTitleAttrSentinel and rollbackIDAttrSentinel mask the run-minted
// catalog attr UUIDs in the checked-in failing request body. Attrs are minted
// crypto-random per isolated DB (observed distinct across fresh runs), so exact
// byte check-in of the raw UUIDs is inappropriate; the replay test resolves
// both live via GET /admin/schema and substitutes exactly.
const (
	rollbackTitleAttrSentinel = "__CF003_TITLE_ATTR__"
	rollbackIDAttrSentinel    = "__CF003_ID_ATTR__"
)

// TestCF003RollbackCaptureReplay replays the checked-in
// transactions-rollback-error raw capture against the production-mounted
// POST /admin/transact and POST /runtime/framework/query routes on the owned
// isolated transactions-rollback-error fixture (the deterministic cf003 seed
// triple: app 00000000-0000-4000-8000-000000000003). The four mutating transact
// successes (baseline seed, same-batch cardinality-one batch, deep-merge
// batch, delete-by-lookup) run live in file order with exact shape asserts
// (200 with strictly advancing positive tx-id) because their responses carry
// tx-id watermarks tied to database state and their requests carry the
// credential-bearing X-admin-token header; the checked-in legs assert exact
// status and exact whole-body bytes in file order. The pre/post rollback
// queries are byte-identical with whole-result DeepEqual lengths-before-
// elements, so a partial write cannot hide behind a subset check. The same-
// lookup concurrent convergence leg is excluded as timing-dependent (the start
// barrier encourages but does not force DB-level overlap); the assembled
// 10x-race proof stands as boundary evidence only. The cardinality,
// merge, and delete legs are boundary evidence for the neighboring
// transactions-cardinality-boundary and transactions-lookup-lifecycle rows and
// do not flip those rows.
// FU-02 Option A report-only; no corpusctl behavior change, no WS leg, no
// v1 claim.
func TestCF003RollbackCaptureReplay(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "corpus", "transactions-rollback-error.json"))
	if err != nil {
		t.Fatal(err)
	}
	var capture rollbackCapture
	if err := json.Unmarshal(raw, &capture); err != nil {
		t.Fatalf("capture decode: %v", err)
	}
	if capture.Scenario != "transactions-rollback-error" || capture.ID != "transactions-rollback-error" ||
		capture.Fixture != "transactions-rollback-error" || capture.Transport != "http" || capture.Status != "covered" {
		t.Fatalf("capture envelope = %q/%q/%q/%q/%q; want transactions-rollback-error/transactions-rollback-error/transactions-rollback-error/http/covered",
			capture.Scenario, capture.ID, capture.Fixture, capture.Transport, capture.Status)
	}
	if len(capture.Exchanges) != 6 {
		t.Fatalf("capture exchanges = %d; want exactly 6 (baseline query, failing batch, post-rollback query, cardinality query, merge query, deleted query)", len(capture.Exchanges))
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

	// The rollback leg provisions the identical deterministic cf003 seed triple
	// declared by corpus/fixtures/transactions-rollback-error.json, so
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
	fixtureRaw, err := os.ReadFile(filepath.Join("..", "..", "corpus", "fixtures", "transactions-rollback-error.json"))
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

	// The live catalog attr UUIDs are minted per isolated DB; the baseline
	// seed provisions the todos/title and todos/id attrs, so it runs before
	// resolving them for the sentinel-rule asserts below.
	txPrev := int64(0)
	liveTransact := func(name, body string) {
		t.Helper()
		status, respBody, _ := cf003Serve(mux, http.MethodPost, "/admin/transact", body,
			map[string]string{"X-admin-token": adminToken})
		if status != http.StatusOK {
			t.Fatalf("live %s transact = %d %q; want 200", name, status, respBody)
		}
		var tx struct {
			TxID int64 `json:"tx-id"`
		}
		if err := json.Unmarshal([]byte(respBody), &tx); err != nil || tx.TxID <= txPrev {
			t.Fatalf("live %s transact body = %q; want positive tx-id advancing past %d", name, respBody, txPrev)
		}
		txPrev = tx.TxID
	}
	replayQuery := func(i int) {
		t.Helper()
		exchange := capture.Exchanges[i]
		status, body, headers := cf003Serve(mux, exchange.Request.Method, exchange.Request.Target,
			exchange.Request.Body, map[string]string{"app-id": appStr})
		if status != exchange.Response.Status || body != exchange.Response.Body {
			t.Fatalf("exchange %d status/body = %d %q; want %d %q",
				i, status, body, exchange.Response.Status, exchange.Response.Body)
		}
		if got := headers.Values("Content-Type"); !reflect.DeepEqual(got, exchange.Response.Headers["Content-Type"]) {
			t.Fatalf("exchange %d Content-Type = %q; want %q",
				i, got, exchange.Response.Headers["Content-Type"])
		}
	}
	liveTransact("seed", cf003JSON(t, map[string]any{
		"app-id": appStr,
		"steps": []any{
			[]any{"update", "todos", cf003EntityID, map[string]any{"title": "tx-baseline"}},
		},
	}))
	idAttr, titleAttr := cf003TransactionAttrs(t, mux, appStr, adminToken)

	// Every checked-in query leg is exactly the unfiltered todos query over
	// the production-mounted query path with the pinned app-id header. The
	// failing batch is exactly the accepted low-level two-step batch with
	// sentinel-masked attr UUIDs and no checked-in credential header; a
	// capture edited to a foreign method, target, app, or credential header
	// fails here before any byte is replayed.
	for i, exchange := range capture.Exchanges {
		if i == 1 {
			if exchange.Request.Method != http.MethodPost || exchange.Request.Target != "/admin/transact" {
				t.Fatalf("exchange %d method/target = %q %q; want POST /admin/transact",
					i, exchange.Request.Method, exchange.Request.Target)
			}
			for key := range exchange.Request.Headers {
				if strings.EqualFold(key, "x-admin-token") {
					t.Fatalf("exchange %d checks in credential header %q; attach it live", i, key)
				}
			}
			if !strings.Contains(exchange.Request.Body, rollbackTitleAttrSentinel) ||
				!strings.Contains(exchange.Request.Body, rollbackIDAttrSentinel) {
				t.Fatalf("exchange %d request body lacks attr sentinels; run-minted attr UUIDs must stay masked", i)
			}
			if strings.Contains(exchange.Request.Body, idAttr) || strings.Contains(exchange.Request.Body, titleAttr) {
				t.Fatalf("exchange %d request body leaks live attr UUIDs; checked-in bytes must carry sentinels only", i)
			}
			continue
		}
		if exchange.Request.Method != http.MethodPost || exchange.Request.Target != "/runtime/framework/query" {
			t.Fatalf("exchange %d method/target = %q %q; want POST /runtime/framework/query",
				i, exchange.Request.Method, exchange.Request.Target)
		}
		if got := exchange.Request.Headers["app-id"]; !reflect.DeepEqual(got, []string{appStr}) {
			t.Fatalf("exchange %d app-id header = %q; want exactly [%q]", i, got, appStr)
		}
		var reqBody struct {
			Query map[string]any `json:"query"`
		}
		if err := json.Unmarshal([]byte(exchange.Request.Body), &reqBody); err != nil {
			t.Fatalf("exchange %d request body is not JSON: %v", i, err)
		}
		if reqBody.Query == nil || reqBody.Query["todos"] == nil {
			t.Fatalf("exchange %d request body lacks query.todos: %q", i, exchange.Request.Body)
		}
	}

	// Baseline seed already ran live above; replay the exact pre-rollback
	// query state.
	replayQuery(0)

	// The failing batch: substitute the sentinels with the live attr UUIDs
	// (each exactly once per step shape), attach the live admin token, and
	// assert the exact 400 with the stable unique-constraint semantic marker.
	failReq := capture.Exchanges[1].Request.Body
	if got := strings.Count(failReq, rollbackTitleAttrSentinel); got != 1 {
		t.Fatalf("title sentinel occurs %d times; want exactly 1", got)
	}
	if got := strings.Count(failReq, rollbackIDAttrSentinel); got != 1 {
		t.Fatalf("id sentinel occurs %d times; want exactly 1", got)
	}
	sent := strings.ReplaceAll(strings.ReplaceAll(failReq, rollbackTitleAttrSentinel, titleAttr), rollbackIDAttrSentinel, idAttr)
	if strings.Contains(sent, rollbackTitleAttrSentinel) || strings.Contains(sent, rollbackIDAttrSentinel) {
		t.Fatalf("sentinel substitution incomplete: %q", sent)
	}
	failExchange := capture.Exchanges[1]
	status, body, headers := cf003Serve(mux, failExchange.Request.Method, failExchange.Request.Target,
		sent, map[string]string{"X-admin-token": adminToken})
	if status != failExchange.Response.Status || body != failExchange.Response.Body {
		t.Fatalf("exchange 1 status/body = %d %q; want %d %q",
			status, body, failExchange.Response.Status, failExchange.Response.Body)
	}
	if got := headers.Values("Content-Type"); !reflect.DeepEqual(got, failExchange.Response.Headers["Content-Type"]) {
		t.Fatalf("exchange 1 Content-Type = %q; want %q",
			got, failExchange.Response.Headers["Content-Type"])
	}
	if !strings.Contains(body, "unique constraint violated") {
		t.Fatalf("exchange 1 400 body lacks the stable unique-constraint semantic marker: %q", body)
	}

	// Post-rollback query replays byte-identical to the baseline: the failed
	// multi-step left no partial write.
	replayQuery(2)
	if capture.Exchanges[2].Response.Body != capture.Exchanges[0].Response.Body {
		t.Fatalf("post-rollback body = %q; want byte-identical baseline %q",
			capture.Exchanges[2].Response.Body, capture.Exchanges[0].Response.Body)
	}

	// Same-batch cardinality-one boundary (live, accepted-leg shape): the
	// final value wins exactly once. Boundary evidence for the neighboring
	// transactions-cardinality-boundary row; that row stays gap.
	liveTransact("cardinality", cf003JSON(t, map[string]any{
		"app-id": appStr,
		"steps": []any{
			[]any{"add-triple", cf003EntityID, titleAttr, "tx-first"},
			[]any{"add-triple", cf003EntityID, titleAttr, "tx-last"},
		},
	}))
	replayQuery(3)

	// Deep-merge preservation (live, accepted-leg shape): the sibling object
	// member and existing title survive.
	liveTransact("merge", cf003JSON(t, map[string]any{
		"app-id": appStr,
		"steps": []any{
			[]any{"update", "todos", cf003EntityID, map[string]any{"meta": map[string]any{"a": 1}}},
			[]any{"merge", "todos", cf003EntityID, map[string]any{"meta": map[string]any{"b": 2}}},
		},
	}))
	replayQuery(4)

	// Delete-by-lookup exact state (live, label-based deterministic lookup on
	// the unique todos/id value): the entity is gone and nothing else changes.
	// Boundary evidence for the neighboring transactions-lookup-lifecycle row;
	// that row stays gap.
	liveTransact("delete", cf003JSON(t, map[string]any{
		"app-id": appStr,
		"steps":  []any{[]any{"delete", "todos", `lookup__id__"` + cf003EntityID + `"`}},
	}))
	replayQuery(5)

	// Whole-result exactness under the exact oracle standard:
	// reflect.DeepEqual with lengths asserted before elements, mirroring the
	// accepted assembled leg state for state.
	todosOf := func(i int) []any {
		t.Helper()
		var envelope struct {
			Data map[string]json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal([]byte(capture.Exchanges[i].Response.Body), &envelope); err != nil {
			t.Fatalf("exchange %d response body = %q: %v", i, capture.Exchanges[i].Response.Body, err)
		}
		var todos []any
		if err := json.Unmarshal(envelope.Data["todos"], &todos); err != nil {
			t.Fatalf("exchange %d todos = %q: %v", i, capture.Exchanges[i].Response.Body, err)
		}
		if todos == nil {
			todos = []any{}
		}
		return todos
	}
	assertTodos := func(i int, want ...any) {
		t.Helper()
		got := todosOf(i)
		if want == nil {
			want = []any{}
		}
		if len(got) != len(want) {
			t.Fatalf("exchange %d todos length = %d; want exactly %d (%#v)", i, len(got), len(want), want)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("exchange %d todos = %#v; want exactly %#v", i, got, want)
		}
	}
	assertTodos(0, map[string]any{"id": cf003EntityID, "title": "tx-baseline"})
	assertTodos(2, map[string]any{"id": cf003EntityID, "title": "tx-baseline"})
	assertTodos(3, map[string]any{"id": cf003EntityID, "title": "tx-last"})
	assertTodos(4, map[string]any{"id": cf003EntityID, "title": "tx-last", "meta": map[string]any{"a": float64(1), "b": float64(2)}})
	assertTodos(5)

	// Every checked-in response body must be a JSON object carrying data (query
	// legs) or message (the exact 400 denial): a truncated or re-encoded
	// capture fails here even if status matches.
	for i, exchange := range capture.Exchanges {
		var envelope map[string]any
		if err := json.Unmarshal([]byte(exchange.Response.Body), &envelope); err != nil {
			t.Fatalf("exchange %d response body is not JSON: %v", i, err)
		}
		if i == 1 {
			message, _ := envelope["message"].(string)
			if message == "" || !strings.Contains(message, "unique constraint violated") {
				t.Fatalf("exchange %d 400 lacks message with the stable marker: %q", i, exchange.Response.Body)
			}
		} else if _, ok := envelope["data"].(map[string]any); !ok {
			t.Fatalf("exchange %d response lacks data object: %q", i, exchange.Response.Body)
		}
		if !strings.HasSuffix(exchange.Response.Body, "\n") {
			t.Fatalf("exchange %d response body lacks trailing newline", i)
		}
	}
}
