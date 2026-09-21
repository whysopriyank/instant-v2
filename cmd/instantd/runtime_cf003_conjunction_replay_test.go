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

// conjunctionCapture is the checked-in raw HTTP capture
// (corpus/query-conjunction-gap.json) bound to the
// query-conjunction-gap coverage row by its self-bound metadata envelope
// (scenario == id == query-conjunction-gap, dedicated fixture
// query-conjunction-gap). Unlike the auth captures, every request carries the
// load-bearing app-id header: the query path reads app identity from the
// header (or ?app_id=), not the body, so the header is pinned exactly and
// replayed on every exchange.
type conjunctionCapture struct {
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

// conjunctionEntityB/C are the fixed second/third seed entities of the
// accepted assembled leg (the first is the shared cf003EntityID helper).
// Fixed UUIDs only: query results project stored fields plus ID-derived
// cursors, so exact whole-body replay is deterministic.
const (
	conjunctionEntityB = "00000000-0000-4000-8000-000000000006"
	conjunctionEntityC = "00000000-0000-4000-8000-000000000007"
)

// TestCF003ConjunctionCaptureReplay replays the checked-in
// query-conjunction-gap raw capture against the production-mounted
// POST /runtime/framework/query route on the owned isolated
// query-conjunction-gap fixture (the same deterministic cf003 seed triple as
// the auth legs: app 00000000-0000-4000-8000-000000000003). Every exchange
// asserts exact status and exact whole-body bytes in file order, so a losing
// or orphan entity cannot hide behind a subset check. The unfiltered
// baseline is exactly the three seeded entities in ID order with no
// page-info; the single-field and multi-field conjunctions select exactly the
// middle row; the contradictory conjunction is exactly empty; asc/desc carry
// ID-derived cursors; and the limit-2 first page plus its cursor-held
// continuation partition the match set. The seed transact is performed live
// (its tx-id watermark and X-admin-token header are never checked in) with
// the same exact assertions as the accepted assembled leg — values asserted
// live, never checked in. Queries are read-only, so a second full replay
// must return byte-identical results: no exchange may claim an unobserved
// mutation.
// FU-02 Option A report-only; no corpusctl behavior change, no WS leg, no
// v1 claim.
func TestCF003ConjunctionCaptureReplay(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "corpus", "query-conjunction-gap.json"))
	if err != nil {
		t.Fatal(err)
	}
	var capture conjunctionCapture
	if err := json.Unmarshal(raw, &capture); err != nil {
		t.Fatalf("capture decode: %v", err)
	}
	if capture.Scenario != "query-conjunction-gap" || capture.ID != "query-conjunction-gap" ||
		capture.Fixture != "query-conjunction-gap" || capture.Transport != "http" || capture.Status != "covered" {
		t.Fatalf("capture envelope = %q/%q/%q/%q/%q; want query-conjunction-gap/query-conjunction-gap/query-conjunction-gap/http/covered",
			capture.Scenario, capture.ID, capture.Fixture, capture.Transport, capture.Status)
	}
	if len(capture.Exchanges) != 8 {
		t.Fatalf("capture exchanges = %d; want exactly 8 (baseline, single-field, contradictory, multi-field, asc, desc, first page, continuation)", len(capture.Exchanges))
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

	// The query leg provisions the identical deterministic cf003 seed triple
	// declared by corpus/fixtures/query-conjunction-gap.json, so
	// cf003PostgresMux is reused here rather than duplicated.
	mux, appID, adminToken := cf003PostgresMux(t)
	appStr := platform.UUIDToStr(appID)

	// The declared fixture must name the actual capture/replay identity: the
	// fixture file's appId, creatorId, adminToken, and txSteps must equal the
	// live replay seed (deterministic triple app
	// 00000000-0000-4000-8000-000000000003, creator
	// 00000000-0000-4000-8000-000000000001, admin
	// 00000000-0000-4000-8000-000000000002, plus exactly-empty txSteps)
	// declared by the seed helper, and every exchange's app-id header must
	// equal the live replay app. A capture edited to a foreign app-id fails
	// here and in the exchange loop below; a fixture edited to a foreign
	// creator, admin, or non-empty txSteps fails here before any byte is
	// replayed.
	fixtureRaw, err := os.ReadFile(filepath.Join("..", "..", "corpus", "fixtures", "query-conjunction-gap.json"))
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
	for i, exchange := range capture.Exchanges {
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

	// The seed transact is excluded from the checked-in bytes (tx-id
	// watermark, credential-bearing admin header), so it runs live with the
	// same shape as the accepted assembled leg: three fixed-UUID todos over
	// /admin/transact, 200 with a positive tx-id asserted live.
	seedStatus, seedBody, _ := cf003Serve(mux, http.MethodPost, "/admin/transact", cf003JSON(t, map[string]any{
		"app-id": appStr,
		"steps": []any{
			[]any{"update", "todos", cf003EntityID, map[string]any{"title": "cf003-alpha", "priority": 1, "team": "red"}},
			[]any{"update", "todos", conjunctionEntityB, map[string]any{"title": "cf003-beta", "priority": 2, "team": "red"}},
			[]any{"update", "todos", conjunctionEntityC, map[string]any{"title": "cf003-gamma", "priority": 3, "team": "blue"}},
		},
	}), map[string]string{"X-admin-token": adminToken})
	if seedStatus != http.StatusOK {
		t.Fatalf("conjunction seed transact = %d %q; want 200", seedStatus, seedBody)
	}
	var seedTx struct {
		TxID int64 `json:"tx-id"`
	}
	if err := json.Unmarshal([]byte(seedBody), &seedTx); err != nil || seedTx.TxID <= 0 {
		t.Fatalf("conjunction seed body = %q; want positive tx-id", seedBody)
	}

	// Replay every checked-in exchange exactly, in file order, with the
	// pinned app-id header carried on each request.
	toHeaderMap := func(headers map[string][]string) map[string]string {
		out := map[string]string{}
		for key, values := range headers {
			if len(values) != 1 {
				t.Fatalf("request header %q has %d values; want exactly 1", key, len(values))
			}
			out[key] = values[0]
		}
		return out
	}
	replayOnce := func(pass string) {
		t.Helper()
		for i, exchange := range capture.Exchanges {
			status, body, headers := cf003Serve(mux, exchange.Request.Method, exchange.Request.Target,
				exchange.Request.Body, toHeaderMap(exchange.Request.Headers))
			if status != exchange.Response.Status || body != exchange.Response.Body {
				t.Fatalf("%s exchange %d status/body = %d %q; want %d %q",
					pass, i, status, body, exchange.Response.Status, exchange.Response.Body)
			}
			if got := headers.Values("Content-Type"); !reflect.DeepEqual(got, exchange.Response.Headers["Content-Type"]) {
				t.Fatalf("%s exchange %d Content-Type = %q; want %q",
					pass, i, got, exchange.Response.Headers["Content-Type"])
			}
		}
	}
	replayOnce("replay")

	// Conjunction and ordering exactness under the exact oracle standard:
	// whole-result reflect.DeepEqual with lengths asserted before elements,
	// mirroring the accepted assembled leg leg for leg.
	wantA := map[string]any{"id": cf003EntityID, "title": "cf003-alpha", "priority": float64(1), "team": "red"}
	wantB := map[string]any{"id": conjunctionEntityB, "title": "cf003-beta", "priority": float64(2), "team": "red"}
	wantC := map[string]any{"id": conjunctionEntityC, "title": "cf003-gamma", "priority": float64(3), "team": "blue"}
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
	assertTodos(0, wantA, wantB, wantC)
	assertTodos(1, wantB)
	assertTodos(2)
	assertTodos(3, wantB)
	assertTodos(4, wantA, wantB, wantC)
	assertTodos(5, wantC, wantB, wantA)
	assertTodos(6, wantA, wantB)
	assertTodos(7, wantC)

	// The unfiltered baseline carries no page-info; every ordered leg carries
	// exact page boundaries. The first page must hold more with its cursor,
	// the continuation must be exhausted, and the continuation request must
	// carry exactly the first page's endCursor.
	pageInfoOf := func(i int) map[string]any {
		t.Helper()
		var envelope struct {
			PageInfo map[string]any `json:"page-info"`
		}
		if err := json.Unmarshal([]byte(capture.Exchanges[i].Response.Body), &envelope); err != nil {
			t.Fatalf("exchange %d response body = %q: %v", i, capture.Exchanges[i].Response.Body, err)
		}
		return envelope.PageInfo
	}
	if info := pageInfoOf(0); info != nil {
		t.Fatalf("baseline page-info = %#v; want absent on the unfiltered leg", info)
	}
	assertPage := func(i int, hasNext, hasPrevious bool) map[string]any {
		t.Helper()
		info := pageInfoOf(i)
		if info == nil {
			t.Fatalf("exchange %d page-info absent; want exact boundaries", i)
		}
		if info["hasNextPage"] != hasNext || info["hasPreviousPage"] != hasPrevious {
			t.Fatalf("exchange %d page-info = %#v; want hasNextPage=%v hasPreviousPage=%v", i, info, hasNext, hasPrevious)
		}
		start, _ := info["startCursor"].(string)
		end, _ := info["endCursor"].(string)
		if start == "" || end == "" {
			t.Fatalf("exchange %d page-info cursors = %#v; want non-empty start/end", i, info)
		}
		return info
	}
	assertPage(4, false, false)
	assertPage(5, false, false)
	firstInfo := assertPage(6, true, false)
	assertPage(7, false, true)
	var page2Req map[string]any
	if err := json.Unmarshal([]byte(capture.Exchanges[7].Request.Body), &page2Req); err != nil {
		t.Fatalf("continuation request body = %q: %v", capture.Exchanges[7].Request.Body, err)
	}
	after, _ := page2Req["query"].(map[string]any)["todos"].(map[string]any)["$"].(map[string]any)["after"].(string)
	if after == "" || after != firstInfo["endCursor"] {
		t.Fatalf("continuation after = %q; want first-page endCursor %q", after, firstInfo["endCursor"])
	}

	// The query path is read-only: a second full replay must return
	// byte-identical results, so no exchange may claim an unobserved
	// mutation. (Direct store counters live behind the pool handle owned by
	// the shared seed helper, which this packet may not modify; observable
	// identity across two exact passes is the production-path proof.)
	replayOnce("second-pass")

	// Every checked-in response body must be a JSON object carrying data:
	// a truncated or re-encoded capture fails here even if status matches.
	for i, exchange := range capture.Exchanges {
		var envelope map[string]any
		if err := json.Unmarshal([]byte(exchange.Response.Body), &envelope); err != nil {
			t.Fatalf("exchange %d response body is not JSON: %v", i, err)
		}
		if _, ok := envelope["data"].(map[string]any); !ok {
			t.Fatalf("exchange %d response lacks data object: %q", i, exchange.Response.Body)
		}
		if !strings.HasSuffix(exchange.Response.Body, "\n") {
			t.Fatalf("exchange %d response body lacks trailing newline", i)
		}
	}
}
