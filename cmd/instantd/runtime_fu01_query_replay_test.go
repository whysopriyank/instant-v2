package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// fu01QueryCapture is the shared self-bound envelope shape for the two
// checked-in raw multi-client SSE captures
// (corpus/query-concurrency-gap.json, corpus/refresh-convergence-concurrency.json),
// each bound to its own coverage row by self-bound metadata (scenario == id
// == the row's own id, dedicated fixture query-concurrency-gap, transport
// sse). Every retained frame carries session/schema-attr-id sentinels in
// place of the live server's random values; entity IDs, the app ID, titles,
// op names and event IDs are fixed literals chosen by the owned fixture and
// are never masked. See each envelope's own "redaction"/"excluded" fields
// for the full masking rule; the stream shape (subscriber, seq, frame)
// mirrors the raw NDJSON produced by the accepted managed-record-multiclient
// recorder (corpusctl --mode managed-record-multiclient).
type fu01QueryCapture struct {
	Scenario  string `json:"scenario"`
	ID        string `json:"id"`
	Fixture   string `json:"fixture"`
	Transport string `json:"transport"`
	Status    string `json:"status"`
	Stream    []struct {
		Subscriber string         `json:"subscriber"`
		Seq        int            `json:"seq"`
		Frame      map[string]any `json:"frame"`
	} `json:"stream"`
}

// fu01LoadCapture loads and self-validates one checked-in envelope: its
// scenario/id must equal its own coverage-row id (self-bound, matching
// internal/corpus's validateCoverageEvidence rule), its fixture must be the
// dedicated query-concurrency-gap fixture, and it must declare sse/covered.
func fu01LoadCapture(t *testing.T, id string) fu01QueryCapture {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "corpus", id+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var capture fu01QueryCapture
	if err := json.Unmarshal(raw, &capture); err != nil {
		t.Fatalf("capture %s decode: %v", id, err)
	}
	if capture.Scenario != id || capture.ID != id || capture.Fixture != "query-concurrency-gap" ||
		capture.Transport != "sse" || capture.Status != "covered" {
		t.Fatalf("capture %s envelope = %q/%q/%q/%q/%q; want %s/%s/query-concurrency-gap/sse/covered",
			id, capture.Scenario, capture.ID, capture.Fixture, capture.Transport, capture.Status, id, id)
	}
	if strings.Contains(string(raw), "sess-") {
		t.Fatalf("capture %s carries a real session value", id)
	}
	return capture
}

// fu01FramesFor returns the checked-in frames for one subscriber in exactly
// the given seq order: a gap, a duplicate, or a misordered seq fails here.
func fu01FramesFor(t *testing.T, capture fu01QueryCapture, subscriber string, wantSeqs []int) []map[string]any {
	t.Helper()
	var frames []map[string]any
	var seqs []int
	for _, record := range capture.Stream {
		if record.Subscriber == subscriber {
			frames = append(frames, record.Frame)
			seqs = append(seqs, record.Seq)
		}
	}
	if !reflect.DeepEqual(seqs, wantSeqs) {
		t.Fatalf("capture subscriber %s seqs = %#v; want exactly %#v", subscriber, seqs, wantSeqs)
	}
	return frames
}

// Sentinel placeholders for values that are random per run. None is
// UUID-shaped and none carries the live "sess-" prefix, so a checked-in real
// value fails the mechanical pre-checks below before any byte is replayed.
// Entity IDs, the app ID, titles, op names and event IDs are fixed literals
// on this owned fixture and are never masked.
const (
	fu01SessionA  = "<fu01-session-A>"
	fu01SessionB  = "<fu01-session-B>"
	fu01SessionC  = "<fu01-session-C>"
	fu01AttrID    = "<fu01-attr-id-todos-id>"
	fu01AttrTitle = "<fu01-attr-id-todos-title>"
)

const (
	fu01EntityB = "00000000-0000-4000-8000-000000000006"
	fu01EntityC = "00000000-0000-4000-8000-000000000007"
)

// TestFU01QueryCaptureReplay replays the two checked-in raw multi-client SSE
// captures (corpus/query-concurrency-gap.json for coverage row
// query-concurrency-gap, corpus/refresh-convergence-concurrency.json for
// coverage row refresh-convergence-concurrency) against the
// production-mounted GET/POST SSE path on the owned deterministic
// query-concurrency-gap fixture (app 00000000-0000-4000-8000-000000000003,
// fixed entity IDs). It reproduces the accepted assembled leg
// TestCF003AssembledSSEQueryConcurrencyOrderedSnapshot step for step, using
// the same retention/derivation/oracle shape proven by
// TestFU01CaptureContractQueryConcurrency: two concurrent subscribers A and
// B race an add-query start barrier onto the identical {todos:{}} query and
// converge on one identical ordered initial whole-result snapshot at
// processed-tx-id exactly 0; one committed admin change (entity B retitled)
// converges A and B to identical node-list refresh-ok frames at the exact
// deterministic trigger watermark (seed tx 1, trigger tx 2 on the fresh
// isolated database); a late joiner C, attached only after the trigger
// commits, converges to the identical final titles via its own init-tree
// snapshot at the same exact tx-id (contract §4). Every retained frame,
// masked back to its checked-in sentinels, must equal the checked-in
// envelope byte for byte (whole-result reflect.DeepEqual); every stream
// goes quiet inside a bounded 250ms window (contract §3/§4). FU-01 2B-2; no
// corpusctl behavior change, no WS leg, no v1 claim.
func TestFU01QueryCaptureReplay(t *testing.T) {
	gap := fu01LoadCapture(t, "query-concurrency-gap")
	convergence := fu01LoadCapture(t, "refresh-convergence-concurrency")
	framesA0 := fu01FramesFor(t, gap, "A", []int{0, 1, 2})
	framesB0 := fu01FramesFor(t, gap, "B", []int{0, 1, 2})
	framesA1 := fu01FramesFor(t, convergence, "A", []int{3})
	framesB1 := fu01FramesFor(t, convergence, "B", []int{3})
	framesC := fu01FramesFor(t, convergence, "C", []int{0, 1, 2})

	// The initial ordered snapshot carries no volatile value at all (fixed
	// entity IDs/titles only): the checked-in copies must already be
	// byte-identical before any live substitution, and neither checked-in
	// envelope may leak a session value.
	if !reflect.DeepEqual(framesA0[2], framesB0[2]) {
		t.Fatalf("checked-in initial snapshots A/B differ: %#v vs %#v", framesA0[2], framesB0[2])
	}

	// The declared fixture must name the actual capture/replay identity: the
	// fixture file's appId/creatorId/adminToken/txSteps must equal the live
	// replay seed (cf003PostgresMux's fixed deterministic identity). A
	// fixture edited to a foreign identity fails here before any byte is
	// replayed.
	fixtureRaw, err := os.ReadFile(filepath.Join("..", "..", "corpus", "fixtures", "query-concurrency-gap.json"))
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
	if declaredFixture.AppID != cf003AppID || declaredFixture.CreatorID != cf003CreatorID ||
		declaredFixture.AdminToken != cf003AdminToken {
		t.Fatalf("declared fixture identity = %q/%q/%q; want seed identity %q/%q/%q",
			declaredFixture.AppID, declaredFixture.CreatorID, declaredFixture.AdminToken,
			cf003AppID, cf003CreatorID, cf003AdminToken)
	}
	if declaredFixture.TxSteps == nil || len(declaredFixture.TxSteps) != 0 {
		t.Fatalf("declared fixture txSteps = %#v; want exactly empty", declaredFixture.TxSteps)
	}
	// This test intentionally does not read corpus/manifest.json: the two
	// coverage rows it proves (query-concurrency-gap,
	// refresh-convergence-concurrency) are integrated by the coordinator
	// packet, not by this change. Each envelope's self-bound identity
	// (scenario == id == fixture owner) is already checked by
	// fu01LoadCapture above; the authoritative manifest<->evidence binding
	// is enforced by internal/corpus's ValidateCorpus once the rows land.

	mux, appID, adminToken := cf003PostgresMux(t)
	appStr := platform.UUIDToStr(appID)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	// Deterministic seed: one 3-step admin transact commits as tx 1 on the
	// fresh isolated database (excluded from the checked-in bytes per the
	// envelope's own "excluded" field: credential-bearing header, run live).
	seedStatus, seedBody, _ := cf003Serve(mux, http.MethodPost, "/admin/transact", cf003JSON(t, map[string]any{
		"app-id": appStr,
		"steps": []any{
			[]any{"update", "todos", cf003EntityID, map[string]any{"title": "conc-alpha"}},
			[]any{"update", "todos", fu01EntityB, map[string]any{"title": "conc-beta"}},
			[]any{"update", "todos", fu01EntityC, map[string]any{"title": "conc-gamma"}},
		},
	}), map[string]string{"X-admin-token": adminToken})
	if seedStatus != http.StatusOK {
		t.Fatalf("seed transact = %d %q", seedStatus, seedBody)
	}
	var seedEnvelope map[string]json.RawMessage
	if err := json.Unmarshal([]byte(seedBody), &seedEnvelope); err != nil || len(seedEnvelope) != 1 {
		t.Fatalf("seed transact envelope = %q: %v", seedBody, err)
	}
	var seedTx int64
	if raw, ok := seedEnvelope["tx-id"]; !ok || json.Unmarshal(raw, &seedTx) != nil || seedTx != 1 {
		t.Fatalf("seed transact tx-id = %q; want exactly 1 (fresh isolated database, checked-in deterministic watermark)", seedBody)
	}

	a := cf003OpenSSE(t, ctx, server, appStr)
	b := cf003OpenSSE(t, ctx, server, appStr)
	if a.sessionID == b.sessionID || a.token == b.token {
		t.Fatalf("SSE clients reused credentials: sessions=%q/%q tokens=%q/%q", a.sessionID, b.sessionID, a.token, b.token)
	}
	var logA, logB, logC []fu01Frame

	// Live substitution map, filled as each connection's protocol session
	// and the schema's attr IDs are observed. Live frames are masked back
	// through it before comparison to the checked-in templates.
	live := map[string]string{}
	maskFrame := func(frame map[string]any) map[string]any {
		t.Helper()
		encoded, err := json.Marshal(frame)
		if err != nil {
			t.Fatalf("live frame is not JSON: %v", err)
		}
		masked := string(encoded)
		for sentinel, value := range live {
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
	assertFrame := func(tag string, got, want map[string]any) {
		t.Helper()
		masked := maskFrame(got)
		if !reflect.DeepEqual(masked, want) {
			gotJSON, _ := json.Marshal(masked)
			wantJSON, _ := json.Marshal(want)
			t.Fatalf("%s masked = %s; want exactly %s", tag, gotJSON, wantJSON)
		}
	}

	a.post(t, ctx, map[string]any{"op": "init", "app-id": appStr})
	initA := fu01Read(t, a, "A", &logA)
	idAttrA, titleAttrA := cf003AssertSSEInit(t, initA, appStr)
	b.post(t, ctx, map[string]any{"op": "init", "app-id": appStr})
	initB := fu01Read(t, b, "B", &logB)
	idAttrB, titleAttrB := cf003AssertSSEInit(t, initB, appStr)
	if idAttrA != idAttrB || titleAttrA != titleAttrB {
		t.Fatalf("SSE attr identity drift: (%q,%q) vs (%q,%q)", idAttrA, titleAttrA, idAttrB, titleAttrB)
	}
	idAttr, titleAttr := idAttrA, titleAttrA
	if _, err := platform.ScanUUIDErr(idAttr); err != nil {
		t.Fatalf("idAttr = %q: %v", idAttr, err)
	}
	if _, err := platform.ScanUUIDErr(titleAttr); err != nil {
		t.Fatalf("titleAttr = %q: %v", titleAttr, err)
	}
	sessionA, _ := initA["session-id"].(string)
	sessionB, _ := initB["session-id"].(string)
	if sessionA == "" || sessionB == "" || sessionA == sessionB {
		t.Fatalf("SSE protocol sessions not distinct: %q vs %q", sessionA, sessionB)
	}
	for _, value := range []string{sessionA, sessionB, idAttr, titleAttr} {
		if strings.Contains(value, "<fu01-") {
			t.Fatalf("live value collides with sentinel space: %q", value)
		}
	}
	live[fu01SessionA] = sessionA
	live[fu01SessionB] = sessionB
	live[fu01AttrID] = idAttr
	live[fu01AttrTitle] = titleAttr
	assertFrame("A protocol-init", initA, framesA0[0])
	assertFrame("B protocol-init", initB, framesB0[0])

	// Concurrent subscription through a start barrier (raw HTTP: helpers
	// fail via t inside spawned goroutines).
	clients := []*cf003SSEClient{a, b}
	eventIDs := []string{"fu01-query-a", "fu01-query-b"}
	type postResult struct {
		status      int
		contentType string
		body        string
		err         error
	}
	results := make([]postResult, len(clients))
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, client := range clients {
		wg.Add(1)
		go func(i int, client *cf003SSEClient) {
			defer wg.Done()
			<-start
			payload, err := json.Marshal(map[string]any{
				"machine_id": "fu01-query-replay", "app_id": appStr,
				"session_id": client.sessionID, "sse_token": client.token,
				"messages": []any{map[string]any{
					"op": "add-query", "q": map[string]any{"todos": map[string]any{}},
					"client-event-id": eventIDs[i],
				}},
			})
			if err != nil {
				results[i] = postResult{err: err}
				return
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/runtime/sse", strings.NewReader(string(payload)))
			if err != nil {
				results[i] = postResult{err: err}
				return
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := server.Client().Do(req)
			if err != nil {
				results[i] = postResult{err: err}
				return
			}
			defer resp.Body.Close()
			raw, err := io.ReadAll(resp.Body)
			if err != nil {
				results[i] = postResult{err: err}
				return
			}
			results[i] = postResult{status: resp.StatusCode, contentType: resp.Header.Get("Content-Type"), body: string(raw)}
		}(i, client)
	}
	close(start)
	wg.Wait()
	for i, result := range results {
		if result.err != nil {
			t.Fatalf("concurrent add-query %d: %v", i, result.err)
		}
		if result.status != http.StatusOK || result.contentType != "application/json" || result.body != "{}\n" {
			t.Fatalf("concurrent add-query %d = %d %q %q; want 200 {}\\n", i, result.status, result.contentType, result.body)
		}
	}

	// Both subscribers observe one identical ordered whole-result snapshot
	// at processed-tx-id exactly 0: an independent structural oracle
	// (cf003AssertSSEOrderedSnapshot) plus the exact checked-in comparison.
	wantInitial := []any{
		map[string]any{"id": cf003EntityID, "title": "conc-alpha"},
		map[string]any{"id": fu01EntityB, "title": "conc-beta"},
		map[string]any{"id": fu01EntityC, "title": "conc-gamma"},
	}
	snapshots := make([]map[string]any, len(clients))
	logs := []*[]fu01Frame{&logA, &logB}
	names := []string{"A", "B"}
	checkedInAddQuery := []map[string]any{framesA0[1], framesB0[1]}
	checkedInSnapshot := []map[string]any{framesA0[2], framesB0[2]}
	for i, client := range clients {
		addQueryOK := fu01Read(t, client, names[i], logs[i])
		cf003AssertSSEAddQuery(t, addQueryOK, eventIDs[i])
		assertFrame(names[i]+" add-query-ok", addQueryOK, checkedInAddQuery[i])
		snapshots[i] = fu01Read(t, client, names[i], logs[i])
		cf003AssertSSEOrderedSnapshot(t, snapshots[i], wantInitial)
		assertFrame(names[i]+" initial snapshot", snapshots[i], checkedInSnapshot[i])
	}
	if !reflect.DeepEqual(snapshots[0], snapshots[1]) {
		t.Fatalf("concurrent snapshots diverged: %#v vs %#v", snapshots[0], snapshots[1])
	}

	// One committed change converges both subscribers at the exact
	// deterministic trigger watermark (tx 2: seed tx 1, trigger tx 2).
	status, body, _ := cf003Serve(mux, http.MethodPost, "/admin/transact", cf003JSON(t, map[string]any{
		"app-id": appStr,
		"steps":  []any{[]any{"update", "todos", fu01EntityB, map[string]any{"title": "conc-beta-2"}}},
	}), map[string]string{"X-admin-token": adminToken})
	if status != http.StatusOK {
		t.Fatalf("concurrency trigger = %d %q", status, body)
	}
	var txEnvelope map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &txEnvelope); err != nil || len(txEnvelope) != 1 {
		t.Fatalf("concurrency trigger envelope = %q", body)
	}
	var txID int64
	if raw, ok := txEnvelope["tx-id"]; !ok || json.Unmarshal(raw, &txID) != nil || txID != 2 {
		t.Fatalf("concurrency trigger tx-id = %q; want exactly 2 (checked-in deterministic watermark)", body)
	}
	wantFinal := map[string]string{
		cf003EntityID: "conc-alpha", fu01EntityB: "conc-beta-2", fu01EntityC: "conc-gamma",
	}
	refreshes := make([]map[string]any, len(clients))
	checkedInRefresh := []map[string]any{framesA1[0], framesB1[0]}
	for i, client := range clients {
		refreshes[i] = fu01Read(t, client, names[i], logs[i])
		cf003AssertSSEOrderedRefresh(t, refreshes[i], idAttr, titleAttr, txID, wantFinal)
		assertFrame(names[i]+" trigger refresh", refreshes[i], checkedInRefresh[i])
	}
	if !reflect.DeepEqual(refreshes[0], refreshes[1]) {
		t.Fatalf("concurrent refreshes diverged at tx %d: %#v vs %#v", txID, refreshes[0], refreshes[1])
	}

	// Retention completeness: exact per-subscriber op sequences, dense
	// 0-based seq, exact subscriber tag.
	if want := []string{"init-ok", "add-query-ok", "refresh-ok", "refresh-ok"}; !reflect.DeepEqual(fu01Ops(logA), want) {
		t.Fatalf("retention A ops = %#v; want exactly %#v", fu01Ops(logA), want)
	}
	if want := []string{"init-ok", "add-query-ok", "refresh-ok", "refresh-ok"}; !reflect.DeepEqual(fu01Ops(logB), want) {
		t.Fatalf("retention B ops = %#v; want exactly %#v", fu01Ops(logB), want)
	}
	fu01AssertRetentionSeq(t, logA, "A", 4)
	fu01AssertRetentionSeq(t, logB, "B", 4)

	// Bounded quiescence on both converged streams before the late joiner
	// attaches.
	fu01AssertQuiet(t, a, "A", 250*time.Millisecond)
	fu01AssertQuiet(t, b, "B", 250*time.Millisecond)

	// Late joiner C attaches the same query after the trigger and must
	// converge to the identical final snapshot at the exact trigger tx via
	// its own init-tree envelope (contract §4), replayed exactly against the
	// dedicated refresh-convergence-concurrency envelope.
	c := cf003OpenSSE(t, ctx, server, appStr)
	if c.sessionID == a.sessionID || c.sessionID == b.sessionID {
		t.Fatalf("late joiner reused session: %q", c.sessionID)
	}
	c.post(t, ctx, map[string]any{"op": "init", "app-id": appStr})
	initC := fu01Read(t, c, "C", &logC)
	idAttrC, titleAttrC := cf003AssertSSEInit(t, initC, appStr)
	if idAttrC != idAttr || titleAttrC != titleAttr {
		t.Fatalf("late joiner attr identity drift: (%q,%q); want (%q,%q)", idAttrC, titleAttrC, idAttr, titleAttr)
	}
	sessionC, _ := initC["session-id"].(string)
	if sessionC == "" || sessionC == sessionA || sessionC == sessionB {
		t.Fatalf("late joiner protocol session not distinct: %q", sessionC)
	}
	if strings.Contains(sessionC, "<fu01-") {
		t.Fatalf("live late joiner session collides with sentinel space: %q", sessionC)
	}
	live[fu01SessionC] = sessionC
	assertFrame("C protocol-init", initC, framesC[0])

	c.post(t, ctx, map[string]any{
		"op": "add-query", "q": map[string]any{"todos": map[string]any{}},
		"client-event-id": "fu01-query-c",
	})
	addC := fu01Read(t, c, "C", &logC)
	cf003AssertSSEAddQuery(t, addC, "fu01-query-c")
	assertFrame("C add-query-ok", addC, framesC[1])

	wantLate := []any{
		map[string]any{"id": cf003EntityID, "title": "conc-alpha"},
		map[string]any{"id": fu01EntityB, "title": "conc-beta-2"},
		map[string]any{"id": fu01EntityC, "title": "conc-gamma"},
	}
	lateSnap := fu01Read(t, c, "C", &logC)
	fu01AssertLateSnapshot(t, lateSnap, txID, wantLate)
	if titles := fu01TreeTitles(t, lateSnap); !reflect.DeepEqual(titles, wantFinal) {
		t.Fatalf("late joiner titles = %#v; want exactly %#v", titles, wantFinal)
	}
	assertFrame("C late snapshot", lateSnap, framesC[2])
	fu01AssertRetentionSeq(t, logC, "C", 3)
	fu01AssertQuiet(t, c, "C", 250*time.Millisecond)

	// No checked-in byte may leak a live value.
	rawGap, err := os.ReadFile(filepath.Join("..", "..", "corpus", "query-concurrency-gap.json"))
	if err != nil {
		t.Fatal(err)
	}
	rawConvergence, err := os.ReadFile(filepath.Join("..", "..", "corpus", "refresh-convergence-concurrency.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{sessionA, sessionB, sessionC, idAttr, titleAttr} {
		if strings.Contains(string(rawGap), value) || strings.Contains(string(rawConvergence), value) {
			t.Fatalf("checked-in capture leaks live value %q", value)
		}
	}

	a.closeAndAwaitUnauthorized(t, ctx)
	b.closeAndAwaitUnauthorized(t, ctx)
	c.closeAndAwaitUnauthorized(t, ctx)
}
