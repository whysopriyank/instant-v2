package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// FU-01 preparatory slice: deterministic harness proving the multi-client
// capture contract shape defined in
// docs/plans/finish-up/fu01-multiclient-capture-contract.md.
//
// Each test runs two (plus one late-join) SSE subscribers over the
// production-mounted GET/POST SSE path against an owned PostgreSQL
// fixture, retains every post-handshake frame per subscriber with
// (subscriber, seq), derives the canonical snapshot by whole-result
// DeepEqual, and replays the exact oracle (whole-result DeepEqual +
// exact float watermark + exact tx ID per refresh) from the retained
// logs alone. Quiescence is proven by a bounded quiet window, never an
// unbounded wait.
//
// This is a contract-shape proof, not a recorder and not a replay leg:
// replay here is same-process re-assertion from retention. Checked-in
// NDJSON legs, corpus replay, and manifest flips remain gated. WS stays
// excluded by enforcement; only SSE is exercised.

// fu01Frame is one retained post-handshake SSE frame.
type fu01Frame struct {
	subscriber string
	seq        int
	frame      map[string]any
}

// fu01Read reads the next SSE data frame and retains a deep copy in log.
func fu01Read(t *testing.T, client *cf003SSEClient, name string, log *[]fu01Frame) map[string]any {
	t.Helper()
	frame := cf003ReadSSE(t, client.scanner)
	raw, err := json.Marshal(frame)
	if err != nil {
		t.Fatalf("FU-01 retain %s: %v", name, err)
	}
	var copy map[string]any
	if err := json.Unmarshal(raw, &copy); err != nil {
		t.Fatalf("FU-01 retain %s: %v", name, err)
	}
	*log = append(*log, fu01Frame{subscriber: name, seq: len(*log), frame: copy})
	return frame
}

// fu01AssertRetentionSeq proves the log carries a dense 0-based sequence
// with the exact subscriber tag: retention with a gap or a misattributed
// frame fails here, not downstream.
func fu01AssertRetentionSeq(t *testing.T, log []fu01Frame, name string, wantLen int) {
	t.Helper()
	if len(log) != wantLen {
		t.Fatalf("FU-01 retention %s length = %d; want exactly %d", name, len(log), wantLen)
	}
	for i, entry := range log {
		if entry.subscriber != name || entry.seq != i {
			t.Fatalf("FU-01 retention %s entry %d = (%q, %d); want (%q, %d)",
				name, i, entry.subscriber, entry.seq, name, i)
		}
		if len(entry.frame) == 0 {
			t.Fatalf("FU-01 retention %s entry %d is empty", name, i)
		}
	}
}

// fu01AssertQuiet proves quiescence by a bounded window: any data frame
// arriving inside the window fails the capture, and the wait never
// extends past the bound. The abandoned reader (on the quiet path) stays
// blocked only until test cleanup closes the stream body, then exits via
// the buffered channel; no further reads may follow on that scanner.
func fu01AssertQuiet(t *testing.T, client *cf003SSEClient, name string, window time.Duration) {
	t.Helper()
	type outcome struct {
		frame map[string]any
		err   error
		ended bool
	}
	ch := make(chan outcome, 1)
	go func() {
		for client.scanner.Scan() {
			line := client.scanner.Text()
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
			t.Fatalf("FU-01 quiet %s: decode error: %v", name, out.err)
		case out.ended:
			t.Fatalf("FU-01 quiet %s: stream ended inside quiet window", name)
		default:
			t.Fatalf("FU-01 quiet %s: unexpected frame inside quiet window: %#v", name, out.frame)
		}
	case <-time.After(window):
	}
}

// fu01Ops returns the op sequence of a retained log for exact comparison.
func fu01Ops(log []fu01Frame) []string {
	ops := make([]string, 0, len(log))
	for _, entry := range log {
		op, _ := entry.frame["op"].(string)
		ops = append(ops, op)
	}
	return ops
}

// TestFU01CaptureContractQueryConcurrency proves the shared-query capture
// shape: two SSE subscribers through a concurrent barrier observe one
// identical ordered whole-result snapshot; one committed admin change
// converges both to identical refresh frames at the exact tx; a late
// joiner converges to the same final snapshot at the same tx; the exact
// oracle replays from retention alone; both streams go quiet inside a
// bounded window.
func TestFU01CaptureContractQueryConcurrency(t *testing.T) {
	mux, appID, adminToken := cf003PostgresMux(t)
	app := platform.UUIDToStr(appID)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	entityB := "00000000-0000-4000-8000-000000000006"
	entityC := "00000000-0000-4000-8000-000000000007"
	status, body, _ := cf003Serve(mux, http.MethodPost, "/admin/transact", cf003JSON(t, map[string]any{
		"app-id": app,
		"steps": []any{
			[]any{"update", "todos", cf003EntityID, map[string]any{"title": "conc-alpha"}},
			[]any{"update", "todos", entityB, map[string]any{"title": "conc-beta"}},
			[]any{"update", "todos", entityC, map[string]any{"title": "conc-gamma"}},
		},
	}), map[string]string{"X-admin-token": adminToken})
	if status != http.StatusOK {
		t.Fatalf("concurrency seed = %d %q", status, body)
	}

	a := cf003OpenSSE(t, ctx, server, app)
	b := cf003OpenSSE(t, ctx, server, app)
	if a.sessionID == b.sessionID || a.token == b.token {
		t.Fatalf("SSE clients reused credentials: sessions=%q/%q tokens=%q/%q", a.sessionID, b.sessionID, a.token, b.token)
	}
	var logA, logB, logC []fu01Frame

	a.post(t, ctx, map[string]any{"op": "init", "app-id": app})
	idAttrA, titleAttrA := cf003AssertSSEInit(t, fu01Read(t, a, "A", &logA), app)
	b.post(t, ctx, map[string]any{"op": "init", "app-id": app})
	idAttrB, titleAttrB := cf003AssertSSEInit(t, fu01Read(t, b, "B", &logB), app)
	if idAttrA != idAttrB || titleAttrA != titleAttrB {
		t.Fatalf("SSE attr identity drift: (%q,%q) vs (%q,%q)", idAttrA, titleAttrA, idAttrB, titleAttrB)
	}
	idAttr, titleAttr := idAttrA, titleAttrA

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
				"machine_id": "fu01-conc", "app_id": app,
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
			defer func() { _ = resp.Body.Close() }()
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

	// Both subscribers observe one identical ordered whole-result snapshot.
	wantInitial := []any{
		map[string]any{"id": cf003EntityID, "title": "conc-alpha"},
		map[string]any{"id": entityB, "title": "conc-beta"},
		map[string]any{"id": entityC, "title": "conc-gamma"},
	}
	snapshots := make([]map[string]any, len(clients))
	logs := []*[]fu01Frame{&logA, &logB}
	names := []string{"A", "B"}
	for i, client := range clients {
		cf003AssertSSEAddQuery(t, fu01Read(t, client, names[i], logs[i]), eventIDs[i])
		snapshots[i] = fu01Read(t, client, names[i], logs[i])
		cf003AssertSSEOrderedSnapshot(t, snapshots[i], wantInitial)
	}
	if !reflect.DeepEqual(snapshots[0], snapshots[1]) {
		t.Fatalf("concurrent snapshots diverged: %#v vs %#v", snapshots[0], snapshots[1])
	}

	// One committed change converges both subscribers at its exact watermark.
	status, body, _ = cf003Serve(mux, http.MethodPost, "/admin/transact", cf003JSON(t, map[string]any{
		"app-id": app,
		"steps":  []any{[]any{"update", "todos", entityB, map[string]any{"title": "conc-beta-2"}}},
	}), map[string]string{"X-admin-token": adminToken})
	if status != http.StatusOK {
		t.Fatalf("concurrency trigger = %d %q", status, body)
	}
	var txEnvelope map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &txEnvelope); err != nil || len(txEnvelope) != 1 {
		t.Fatalf("concurrency trigger envelope = %q", body)
	}
	var txID int64
	if raw, ok := txEnvelope["tx-id"]; !ok || json.Unmarshal(raw, &txID) != nil || txID <= 0 {
		t.Fatalf("concurrency trigger tx-id = %q", body)
	}
	wantFinal := map[string]string{
		cf003EntityID: "conc-alpha", entityB: "conc-beta-2", entityC: "conc-gamma",
	}
	refreshes := make([]map[string]any, len(clients))
	for i, client := range clients {
		refreshes[i] = fu01Read(t, client, names[i], logs[i])
		cf003AssertSSEOrderedRefresh(t, refreshes[i], idAttr, titleAttr, txID, wantFinal)
	}
	if !reflect.DeepEqual(refreshes[0], refreshes[1]) {
		t.Fatalf("concurrent refreshes diverged at tx %d: %#v vs %#v", txID, refreshes[0], refreshes[1])
	}

	// Retention completeness: exact per-subscriber op sequences.
	if want := []string{"init-ok", "add-query-ok", "refresh-ok", "refresh-ok"}; !reflect.DeepEqual(fu01Ops(logA), want) {
		t.Fatalf("retention A ops = %#v; want exactly %#v", fu01Ops(logA), want)
	}
	if want := []string{"init-ok", "add-query-ok", "refresh-ok", "refresh-ok"}; !reflect.DeepEqual(fu01Ops(logB), want) {
		t.Fatalf("retention B ops = %#v; want exactly %#v", fu01Ops(logB), want)
	}
	fu01AssertRetentionSeq(t, logA, "A", 4)
	fu01AssertRetentionSeq(t, logB, "B", 4)

	// Replay from retention alone: the canonical snapshot/refresh derive
	// from the logs, and the exact oracle re-asserts without touching the
	// live streams.
	replaySnaps := []map[string]any{logA[2].frame, logB[2].frame}
	for _, snap := range replaySnaps {
		cf003AssertSSEOrderedSnapshot(t, snap, wantInitial)
	}
	if !reflect.DeepEqual(replaySnaps[0], replaySnaps[1]) {
		t.Fatalf("replayed snapshots diverged: %#v vs %#v", replaySnaps[0], replaySnaps[1])
	}
	replayRefresh := []map[string]any{logA[3].frame, logB[3].frame}
	for _, frame := range replayRefresh {
		cf003AssertSSEOrderedRefresh(t, frame, idAttr, titleAttr, txID, wantFinal)
	}
	if !reflect.DeepEqual(replayRefresh[0], replayRefresh[1]) {
		t.Fatalf("replayed refreshes diverged at tx %d: %#v vs %#v", txID, replayRefresh[0], replayRefresh[1])
	}

	// Bounded quiescence on both converged streams.
	fu01AssertQuiet(t, a, "A", 250*time.Millisecond)
	fu01AssertQuiet(t, b, "B", 250*time.Millisecond)

	// Late joiner C attaches the same query after the trigger and must
	// converge to the identical final snapshot at the exact trigger tx.
	c := cf003OpenSSE(t, ctx, server, app)
	if c.sessionID == a.sessionID || c.sessionID == b.sessionID {
		t.Fatalf("late joiner reused session: %q", c.sessionID)
	}
	c.post(t, ctx, map[string]any{"op": "init", "app-id": app})
	lateInit := fu01Read(t, c, "C", &logC)
	if attrs, ok := lateInit["attrs"].([]any); !ok || len(attrs) != 2 {
		t.Fatalf("late joiner attrs = %#v; want exactly two", lateInit["attrs"])
	}
	c.post(t, ctx, map[string]any{
		"op": "add-query", "q": map[string]any{"todos": map[string]any{}},
		"client-event-id": "fu01-query-c",
	})
	cf003AssertSSEAddQuery(t, fu01Read(t, c, "C", &logC), "fu01-query-c")
	lateSnap := fu01Read(t, c, "C", &logC)
	// A fresh subscriber's first answer is the init-query object tree at
	// the current tx — same exact tx and titles as the peers'
	// transactional node-list refresh, different envelope shape. Both
	// shapes carry exact oracles; conflating them would be a lie.
	wantLate := []any{
		map[string]any{"id": cf003EntityID, "title": "conc-alpha"},
		map[string]any{"id": entityB, "title": "conc-beta-2"},
		map[string]any{"id": entityC, "title": "conc-gamma"},
	}
	fu01AssertLateSnapshot(t, lateSnap, txID, wantLate)
	if titles := fu01TreeTitles(t, lateSnap); !reflect.DeepEqual(titles, wantFinal) {
		t.Fatalf("late joiner titles = %#v; want exactly %#v", titles, wantFinal)
	}
	fu01AssertRetentionSeq(t, logC, "C", 3)
	// Replay C from retention alone under the same oracle: the retained
	// snapshot re-asserts without touching the live stream.
	replayLate := logC[2].frame
	fu01AssertLateSnapshot(t, replayLate, txID, wantLate)
	if titles := fu01TreeTitles(t, replayLate); !reflect.DeepEqual(titles, wantFinal) {
		t.Fatalf("replayed late joiner titles = %#v; want exactly %#v", titles, wantFinal)
	}
	fu01AssertQuiet(t, c, "C", 250*time.Millisecond)

	a.closeAndAwaitUnauthorized(t, ctx)
	b.closeAndAwaitUnauthorized(t, ctx)
	c.closeAndAwaitUnauthorized(t, ctx)
}

// fu01AssertLateSnapshot asserts a fresh subscriber's first answer: the
// init-query object tree at the exact current tx with the exact ordered
// whole-result list (lengths before elements). This is the envelope a
// late joiner actually receives; the peers' transactional refreshes use
// the node-list envelope instead (cf003AssertSSEOrderedRefresh). Both
// are exact; neither is accepted as a substitute for the other.
func fu01AssertLateSnapshot(t *testing.T, frame map[string]any, txID int64, want []any) {
	t.Helper()
	if len(want) == 0 {
		t.Fatalf("late snapshot oracle must be non-empty")
	}
	gotTx, ok := frame["processed-tx-id"].(float64)
	if !ok || gotTx != float64(txID) {
		t.Fatalf("late snapshot tx = %#v; want %d", frame["processed-tx-id"], txID)
	}
	if len(frame) != 3 || frame["op"] != "refresh-ok" {
		t.Fatalf("late snapshot envelope = %#v; want tx %d", frame, txID)
	}
	computations, _ := frame["computations"].([]any)
	if len(computations) != 1 {
		t.Fatalf("late snapshot computations = %#v", frame["computations"])
	}
	entry, _ := computations[0].(map[string]any)
	if len(entry) != 2 || !reflect.DeepEqual(entry["instaql-query"], map[string]any{"todos": map[string]any{}}) {
		t.Fatalf("late snapshot computation = %#v", entry)
	}
	if _, hasDelta := entry["delta"]; hasDelta {
		t.Fatalf("late snapshot carried delta: %#v", entry)
	}
	result, _ := entry["instaql-result"].(map[string]any)
	if len(result) != 1 {
		t.Fatalf("late snapshot result keys = %#v", entry["instaql-result"])
	}
	rows, _ := result["todos"].([]any)
	if len(rows) != len(want) {
		t.Fatalf("late snapshot rows = %#v; want exactly %#v", result["todos"], want)
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("late snapshot rows = %#v; want exactly %#v", rows, want)
	}
}

// fu01TreeTitles reduces an init-tree snapshot to id→title for the
// cross-shape convergence check against the node-list refresh titles.
func fu01TreeTitles(t *testing.T, frame map[string]any) map[string]string {
	t.Helper()
	computations, _ := frame["computations"].([]any)
	entry, _ := computations[0].(map[string]any)
	result, _ := entry["instaql-result"].(map[string]any)
	rows, _ := result["todos"].([]any)
	titles := map[string]string{}
	for _, rawRow := range rows {
		row, _ := rawRow.(map[string]any)
		if len(row) != 2 {
			t.Fatalf("late snapshot row = %#v", rawRow)
		}
		id, _ := row["id"].(string)
		title, _ := row["title"].(string)
		if id == "" || title == "" {
			t.Fatalf("late snapshot row = %#v", row)
		}
		if _, dup := titles[id]; dup {
			t.Fatalf("late snapshot duplicate id %q", id)
		}
		titles[id] = title
	}
	return titles
}

// fu01AwaitAckAndPresence is the retained variant of
// cf003AwaitSSERoomAckAndPresence: the actor's ack and its caused
// presence may arrive in either order, with no other frame interleaved.
func fu01AwaitAckAndPresence(t *testing.T, client *cf003SSEClient, name string, log *[]fu01Frame, ackOp string, wantAck, wantPresence map[string]any) {
	t.Helper()
	ackSeen, presenceSeen := false, false
	for i := 0; i < 2; i++ {
		frame := fu01Read(t, client, name, log)
		switch frame["op"] {
		case ackOp:
			if ackSeen {
				t.Fatalf("FU-01 %s duplicate ack: %#v", name, frame)
			}
			cf003SSEExact(t, frame, wantAck)
			ackSeen = true
		case "refresh-presence":
			if presenceSeen {
				t.Fatalf("FU-01 %s duplicate presence: %#v", name, frame)
			}
			cf003AssertSSERoomPresence(t, frame, wantPresence)
			presenceSeen = true
		default:
			t.Fatalf("FU-01 %s unexpected frame during %s: %#v", name, ackOp, frame)
		}
	}
	if !ackSeen || !presenceSeen {
		t.Fatalf("FU-01 %s ack/presence incomplete: ack=%v presence=%v", name, ackSeen, presenceSeen)
	}
}

// TestFU01CaptureContractRoomFanout proves the shared-room capture shape:
// A joins solo, B late-joins to the exact two-member snapshot, B resyncs
// explicitly, A updates presence for both peers, A's broadcast reaches
// only the peer with the exact sender session, B leaves, and A converges
// to the exact one-member snapshot — all retained per subscriber and
// replayed from retention alone with lengths-before-elements DeepEqual.
func TestFU01CaptureContractRoomFanout(t *testing.T) {
	mux, appID, _ := cf003PostgresMux(t)
	app := platform.UUIDToStr(appID)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	a := cf003OpenSSE(t, ctx, server, app)
	b := cf003OpenSSE(t, ctx, server, app)
	if a.sessionID == b.sessionID || a.token == b.token {
		t.Fatalf("SSE room clients reused credentials: sessions=%q/%q tokens=%q/%q", a.sessionID, b.sessionID, a.token, b.token)
	}
	var logA, logB []fu01Frame

	a.post(t, ctx, map[string]any{"op": "init", "app-id": app})
	aSess := cf003AssertSSERoomInit(t, fu01Read(t, a, "A", &logA), app)
	b.post(t, ctx, map[string]any{"op": "init", "app-id": app})
	bSess := cf003AssertSSERoomInit(t, fu01Read(t, b, "B", &logB), app)
	if aSess == "" || bSess == "" || aSess == bSess {
		t.Fatalf("SSE room sessions not distinct: %q vs %q", aSess, bSess)
	}

	// A joins alone: ack plus the exact solo presence, in either order.
	a.post(t, ctx, map[string]any{
		"op": "join-room", "room-id": "cf003-room", "peer-id": "peer-a",
		"data": map[string]any{"mood": "ok"}, "client-event-id": "join-a",
	})
	wantSolo := map[string]any{
		aSess: map[string]any{"peer": "peer-a", "data": map[string]any{"mood": "ok"}},
	}
	fu01AwaitAckAndPresence(t, a, "A", &logA, "join-room-ok", map[string]any{
		"op": "join-room-ok", "room-id": "cf003-room", "client-event-id": "join-a",
	}, wantSolo)

	// B late-joins: both peers converge to the exact two-member snapshot.
	wantBoth := map[string]any{
		aSess: map[string]any{"peer": "peer-a", "data": map[string]any{"mood": "ok"}},
		bSess: map[string]any{"peer": "peer-b", "data": map[string]any{}},
	}
	b.post(t, ctx, map[string]any{
		"op": "join-room", "room-id": "cf003-room", "peer-id": "peer-b",
		"data": map[string]any{}, "client-event-id": "join-b",
	})
	fu01AwaitAckAndPresence(t, b, "B", &logB, "join-room-ok", map[string]any{
		"op": "join-room-ok", "room-id": "cf003-room", "client-event-id": "join-b",
	}, wantBoth)
	cf003AssertSSERoomPresence(t, fu01Read(t, a, "A", &logA), wantBoth)

	// B resyncs explicitly and sees the same snapshot.
	b.post(t, ctx, map[string]any{"op": "refresh-presence", "room-id": "cf003-room"})
	cf003AssertSSERoomPresence(t, fu01Read(t, b, "B", &logB), wantBoth)

	// Presence update fans the exact updated snapshot to both peers.
	a.post(t, ctx, map[string]any{
		"op": "set-presence", "room-id": "cf003-room",
		"data": map[string]any{"mood": "great"}, "client-event-id": "presence-a",
	})
	wantUpdated := map[string]any{
		aSess: map[string]any{"peer": "peer-a", "data": map[string]any{"mood": "great"}},
		bSess: map[string]any{"peer": "peer-b", "data": map[string]any{}},
	}
	fu01AwaitAckAndPresence(t, a, "A", &logA, "set-presence-ok", map[string]any{
		"op": "set-presence-ok", "room-id": "cf003-room", "client-event-id": "presence-a",
	}, wantUpdated)
	cf003AssertSSERoomPresence(t, fu01Read(t, b, "B", &logB), wantUpdated)

	// Peer-only broadcast: sender sees only its ack; peer sees the exact
	// server-broadcast carrying the sender session.
	a.post(t, ctx, map[string]any{
		"op": "client-broadcast", "room-id": "cf003-room", "topic": "chat",
		"data": map[string]any{"message": "hello"}, "client-event-id": "broadcast-a",
	})
	cf003SSEExact(t, fu01Read(t, a, "A", &logA), map[string]any{
		"op": "client-broadcast-ok", "client-event-id": "broadcast-a",
	})
	wantBroadcast := map[string]any{
		"op": "server-broadcast", "room-id": "cf003-room", "topic": "chat",
		"session-id": aSess,
		"data":       map[string]any{"peer": "peer-a", "data": map[string]any{"message": "hello"}},
	}
	cf003SSEExact(t, fu01Read(t, b, "B", &logB), wantBroadcast)

	// Leave converges the survivor to the exact one-member snapshot.
	b.post(t, ctx, map[string]any{
		"op": "leave-room", "room-id": "cf003-room", "client-event-id": "leave-b",
	})
	cf003SSEExact(t, fu01Read(t, b, "B", &logB), map[string]any{
		"op": "leave-room-ok", "room-id": "cf003-room", "client-event-id": "leave-b",
	})
	wantA := map[string]any{
		aSess: map[string]any{"peer": "peer-a", "data": map[string]any{"mood": "great"}},
	}
	cf003AssertSSERoomPresence(t, fu01Read(t, a, "A", &logA), wantA)

	// Retention completeness: exact per-subscriber op sequences. Either
	// order inside each ack/presence pair is normalized here by checking
	// the pair as a set.
	fu01AssertRoomOps(t, logA, "A", [][]string{
		{"init-ok"},
		{"join-room-ok", "refresh-presence"},
		{"refresh-presence"},
		{"set-presence-ok", "refresh-presence"},
		{"client-broadcast-ok"},
		{"refresh-presence"},
	})
	fu01AssertRoomOps(t, logB, "B", [][]string{
		{"init-ok"},
		{"join-room-ok", "refresh-presence"},
		{"refresh-presence"},
		{"refresh-presence"},
		{"server-broadcast"},
		{"leave-room-ok"},
	})
	fu01AssertRetentionSeq(t, logA, "A", 8)
	fu01AssertRetentionSeq(t, logB, "B", 7)

	// Replay from retention alone: every retained presence frame
	// re-asserts against the recorded session facts, and the broadcast
	// re-asserts the exact sender session — no live stream is touched.
	fu01ReplayRoomPresence(t, logA, "A", []map[string]any{wantSolo, wantBoth, wantUpdated, wantA})
	fu01ReplayRoomPresence(t, logB, "B", []map[string]any{wantBoth, wantBoth, wantUpdated})
	cf003SSEExact(t, fu01FindOp(t, logB, "B", "server-broadcast"), wantBroadcast)

	// Bounded quiescence on the survivor stream.
	fu01AssertQuiet(t, a, "A", 250*time.Millisecond)

	a.closeAndAwaitUnauthorized(t, ctx)
	b.closeAndAwaitUnauthorized(t, ctx)
}

// fu01AssertRoomOps asserts the retained op sequence step by step, where
// each step is the exact set of ops (order-free within the step, exact
// across steps).
func fu01AssertRoomOps(t *testing.T, log []fu01Frame, name string, wantSteps [][]string) {
	t.Helper()
	got := fu01Ops(log)
	total := 0
	for _, step := range wantSteps {
		total += len(step)
	}
	if len(got) != total {
		t.Fatalf("FU-01 retention %s ops = %#v; want %d frames in %d steps", name, got, total, len(wantSteps))
	}
	offset := 0
	for s, step := range wantSteps {
		window := got[offset : offset+len(step)]
		if len(window) != len(step) {
			t.Fatalf("FU-01 retention %s step %d = %#v; want exactly %#v", name, s, window, step)
		}
		counts := map[string]int{}
		for _, op := range window {
			counts[op]++
		}
		wantCounts := map[string]int{}
		for _, op := range step {
			wantCounts[op]++
		}
		if !reflect.DeepEqual(counts, wantCounts) {
			t.Fatalf("FU-01 retention %s step %d = %#v; want exactly %#v", name, s, window, step)
		}
		offset += len(step)
	}
}

// fu01ReplayRoomPresence re-asserts every retained refresh-presence frame
// against the expected snapshot sequence (lengths before elements via
// cf003AssertSSERoomPresence).
func fu01ReplayRoomPresence(t *testing.T, log []fu01Frame, name string, want []map[string]any) {
	t.Helper()
	var presence []map[string]any
	for _, entry := range log {
		if entry.frame["op"] == "refresh-presence" {
			presence = append(presence, entry.frame)
		}
	}
	if len(presence) != len(want) {
		t.Fatalf("FU-01 replay %s presence frames = %d; want exactly %d", name, len(presence), len(want))
	}
	for i := range want {
		cf003AssertSSERoomPresence(t, presence[i], want[i])
	}
}

// fu01FindOp returns the exactly-one retained frame with the given op.
func fu01FindOp(t *testing.T, log []fu01Frame, name, op string) map[string]any {
	t.Helper()
	var found map[string]any
	count := 0
	for _, entry := range log {
		if entry.frame["op"] == op {
			found = entry.frame
			count++
		}
	}
	if count != 1 {
		t.Fatalf("FU-01 replay %s op %q count = %d; want exactly 1", name, op, count)
	}
	return found
}
