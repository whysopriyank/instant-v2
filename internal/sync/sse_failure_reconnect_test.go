package sync_test

// RT-002 task D: SSE failure/reconnect evidence in ONE test.
//
//  1. Real SSE session + query (blocking-writer GET on the production
//     SSEHandler + production POST init/add-query).
//  2. Baseline state + watermark from the initial refresh-ok tree.
//  3. Deterministic queue overflow via the production dispatch path
//     (129 sub.Emit calls against the 128-event buffer while the writer
//     is parked on the initial refresh-ok, mirroring
//     TestAdminSSEOverflowClosesStream).
//  4. Old session/subscription removed (ConnCount 0, Store empty).
//  5. Transact after failure via a healthy sibling WS (production
//     transact -> OnCommit -> Notify chain).
//  6. Fresh SSE session (new GET/handshake/token via mounted routes).
//  7. Re-add query.
//  8. Full state includes the missed tx.
//  9. Watermark includes the missed tx (equals sibling's).
//  10. No empty/malformed frame on either SSE stream.
//  11. Continued delivery after recovery.
//
// Explicit disconnect + full replay (DEC-001): transport failure detaches;
// recovery is fresh + full replay; no exactly-once is claimed.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
)

// sseFailureWriter parks the first refresh-ok write so the bounded runtime
// SSE queue can be filled deterministically through the production dispatch
// path. Mirrors adminOverflowWriter (admin_sse_delivery_test.go) for the
// runtime /runtime/sse stream.
type sseFailureWriter struct {
	header http.Header

	mu   sync.Mutex
	body bytes.Buffer

	handshakeWritten chan struct{}
	addQueryWritten  chan struct{}
	refreshBlocked   chan struct{}
	allowRefresh     chan struct{}

	handshakeOnce sync.Once
	addQueryOnce  sync.Once
	refreshOnce   sync.Once
}

func newSSEFailureWriter() *sseFailureWriter {
	return &sseFailureWriter{
		header:           make(http.Header),
		handshakeWritten: make(chan struct{}),
		addQueryWritten:  make(chan struct{}),
		refreshBlocked:   make(chan struct{}),
		allowRefresh:     make(chan struct{}),
	}
}

func (w *sseFailureWriter) Header() http.Header { return w.header }

func (*sseFailureWriter) WriteHeader(int) {}

func (w *sseFailureWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	_, _ = w.body.Write(p)
	w.mu.Unlock()
	if bytes.Contains(p, []byte(`"init-ok"`)) {
		w.handshakeOnce.Do(func() { close(w.handshakeWritten) })
	}
	if bytes.Contains(p, []byte(`"add-query-ok"`)) {
		w.addQueryOnce.Do(func() { close(w.addQueryWritten) })
	}
	if bytes.Contains(p, []byte(`"refresh-ok"`)) {
		w.refreshOnce.Do(func() {
			close(w.refreshBlocked)
			<-w.allowRefresh
		})
	}
	return len(p), nil
}

func (*sseFailureWriter) Flush() {}

func (*sseFailureWriter) SetWriteDeadline(time.Time) error { return nil }

func (w *sseFailureWriter) Bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.body.Bytes()...)
}

// sseDataFrames parses every `data:` line in raw SSE bytes, failing on any
// empty payload, malformed JSON, or missing op (step 10).
func sseDataFrames(t *testing.T, raw []byte) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := strings.TrimPrefix(line, "data: ")
		if strings.TrimSpace(payload) == "" {
			t.Fatal("empty SSE data frame emitted")
		}
		var f map[string]any
		if err := json.Unmarshal([]byte(payload), &f); err != nil {
			t.Fatalf("malformed SSE data frame %q: %v", line, err)
		}
		op, _ := f["op"].(string)
		if op == "" {
			t.Fatalf("SSE frame missing op: %q", line)
		}
		out = append(out, f)
	}
	if len(out) == 0 {
		t.Fatal("no SSE data frames captured")
	}
	return out
}

func findOp(frames []map[string]any, op string) map[string]any {
	for _, f := range frames {
		if f["op"] == op {
			return f
		}
	}
	return nil
}

func TestSSEFailureReconnectFullReplay(t *testing.T) {
	env := newWSEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	titleAttr := uuidStr(env.IDs.title)
	titleTopic := platform.UUIDToStr(env.IDs.title)

	// ---- 1. Real SSE session + query -----------------------------------
	w := newSSEFailureWriter()
	getReq, err := http.NewRequestWithContext(ctx, http.MethodGet,
		env.Server.URL+"/runtime/sse?app_id="+env.AppID, nil)
	if err != nil {
		t.Fatal(err)
	}
	getReq.RemoteAddr = "127.0.0.1:19876"
	streamDone := make(chan struct{})
	go func() {
		defer close(streamDone)
		env.SSE.ServeHTTP(w, getReq)
	}()
	select {
	case <-w.handshakeWritten:
	case <-ctx.Done():
		t.Fatal("old SSE handshake was not written")
	}

	var hello map[string]any
	for _, f := range sseDataFrames(t, w.Bytes()) {
		hello = f
		break
	}
	oldSessID, _ := hello["session-id"].(string)
	oldToken, _ := hello["sse-token"].(string)
	if oldSessID == "" || oldToken == "" {
		t.Fatalf("old handshake missing session/token: %v", hello)
	}

	postDirect := func(msgs ...map[string]any) {
		t.Helper()
		parts := make([]string, 0, len(msgs))
		for _, m := range msgs {
			b, _ := json.Marshal(m)
			parts = append(parts, string(b))
		}
		body := fmt.Sprintf(`{"machine_id":"m-sse-fail","app_id":%q,"session_id":%q,"sse_token":%q,"messages":[%s]}`,
			env.AppID, oldSessID, oldToken, strings.Join(parts, ","))
		req := httptest.NewRequest(http.MethodPost, "/runtime/sse", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		env.SSE.ServeHTTP(rec, req)
		if rec.Code != 200 {
			t.Fatalf("old SSE POST status %d: %s", rec.Code, rec.Body.String())
		}
	}
	postDirect(map[string]any{"op": "init", "app-id": env.AppID})
	postDirect(map[string]any{
		"op": "add-query", "q": map[string]any{"todos": map[string]any{}},
		"client-event-id": "q-old",
	})
	select {
	case <-w.addQueryWritten:
	case <-time.After(10 * time.Second):
		t.Fatal("old SSE add-query-ok was not written")
	}
	select {
	case <-w.refreshBlocked:
	case <-time.After(10 * time.Second):
		t.Fatal("old SSE initial refresh-ok did not reach the writer")
	}

	// ---- 2. Baseline state + watermark ----------------------------------
	oldFrames := sseDataFrames(t, w.Bytes())
	if ack := findOp(oldFrames, "add-query-ok"); ack == nil {
		t.Fatalf("old stream missing add-query-ok: %v", oldFrames)
	}
	oldSnap := findOp(oldFrames, "refresh-ok")
	if oldSnap == nil {
		t.Fatalf("old stream missing initial refresh-ok: %v", oldFrames)
	}
	oldEntry := computationsOf(t, oldSnap)
	oldTreeCanon := canonicalJSON(t, oldEntry["instaql-result"])
	oldTx, _ := oldSnap["processed-tx-id"].(float64)
	if tree, ok := oldEntry["instaql-result"].(map[string]any); !ok {
		t.Fatalf("old initial answer is not a tree object: %T", oldEntry["instaql-result"])
	} else {
		if _, wrapped := tree["data"]; wrapped {
			t.Fatalf("old SSE tree must not carry a data wrapper: %v", tree)
		}
		if _, hasTodos := tree["todos"]; !hasTodos {
			t.Fatalf("old tree missing todos level: %v", tree)
		}
	}
	if got := env.SSE.ConnCount(); got != 1 {
		t.Fatalf("old SSE ConnCount = %d, want 1", got)
	}
	subs := env.Mgr.Deps.Store.SubsForTopics([]string{titleTopic})
	if len(subs) != 1 {
		t.Fatalf("old subscriptions = %d, want one live query", len(subs))
	}
	sub := subs[0]

	// ---- 3. Deterministic queue overflow ---------------------------------
	// The writer is parked inside the initial refresh-ok write, so the
	// event channel is empty. 128 production dispatches fill it; the 129th
	// takes the overflow path (signalOverflow + detach), exactly as a live
	// notifier burst would.
	gen := sub.Gen.Load()
	snap := sub.Snapshot()
	if snap == nil {
		t.Fatal("old subscription has no snapshot to re-emit")
	}
	for i := 0; i < 129; i++ {
		_ = sub.Emit(reactive.Frame{
			SubID:         sub.ID,
			QueryJSON:     sub.Query,
			ResultJSON:    snap,
			ProcessedTxID: int64(1000 + i),
			Gen:           gen,
		})
	}
	close(w.allowRefresh)
	select {
	case <-streamDone:
	case <-time.After(10 * time.Second):
		t.Fatal("old SSE stream stayed open after queue overflow")
	}

	// ---- 4. Old session/subscription removed -----------------------------
	if got := env.SSE.ConnCount(); got != 0 {
		t.Fatalf("live SSE connections = %d, want 0 after overflow", got)
	}
	if got := env.Mgr.Deps.Store.SubsForTopics([]string{titleTopic}); len(got) != 0 {
		t.Fatalf("subscriptions after overflow teardown = %d, want zero", len(got))
	}
	if n := env.Mgr.Deps.Store.Len(); n != 0 {
		t.Fatalf("store size after overflow teardown = %d, want zero", n)
	}

	// ---- 5. Transact after failure via a healthy sibling WS --------------
	// Non-delta client version: every generation arrives as a full
	// refresh-ok carrying the watermark (a 0.23.0 sibling would get
	// refresh-ok-delta patches once a diffable baseline exists).
	wsConn, wsFrames := dialInit(t, ctx, env, "0.22.9")
	sibAck := addQueryTodos(t, ctx, wsConn, wsFrames)
	if sibAck["result"] == nil {
		t.Fatalf("sibling add-query-ok missing initial result: %v", sibAck)
	}
	const missedTitle = "sse-failure-missed"
	transactTodo(t, ctx, wsConn, wsFrames, "add-triple", uuidStr(rand16()), titleAttr, missedTitle)
	sibRefresh := awaitRefreshOk(wsFrames, 10*time.Second)
	if sibRefresh == nil {
		t.Fatal("sibling got no refresh-ok for the missed tx; reactive loop broken")
	}
	if body := frameBytes(t, sibRefresh); !strings.Contains(body, missedTitle) {
		t.Fatalf("sibling refresh-ok missing missed row: %s", body)
	}
	sibTx, _ := sibRefresh["processed-tx-id"].(float64)
	if sibTx == 0 {
		t.Fatalf("sibling refresh-ok carries no watermark: %v", sibRefresh)
	}
	if n := env.Mgr.Deps.Store.Len(); n != 1 {
		t.Fatalf("store size with sibling live = %d, want one shared group", n)
	}

	// ---- 6+7. Fresh SSE session + re-add query (mounted routes) ----------
	freshReq, _ := http.NewRequestWithContext(ctx, http.MethodGet,
		env.Server.URL+"/runtime/sse?app_id="+env.AppID, nil)
	freshResp, err := env.Server.Client().Do(freshReq)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = freshResp.Body.Close() }()
	if freshResp.StatusCode != 200 {
		t.Fatalf("fresh SSE GET status %d", freshResp.StatusCode)
	}
	scanner := bufio.NewScanner(freshResp.Body)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	var freshRaw []string
	nextFresh := func() map[string]any {
		t.Helper()
		f := nextEvent(t, scanner)
		b, _ := json.Marshal(f)
		freshRaw = append(freshRaw, "data: "+string(b))
		if op, _ := f["op"].(string); op == "" {
			t.Fatalf("fresh SSE frame missing op: %v", f)
		}
		return f
	}
	freshHello := nextFresh()
	freshSessID, _ := freshHello["session-id"].(string)
	freshToken, _ := freshHello["sse-token"].(string)
	if freshSessID == "" || freshToken == "" {
		t.Fatalf("fresh handshake missing session/token: %v", freshHello)
	}
	if freshToken == oldToken {
		t.Fatal("fresh SSE reused the failed stream token")
	}
	postFresh := func(msgs ...map[string]any) {
		t.Helper()
		b, _ := json.Marshal(map[string]any{
			"machine_id": "m-sse-fresh", "app_id": env.AppID,
			"session_id": freshSessID, "sse_token": freshToken,
			"messages": msgs,
		})
		r2, err := http.NewRequestWithContext(ctx, http.MethodPost,
			env.Server.URL+"/runtime/sse", bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		r2.Header.Set("Content-Type", "application/json")
		resp2, err := env.Server.Client().Do(r2)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp2.Body.Close()
		if resp2.StatusCode != 200 {
			t.Fatalf("fresh SSE POST status %d", resp2.StatusCode)
		}
	}
	postFresh(map[string]any{"op": "init", "app-id": env.AppID})
	if f := nextFresh(); f["op"] != "init-ok" {
		t.Fatalf("fresh SSE expected init-ok, got %v", f)
	}
	postFresh(map[string]any{
		"op": "add-query", "q": map[string]any{"todos": map[string]any{}},
		"client-event-id": "q-fresh",
	})
	if f := nextFresh(); f["op"] != "add-query-ok" {
		t.Fatalf("fresh SSE expected add-query-ok, got %v", f)
	}
	rejoinSnap := nextFresh()
	if rejoinSnap["op"] != "refresh-ok" {
		t.Fatalf("fresh SSE expected initial refresh-ok, got %v", rejoinSnap)
	}
	if n := env.Mgr.Deps.Store.Len(); n != 1 {
		t.Fatalf("store size after fresh SSE rejoin = %d, want one shared group", n)
	}

	// ---- 8. Full state includes the missed tx -----------------------------
	rejoinEntry := computationsOf(t, rejoinSnap)
	rejoinTreeCanon := canonicalJSON(t, rejoinEntry["instaql-result"])
	if !strings.Contains(rejoinTreeCanon, missedTitle) {
		t.Fatalf("rejoined SSE state missing missed row %q: %s", missedTitle, rejoinTreeCanon)
	}
	if strings.Contains(oldTreeCanon, missedTitle) {
		t.Fatal("baseline unexpectedly contains the missed row; failure/transact ordering broken")
	}

	// ---- 9. Watermark includes the missed tx -------------------------------
	rejoinTx, _ := rejoinSnap["processed-tx-id"].(float64)
	if rejoinTx == 0 {
		t.Fatalf("rejoined SSE carries no watermark: %v", rejoinSnap)
	}
	if rejoinTx != sibTx {
		t.Fatalf("rejoined watermark %v != sibling certified %v", rejoinTx, sibTx)
	}
	if rejoinTx <= oldTx {
		t.Fatalf("rejoined watermark %v did not advance past baseline %v", rejoinTx, oldTx)
	}

	// ---- 11. Continued delivery after recovery ------------------------------
	const afterTitle = "sse-failure-after"
	transactTodo(t, ctx, wsConn, wsFrames, "add-triple", uuidStr(rand16()), titleAttr, afterTitle)
	sibAfter := awaitRefreshOk(wsFrames, 10*time.Second)
	if sibAfter == nil {
		t.Fatal("sibling got no refresh-ok after the second transact")
	}
	if body := frameBytes(t, sibAfter); !strings.Contains(body, afterTitle) {
		t.Fatalf("sibling post-recovery refresh-ok missing row: %s", body)
	}
	freshAfter := nextFresh()
	if freshAfter["op"] != "refresh-ok" {
		t.Fatalf("fresh SSE expected post-recovery refresh-ok, got %v", freshAfter)
	}
	if body := frameBytes(t, freshAfter); !strings.Contains(body, afterTitle) {
		t.Fatalf("fresh SSE post-recovery refresh-ok missing row: %s", body)
	}
	afterTx, _ := freshAfter["processed-tx-id"].(float64)
	sibAfterTx, _ := sibAfter["processed-tx-id"].(float64)
	if afterTx == 0 || afterTx != sibAfterTx {
		t.Fatalf("post-recovery watermarks diverged: fresh %v sibling %v", afterTx, sibAfterTx)
	}
	if afterTx <= rejoinTx {
		t.Fatalf("post-recovery watermark %v did not advance past rejoin %v", afterTx, rejoinTx)
	}

	// ---- 10. No empty/malformed frame on either stream -----------------------
	// Old stream: every buffered data line must already have parsed above;
	// re-validate the final buffer (includes the 128 flood envelopes).
	sseDataFrames(t, w.Bytes())
	for _, line := range freshRaw {
		payload := strings.TrimPrefix(line, "data: ")
		if strings.TrimSpace(payload) == "" {
			t.Fatal("fresh SSE emitted an empty data frame")
		}
		var f map[string]any
		if err := json.Unmarshal([]byte(payload), &f); err != nil {
			t.Fatalf("fresh SSE malformed frame %q: %v", line, err)
		}
		if op, _ := f["op"].(string); op == "" {
			t.Fatalf("fresh SSE frame missing op: %q", line)
		}
	}
}
