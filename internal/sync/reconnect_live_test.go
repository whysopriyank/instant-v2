package sync_test

// RT-002d live-socket leg: drop → reconnect convergence over real
// sockets, real PostgreSQL, and the production WS handler.
//
// Companion to TestReconnectConvergesAfterSendFailure (white-box seam
// test), which pins the send-failure → detach → snapshot-reuse path with
// an injected SendRaw error. A server-side write to a dead-but-unreaped
// socket is inherently racy, so no deterministic live test can stage
// failMember itself; this test pins what the owner can observe on the
// wire after a drop:
//
//  1. Two live members share one group and are both certified at one
//     generation (same result, same processed-tx-id).
//  2. One member drops (client close; server DetachAll runs
//     asynchronously — no assertion depends on its timing).
//  3. A fresh socket rejoins the same query while the sibling keeps the
//     group (and its snapshot) alive. Its add-query-ok initial answer
//     must equal the certified generation exactly — same canonical
//     result AND same watermark — with zero recompute (Refresh-call
//     counter stays flat: the snapshot, not a re-query, serves it).
//  4. The rejoined member is fully live: a transact on it reaches both
//     members.
//
// All synchronization is frame-driven (expectOp/awaitRefreshOk bounded
// waits); there are no scheduler sleeps.

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
	"github.com/instant-v2/instant-v2/internal/storage"
	syncpkg "github.com/instant-v2/instant-v2/internal/sync"
)

// canonicalJSON re-marshals v so semantically equal payloads compare
// equal regardless of wire key order or envelope shape.
func canonicalJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var anyv any
	if err := json.Unmarshal(b, &anyv); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	c, err := json.Marshal(anyv)
	if err != nil {
		t.Fatalf("re-marshal: %v", err)
	}
	return string(c)
}

type liveConn struct {
	conn   *websocket.Conn
	frames chan map[string]any
	send   func(v any)
}

func dialLive(t *testing.T, wsURL string) *liveConn {
	t.Helper()
	dctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	conn, _, err := websocket.Dial(dctx, wsURL, &websocket.DialOptions{CompressionMode: websocket.CompressionDisabled})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close(websocket.StatusNormalClosure, "") })
	lc := &liveConn{conn: conn, frames: make(chan map[string]any, 64)}
	go func() {
		defer close(lc.frames)
		for {
			_, data, err := conn.Read(dctx)
			if err != nil {
				return
			}
			var f map[string]any
			_ = json.Unmarshal(data, &f)
			lc.frames <- f
		}
	}()
	lc.send = func(v any) {
		b, _ := json.Marshal(v)
		if err := conn.Write(dctx, websocket.MessageText, b); err != nil {
			t.Errorf("write: %v", err)
		}
	}
	return lc
}

func TestLiveReconnectConvergesAfterDrop(t *testing.T) {
	pool := newPostgres(t)
	ctx := context.Background()

	st := storage.New(pool)
	cats := platform.NewCatalogCache(pool, pool)
	appID := rand16()
	seedApp(t, st, appID)

	var title platform.Attr
	if err := st.WithTx(ctx, func(tx pgx.Tx) error {
		var err error
		title, err = platform.GetOrCreateAttr(ctx, tx, appID, "todos", "text", "blob", "one", false, true)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := cats.For(ctx, uuidStr(appID)); err != nil {
		t.Fatal(err)
	}
	persistRules(t, pool, cats, appID, `{"todos":{"allow":{"view":"true"}}}`)

	store := reactive.NewStore()
	var mgr *syncpkg.Manager
	base := gateRespectingRefresh(pool, cats, func() *syncpkg.Manager { return mgr })
	var refreshCalls atomic.Int64
	refresh := func(c context.Context, sub *reactive.Subscription) (json.RawMessage, error) {
		refreshCalls.Add(1)
		return base(c, sub)
	}
	notifier := &reactive.Notifier{
		Store:   store,
		Refresh: refresh,
		Inc: &reactive.Incremental{
			Source: &reactive.InstaqlSource{DB: pool, Catalog: cats.For},
		},
		Revalidate: func(c context.Context, sub *reactive.Subscription) (bool, error) {
			_, changed, err := mgr.RefreshGate(c, sub)
			return changed, err
		},
	}
	runNotifier(t, notifier)

	mgr = syncpkg.NewManager(syncpkg.Deps{
		Rooms:    syncpkg.NewRoomHub(),
		DB:       st,
		Catalogs: cats,
		Store:    store,
		Rules:    cats.RuleDocFor,
		OnCommit: func(c context.Context, appID string, attrIDs []string, txID int64, attrsChanged bool) {
			notifier.Notify(c, appID, attrIDs, txID)
		},
	})
	srv := httptest.NewServer(&syncpkg.WSHandler{Manager: mgr, Store: store, Refresh: refresh})
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	query := map[string]any{"todos": map[string]any{}}
	attachLive := func(lc *liveConn) map[string]any {
		t.Helper()
		lc.send(map[string]any{"op": "init", "app-id": uuidStr(appID)})
		expectOp(t, lc.frames, "init-ok")
		lc.send(map[string]any{"op": "add-query", "q": query, "client-event-id": "q1"})
		return expectOp(t, lc.frames, "add-query-ok")
	}

	// Two live members share the query. Shared-group membership is
	// proved, not assumed: the first attach computes the initial answer
	// while the second reuses its snapshot, so exactly one Refresh
	// execution covers both attaches. (Byte-equal frames alone would not
	// prove this — two independent groups over the same data render
	// identical bytes.)
	sib := dialLive(t, wsURL)
	if ack := attachLive(sib); ack["result"] == nil {
		t.Fatalf("sibling add-query-ok missing initial result: %v", ack)
	}
	doomed := dialLive(t, wsURL)
	if ack := attachLive(doomed); ack["result"] == nil {
		t.Fatalf("doomed add-query-ok missing initial result: %v", ack)
	}
	if n := refreshCalls.Load(); n != 1 {
		t.Fatalf("attaches computed %d time(s); want exactly 1 (shared group, snapshot reuse)", n)
	}

	// Certify one generation on both members.
	sib.send(map[string]any{
		"op": "transact",
		"tx-steps": []any{
			[]any{"add-triple", uuidStr(rand16()), uuidStr(title.ID), "rt002d-live"},
		},
		"client-event-id": "t1",
	})
	expectOp(t, sib.frames, "transact-ok")
	certified := awaitRefreshOk(sib.frames, 5*time.Second)
	if certified == nil {
		t.Fatal("no refresh-ok after seeding transact; reactive loop is broken")
	}
	if body := frameBytes(t, certified); !strings.Contains(body, "rt002d-live") {
		t.Fatalf("setup: certified refresh-ok missing seeded row: %s", body)
	}
	if dropped := awaitRefreshOk(doomed.frames, 5*time.Second); dropped == nil {
		t.Fatal("doomed member never certified; shared-group premise unproven")
	} else if frameBytes(t, dropped) != frameBytes(t, certified) {
		t.Fatalf("siblings certified different generations:\n g=%s\n f=%s", dropped, certified)
	}
	comps, _ := certified["computations"].([]any)
	if len(comps) != 1 {
		t.Fatalf("certified refresh-ok has %d computations; want 1: %v", len(comps), certified)
	}
	wantResult := canonicalJSON(t, comps[0].(map[string]any)["instaql-result"])
	wantTx, _ := certified["processed-tx-id"].(float64)
	if wantTx == 0 {
		t.Fatalf("certified refresh-ok carries no watermark: %v", certified)
	}

	// Drop one member. Server-side detach races the close handshake;
	// nothing below waits for it — the rejoin assertions hold whether
	// the detach has landed or not, because the sibling keeps the
	// group and its snapshot alive either way.
	doomed.conn.Close(websocket.StatusGoingAway, "rt-002d drop")

	// The counter must have observed the attach-time computes above;
	// otherwise the no-recompute assertion below would pass vacuously.
	if refreshCalls.Load() == 0 {
		t.Fatal("refresh counter never fired during attach; no-recompute assertion is vacuous")
	}

	// Rejoin on a fresh socket while the sibling holds the group.
	refreshCalls.Store(0)
	rejoined := dialLive(t, wsURL)
	ack := attachLive(rejoined)
	if got := canonicalJSON(t, ack["result"]); got != wantResult {
		t.Fatalf("rejoiner diverged from the certified generation:\n got %s\nwant %s", got, wantResult)
	}
	if gotTx, _ := ack["processed-tx-id"].(float64); gotTx != wantTx {
		t.Fatalf("rejoiner watermark %v; want certified %v (skipped or extra generation)", gotTx, wantTx)
	}
	if n := refreshCalls.Load(); n != 0 {
		t.Fatalf("rejoin recomputed %d time(s) instead of converging on the live snapshot", n)
	}

	// The rejoined member is fully live in the group.
	rejoined.send(map[string]any{
		"op": "transact",
		"tx-steps": []any{
			[]any{"add-triple", uuidStr(rand16()), uuidStr(title.ID), "rt002d-after-rejoin"},
		},
		"client-event-id": "t2",
	})
	expectOp(t, rejoined.frames, "transact-ok")
	afterR := awaitRefreshOk(rejoined.frames, 5*time.Second)
	if afterR == nil {
		t.Fatal("rejoined member got no refresh-ok after its own transact; rejoin is not live")
	}
	if body := frameBytes(t, afterR); !strings.Contains(body, "rt002d-after-rejoin") {
		t.Fatalf("rejoined refresh-ok missing its own row: %s", body)
	}
	afterS := awaitRefreshOk(sib.frames, 5*time.Second)
	if afterS == nil {
		t.Fatal("sibling got no refresh-ok after the rejoiner's transact; group fan-out is broken")
	}
	if body := frameBytes(t, afterS); !strings.Contains(body, "rt002d-after-rejoin") {
		t.Fatalf("sibling refresh-ok missing rejoiner row: %s", body)
	}
}
