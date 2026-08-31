package sync_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
	"github.com/instant-v2/instant-v2/internal/storage"
	syncpkg "github.com/instant-v2/instant-v2/internal/sync"
)

// fakeDeps builds a Manager+Store+Refresh wired to a live DB without HTTP.
func fakeStack(t *testing.T) (*syncpkg.Manager, *reactive.Store, *reactive.Notifier, *instaql.Executor, [16]byte, *platform.CatalogCache, qattr) {
	t.Helper()
	pool := newPostgres(t)
	ctx := context.Background()

	st := storage.New(pool)
	cats := platform.NewCatalogCache(pool, pool)
	appID := rand16()
	seedApp(t, st, appID)

	var title, done, link platform.Attr
	if err := st.WithTx(ctx, func(tx pgx.Tx) error {
		var e1, e2, e3 error
		title, e1 = platform.GetOrCreateAttr(ctx, tx, appID, "todos", "text", "blob", "one", false, true)
		done, e2 = platform.GetOrCreateAttr(ctx, tx, appID, "todos", "done", "boolean", "one", false, true)
		link, e3 = platform.GetOrCreateAttrRev(ctx, tx, appID,
			"comments", "todo", strptr("todos"), strptr("comments"), "ref", "many", false, false)
		if e1 != nil {
			return e1
		}
		if e2 != nil {
			return e2
		}
		return e3
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := cats.For(ctx, uuidStr(appID)); err != nil {
		t.Fatal(err)
	}

	store := reactive.NewStore()
	ex := &instaql.Executor{DB: pool}
	notifier := &reactive.Notifier{
		Store: store,
		Refresh: func(ctx context.Context, sub *reactive.Subscription) (json.RawMessage, error) {
			return runQuery(ex, cats, sub)
		},
	}
	runNotifier(t, notifier)

	mgr := syncpkg.NewManager(syncpkg.Deps{
		Rooms:    syncpkg.NewRoomHub(),
		DB:       st,
		Catalogs: cats,
		Store:    store,
		OnCommit: func(ctx context.Context, appID string, attrIDs []string, txID int64, attrsChanged bool) {
			notifier.Notify(ctx, appID, attrIDs, txID)
		},
	})
	return mgr, store, notifier, ex, appID, cats, qattr{title: title.ID, done: done.ID, link: link.ID}
}

type qattr struct{ title, done, link [16]byte }

// TestSessionFlow runs init → add-query → transact → refresh-ok over an
// httptest WS server, proving the reactive loop end-to-end.
func TestSessionFlow(t *testing.T) {
	mgr, store, notifier, ex, appID, cats, ids := fakeStack(t)
	wsHandler := &syncpkg.WSHandler{
		Manager: mgr,
		Store:   store,
		Refresh: func(ctx context.Context, sub *reactive.Subscription) (json.RawMessage, error) {
			return runQuery(ex, cats, sub)
		},
	}
	srv := httptest.NewServer(wsHandler)
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{CompressionMode: websocket.CompressionDisabled})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	frames := make(chan map[string]any, 64)
	go func() {
		defer close(frames)
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var f map[string]any
			_ = json.Unmarshal(data, &f)
			frames <- f
		}
	}()

	send := func(v any) {
		b, _ := json.Marshal(v)
		if err := conn.Write(ctx, websocket.MessageText, b); err != nil {
			t.Errorf("write: %v", err)
		}
	}

	// 1. init
	send(map[string]any{
		"op": "init", "app-id": uuidStr(appID),
		"versions": map[string]string{"@instantdb/core": "0.22.0"},
	})
	expectOp(t, frames, "init-ok")

	// 2. add-query todos
	q := map[string]any{"todos": map[string]any{}}
	send(map[string]any{"op": "add-query", "q": q, "client-event-id": "q1"})
	ack := expectOp(t, frames, "add-query-ok")
	if _, ok := ack["result"]; !ok { // v1: initial answer rides the ack
		t.Fatalf("add-query-ok missing initial result: %v", ack)
	}

	// 3. transact: create one todo
	steps := []any{
		[]any{"add-triple", uuidStr(rand16()), uuidStr(ids.title), "hello"},
	}
	send(map[string]any{"op": "transact", "tx-steps": steps, "client-event-id": "t1"})
	expectOp(t, frames, "transact-ok")

	// After transact commits, the direct notifier should push another
	// refresh-ok through the reactive loop.
	expectOp(t, frames, "refresh-ok")
	_ = notifier
}

func expectOp(t *testing.T, frames chan map[string]any, op string) map[string]any {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", op)
		case f, ok := <-frames:
			if !ok {
				t.Fatalf("connection closed waiting for %s", op)
			}
			if got, _ := f["op"].(string); got == op {
				return f
			}
		}
	}
}

func runQuery(ex *instaql.Executor, cats *platform.CatalogCache, sub *reactive.Subscription) (json.RawMessage, error) {
	q, err := instaql.Coerce(rawMap(sub.Query))
	if err != nil {
		return nil, err
	}
	cat, err := cats.For(context.Background(), sub.AppID)
	if err != nil {
		return nil, err
	}
	appID, err := platform.ScanUUIDErr(sub.AppID)
	if err != nil {
		return nil, err
	}
	res, err := ex.Run(context.Background(), q, cat, appID)
	if err != nil {
		return nil, err
	}
	return json.Marshal(res)
}

var _ = fmt.Sprintf
