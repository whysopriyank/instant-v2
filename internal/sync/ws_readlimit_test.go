package sync_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
	"github.com/instant-v2/instant-v2/internal/storage"
	syncpkg "github.com/instant-v2/instant-v2/internal/sync"
)

// Audit H3: inbound frames must be bounded by a server-chosen limit, not a
// fixed 64 MiB. A session that exceeds the configured read limit gets its
// connection closed before the payload is parsed.
func TestWSReadLimitClosesOversizedFrames(t *testing.T) {
	pool := newPostgres(t)
	ctx := context.Background()

	st := storage.New(pool)
	cats := platform.NewCatalogCache(pool, pool)
	appID := rand16()
	seedApp(t, st, appID)
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

	wsHandler := &syncpkg.WSHandler{
		Manager:   mgr,
		Store:     store,
		ReadLimit: 1 << 10, // 1 KiB — deliberately tiny
		Refresh: func(ctx context.Context, sub *reactive.Subscription) (json.RawMessage, error) {
			return runQuery(ex, cats, sub)
		},
	}
	srv := httptest.NewServer(wsHandler)
	defer srv.Close()

	dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(dctx, "ws"+strings.TrimPrefix(srv.URL, "http"),
		&websocket.DialOptions{CompressionMode: websocket.CompressionDisabled})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	initFrame, _ := json.Marshal(map[string]any{"op": "init", "app-id": uuidStr(appID)})
	if err := conn.Write(dctx, websocket.MessageText, initFrame); err != nil {
		t.Fatalf("init write: %v", err)
	}
	if _, _, err := conn.Read(dctx); err != nil { // init-ok
		t.Fatalf("read init-ok: %v", err)
	}

	big := bytes.Repeat([]byte{'a'}, 64<<10)
	if err := conn.Write(dctx, websocket.MessageText, big); err != nil {
		t.Fatalf("oversized write (client side): %v", err)
	}
	// The server must close the connection on the oversized frame.
	errCh := make(chan error, 1)
	go func() {
		for {
			if _, _, err := conn.Read(dctx); err != nil {
				errCh <- err
				return
			}
		}
	}()
	select {
	case <-errCh:
		// expected: read limit breach closed the connection
	case <-time.After(5 * time.Second):
		t.Fatal("oversized frame was accepted; connection stayed open")
	}
}
