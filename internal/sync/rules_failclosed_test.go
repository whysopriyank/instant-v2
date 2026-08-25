package sync_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
	"github.com/instant-v2/instant-v2/internal/storage"
	syncpkg "github.com/instant-v2/instant-v2/internal/sync"
)

// TestSyncFailsClosedOnRulesLoadError pins the security invariant: when the
// rule-doc loader errors (PG restart, pool exhaustion, corrupt rules row),
// the sync plane must REFUSE ops — never degrade to default-open enforcement.
// The HTTP transact plane already fails closed; this proves WS parity.
func TestSyncFailsClosedOnRulesLoadError(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set")
	}
	ctx := context.Background()
	pool := mustPool(t, dsn)
	resetSchema(t, pool)
	migrate(t, dsn)

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
	go notifier.Run(ctx)

	rulesErr := errors.New("pg: connection refused (simulated outage)")
	mgr := syncpkg.NewManager(syncpkg.Deps{
		Rooms:    syncpkg.NewRoomHub(),
		DB:       st,
		Catalogs: cats,
		Store:    store,
		Rules: func(ctx context.Context, app string) (*perms.RuleDoc, error) {
			return nil, rulesErr
		},
		OnCommit: func(ctx context.Context, appID string, attrIDs []string, txID int64, attrsChanged bool) {
			notifier.Notify(ctx, appID, attrIDs, txID)
		},
	})

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

	dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(dctx, wsURL, &websocket.DialOptions{CompressionMode: websocket.CompressionDisabled})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	frames := make(chan map[string]any, 64)
	go func() {
		defer close(frames)
		for {
			_, data, err := conn.Read(dctx)
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
		if err := conn.Write(dctx, websocket.MessageText, b); err != nil {
			t.Errorf("write: %v", err)
		}
	}
	expectFrame := func() map[string]any {
		select {
		case f := <-frames:
			return f
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for frame")
			return nil
		}
	}

	send(map[string]any{"op": "init", "app-id": uuidStr(appID)})
	initReply := expectFrame()
	if initReply["op"] != "init-ok" {
		t.Fatalf("expected init-ok, got %v", initReply)
	}

	// add-query must be refused while rules are unloadable.
	send(map[string]any{"op": "add-query", "q": map[string]any{"todos": map[string]any{}}, "client-event-id": "q1"})
	f := expectFrame()
	if f["type"] != "rules-unavailable" {
		t.Fatalf("add-query under failed rule load: want type=rules-unavailable, got %v", f)
	}

	// transact must be refused too.
	send(map[string]any{
		"op":              "transact",
		"tx-steps":        []any{[]any{"add-triple", uuidStr(rand16()), uuidStr(rand16()), "x"}},
		"client-event-id": "t1",
	})
	f = expectFrame()
	if f["type"] != "rules-unavailable" {
		t.Fatalf("transact under failed rule load: want type=rules-unavailable, got %v", f)
	}
}
