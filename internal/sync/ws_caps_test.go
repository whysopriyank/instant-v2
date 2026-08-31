package sync_test

import (
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

// Audit F6/M6: re-initializing a session must detach the previous session's
// subscriptions and room memberships. Pre-fix, the orphaned member kept its
// per-app subscription slot forever — an unauthenticated cap-exhaustion DoS.
func TestWSReInitReleasesSubscriptionSlot(t *testing.T) {
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
	store.MaxSubsPerApp = 1 // the slot whose leak this test detects
	ex := &instaql.Executor{DB: pool}
	notifier := &reactive.Notifier{
		Store: store,
		Refresh: func(ctx context.Context, sub *reactive.Subscription) (json.RawMessage, error) {
			return runQuery(ex, cats, sub)
		},
	}
	runNotifier(t, notifier)
	mgr := syncpkg.NewManager(syncpkg.Deps{
		Rooms: syncpkg.NewRoomHub(), DB: st, Catalogs: cats, Store: store,
		OnCommit: func(ctx context.Context, appID string, attrIDs []string, txID int64, attrsChanged bool) {
			notifier.Notify(ctx, appID, attrIDs, txID)
		},
	})

	srv := httptest.NewServer(&syncpkg.WSHandler{
		Manager: mgr,
		Store:   store,
		Refresh: func(ctx context.Context, sub *reactive.Subscription) (json.RawMessage, error) {
			return runQuery(ex, cats, sub)
		},
	})
	defer srv.Close()

	dctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(dctx, "ws"+strings.TrimPrefix(srv.URL, "http"),
		&websocket.DialOptions{CompressionMode: websocket.CompressionDisabled})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	frames := make(chan map[string]any, 32)
	go func() {
		for {
			_, data, err := conn.Read(dctx)
			if err != nil {
				close(frames)
				return
			}
			var f map[string]any
			_ = json.Unmarshal(data, &f)
			frames <- f
		}
	}()
	send := func(v any) map[string]any {
		b, _ := json.Marshal(v)
		if err := conn.Write(dctx, websocket.MessageText, b); err != nil {
			t.Fatalf("write: %v", err)
		}
		select {
		case f := <-frames:
			return f
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for reply")
			return nil
		}
	}

	q := map[string]any{"todos": map[string]any{}}
	if f := send(map[string]any{"op": "init", "app-id": uuidStr(appID)}); f["op"] != "init-ok" {
		t.Fatalf("init1: %v", f)
	}
	if f := send(map[string]any{"op": "add-query", "q": q, "client-event-id": "q1"}); f["op"] != "add-query-ok" {
		t.Fatalf("add-query1: %v", f)
	}
	// Re-init: sess2 replaces sess1. The old member must be detached NOW,
	// not left holding the app's only subscription slot.
	if f := send(map[string]any{"op": "init", "app-id": uuidStr(appID)}); f["op"] != "init-ok" {
		t.Fatalf("init2: %v", f)
	}
	f := send(map[string]any{"op": "add-query", "q": q, "client-event-id": "q2"})
	if f["type"] == "subscription-limit" {
		t.Fatal("re-init leaked the previous session's subscription slot")
	}
	if f["op"] != "add-query-ok" {
		t.Fatalf("add-query after re-init: %v", f)
	}
}

// Audit M4: presence payloads are bounded — a multi-megabyte blob would be
// fanned to every room member on every mutation.
func TestWSRejectsOversizedPresenceData(t *testing.T) {
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
		Rooms: syncpkg.NewRoomHub(), DB: st, Catalogs: cats, Store: store,
	})

	srv := httptest.NewServer(&syncpkg.WSHandler{
		Manager: mgr,
		Store:   store,
		Refresh: func(ctx context.Context, sub *reactive.Subscription) (json.RawMessage, error) {
			return runQuery(ex, cats, sub)
		},
	})
	defer srv.Close()

	dctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(dctx, "ws"+strings.TrimPrefix(srv.URL, "http"),
		&websocket.DialOptions{CompressionMode: websocket.CompressionDisabled})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	frames := make(chan map[string]any, 32)
	go func() {
		for {
			_, data, err := conn.Read(dctx)
			if err != nil {
				close(frames)
				return
			}
			var f map[string]any
			_ = json.Unmarshal(data, &f)
			frames <- f
		}
	}()
	send := func(v any) map[string]any {
		b, _ := json.Marshal(v)
		if err := conn.Write(dctx, websocket.MessageText, b); err != nil {
			t.Fatalf("write: %v", err)
		}
		select {
		case f := <-frames:
			return f
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for reply")
			return nil
		}
	}

	if f := send(map[string]any{"op": "init", "app-id": uuidStr(appID)}); f["op"] != "init-ok" {
		t.Fatalf("init: %v", f)
	}
	if f := send(map[string]any{"op": "join-room", "room-id": "r1"}); f["op"] != "join-room-ok" {
		t.Fatalf("join: %v", f)
	}
	blob := strings.Repeat("x", 8<<10) // 8 KiB > 4 KiB cap
	f := send(map[string]any{"op": "set-presence", "room-id": "r1", "data": blob})
	if f["op"] == "set-presence-ok" || f["type"] == "" {
		t.Fatalf("oversized presence data must be rejected, got %v", f)
	}
}
