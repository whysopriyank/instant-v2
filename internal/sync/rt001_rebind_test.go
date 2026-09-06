package sync_test

// RT-001a preparation: allow→deny rebinding red test.
//
// Desired invariant: an allow→deny persisted rule update prevents all
// subsequent protected frames on the existing subscription. This test records
// the DESIRED behavior, so it FAILS while the leak exists (PROVEN_RED) and
// passes once RT-001 reauthorizes, detaches, or version-binds the group.
//
// Production path under test: attach captures QueryGate at
// internal/sync/groups.go:143; refresh executors reproduce attach-time
// visibility from that stale gate (cmd/instantd/refresh.go:28-31,
// cmd/instantd/routes.go:79-81). The Refresh below mirrors that production
// executor exactly, including the stale-gate read.
//
// Trigger model: after the deny is persisted, a second WS transact commits
// (create/update checks default open for unlisted actions per perms.Check),
// which routes OnCommit → notifier.Notify exactly as a peer commit would.
// No scheduler sleeps; all synchronization is frame-driven.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
	"github.com/instant-v2/instant-v2/internal/storage"
	syncpkg "github.com/instant-v2/instant-v2/internal/sync"
)

func persistRules(t *testing.T, pool *pgxpool.Pool, cats *platform.CatalogCache, appID [16]byte, code string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO rules (app_id, code) VALUES ($1::uuid, $2::jsonb)
		 ON CONFLICT (app_id) DO UPDATE SET code = EXCLUDED.code`,
		appID, code); err != nil {
		t.Fatalf("persist rules: %v", err)
	}
	cats.Invalidate(uuidStr(appID))
}

// gateRespectingRefresh mirrors the production executor seam: every
// generation runs under Manager.RefreshGate (transparent re-gate), and a
// reload failure drops the generation. getMgr is late-bound because the
// notifier needs Refresh before the Manager exists — same construction order
// as cmd/instantd.
func gateRespectingRefresh(pool *pgxpool.Pool, cats *platform.CatalogCache, getMgr func() *syncpkg.Manager) func(context.Context, *reactive.Subscription) (json.RawMessage, error) {
	return func(ctx context.Context, sub *reactive.Subscription) (json.RawMessage, error) {
		q, err := instaql.Coerce(rawMap(sub.Query))
		if err != nil {
			return nil, err
		}
		cat, err := cats.For(ctx, sub.AppID)
		if err != nil {
			return nil, err
		}
		appID, err := platform.ScanUUIDErr(sub.AppID)
		if err != nil {
			return nil, err
		}
		runner := &instaql.Executor{DB: pool}
		gate, _, rerr := getMgr().RefreshGate(ctx, sub)
		if rerr != nil {
			return nil, rerr
		}
		gated := gate != nil
		if gated {
			runner = &instaql.Executor{DB: pool, Rules: gate.Rules, Admin: gate.Admin}
		}
		res, err := runner.Run(ctx, q, cat, appID)
		if err != nil {
			return nil, err
		}
		if gated {
			// Mirror of the production publication-time guard
			// (cmd/instantd/refresh.go): drop generations a concurrent
			// re-gate overtook mid-run.
			now, _, rerr := getMgr().RefreshGate(ctx, sub)
			if rerr != nil {
				return nil, rerr
			}
			if now == nil || syncpkg.GateHash(now.Rules) != syncpkg.GateHash(gate.Rules) {
				return nil, errors.New("test mirror: generation superseded by a concurrent rule change")
			}
		}
		return json.Marshal(res)
	}
}

func frameBytes(t *testing.T, f map[string]any) string {
	t.Helper()
	b, err := json.Marshal(f)
	if err != nil {
		t.Fatalf("marshal frame: %v", err)
	}
	return string(b)
}

// awaitRefreshOk returns the first refresh-ok frame within the window, or nil
// on timeout. A timeout is a legal deny outcome (detached/no further frames),
// never a leak.
func awaitRefreshOk(frames chan map[string]any, window time.Duration) map[string]any {
	deadline := time.After(window)
	for {
		select {
		case <-deadline:
			return nil
		case f, ok := <-frames:
			if !ok {
				return nil
			}
			if got, _ := f["op"].(string); got == "refresh-ok" {
				return f
			}
		}
	}
}

func TestRealtimeRebindDeniesExistingSubscription(t *testing.T) {
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

	// Persisted rules start open; the loader is the production one.
	persistRules(t, pool, cats, appID, `{"todos":{"allow":{"view":"true"}}}`)

	store := reactive.NewStore()
	var mgr *syncpkg.Manager
	refresh := gateRespectingRefresh(pool, cats, func() *syncpkg.Manager { return mgr })
	// Production-faithful wiring (cmd/instantd/runtime.go + mountRoutes):
	// incremental maintenance is enabled so the red covers the splice path
	// too, and Revalidate forces full recompute on a rule change.
	notifier := &reactive.Notifier{
		Store:   store,
		Refresh: refresh,
		Inc: &reactive.Incremental{
			Source: &reactive.InstaqlSource{DB: pool, Catalog: cats.For},
		},
		Revalidate: func(ctx context.Context, sub *reactive.Subscription) (bool, error) {
			_, changed, err := mgr.RefreshGate(ctx, sub)
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
		OnCommit: func(ctx context.Context, appID string, attrIDs []string, txID int64, attrsChanged bool) {
			notifier.Notify(ctx, appID, attrIDs, txID)
		},
	})
	wsHandler := &syncpkg.WSHandler{Manager: mgr, Store: store, Refresh: refresh}
	srv := httptest.NewServer(wsHandler)
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	dctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	send(map[string]any{"op": "init", "app-id": uuidStr(appID)})
	expectOp(t, frames, "init-ok")

	// Attach under allow.
	send(map[string]any{"op": "add-query", "q": map[string]any{"todos": map[string]any{}}, "client-event-id": "q1"})
	ack := expectOp(t, frames, "add-query-ok")
	if _, ok := ack["result"]; !ok {
		t.Fatalf("add-query-ok missing initial result: %v", ack)
	}

	// Seed one protected todo; live delivery under allow must carry it.
	send(map[string]any{
		"op": "transact",
		"tx-steps": []any{
			[]any{"add-triple", uuidStr(rand16()), uuidStr(title.ID), "leak-me"},
		},
		"client-event-id": "t1",
	})
	expectOp(t, frames, "transact-ok")
	seeded := awaitRefreshOk(frames, 5*time.Second)
	if seeded == nil {
		t.Fatal("no refresh-ok after seeding transact; reactive loop is broken, red is meaningless")
	}
	if !strings.Contains(frameBytes(t, seeded), "leak-me") {
		t.Fatalf("setup: seeding refresh-ok does not contain the todo: %v", seeded)
	}

	// Persist deny through the real loader path.
	persistRules(t, pool, cats, appID, `{"todos":{"allow":{"view":"false"}}}`)

	// Peer-style commit after deny; the existing subscription must not
	// receive newly denied data afterwards.
	send(map[string]any{
		"op": "transact",
		"tx-steps": []any{
			[]any{"add-triple", uuidStr(rand16()), uuidStr(title.ID), "after-deny"},
		},
		"client-event-id": "t2",
	})
	expectOp(t, frames, "transact-ok")
	after := awaitRefreshOk(frames, 5*time.Second)
	if after == nil {
		t.Log("no post-deny refresh-ok: deny enforced by detach; desired behavior holds")
		return
	}
	if body := frameBytes(t, after); strings.Contains(body, "leak-me") || strings.Contains(body, "after-deny") {
		t.Fatalf("RT-001a RED: post-deny refresh-ok leaks protected data: %s", body)
	}
}

// TestRealtimeRegrantRestoresExistingSubscription pins RT-001b: a
// deny→allow persisted rule change takes effect on the existing
// subscription at the next refresh boundary — no resubscribe, and the new
// generation never runs under the stale deny. Mirror image of RT-001a:
// without transparent re-gating the re-grant would never arrive.
func TestRealtimeRegrantRestoresExistingSubscription(t *testing.T) {
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

	// Persisted rules start closed; the loader is the production one.
	persistRules(t, pool, cats, appID, `{"todos":{"allow":{"view":"false"}}}`)

	store := reactive.NewStore()
	var mgr *syncpkg.Manager
	refresh := gateRespectingRefresh(pool, cats, func() *syncpkg.Manager { return mgr })
	notifier := &reactive.Notifier{
		Store:   store,
		Refresh: refresh,
		Inc: &reactive.Incremental{
			Source: &reactive.InstaqlSource{DB: pool, Catalog: cats.For},
		},
		Revalidate: func(ctx context.Context, sub *reactive.Subscription) (bool, error) {
			_, changed, err := mgr.RefreshGate(ctx, sub)
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
		OnCommit: func(ctx context.Context, appID string, attrIDs []string, txID int64, attrsChanged bool) {
			notifier.Notify(ctx, appID, attrIDs, txID)
		},
	})
	wsHandler := &syncpkg.WSHandler{Manager: mgr, Store: store, Refresh: refresh}
	srv := httptest.NewServer(wsHandler)
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	dctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	send(map[string]any{"op": "init", "app-id": uuidStr(appID)})
	expectOp(t, frames, "init-ok")

	// Attach under deny.
	send(map[string]any{"op": "add-query", "q": map[string]any{"todos": map[string]any{}}, "client-event-id": "q1"})
	ack := expectOp(t, frames, "add-query-ok")
	if _, ok := ack["result"]; !ok {
		t.Fatalf("add-query-ok missing initial result: %v", ack)
	}

	// Seed one todo; under deny it must not be delivered.
	send(map[string]any{
		"op": "transact",
		"tx-steps": []any{
			[]any{"add-triple", uuidStr(rand16()), uuidStr(title.ID), "deny-seed"},
		},
		"client-event-id": "t1",
	})
	expectOp(t, frames, "transact-ok")
	if denied := awaitRefreshOk(frames, 5*time.Second); denied != nil {
		if body := frameBytes(t, denied); strings.Contains(body, "deny-seed") {
			t.Fatalf("setup: deny not enforced at attach: %v", denied)
		}
	} else {
		t.Log("no refresh-ok under deny: deny enforced by detach; re-grant leg remains decisive")
	}

	// Persist allow through the real loader path.
	persistRules(t, pool, cats, appID, `{"todos":{"allow":{"view":"true"}}}`)

	// Peer-style commit after re-grant; the SAME subscription must deliver
	// the newly allowed data without any resubscribe.
	send(map[string]any{
		"op": "transact",
		"tx-steps": []any{
			[]any{"add-triple", uuidStr(rand16()), uuidStr(title.ID), "after-allow"},
		},
		"client-event-id": "t2",
	})
	expectOp(t, frames, "transact-ok")
	after := awaitRefreshOk(frames, 5*time.Second)
	if after == nil {
		t.Fatal("RT-001b RED: no refresh-ok after re-grant; stale deny retained without resubscribe (or the reactive loop is broken)")
	}
	if body := frameBytes(t, after); !strings.Contains(body, "after-allow") {
		t.Fatalf("RT-001b RED: re-grant refresh-ok missing new data (stale deny?): %s", body)
	}
}

// TestRealtimeRuleOutageDropsAndRecovers pins RT-001e: while the rule
// loader fails, refresh generations are dropped — never served under the
// retained allow gate — and when loading recovers, the same subscription
// serves current data again. Neither a stale serve nor a pinned failure.
//
// The outage is injected behind the manager's loader, and invalidations
// are raised directly through the notifier: the transact path also loads
// rules, so a transact would fail before it could invalidate.
func TestRealtimeRuleOutageDropsAndRecovers(t *testing.T) {
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
	refresh := gateRespectingRefresh(pool, cats, func() *syncpkg.Manager { return mgr })
	notifier := &reactive.Notifier{
		Store:   store,
		Refresh: refresh,
		Inc: &reactive.Incremental{
			Source: &reactive.InstaqlSource{DB: pool, Catalog: cats.For},
		},
		Revalidate: func(ctx context.Context, sub *reactive.Subscription) (bool, error) {
			_, changed, err := mgr.RefreshGate(ctx, sub)
			return changed, err
		},
	}
	runNotifier(t, notifier)

	// Swappable loader behind its own mutex: the manager treats
	// Deps.Rules as immutable after construction, so the outage is
	// injected through an indirection rather than a racy reassignment.
	var rulesMu sync.Mutex
	rulesFn := cats.RuleDocFor
	mgr = syncpkg.NewManager(syncpkg.Deps{
		Rooms:    syncpkg.NewRoomHub(),
		DB:       st,
		Catalogs: cats,
		Store:    store,
		Rules: func(ctx context.Context, appID string) (*perms.RuleDoc, error) {
			rulesMu.Lock()
			defer rulesMu.Unlock()
			return rulesFn(ctx, appID)
		},
		OnCommit: func(ctx context.Context, appID string, attrIDs []string, txID int64, attrsChanged bool) {
			notifier.Notify(ctx, appID, attrIDs, txID)
		},
	})
	wsHandler := &syncpkg.WSHandler{Manager: mgr, Store: store, Refresh: refresh}
	srv := httptest.NewServer(wsHandler)
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	dctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	send(map[string]any{"op": "init", "app-id": uuidStr(appID)})
	expectOp(t, frames, "init-ok")
	send(map[string]any{"op": "add-query", "q": map[string]any{"todos": map[string]any{}}, "client-event-id": "q1"})
	expectOp(t, frames, "add-query-ok")

	// Sanity: the loop delivers under allow.
	send(map[string]any{
		"op": "transact",
		"tx-steps": []any{
			[]any{"add-triple", uuidStr(rand16()), uuidStr(title.ID), "pre-outage"},
		},
		"client-event-id": "t1",
	})
	expectOp(t, frames, "transact-ok")
	if sane := awaitRefreshOk(frames, 5*time.Second); sane == nil {
		t.Fatal("no refresh-ok under allow; reactive loop is broken, red is meaningless")
	} else if body := frameBytes(t, sane); !strings.Contains(body, "pre-outage") {
		t.Fatalf("setup: allow refresh-ok missing data: %s", body)
	}

	// Break the loader. Generations from here must be dropped, never
	// served under the retained allow gate.
	rulesMu.Lock()
	rulesFn = func(context.Context, string) (*perms.RuleDoc, error) {
		return nil, errors.New("injected rules outage")
	}
	rulesMu.Unlock()
	notifier.Notify(ctx, uuidStr(appID), []string{uuidStr(title.ID)}, 4242)
	if dropped := awaitRefreshOk(frames, 5*time.Second); dropped != nil {
		t.Fatalf("RT-001e RED: generation served during rules outage (stale allow?): %v", dropped)
	}

	// Restore the production loader. The same subscription must serve
	// current data again — the outage pins neither failure nor staleness.
	rulesMu.Lock()
	rulesFn = cats.RuleDocFor
	rulesMu.Unlock()
	notifier.Notify(ctx, uuidStr(appID), []string{uuidStr(title.ID)}, 4243)
	after := awaitRefreshOk(frames, 10*time.Second)
	if after == nil {
		t.Fatal("RT-001e RED: subscription pinned after outage recovery (or the connection died)")
	}
	if body := frameBytes(t, after); !strings.Contains(body, "pre-outage") {
		t.Fatalf("RT-001e RED: recovered generation missing data: %s", body)
	}

	// The connection is still fully live.
	send(map[string]any{
		"op": "transact",
		"tx-steps": []any{
			[]any{"add-triple", uuidStr(rand16()), uuidStr(title.ID), "post-recovery"},
		},
		"client-event-id": "t2",
	})
	expectOp(t, frames, "transact-ok")
}

// TestSteadyDenySecondCommitDoesNotLeak pins the splice-authorization
// boundary: a subscription held under steady deny must not serve denied
// rows on a LATER commit even after an earlier commit armed incremental
// state. The first commit under deny serves empty (and may arm the
// splice engine); the second commit must still serve empty — the splice
// membership probe applies no view rules, so serving from spliced state
// here would leak rows the full-query oracle excludes.
func TestSteadyDenySecondCommitDoesNotLeak(t *testing.T) {
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

	// Persisted rules stay closed for the whole test.
	persistRules(t, pool, cats, appID, `{"todos":{"allow":{"view":"false"}}}`)

	store := reactive.NewStore()
	var mgr *syncpkg.Manager
	refresh := gateRespectingRefresh(pool, cats, func() *syncpkg.Manager { return mgr })
	notifier := &reactive.Notifier{
		Store:   store,
		Refresh: refresh,
		Inc: &reactive.Incremental{
			Source:    &reactive.InstaqlSource{DB: pool, Catalog: cats.For},
			Authorize: syncpkg.SpliceAuthorize,
		},
		Revalidate: func(ctx context.Context, sub *reactive.Subscription) (bool, error) {
			_, changed, err := mgr.RefreshGate(ctx, sub)
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
		OnCommit: func(ctx context.Context, appID string, attrIDs []string, txID int64, attrsChanged bool) {
			notifier.Notify(ctx, appID, attrIDs, txID)
		},
		// Production-faithful change routing (cmd/instantd/routes.go):
		// plain triple writes carry entity identity so the incremental
		// engine can splice. Without this the test would exercise only
		// the topic-wide full-refresh path and miss splice behavior.
		OnCommitChanges: func(ctx context.Context, appID string, changes []reactive.Change, txID int64, attrsChanged bool) {
			notifier.NotifyChanges(ctx, appID, changes, txID)
		},
	})
	wsHandler := &syncpkg.WSHandler{Manager: mgr, Store: store, Refresh: refresh}
	srv := httptest.NewServer(wsHandler)
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	dctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	send(map[string]any{"op": "init", "app-id": uuidStr(appID)})
	expectOp(t, frames, "init-ok")
	send(map[string]any{"op": "add-query", "q": map[string]any{"todos": map[string]any{}}, "client-event-id": "q1"})
	expectOp(t, frames, "add-query-ok")

	transact := func(id, val string) {
		t.Helper()
		send(map[string]any{
			"op": "transact",
			"tx-steps": []any{
				[]any{"add-triple", uuidStr(rand16()), uuidStr(title.ID), val},
			},
			"client-event-id": id,
		})
		expectOp(t, frames, "transact-ok")
	}
	assertNoLeak := func(val string) {
		t.Helper()
		// A generation MUST be served (empty) here: silence would make
		// the assertion vacuous. The served-empty frame proves the
		// deny-window generation ran to completion without the row.
		f := awaitRefreshOk(frames, 5*time.Second)
		if f == nil {
			t.Fatalf("no refresh-ok served for %s; leak assertion is vacuous", val)
		}
		if body := frameBytes(t, f); strings.Contains(body, val) {
			t.Fatalf("steady-deny leak: denied row served: %s", body)
		}
	}

	// First commit under deny: serves empty (and arms incremental state).
	transact("t1", "deny-seed-1")
	assertNoLeak("deny-seed-1")

	// Second commit under UNCHANGED deny: splice state is now armed — a
	// rule-unaware splice would serve this row although the oracle
	// excludes it.
	transact("t2", "deny-seed-2")
	assertNoLeak("deny-seed-2")
}

// TestSupersededGenerationDrops deterministically stages the stale
// publication interleaving: the pre-run reload serves allow and parks on
// a barrier; the test persists deny through the production loader path
// and releases; the post-run reload then sees deny. The generation must
// be dropped — never served under the stale allow. No sleeps: channel
// ordering makes the re-gate land strictly between the two reloads.
func TestSupersededGenerationDrops(t *testing.T) {
	pool := newPostgres(t)
	ctx := context.Background()

	st := storage.New(pool)
	cats := platform.NewCatalogCache(pool, pool)
	appID := rand16()
	seedApp(t, st, appID)
	if err := st.WithTx(ctx, func(tx pgx.Tx) error {
		_, err := platform.GetOrCreateAttr(ctx, tx, appID, "todos", "text", "blob", "one", false, true)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := cats.For(ctx, uuidStr(appID)); err != nil {
		t.Fatal(err)
	}
	persistRules(t, pool, cats, appID, `{"todos":{"allow":{"view":"true"}}}`)
	doc0, err := cats.RuleDocFor(ctx, uuidStr(appID))
	if err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int64
	entered := make(chan struct{})
	release := make(chan struct{})
	var mgr *syncpkg.Manager
	mgr = syncpkg.NewManager(syncpkg.Deps{
		Rules: func(ctx context.Context, appID string) (*perms.RuleDoc, error) {
			if calls.Add(1) == 1 {
				d, e := cats.RuleDocFor(ctx, appID)
				close(entered)
				<-release
				return d, e
			}
			return cats.RuleDocFor(ctx, appID)
		},
	})
	refresh := gateRespectingRefresh(pool, cats, func() *syncpkg.Manager { return mgr })
	sub := &reactive.Subscription{
		ID:        "supersede",
		AppID:     uuidStr(appID),
		Query:     json.RawMessage(`{"todos":{}}`),
		AttachCtx: syncpkg.NewQueryGate(doc0, false),
	}
	type outcome struct {
		res json.RawMessage
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		res, err := refresh(ctx, sub)
		done <- outcome{res, err}
	}()

	<-entered
	persistRules(t, pool, cats, appID, `{"todos":{"allow":{"view":"false"}}}`)
	close(release)

	out := <-done
	if out.err == nil {
		t.Fatalf("stale generation served after concurrent re-gate: %s", out.res)
	}
	if !strings.Contains(out.err.Error(), "superseded") {
		t.Fatalf("expected superseded drop, got: %v", out.err)
	}
	gate, _, err := mgr.RefreshGate(ctx, sub)
	if err != nil {
		t.Fatal(err)
	}
	if gate == nil || syncpkg.GateHash(gate.Rules) == syncpkg.GateHash(doc0) {
		t.Fatal("expected the subscription re-gated to deny")
	}
}
