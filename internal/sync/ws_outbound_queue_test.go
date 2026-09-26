package sync

// RT-004 R2/R3/R5: per-session WS outbound queue behaviour.
//
//   - TestWSOutboundQueuePreservesOrder: direct replies and fan-out frames
//     share one FIFO writer — transact-ok vs refresh-ok order is exactly the
//     enqueue order, as before.
//   - TestWSQueuedEnvelopeDroppedAfterRevoke: a queued envelope superseded by
//     a re-gate is dropped at write time (mirrors the SSE RT-001f test), and
//     the healing generation under the new gate is still served.
//   - TestWSOverflowClosesSlowMemberWith1013: a full queue sheds only the slow
//     member with close status 1013 while siblings keep receiving; overflow
//     logs at Warn with the session id and bumps the overflow counter.
//   - TestWSWriterExitsOnClose: the writer goroutine exits on close (no leak;
//     goleak is not a direct dependency, so a goroutine-count check).
//
// Hermetic: real WebSocket connections through httptest + the production
// WSHandler with stubbed catalogs — no database, no notifier.

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/instant-v2/instant-v2/internal/metrics"
	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
)

// holEnv is a hermetic WS stack: production handler, stub catalogs, canned
// refresh payload.
type holEnv struct {
	mgr     *Manager
	handler *WSHandler
	server  *httptest.Server
	wsURL   string
	appID   string
	rawQ    json.RawMessage
	key     string
	big     json.RawMessage
}

func newHolEnv(t *testing.T, refresh func(context.Context, *reactive.Subscription) (json.RawMessage, error)) *holEnv {
	t.Helper()
	cats := platform.NewCatalogCache(holStubQueryer{}, nil)
	store := reactive.NewStore()
	mgr := NewManager(Deps{Store: store, Catalogs: cats, Rooms: NewRoomHub()})
	big := holBigResult(t, 1<<20)
	if refresh == nil {
		refresh = func(context.Context, *reactive.Subscription) (json.RawMessage, error) { return big, nil }
	}
	handler := &WSHandler{Manager: mgr, Store: store, Refresh: refresh}
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	rawQ := json.RawMessage(`{"todos":{}}`)
	appID := "22222222-2222-4222-8222-222222222222"
	return &holEnv{
		mgr: mgr, handler: handler, server: srv,
		wsURL: "ws" + strings.TrimPrefix(srv.URL, "http"),
		appID: appID, rawQ: rawQ,
		key: groupKey(appID, wireNodelist, rawQ, false),
		big: big,
	}
}

func (e *holEnv) group(t *testing.T) *queryGroup {
	t.Helper()
	e.mgr.groupsMu.Lock()
	defer e.mgr.groupsMu.Unlock()
	g, ok := e.mgr.groups[e.key]
	if !ok {
		t.Fatalf("no group for the shared query; shared-group premise unproven")
	}
	return g
}

func (e *holEnv) members() []*Session {
	e.mgr.groupsMu.Lock()
	g := e.mgr.groups[e.key]
	e.mgr.groupsMu.Unlock()
	if g == nil {
		return nil
	}
	mems, _ := g.snapshotMembers()
	out := make([]*Session, 0, len(mems))
	for _, m := range mems {
		out = append(out, m.sess)
	}
	return out
}

// holReadOp reads one frame and returns its op.
func holReadOp(t *testing.T, ctx context.Context, c *holConn, timeout time.Duration) string {
	t.Helper()
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, data, err := c.conn.Read(rctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var f map[string]any
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	op, _ := f["op"].(string)
	return op
}

func TestWSOutboundQueuePreservesOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	env := newHolEnv(t, nil)

	c := holDial(t, ctx, env.wsURL)
	holAttach(t, ctx, c, env.appID, map[string]any{"todos": map[string]any{}})
	g := env.group(t)
	mems := env.members()
	if len(mems) != 1 {
		t.Fatalf("group has %d members; want 1", len(mems))
	}
	sess := mems[0]
	gen := g.sub.Gen.Load()

	// One goroutine enqueues transact-ok, a refresh, then an error reply.
	// The single writer must emit them in exactly this order.
	if err := sess.Send(Frame{
		"op":    json.RawMessage(`"transact-ok"`),
		"tx-id": json.RawMessage(`7`),
	}); err != nil {
		t.Fatalf("send transact-ok: %v", err)
	}
	if err := g.sub.Emit(reactive.Frame{
		SubID:         env.key,
		QueryJSON:     env.rawQ,
		ResultJSON:    json.RawMessage(`{"data":{"todos":[]}}`),
		ProcessedTxID: 8,
		Gen:           gen,
	}); err != nil {
		t.Fatalf("emit refresh: %v", err)
	}
	if err := sess.Send(ErrFrame(400, "bad-frame", "boom")); err != nil {
		t.Fatalf("send error frame: %v", err)
	}

	for _, want := range []string{"transact-ok", "refresh-ok", "error"} {
		if got := holReadOp(t, ctx, c, 10*time.Second); got != want {
			t.Fatalf("wire order: got %q, want %q (per-session FIFO broken)", got, want)
		}
	}
}

func TestWSQueuedEnvelopeDroppedAfterRevoke(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	allowDoc := &perms.RuleDoc{Raw: json.RawMessage(`{"allow":"all"}`)}
	denyDoc := &perms.RuleDoc{Raw: json.RawMessage(`{"deny":"all"}`)}
	var current atomic.Pointer[perms.RuleDoc]
	current.Store(allowDoc)
	cats := platform.NewCatalogCache(holStubQueryer{}, nil)
	store := reactive.NewStore()
	mgr := NewManager(Deps{
		Store:    store,
		Catalogs: cats,
		Rooms:    NewRoomHub(),
		Rules: func(context.Context, string) (*perms.RuleDoc, error) {
			return current.Load(), nil
		},
	})
	big := holBigResult(t, 1<<20)
	handler := &WSHandler{Manager: mgr, Store: store,
		Refresh: func(context.Context, *reactive.Subscription) (json.RawMessage, error) { return big, nil }}
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	appID := "33333333-3333-4333-8333-333333333333"
	rawQ := json.RawMessage(`{"todos":{}}`)
	key := groupKey(appID, wireNodelist, rawQ, false)

	c := holDial(t, ctx, wsURL)
	holAttach(t, ctx, c, appID, map[string]any{"todos": map[string]any{}})

	mgr.groupsMu.Lock()
	g := mgr.groups[key]
	mgr.groupsMu.Unlock()
	if g == nil {
		t.Fatal("no group for the shared query")
	}
	mems, _ := g.snapshotMembers()
	if len(mems) != 1 {
		t.Fatalf("group has %d members; want 1", len(mems))
	}
	sess := mems[0].sess
	gen0 := g.sub.Gen.Load()

	// Revoke: real re-gate installs deny and bumps the epoch.
	current.Store(denyDoc)
	if _, _, err := mgr.RefreshGate(ctx, g.sub); err != nil {
		t.Fatalf("re-gate: %v", err)
	}
	if gen1 := g.sub.Gen.Load(); gen1 == gen0 {
		t.Fatal("re-gate did not bump epoch; test staged nothing")
	}

	// A stale envelope queued after the revoke must be dropped at write
	// time — the client observes a quiet stream, never superseded bytes.
	// One reader goroutine for the rest of the test: a Read context that
	// expires would break the conn for later reads (coder readMu), so the
	// quiet window is a select timeout, never a read deadline.
	stale := []byte(`{"op":"refresh-ok","stale":true,"processed-tx-id":9}`)
	if err := sess.SendRawGen(stale, gen0, g.sub); err != nil {
		t.Fatalf("enqueue stale: %v", err)
	}
	type holReadRes struct {
		data []byte
		err  error
	}
	probe := make(chan holReadRes, 1)
	go func() {
		_, data, err := c.conn.Read(ctx)
		probe <- holReadRes{data, err}
	}()
	select {
	case r := <-probe:
		var ce websocket.CloseError
		if errors.As(r.err, &ce) {
			t.Fatalf("connection closed during drop probe: %v", r.err)
		}
		if r.err == nil {
			t.Fatalf("queued stale envelope hit the wire after revoke: %s", r.data)
		}
		t.Fatalf("probe read err: %v", r.err)
	case <-time.After(time.Second):
		// Drop held for the quiet window. Expected.
	}

	// Healing: the generation under the new gate is still served; the
	// parked probe reader receives it.
	if err := g.sub.Emit(reactive.Frame{
		SubID:         key,
		QueryJSON:     rawQ,
		ResultJSON:    json.RawMessage(`{"data":{"todos":[]}}`),
		ProcessedTxID: 10,
		Gen:           g.sub.Gen.Load(),
	}); err != nil {
		t.Fatalf("healing emit: %v", err)
	}
	select {
	case r := <-probe:
		if r.err != nil {
			t.Fatalf("healing read: %v", r.err)
		}
		var f map[string]any
		if err := json.Unmarshal(r.data, &f); err != nil {
			t.Fatalf("healing unmarshal: %v", err)
		}
		if got, _ := f["op"].(string); got != "refresh-ok" {
			t.Fatalf("healing frame op = %q, want refresh-ok (commit path broken, not strict)", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no healing frame after revoke drop")
	}
}

// holLogCapture is a minimal slog.Handler retaining records for assertions.
type holLogCapture struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (h *holLogCapture) Enabled(context.Context, slog.Level) bool { return true }
func (h *holLogCapture) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recs = append(h.recs, r.Clone())
	return nil
}
func (h *holLogCapture) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *holLogCapture) WithGroup(string) slog.Handler      { return h }

func (h *holLogCapture) warnsWithAttr(key, val string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.recs {
		if r.Level != slog.LevelWarn {
			continue
		}
		hit := false
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == key && a.Value.String() == val {
				hit = true
				return false
			}
			return true
		})
		if hit {
			return true
		}
	}
	return false
}

func TestWSOverflowClosesSlowMemberWith1013(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Capture Warn records: overflow must log at Warn with the session id.
	cap := &holLogCapture{}
	prev := slog.Default()
	slog.SetDefault(slog.New(cap))
	t.Cleanup(func() { slog.SetDefault(prev) })

	env := newHolEnv(t, nil)
	before := testutil.ToFloat64(metrics.WSOverflowCloses)

	query := map[string]any{"todos": map[string]any{}}
	// Attach the slow member FIRST, while it is the sole member, so its
	// *Session is identified exactly (group membership is unordered).
	slow := holDial(t, ctx, env.wsURL)
	slowID := holAttach(t, ctx, slow, env.appID, query)
	g := env.group(t)
	gen := g.sub.Gen.Load()
	solo := env.members()
	if len(solo) != 1 {
		t.Fatalf("group has %d members; want exactly the slow one", len(solo))
	}
	slowSess := solo[0]

	h1 := holDial(t, ctx, env.wsURL)
	holAttach(t, ctx, h1, env.appID, query)
	pump1 := holPump(ctx, h1)
	h2 := holDial(t, ctx, env.wsURL)
	holAttach(t, ctx, h2, env.appID, query)
	pump2 := holPump(ctx, h2)
	if n := len(env.members()); n != 3 {
		t.Fatalf("group has %d members; want 3", n)
	}

	// The slow member drains its socket, but at a fixed 50 ms/frame cadence:
	// its kernel buffers saturate under the ~ms fill burst, the single
	// writer falls behind to the client's tempo, and only the slow
	// in-memory queue accumulates to the 128 cap. The writer is never
	// parked in a 10 s timeout (writes complete on the client's read
	// cadence), so the shed close handshake stays clean and the client
	// observes the explicit status.
	slowDone := make(chan error, 1)
	go func() {
		for {
			_, _, err := slow.conn.Read(ctx)
			if err != nil {
				slowDone <- err
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	isSlowGone := func() bool { return len(env.members()) == 2 }

	// Pressure targets ONLY the slow queue, through its own session writer:
	// hundreds of direct enqueues land in ~ms (no per-emit render cost)
	// while the single writer is throttled to the client's 50 ms read tempo
	// after its socket buffers saturate — so the 128-cap queue MUST shed
	// regardless of CPU contention (a slower writer only accumulates
	// faster; draining hundreds of frames in ~ms would need tens of GB/s).
	// Healthy queues are untouched: their writers drain to reading pumps.
	filler := holBigResult(t, 1<<16)[:1<<16]

	// Trigger the shed through the production fan-out entry: the slow
	// member's SendRawGen hits the full queue → detach-and-continue exactly
	// like SSE backpressure (failMember detaches + closes 1013), while
	// siblings certify the generation. (If the writer already observed the
	// overflow first, the deferred teardown detached it with the same 1013
	// — every assertion below holds for both orders.)
	//
	// Sustained pressure, not a single shot: the writer drains concurrently
	// (a drain landing in the fill→Emit gap frees one slot and serves that
	// Emit without detaching), so top the queue up and trigger repeatedly —
	// the soak's own shape (continuous writes against a stuck reader).
	// Each round re-saturates faster than any drain; the first success
	// detaches and the rest no-op.
	tx := int64(0)
	for round := 0; round < 40 && !isSlowGone(); round++ {
		for i := 0; i < 300; i++ {
			_ = slowSess.SendRaw(filler)
		}
		tx++
		_ = g.sub.Emit(reactive.Frame{
			SubID:         env.key,
			QueryJSON:     env.rawQ,
			ResultJSON:    json.RawMessage(`{"data":{"todos":[]}}`),
			ProcessedTxID: tx,
			Gen:           gen,
		})
	}
	if !isSlowGone() {
		t.Fatalf("slow member never overflowed; shed path unstaged (members=%d)", len(env.members()))
	}

	// The slow member observes the explicit shed status after draining its
	// backlog at its own tempo.
	select {
	case err := <-slowDone:
		var ce websocket.CloseError
		if !errors.As(err, &ce) {
			t.Fatalf("slow read err = %v; want close error 1013", err)
		}
		if ce.Code != websocket.StatusTryAgainLater {
			t.Fatalf("slow close code = %v; want 1013 Try Again Later", ce.Code)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("slow member never observed the 1013 shed close")
	}

	// Siblings are unaffected: the next refresh reaches both promptly.
	tx++
	_ = g.sub.Emit(reactive.Frame{
		SubID:         env.key,
		QueryJSON:     env.rawQ,
		ResultJSON:    json.RawMessage(`{"data":{"todos":[]}}`),
		ProcessedTxID: tx,
		Gen:           gen,
	})
	for i, pump := range []chan time.Time{pump1, pump2} {
		select {
		case <-pump:
		case <-time.After(10 * time.Second):
			t.Fatalf("healthy member %d got no refresh after the sibling shed", i)
		}
	}

	if got := testutil.ToFloat64(metrics.WSOverflowCloses); got != before+1 {
		t.Fatalf("ws_overflow_closes_total = %v; want exactly one shed (before=%v)", got, before)
	}
	if !cap.warnsWithAttr("session", slowID) {
		t.Fatalf("no Warn log with session=%q on overflow (check level/id)", slowID)
	}
}

// holSettleN0 polls runtime goroutines until the count is stable, then
// returns it as a quiescence baseline (prior tests may leave writers parked
// in bounded socket writes; stability means they have exited).
func holSettleN0(t *testing.T) int {
	t.Helper()
	last := -1
	stable := 0
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		n := runtime.NumGoroutine()
		if n == last {
			stable++
			if stable >= 5 {
				return n
			}
		} else {
			stable = 0
			last = n
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("goroutines never settled (last=%d)", last)
	return last
}

func TestWSWriterExitsOnClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	env := newHolEnv(t, nil)
	n0 := holSettleN0(t)

	c := holDial(t, ctx, env.wsURL)
	holAttach(t, ctx, c, env.appID, map[string]any{"todos": map[string]any{}})
	if n := env.handler.ConnCount(); n != 1 {
		t.Fatalf("live conns = %d; want 1", n)
	}

	// Close from the client: the server read loop exits, teardown runs, and
	// the single writer must exit — no lingering goroutine per session.
	_ = c.conn.Close(websocket.StatusNormalClosure, "")
	deadline := time.Now().Add(15 * time.Second)
	for {
		if env.handler.ConnCount() == 0 && runtime.NumGoroutine() <= n0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("writer/conns leaked: conns=%d goroutines=%d baseline=%d",
				env.handler.ConnCount(), runtime.NumGoroutine(), n0)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
