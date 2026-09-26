package sync

// RT-004 R1 (PROVEN_RED): WebSocket fan-out head-of-line blocking.
//
// One query group holds ≥2 healthy members plus 1 member whose client never
// reads. Refreshes carry frames large enough to fill the stuck member's
// socket buffers. Every healthy member must receive every refresh within a
// tight bound (≤ 1 s) regardless of the stuck member.
//
// On the pre-fix code WS fan-out is a sequential loop over group members
// calling the blocking SendRaw (10 s wsWriteTimeout per wedged member), so
// any healthy member ordered after the stuck member waits out the full
// timeout: this test FAILS there (healthy latency ≈ 10 s). After the fix
// (per-session outbound queue, R2) dispatch never blocks and the test passes.
//
// Hermetic: real WebSocket connections through httptest + the production
// WSHandler, but catalogs are stubbed (empty) and refreshes are driven via
// the production group Emit — no database, no notifier.

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
)

// holEmptyRows is a pgx.Rows stub yielding zero rows, so LoadAttrCatalog
// succeeds hermetically with an empty catalog.
type holEmptyRows struct{}

func (holEmptyRows) Close()                                       {}
func (holEmptyRows) Err() error                                   { return nil }
func (holEmptyRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (holEmptyRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (holEmptyRows) Next() bool                                   { return false }
func (holEmptyRows) Scan(...any) error                            { return nil }
func (holEmptyRows) Values() ([]any, error)                       { return nil, nil }
func (holEmptyRows) RawValues() [][]byte                          { return nil }
func (holEmptyRows) Conn() *pgx.Conn                              { return nil }

// holStubQueryer backs a CatalogCache without a database.
type holStubQueryer struct{}

func (holStubQueryer) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return holEmptyRows{}, nil
}

// holBigResult builds a flat instaql envelope of ~targetBytes with entities
// carrying string ids (required by BuildNodeList even on an empty catalog).
func holBigResult(t *testing.T, targetBytes int) json.RawMessage {
	t.Helper()
	title := strings.Repeat("x", 1024)
	ents := make([]map[string]any, 0, 2048)
	size := 0
	for i := 0; size < targetBytes; i++ {
		e := map[string]any{"id": strings.Repeat("e", 8) + "-" + strings.Repeat("0", 4) + "-" + holItoa(i), "title": title}
		ents = append(ents, e)
		size += 1050
	}
	raw, err := json.Marshal(map[string]any{"data": map[string]any{"todos": ents}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func holItoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}

// holConn wraps one test client connection.
type holConn struct {
	conn *websocket.Conn
}

// holDial opens a connection with a large read limit (refresh frames are MBs;
// the default 32 KiB client limit would fail the read).
func holDial(t *testing.T, ctx context.Context, wsURL string) *holConn {
	t.Helper()
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{CompressionMode: websocket.CompressionDisabled})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn.SetReadLimit(128 << 20)
	t.Cleanup(func() { _ = conn.Close(websocket.StatusNormalClosure, "") })
	return &holConn{conn: conn}
}

func holWrite(t *testing.T, ctx context.Context, c *holConn, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.conn.Write(ctx, websocket.MessageText, b); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// holReadOne reads a single frame with a bound.
func holReadOne(t *testing.T, ctx context.Context, c *holConn) map[string]any {
	t.Helper()
	rctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_, data, err := c.conn.Read(rctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var f map[string]any
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return f
}

// holAttach runs init + add-query and consumes both acks, returning the
// server-assigned session id from init-ok.
func holAttach(t *testing.T, ctx context.Context, c *holConn, appID string, q any) string {
	t.Helper()
	holWrite(t, ctx, c, map[string]any{"op": "init", "app-id": appID})
	initOk := holReadOne(t, ctx, c)
	if got, _ := initOk["op"].(string); got != "init-ok" {
		t.Fatalf("init ack op = %q, want init-ok", got)
	}
	sid, _ := initOk["session-id"].(string)
	if sid == "" {
		t.Fatalf("init-ok missing session-id: %v", initOk)
	}
	holWrite(t, ctx, c, map[string]any{"op": "add-query", "q": q, "client-event-id": "q1"})
	ack := holReadOne(t, ctx, c)
	if got, _ := ack["op"].(string); got != "add-query-ok" {
		t.Fatalf("add-query ack op = %q, want add-query-ok", got)
	}
	if _, ok := ack["result"]; !ok {
		t.Fatalf("add-query-ok missing initial result")
	}
	return sid
}

// holPump records arrival times of raw frames in order (no parsing: refresh
// frames are MBs and per-session order is FIFO on both pre- and post-fix
// code, so arrival[i] is generation i).
func holPump(ctx context.Context, c *holConn) chan time.Time {
	arr := make(chan time.Time, 1024)
	go func() {
		defer close(arr)
		for {
			_, _, err := c.conn.Read(ctx)
			if err != nil {
				return
			}
			select {
			case arr <- time.Now():
			default:
			}
		}
	}()
	return arr
}

func TestWSFanoutStuckMemberDoesNotStallGroup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	cats := platform.NewCatalogCache(holStubQueryer{}, nil)
	store := reactive.NewStore()
	mgr := NewManager(Deps{Store: store, Catalogs: cats, Rooms: NewRoomHub()})
	big := holBigResult(t, 1<<20)
	handler := &WSHandler{
		Manager: mgr,
		Store:   store,
		Refresh: func(context.Context, *reactive.Subscription) (json.RawMessage, error) { return big, nil },
	}
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	appID := "11111111-1111-4111-8111-111111111111"
	query := map[string]any{"todos": map[string]any{}}
	rawQ := json.RawMessage(`{"todos":{}}`)
	key := groupKey(appID, wireNodelist, rawQ, false)

	// Two... three healthy members with arrival pumps (contract needs ≥2).
	const healthyN = 3
	type healthy struct {
		arr chan time.Time
	}
	healthies := make([]healthy, 0, healthyN)
	for i := 0; i < healthyN; i++ {
		c := holDial(t, ctx, wsURL)
		holAttach(t, ctx, c, appID, query)
		healthies = append(healthies, healthy{arr: holPump(ctx, c)})
	}
	// Healthy session identities, so each round can resolve its fresh stuck
	// member (group membership is unordered).
	mgr.groupsMu.Lock()
	gh, ok := mgr.groups[key]
	if !ok {
		mgr.groupsMu.Unlock()
		t.Fatal("no group after healthy attaches")
	}
	gh.mu.Lock()
	healthySet := make(map[*Session]bool, len(gh.members))
	for s := range gh.members {
		healthySet[s] = true
	}
	gh.mu.Unlock()
	mgr.groupsMu.Unlock()

	var worstHealthy time.Duration
	failures := 0
	const rounds = 4
	const emitsPerRound = 12
	var tx int64

	var prevStuck *Session
	var prevStuckConn *holConn
	for r := 0; r < rounds; r++ {
		// Fresh stuck member: attached, then never reads again. Detach the
		// previous round's stuck member first: post-fix nothing sheds it
		// promptly (its small queue never fills; its writer drains into the
		// 10 s socket timeout and only then tears down), so without explicit
		// detach it would linger and break the exact-membership check below
		// whenever a round outruns the teardown — a test-accounting
		// artifact, not fan-out blocking.
		if prevStuck != nil {
			mgr.detachMember(prevStuck, key)
			_ = prevStuckConn.conn.Close(websocket.StatusNormalClosure, "")
		}
		// Fresh stuck member: attached, then never reads again.
		stuck := holDial(t, ctx, wsURL)
		holAttach(t, ctx, stuck, appID, query)

		mgr.groupsMu.Lock()
		g, ok := mgr.groups[key]
		var members int
		var freshStuck *Session
		if ok {
			g.mu.Lock()
			members = len(g.members)
			for s := range g.members {
				if !healthySet[s] {
					freshStuck = s
				}
			}
			g.mu.Unlock()
		}
		mgr.groupsMu.Unlock()
		if !ok {
			t.Fatalf("round %d: no group for the shared query; shared-group premise unproven", r)
		}
		if members != healthyN+1 {
			t.Fatalf("round %d: group has %d members; want %d (healthy) + 1 (stuck)", r, members, healthyN)
		}
		if freshStuck == nil {
			t.Fatalf("round %d: stuck member unresolvable; healthy set diverged", r)
		}
		gen := g.sub.Gen.Load()

		// Drive refreshes through the production fan-out entry. Early
		// frames fill the stuck member's socket buffers; once full, a
		// wedged write stalls every group member ordered after it.
		t0s := make([]time.Time, 0, emitsPerRound)
		for i := 0; i < emitsPerRound; i++ {
			tx++
			fr := reactive.Frame{
				SubID:         key,
				QueryJSON:     rawQ,
				ResultJSON:    big,
				ProcessedTxID: tx,
				Gen:           gen,
			}
			t0 := time.Now()
			_ = g.sub.Emit(fr) // delivery failures detach; certified outcome is asserted via arrivals
			t0s = append(t0s, t0)
		}

		// Collect one arrival per healthy member per emission and bound it.
		for i := 0; i < emitsPerRound; i++ {
			for h := range healthies {
				select {
				case at, ok := <-healthies[h].arr:
					if !ok {
						t.Fatalf("round %d emit %d: healthy member %d stream closed; group fan-out is broken", r, i, h)
					}
					if lat := at.Sub(t0s[i]); lat > worstHealthy {
						worstHealthy = lat
					}
					if lat := at.Sub(t0s[i]); lat > time.Second {
						failures++
						t.Logf("round %d emit %d healthy %d latency %v exceeds 1 s (HOL stall)", r, i, h, lat)
					}
				case <-time.After(60 * time.Second):
					t.Fatalf("round %d emit %d: healthy member %d got no refresh (want every refresh ≤ 1 s)", r, i, h)
				}
			}
		}
		prevStuck = freshStuck
		prevStuckConn = stuck
	}

	// Drain remaining pump state on success path only; failure already logged.
	if failures > 0 {
		t.Fatalf("head-of-line blocking: %d healthy refresh deliveries exceeded 1 s (worst %v); one stuck reader stalled its group", failures, worstHealthy)
	}
}
