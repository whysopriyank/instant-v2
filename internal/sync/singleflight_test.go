package sync_test

// WS-level single-flight test (docs/08-tier1-hotpath.md §T1.1): N sessions
// racing add-query for the same cold group produce exactly ONE refresh; each
// session still receives its own enriched add-query-ok ack.

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/instant-v2/instant-v2/internal/reactive"
)

func TestAddQuerySingleFlightAcrossSessions(t *testing.T) {
	env := newWSEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Dial + init each client up front (test goroutine — dial helpers use
	// t.Fatalf, which is only legal here).
	type client struct {
		conn   *websocket.Conn
		frames chan map[string]any
	}
	const n = 3
	clients := make([]client, n)
	for i := range clients {
		conn, frames := dialInit(t, ctx, env, "0.22.0")
		clients[i] = client{conn: conn, frames: frames}
	}

	orig := env.WS.Refresh
	var calls atomic.Int64
	hold := make(chan struct{})
	env.WS.Refresh = func(ctx context.Context, sub *reactive.Subscription) (json.RawMessage, error) {
		calls.Add(1)
		<-hold // keep the leader in-flight until every racer has arrived
		return orig(ctx, sub)
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(hold) }) }
	t.Cleanup(release) // never leak a blocked handler goroutine

	// Fire all add-queries concurrently (safe send: t.Error, not t.Fatal).
	fire := make(chan struct{})
	var wg sync.WaitGroup
	for i := range clients {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-fire
			b, err := json.Marshal(map[string]any{
				"op":              "add-query",
				"q":               map[string]any{"todos": map[string]any{}},
				"client-event-id": "q1",
			})
			if err != nil {
				t.Error(err)
				return
			}
			if err := clients[i].conn.Write(ctx, websocket.MessageText, b); err != nil {
				t.Errorf("client %d write: %v", i, err)
			}
		}(i)
	}
	close(fire)
	time.Sleep(100 * time.Millisecond) // let racers pile onto the flight
	release()

	acks := make([]map[string]any, n)
	for i := range clients {
		acks[i] = expectOp(t, clients[i].frames, "add-query-ok")
	}
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("refresh ran %d times across %d racing add-queries, want 1", got, n)
	}
	for i, ack := range acks {
		if _, ok := ack["result"]; !ok {
			t.Fatalf("client %d add-query-ok missing initial result: %v", i, ack)
		}
		if got, _ := ack["client-event-id"].(string); got != "q1" {
			t.Fatalf("client %d ack event id=%q, want q1", i, got)
		}
		if i > 0 && !reflect.DeepEqual(ack["result"], acks[0]["result"]) {
			t.Fatalf("client %d received a divergent initial result", i)
		}
	}
}
