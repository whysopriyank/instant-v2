package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/instant-v2/instant-v2/internal/platform"
)

func TestCF003AssembledRoomLifecycle(t *testing.T) {
	mux, appID, _ := cf003PostgresMux(t)
	app := platform.UUIDToStr(appID)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	a := cf003OpenWS(t, ctx, server, app)
	b := cf003OpenWS(t, ctx, server, app)
	if a.sessionID == b.sessionID {
		t.Fatalf("room clients reused session %q", a.sessionID)
	}

	a.send(t, ctx, map[string]any{
		"op": "join-room", "room-id": "cf003-room", "peer-id": "peer-a",
		"data": map[string]any{"mood": "ok"}, "client-event-id": "join-a",
	})
	cf003ExactWS(t, a.nextOp(t, "join-room-ok"), map[string]any{
		"op": "join-room-ok", "room-id": "cf003-room", "client-event-id": "join-a",
	})

	wantBoth := map[string]any{
		a.sessionID: map[string]any{"peer": "peer-a", "data": map[string]any{"mood": "ok"}},
		b.sessionID: map[string]any{"peer": "peer-b", "data": map[string]any{}},
	}
	b.send(t, ctx, map[string]any{
		"op": "join-room", "room-id": "cf003-room", "peer-id": "peer-b",
		"data": map[string]any{}, "client-event-id": "join-b",
	})
	cf003AwaitAckAndPresence(t, b, "join-room-ok", map[string]any{
		"op": "join-room-ok", "room-id": "cf003-room", "client-event-id": "join-b",
	}, wantBoth)
	cf003ExactPresence(t, a.nextPresence(t, wantBoth), wantBoth)

	// B requests the current state explicitly so join-ack/presence scheduling
	// cannot hide whether its mounted session sees the same room snapshot.
	b.send(t, ctx, map[string]any{"op": "refresh-presence", "room-id": "cf003-room"})
	cf003ExactPresence(t, b.nextPresence(t, wantBoth), wantBoth)

	a.send(t, ctx, map[string]any{
		"op": "set-presence", "room-id": "cf003-room",
		"data": map[string]any{"mood": "great"}, "client-event-id": "presence-a",
	})
	wantUpdated := map[string]any{
		a.sessionID: map[string]any{"peer": "peer-a", "data": map[string]any{"mood": "great"}},
		b.sessionID: map[string]any{"peer": "peer-b", "data": map[string]any{}},
	}
	cf003AwaitAckAndPresence(t, a, "set-presence-ok", map[string]any{
		"op": "set-presence-ok", "room-id": "cf003-room", "client-event-id": "presence-a",
	}, wantUpdated)
	cf003ExactPresence(t, b.nextPresence(t, wantUpdated), wantUpdated)

	a.send(t, ctx, map[string]any{
		"op": "client-broadcast", "room-id": "cf003-room", "topic": "chat",
		"data": map[string]any{"message": "hello"}, "client-event-id": "broadcast-a",
	})
	cf003ExactWS(t, a.nextFrame(t), map[string]any{
		"op": "client-broadcast-ok", "client-event-id": "broadcast-a",
	})
	cf003ExactWS(t, b.nextOp(t, "server-broadcast"), map[string]any{
		"op": "server-broadcast", "room-id": "cf003-room", "topic": "chat",
		"session-id": a.sessionID,
		"data":       map[string]any{"peer": "peer-a", "data": map[string]any{"message": "hello"}},
	})
	a.assertQuiet(t, 100*time.Millisecond)

	b.send(t, ctx, map[string]any{
		"op": "leave-room", "room-id": "cf003-room", "client-event-id": "leave-b",
	})
	cf003ExactWS(t, b.nextOp(t, "leave-room-ok"), map[string]any{
		"op": "leave-room-ok", "room-id": "cf003-room", "client-event-id": "leave-b",
	})
	wantA := map[string]any{
		a.sessionID: map[string]any{"peer": "peer-a", "data": map[string]any{"mood": "great"}},
	}
	cf003ExactPresence(t, a.nextPresence(t, wantA), wantA)
}

type cf003WSClient struct {
	conn      *websocket.Conn
	frames    chan map[string]any
	sessionID string
}

func cf003OpenWS(t *testing.T, ctx context.Context, server *httptest.Server, app string) *cf003WSClient {
	t.Helper()
	client := cf003DialWS(t, ctx, server)
	client.send(t, ctx, map[string]any{"op": "init", "app-id": app})
	init := client.nextOp(t, "init-ok")
	client.sessionID, _ = init["session-id"].(string)
	if client.sessionID == "" {
		t.Fatalf("WS init session = %#v", init)
	}
	cf003ExactWS(t, init, map[string]any{
		"op": "init-ok", "session-id": client.sessionID, "attrs": []any{},
		"app-status": map[string]any{"status": "active"},
		"auth":       map[string]any{"admin?": false, "app": map[string]any{"id": app}, "user": nil},
	})
	return client
}

func cf003DialWS(t *testing.T, ctx context.Context, server *httptest.Server) *cf003WSClient {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/runtime/session"
	conn, response, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{"http://cf003.test"}},
	})
	if err != nil {
		if response != nil {
			t.Fatalf("WS dial = %d: %v", response.StatusCode, err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(websocket.StatusNormalClosure, "") })
	client := &cf003WSClient{conn: conn, frames: make(chan map[string]any, 32)}
	go func() {
		defer close(client.frames)
		for {
			_, payload, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var frame map[string]any
			if err := json.Unmarshal(payload, &frame); err != nil {
				client.frames <- map[string]any{
					"op": "__decode-error__", "error": err.Error(), "raw": string(payload),
				}
				continue
			}
			client.frames <- frame
		}
	}()
	return client
}

func (c *cf003WSClient) send(t *testing.T, ctx context.Context, frame map[string]any) {
	t.Helper()
	payload, err := json.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.conn.Write(ctx, websocket.MessageText, payload); err != nil {
		t.Fatalf("WS write %#v: %v", frame, err)
	}
}

func (c *cf003WSClient) nextOp(t *testing.T, op string) map[string]any {
	t.Helper()
	for {
		frame := c.nextFrame(t)
		if frame["op"] == "__decode-error__" {
			t.Fatalf("WS malformed frame: %#v", frame)
		}
		if frame["op"] == op {
			return frame
		}
	}
}

func (c *cf003WSClient) nextFrame(t *testing.T) map[string]any {
	t.Helper()
	select {
	case frame, ok := <-c.frames:
		if !ok {
			t.Fatal("WS closed waiting for frame")
		}
		return frame
	case <-time.After(5 * time.Second):
		t.Fatal("WS timed out waiting for frame")
		return nil
	}
}

func (c *cf003WSClient) assertQuiet(t *testing.T, duration time.Duration) {
	t.Helper()
	select {
	case frame, ok := <-c.frames:
		if !ok {
			t.Fatal("WS closed while checking sender isolation")
		}
		t.Fatalf("broadcast sender received unexpected frame: %#v", frame)
	case <-time.After(duration):
	}
}

func (c *cf003WSClient) nextPresence(t *testing.T, want map[string]any) map[string]any {
	t.Helper()
	for {
		frame := c.nextOp(t, "refresh-presence")
		if reflect.DeepEqual(frame["data"], want) {
			return frame
		}
	}
}

func cf003AwaitAckAndPresence(t *testing.T, client *cf003WSClient, ackOp string, wantAck, wantPresence map[string]any) {
	t.Helper()
	ackSeen, presenceSeen := false, false
	deadline := time.After(5 * time.Second)
	for !ackSeen || !presenceSeen {
		select {
		case frame, ok := <-client.frames:
			if !ok {
				t.Fatal("WS closed waiting for join ack and presence")
			}
			switch frame["op"] {
			case ackOp:
				cf003ExactWS(t, frame, wantAck)
				ackSeen = true
			case "refresh-presence":
				cf003ExactPresence(t, frame, wantPresence)
				presenceSeen = true
			default:
				t.Fatalf("unexpected WS frame during join: %#v", frame)
			}
		case <-deadline:
			t.Fatalf("WS timed out waiting for join ack/presence: ack=%v presence=%v", ackSeen, presenceSeen)
		}
	}
}

func cf003ExactPresence(t *testing.T, frame map[string]any, data map[string]any) {
	t.Helper()
	cf003ExactWS(t, frame, map[string]any{
		"op": "refresh-presence", "room-id": "cf003-room", "data": data,
	})
}

func cf003ExactWS(t *testing.T, got, want map[string]any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("WS frame = %#v; want %#v", got, want)
	}
}
