package sync_test

import (
	"context"
	"testing"

	"github.com/coder/websocket"
	"time"
)

// TestRoomPresenceFanOut proves the in-process ephemeral layer: two clients,
// join/set-presence/broadcast/leave, with refresh-presence snapshots fanned to
// all members and server-broadcast delivered to peers only.
func TestRoomPresenceFanOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	env := newWSEnv(t)

	connA, framesA := env.dial(t, ctx)
	initWS(t, connA, framesA, ctx, env.AppID)
	connB, framesB := env.dial(t, ctx)
	initWS(t, connB, framesB, ctx, env.AppID)

	// A joins the room.
	sendFrame(t, connA, ctx, map[string]any{
		"op": "join-room", "room-id": "lobby",
		"data": map[string]any{"mood": "ok"}, "peer-id": "peer-a",
	})
	if f := expectOp(t, framesA, "join-room-ok"); f["room-id"] != "lobby" {
		t.Fatalf("join-room-ok payload: %v", f)
	}

	// B joins → BOTH receive refresh-presence containing B. Note each client
	// first receives its OWN join snapshot (1 member); wait for 2.
	sendFrame(t, connB, ctx, map[string]any{
		"op": "join-room", "room-id": "lobby",
		"data": map[string]any{}, "peer-id": "peer-b",
	})
	expectOp(t, framesB, "join-room-ok")
	pa := expectPresenceWith(t, framesA, "lobby", func(data map[string]any) bool {
		return len(data) == 2
	})
	if len(pa) != 2 {
		t.Fatalf("after B joins, A must see 2 members, got %v", pa)
	}
	expectPresenceWith(t, framesB, "lobby", func(data map[string]any) bool {
		return len(data) == 2
	})

	// A updates presence → B receives a refreshed snapshot reflecting it.
	sendFrame(t, connA, ctx, map[string]any{
		"op": "set-presence", "room-id": "lobby",
		"data": map[string]any{"mood": "great"},
	})
	expectOp(t, framesA, "set-presence-ok")
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for presence update on B")
		case f := <-framesB:
			if op, _ := f["op"].(string); op == "refresh-presence" && presenceHas(f, "mood", "great") {
				goto done
			}
		}
	}
done:

	// client-broadcast from A arrives at B as server-broadcast.
	sendFrame(t, connA, ctx, map[string]any{
		"op": "client-broadcast", "room-id": "lobby",
		"topic": "chat", "data": map[string]any{"msg": "hi"},
	})
	expectOp(t, framesA, "client-broadcast-ok")
	bf := expectOp(t, framesB, "server-broadcast")
	if bf["topic"] != "chat" {
		t.Fatalf("broadcast topic: %v", bf)
	}

	// B leaves → A gets a snapshot down to 1 member.
	sendFrame(t, connB, ctx, map[string]any{"op": "leave-room", "room-id": "lobby"})
	expectOp(t, framesB, "leave-room-ok")
	for {
		select {
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for post-leave presence on A")
		case f := <-framesA:
			if op, _ := f["op"].(string); op == "refresh-presence" {
				data := f["data"].(map[string]any)
				if len(data) == 1 {
					return
				}
			}
		}
	}
}

func initWS(t *testing.T, conn *websocket.Conn, frames chan map[string]any, ctx context.Context, appID string) {
	t.Helper()
	sendFrame(t, conn, ctx, map[string]any{"op": "init", "app-id": appID})
	expectOp(t, frames, "init-ok")
}

func expectPresenceWith(t *testing.T, frames chan map[string]any, room string, pred func(map[string]any) bool) map[string]any {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for refresh-presence %s", room)
		case f, ok := <-frames:
			if !ok {
				t.Fatalf("conn closed waiting for presence")
			}
			if op, _ := f["op"].(string); op == "refresh-presence" && f["room-id"] == room {
				data, _ := f["data"].(map[string]any)
				if pred(data) {
					return data
				}
			}
		}
	}
}

func presenceHas(f map[string]any, key string, want any) bool {
	data, _ := f["data"].(map[string]any)
	for _, v := range data {
		entry, _ := v.(map[string]any)
		d, _ := entry["data"].(map[string]any)
		if d[key] == want {
			return true
		}
	}
	return false
}
