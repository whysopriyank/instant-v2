package main

import (
	"context"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// TestCF003AssembledRoomFanoutSSE closes the CF-003 rooms-fanout-positive
// SSE-transport remainder (manifest rooms-fanout-positive, surface
// ws.rooms.fanout, transport SSE) over the production-mounted GET/POST SSE
// path against an owned PostgreSQL fixture. Two SSE subscribers share one
// room through the same Manager.Handle path as WebSocket frames: join
// fans ordered presence to both peers, an explicit refresh-presence resync
// converges the joiner, set-presence fans the update to both peers, a
// client-broadcast reaches only the peer with the exact sender session, and
// leave converges the survivor to the exact one-member snapshot. Every leg
// asserts lengths before elements with exact DeepEqual so a partial,
// reordered, or diverged fanout cannot hide behind a subset check. This is
// mounted-route positive evidence only; no corpus NDJSON leg is added or
// claimed.
func TestCF003AssembledRoomFanoutSSE(t *testing.T) {
	mux, appID, _ := cf003PostgresMux(t)
	app := platform.UUIDToStr(appID)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	a := cf003OpenSSE(t, ctx, server, app)
	b := cf003OpenSSE(t, ctx, server, app)
	if a.sessionID == b.sessionID || a.token == b.token {
		t.Fatalf("SSE room clients reused credentials: sessions=%q/%q tokens=%q/%q", a.sessionID, b.sessionID, a.token, b.token)
	}

	a.post(t, ctx, map[string]any{"op": "init", "app-id": app})
	aSess := cf003AssertSSERoomInit(t, cf003ReadSSE(t, a.scanner), app)
	b.post(t, ctx, map[string]any{"op": "init", "app-id": app})
	bSess := cf003AssertSSERoomInit(t, cf003ReadSSE(t, b.scanner), app)
	if aSess == "" || bSess == "" || aSess == bSess {
		t.Fatalf("SSE room sessions not distinct: %q vs %q", aSess, bSess)
	}

	// A joins alone: ack plus the exact solo presence, in either order.
	a.post(t, ctx, map[string]any{
		"op": "join-room", "room-id": "cf003-room", "peer-id": "peer-a",
		"data": map[string]any{"mood": "ok"}, "client-event-id": "join-a",
	})
	wantSolo := map[string]any{
		aSess: map[string]any{"peer": "peer-a", "data": map[string]any{"mood": "ok"}},
	}
	cf003AwaitSSERoomAckAndPresence(t, a, "join-room-ok", map[string]any{
		"op": "join-room-ok", "room-id": "cf003-room", "client-event-id": "join-a",
	}, wantSolo)

	// B joins: both peers converge to the exact two-member snapshot; the
	// joiner's ack and presence may arrive in either order.
	wantBoth := map[string]any{
		aSess: map[string]any{"peer": "peer-a", "data": map[string]any{"mood": "ok"}},
		bSess: map[string]any{"peer": "peer-b", "data": map[string]any{}},
	}
	b.post(t, ctx, map[string]any{
		"op": "join-room", "room-id": "cf003-room", "peer-id": "peer-b",
		"data": map[string]any{}, "client-event-id": "join-b",
	})
	cf003AwaitSSERoomAckAndPresence(t, b, "join-room-ok", map[string]any{
		"op": "join-room-ok", "room-id": "cf003-room", "client-event-id": "join-b",
	}, wantBoth)
	cf003AssertSSERoomPresence(t, cf003ReadSSE(t, a.scanner), wantBoth)

	// B requests the current state explicitly so join-ack/presence
	// scheduling cannot hide whether its mounted session sees the same
	// room snapshot.
	b.post(t, ctx, map[string]any{"op": "refresh-presence", "room-id": "cf003-room"})
	cf003AssertSSERoomPresence(t, cf003ReadSSE(t, b.scanner), wantBoth)

	// Presence update fans the exact updated snapshot to both peers.
	a.post(t, ctx, map[string]any{
		"op": "set-presence", "room-id": "cf003-room",
		"data": map[string]any{"mood": "great"}, "client-event-id": "presence-a",
	})
	wantUpdated := map[string]any{
		aSess: map[string]any{"peer": "peer-a", "data": map[string]any{"mood": "great"}},
		bSess: map[string]any{"peer": "peer-b", "data": map[string]any{}},
	}
	cf003AwaitSSERoomAckAndPresence(t, a, "set-presence-ok", map[string]any{
		"op": "set-presence-ok", "room-id": "cf003-room", "client-event-id": "presence-a",
	}, wantUpdated)
	cf003AssertSSERoomPresence(t, cf003ReadSSE(t, b.scanner), wantUpdated)

	// Peer-only broadcast: the sender sees only its ack, the peer sees the
	// exact server-broadcast carrying the sender session.
	a.post(t, ctx, map[string]any{
		"op": "client-broadcast", "room-id": "cf003-room", "topic": "chat",
		"data": map[string]any{"message": "hello"}, "client-event-id": "broadcast-a",
	})
	cf003SSEExact(t, cf003ReadSSE(t, a.scanner), map[string]any{
		"op": "client-broadcast-ok", "client-event-id": "broadcast-a",
	})
	cf003SSEExact(t, cf003ReadSSE(t, b.scanner), map[string]any{
		"op": "server-broadcast", "room-id": "cf003-room", "topic": "chat",
		"session-id": aSess,
		"data":       map[string]any{"peer": "peer-a", "data": map[string]any{"message": "hello"}},
	})
	// Sender isolation is proven by ordering: the sender's next frame after
	// its broadcast ack must be the leave-driven presence below, not a
	// server-broadcast echo. Any echo would arrive before that presence and
	// fail the exact one-member assertion.

	// Leave converges the survivor to the exact one-member snapshot.
	b.post(t, ctx, map[string]any{
		"op": "leave-room", "room-id": "cf003-room", "client-event-id": "leave-b",
	})
	cf003SSEExact(t, cf003ReadSSE(t, b.scanner), map[string]any{
		"op": "leave-room-ok", "room-id": "cf003-room", "client-event-id": "leave-b",
	})
	wantA := map[string]any{
		aSess: map[string]any{"peer": "peer-a", "data": map[string]any{"mood": "great"}},
	}
	cf003AssertSSERoomPresence(t, cf003ReadSSE(t, a.scanner), wantA)

	a.closeAndAwaitUnauthorized(t, ctx)
	b.closeAndAwaitUnauthorized(t, ctx)
}

func cf003AssertSSERoomInit(t *testing.T, frame map[string]any, appID string) string {
	t.Helper()
	if len(frame) != 5 || frame["op"] != "init-ok" {
		t.Fatalf("SSE room init shape = %#v; want exactly five keys with op init-ok", frame)
	}
	sessionID, _ := frame["session-id"].(string)
	if sessionID == "" {
		t.Fatalf("SSE room session id = %#v", frame["session-id"])
	}
	attrs, ok := frame["attrs"].([]any)
	if !ok || len(attrs) != 0 {
		t.Fatalf("SSE room attrs = %#v; want exactly empty", frame["attrs"])
	}
	if want := map[string]any{"status": "active"}; !reflect.DeepEqual(frame["app-status"], want) {
		t.Fatalf("SSE room app-status = %#v; want exactly %#v", frame["app-status"], want)
	}
	if want := map[string]any{"admin?": false, "app": map[string]any{"id": appID}, "user": nil}; !reflect.DeepEqual(frame["auth"], want) {
		t.Fatalf("SSE room auth = %#v; want exactly %#v", frame["auth"], want)
	}
	return sessionID
}

func cf003SSEExact(t *testing.T, got, want map[string]any) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("SSE room frame keys = %#v; want exactly %#v", got, want)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SSE room frame = %#v; want exactly %#v", got, want)
	}
	// Lengths before elements for nested broadcast payloads: the exact
	// peer/data shape must not hide an extra user key or a missing data key.
	if gotData, ok := got["data"]; ok {
		wantData, _ := want["data"].(map[string]any)
		if wantData != nil {
			gotMap, ok := gotData.(map[string]any)
			if !ok || len(gotMap) != len(wantData) {
				t.Fatalf("SSE room data keys = %#v; want exactly %#v", gotData, wantData)
			}
		}
	}
}

func cf003AssertSSERoomPresence(t *testing.T, frame map[string]any, want map[string]any) {
	t.Helper()
	if len(want) == 0 {
		t.Fatalf("SSE room presence oracle must be non-empty")
	}
	if len(frame) != 3 || frame["op"] != "refresh-presence" || frame["room-id"] != "cf003-room" {
		t.Fatalf("SSE room presence envelope = %#v", frame)
	}
	data, ok := frame["data"].(map[string]any)
	if !ok || len(data) != len(want) {
		t.Fatalf("SSE room presence members = %#v; want exactly %#v", frame["data"], want)
	}
	// Lengths before elements for each member: every entry must carry
	// exactly peer plus data with no user disclosure for anonymous peers.
	for sid, rawWant := range want {
		wantMember, _ := rawWant.(map[string]any)
		if wantMember == nil {
			t.Fatalf("SSE room presence oracle member %q = %#v", sid, rawWant)
		}
		gotMember, ok := data[sid].(map[string]any)
		if !ok || len(gotMember) != len(wantMember) {
			t.Fatalf("SSE room member %q = %#v; want exactly %#v", sid, data[sid], wantMember)
		}
	}
	if !reflect.DeepEqual(data, want) {
		t.Fatalf("SSE room presence = %#v; want exactly %#v", data, want)
	}
}

func cf003AwaitSSERoomAckAndPresence(t *testing.T, client *cf003SSEClient, ackOp string, wantAck, wantPresence map[string]any) {
	t.Helper()
	if len(wantPresence) == 0 {
		t.Fatalf("SSE room presence oracle must be non-empty")
	}
	ackSeen, presenceSeen := false, false
	for i := 0; i < 2; i++ {
		frame := cf003ReadSSE(t, client.scanner)
		switch frame["op"] {
		case ackOp:
			if ackSeen {
				t.Fatalf("duplicate SSE room ack: %#v", frame)
			}
			cf003SSEExact(t, frame, wantAck)
			ackSeen = true
		case "refresh-presence":
			if presenceSeen {
				t.Fatalf("duplicate SSE room presence: %#v", frame)
			}
			cf003AssertSSERoomPresence(t, frame, wantPresence)
			presenceSeen = true
		default:
			t.Fatalf("unexpected SSE room frame during %s: %#v", ackOp, frame)
		}
	}
	if !ackSeen || !presenceSeen {
		t.Fatalf("SSE room ack/presence incomplete: ack=%v presence=%v", ackSeen, presenceSeen)
	}
}
