package sync

import (
	"context"
	"encoding/json"
	"testing"
)

func TestPlaceholderOperationsAreExplicitlyUnsupported(t *testing.T) {
	ops := []string{
		"server-broadcast", "start-sync", "remove-sync", "refresh-sync-table",
		"resync-table", "start-stream", "append-stream", "subscribe-stream",
		"unsubscribe-stream",
	}
	m := NewManager(Deps{})
	for _, op := range ops {
		t.Run(op, func(t *testing.T) {
			sess := &Session{
				ID: "session-1", AppID: "app-1",
				Subs:  map[string]bool{"existing-sub": true},
				Rooms: map[string]bool{"existing-room": true},
			}
			beforeSubs := len(sess.Subs)
			beforeRooms := len(sess.Rooms)

			frames, err := m.Handle(context.Background(), sess, Frame{
				"op":              json.RawMessage(mustJSON(op)),
				"client-event-id": json.RawMessage(`"event-1"`),
			})
			if err != nil {
				t.Fatalf("Handle error = %v", err)
			}
			if len(frames) != 1 {
				t.Fatalf("frames = %d; want one explicit error", len(frames))
			}
			gotOp, _ := frames[0].GetOp()
			if gotOp != "error" {
				t.Fatalf("response op = %q; want error: %#v", gotOp, frames[0])
			}
			var status int
			if err := json.Unmarshal(frames[0]["status"], &status); err != nil || status != 501 {
				t.Fatalf("status = %s; want 501", frames[0]["status"])
			}
			var typ, msg, gotOperation, eventID string
			_ = json.Unmarshal(frames[0]["type"], &typ)
			_ = json.Unmarshal(frames[0]["message"], &msg)
			_ = json.Unmarshal(frames[0]["operation"], &gotOperation)
			_ = json.Unmarshal(frames[0]["client-event-id"], &eventID)
			if typ != "unsupported" || msg != "sync operation is unsupported" || gotOperation != op || eventID != "event-1" {
				t.Fatalf("unsupported response fields: %#v", frames[0])
			}
			if len(sess.Subs) != beforeSubs || len(sess.Rooms) != beforeRooms || !sess.Subs["existing-sub"] || !sess.Rooms["existing-room"] {
				t.Fatalf("unsupported operation mutated session state: subs=%v rooms=%v", sess.Subs, sess.Rooms)
			}
		})
	}
}
