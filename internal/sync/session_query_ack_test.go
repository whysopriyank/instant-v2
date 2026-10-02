package sync

import (
	"context"
	"encoding/json"
	"testing"
)

// Pinned v1's handle-add-query!/handle-remove-query! echo both fields, with
// explicit null when client-event-id is absent (CF-005 scenario 02).
func TestQueryLifecycleAckV1Fields(t *testing.T) {
	for _, op := range []string{"add-query", "remove-query"} {
		for _, eventID := range []json.RawMessage{nil, json.RawMessage(`"cccccccc-cccc-4ccc-8ccc-cccccccccccc"`)} {
			t.Run(op+"/"+string(eventID), func(t *testing.T) {
				q := json.RawMessage(`{"posts":{}}`)
				mgr := NewManager(Deps{})
				sess := &Session{AppID: "22222222-2222-4222-8222-222222222222", Subs: map[string]bool{}}
				key := groupKey(sess.AppID, wireNodelist, q, false)
				sess.Subs[key] = true
				f := Frame{"op": json.RawMessage(mustJSON(op)), "q": q}
				if eventID != nil {
					f["client-event-id"] = eventID
				}
				frames, err := mgr.Handle(context.Background(), sess, f)
				if err != nil || len(frames) != 1 {
					t.Fatalf("Handle: frames=%v, err=%v", frames, err)
				}
				got, err := frames[0].Encode()
				if err != nil {
					t.Fatal(err)
				}
				ackOp := "add-query-exists"
				if op == "remove-query" {
					ackOp = "remove-query-ok"
					if sess.Subs[key] {
						t.Fatal("remove-query left subscription attached")
					}
				}
				wantID := "null"
				if eventID != nil {
					wantID = string(eventID)
				}
				want := `{"client-event-id":` + wantID + `,"op":"` + ackOp + `","q":{"posts":{}}}`
				if string(got) != want {
					t.Fatalf("ack=%s, pinned v1 requires %s", got, want)
				}
			})
		}
	}
}
