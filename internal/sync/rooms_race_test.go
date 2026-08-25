package sync

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
)

// TestSetPresenceConcurrentNoRace pins the rebuild-and-swap discipline in
// SetPresence: presence writers and roomDataJSON readers must never touch
// member fields unsynchronized. Run under -race; a regression to in-place
// mutation of the shared *roomMember trips the detector immediately.
func TestSetPresenceConcurrentNoRace(t *testing.T) {
	h := NewRoomHub()
	ctx := context.Background()
	k := key("app", "lobby")
	s1 := &Session{ID: "s1", AppID: "app", Rooms: map[string]bool{"r": true}, Send: func(Frame) error { return nil }}
	s2 := &Session{ID: "s2", AppID: "app", Rooms: map[string]bool{"r": true}, Send: func(Frame) error { return nil }}

	h.mu.Lock()
	h.rooms[k] = map[string]*roomMember{
		"s1": {sessionID: "s1", peer: "p1", data: map[string]any{}, sess: s1},
		"s2": {sessionID: "s2", peer: "p2", data: map[string]any{}, sess: s2},
	}
	h.mu.Unlock()

	frame := func(mood string) Frame {
		return Frame{
			"room-id": json.RawMessage(`"lobby"`),
			"data":    json.RawMessage(`{"mood":"` + mood + `"}`),
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if _, err := h.SetPresence(ctx, s1, frame("a")); err != nil {
					t.Errorf("s1 set-presence: %v", err)
					return
				}
				if _, err := h.SetPresence(ctx, s2, frame("b")); err != nil {
					t.Errorf("s2 set-presence: %v", err)
					return
				}
			}
		}(i)
	}
	// Concurrent reader hammering the same members' maps.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 400; j++ {
			_ = h.roomDataJSON(k)
		}
	}()
	wg.Wait()
}
