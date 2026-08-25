package sync

// In-process rooms/presence hub. Ports the observable semantics of v1's
// reactive/ephemeral.clj for a single node (v1's Hazelcast IMap collapses to
// one map; cross-node fan-out is a Phase 6 concern behind this interface).
//
// Wire facts pinned from v1 @ a4d2ef33:
//   - join-room {room-id, data?, peer-id?} → join-room-ok {room-id}
//   - leave-room → leave-room-ok; set-presence → set-presence-ok
//   - Presence state per member: {peer, user{id}, data}; every mutation
//     fans `refresh-presence {room-id, data: room-data}` to ALL members.
//     Deviation (documented): v1 additionally sends editscript patches as
//     patch-presence to ≥0.17.5 clients; v2 always sends full snapshots,
//     which is semantically correct for every client version — the patch is
//     a bandwidth optimization only.
//   - client-broadcast {room-id, topic?, data} delivers
//     server-broadcast {op, room-id, topic, session-id, data:{peer,user,data}}
//     to every OTHER member; sender gets client-broadcast-ok.

import (
	"context"
	"encoding/json"
	"sync"
)

type roomKey struct {
	appID, roomID string
}

type roomMember struct {
	sessionID string
	peer      string
	userID    string
	data      map[string]any
	sess      *Session
}

// RoomHub owns all rooms for the process. Safe for concurrent use; Send on
// each Session is serialized by the websocket writer.
type RoomHub struct {
	mu    sync.RWMutex
	rooms map[roomKey]map[string]*roomMember
}

func NewRoomHub() *RoomHub {
	return &RoomHub{rooms: map[roomKey]map[string]*roomMember{}}
}

func key(appID, roomID string) roomKey { return roomKey{appID: appID, roomID: roomID} }

// LeaveAll removes a session from every room on disconnect.
// Safe on a nil hub (tests / rooms disabled).
func (h *RoomHub) LeaveAll(sess *Session) {
	if h == nil {
		return
	}
	h.mu.Lock()
	var touched []roomKey
	for k, members := range h.rooms {
		if _, ok := members[sess.ID]; ok {
			delete(members, sess.ID)
			if len(members) == 0 {
				delete(h.rooms, k)
			}
			touched = append(touched, k)
		}
	}
	h.mu.Unlock()
	for _, k := range touched {
		h.broadcastPresence(k)
	}
}

func (h *RoomHub) Join(ctx context.Context, sess *Session, f Frame) ([]Frame, error) {
	roomID, _ := f.String("room-id")
	if roomID == "" {
		return []Frame{ErrFrame(400, "bad-request", "join-room requires room-id")}, nil
	}
	m := &roomMember{
		sessionID: sess.ID,
		peer:      strOr(f, "peer-id", sess.ID),
		sess:      sess,
	}
	if raw, ok := f["data"]; ok {
		_ = json.Unmarshal(raw, &m.data)
	}
	if sess.AuthUser != nil {
		if id, ok := sess.AuthUser["id"].(string); ok {
			m.userID = id
		}
	}
	k := key(sess.AppID, roomID)
	h.mu.Lock()
	if h.rooms[k] == nil {
		h.rooms[k] = map[string]*roomMember{}
	}
	h.rooms[k][sess.ID] = m
	h.mu.Unlock()

	sess.mu.Lock()
	sess.Rooms[roomID] = true
	sess.mu.Unlock()

	reply := roomField(f, "join-room-ok", "room-id", roomID)
	go h.broadcastPresence(k) // async like v1's ephemeral event queue
	return []Frame{reply}, nil
}

func (h *RoomHub) Leave(ctx context.Context, sess *Session, f Frame) ([]Frame, error) {
	roomID, _ := f.String("room-id")
	k := key(sess.AppID, roomID)
	h.mu.Lock()
	if members := h.rooms[k]; members != nil {
		delete(members, sess.ID)
		if len(members) == 0 {
			delete(h.rooms, k)
		}
	}
	h.mu.Unlock()
	sess.mu.Lock()
	delete(sess.Rooms, roomID)
	sess.mu.Unlock()
	go h.broadcastPresence(k)
	return []Frame{roomField(f, "leave-room-ok", "room-id", roomID)}, nil
}

func (h *RoomHub) SetPresence(ctx context.Context, sess *Session, f Frame) ([]Frame, error) {
	roomID, _ := f.String("room-id")
	k := key(sess.AppID, roomID)
	h.mu.RLock()
	cur := h.rooms[k][sess.ID]
	h.mu.RUnlock()
	if cur == nil {
		return []Frame{ErrFrame(400, "not-in-room", "set-presence requires join-room first")}, nil
	}
	// Rebuild-and-swap under the write lock: mutating cur.data/cur.peer in
	// place raced against roomDataJSON's RLock readers (torn map[string]any).
	h.mu.Lock()
	latest := h.rooms[k][sess.ID]
	if latest == nil { // left the room concurrently
		h.mu.Unlock()
		return []Frame{ErrFrame(400, "not-in-room", "set-presence requires join-room first")}, nil
	}
	nm := *latest // preserve sessionID/userID/sess
	if raw, ok := f["data"]; ok {
		var d map[string]any
		_ = json.Unmarshal(raw, &d)
		nm.data = d
	}
	if p := strOr(f, "peer-id", ""); p != "" {
		nm.peer = p
	}
	h.rooms[k][sess.ID] = &nm
	h.mu.Unlock()
	go h.broadcastPresence(k)
	return []Frame{roomField(f, "set-presence-ok", "room-id", roomID)}, nil
}

func (h *RoomHub) RefreshPresence(ctx context.Context, sess *Session, f Frame) ([]Frame, error) {
	roomID, _ := f.String("room-id")
	k := key(sess.AppID, roomID)
	frame := Frame{
		"op":      json.RawMessage(`"refresh-presence"`),
		"room-id": json.RawMessage(mustJSON(roomID)),
		"data":    h.roomDataJSON(k),
	}
	return nil, sendTo(sess, frame) // direct push, no -ok reply
}

func (h *RoomHub) ClientBroadcast(ctx context.Context, sess *Session, f Frame) ([]Frame, error) {
	roomID, _ := f.String("room-id")
	topic, _ := f.String("topic")
	k := key(sess.AppID, roomID)

	h.mu.RLock()
	mem := h.rooms[k][sess.ID]
	others := make([]*Session, 0, len(h.rooms[k]))
	for sid, m := range h.rooms[k] {
		if sid != sess.ID {
			others = append(others, m.sess)
		}
	}
	h.mu.RUnlock()
	if mem == nil {
		return []Frame{ErrFrame(400, "not-in-room", "client-broadcast requires join-room first")}, nil
	}

	payload := map[string]any{"peer": mem.peer, "data": json.RawMessage(`null`)}
	if raw, ok := f["data"]; ok {
		payload["data"] = raw
	}
	if mem.userID != "" {
		payload["user"] = map[string]any{"id": mem.userID}
	}
	out := Frame{
		"op":         json.RawMessage(`"server-broadcast"`),
		"room-id":    json.RawMessage(mustJSON(roomID)),
		"session-id": json.RawMessage(mustJSON(sess.ID)),
		"data":       json.RawMessage(mustJSON(payload)),
	}
	if topic != "" {
		out["topic"] = json.RawMessage(mustJSON(topic))
	}
	for _, o := range others {
		_ = sendTo(o, out)
	}
	return []Frame{opFrame(f, "client-broadcast-ok")}, nil
}

// broadcastPresence pushes the FULL room snapshot to every member (see file
// comment for the patch-presence deviation).
func (h *RoomHub) broadcastPresence(k roomKey) {
	data := h.roomDataJSON(k)
	h.mu.RLock()
	members := make([]*Session, 0, len(h.rooms[k]))
	for _, m := range h.rooms[k] {
		members = append(members, m.sess)
	}
	h.mu.RUnlock()
	frame := Frame{
		"op":      json.RawMessage(`"refresh-presence"`),
		"room-id": json.RawMessage(mustJSON(k.roomID)),
		"data":    data,
	}
	for _, s := range members {
		_ = sendTo(s, frame)
	}
}

func (h *RoomHub) roomDataJSON(k roomKey) json.RawMessage {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := map[string]any{}
	for sid, m := range h.rooms[k] {
		entry := map[string]any{"peer": m.peer, "data": m.data}
		if m.userID != "" {
			entry["user"] = map[string]any{"id": m.userID}
		}
		out[sid] = entry
	}
	b, _ := json.Marshal(out)
	return b
}

// opFrame echoes client-event-id onto a bare ack frame.
func opFrame(f Frame, op string) Frame {
	fr := Frame{"op": json.RawMessage(mustJSON(op))}
	if eid, _ := f.String("client-event-id"); eid != "" {
		fr["client-event-id"] = json.RawMessage(mustJSON(eid))
	}
	return fr
}

// roomField is opFrame plus one extra string field (e.g. room-id).
func roomField(f Frame, op, field, val string) Frame {
	fr := opFrame(f, op)
	fr[field] = json.RawMessage(mustJSON(val))
	return fr
}

func strOr(f Frame, key, def string) string {
	if s, ok := f.String(key); ok && s != "" {
		return s
	}
	return def
}

func sendTo(sess *Session, fr Frame) error {
	if sess.Send == nil {
		return nil
	}
	return sess.Send(fr)
}
