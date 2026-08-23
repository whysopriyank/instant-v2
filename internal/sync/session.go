// Package sync owns the WebSocket session protocol at /runtime/session.
// Port surface of v1's reactive/session.clj op dispatcher + lib/ring/websocket,
// collapsed onto coder/websocket. The frozen surface (docs/03-protocol.md):
//
//	client→server: init, add-query, remove-query, transact, error,
//	  join-room/leave-room/set-presence/client-broadcast (rooms), …
//	server→client: init-ok{session-id,auth?,attrs}, add-query-ok,
//	  remove-query-ok, refresh-ok{computations,processed-tx-id,…},
//	  transact-ok{tx-id,isn?}, error{status,type,message,hint}
//
// Feature gates by client version (session.clj:73-107):
//
//	skip-attrs ≥ 0.20.4, patch-presence ≥ 0.17.5, batch-messages ≥ 0.22.75
package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/transact"
)

// Feature gates and their minimum SDK versions.
type feature struct {
	name string
	min  [3]int
}

var gates = []feature{
	{"skip-attrs", [3]int{0, 20, 4}},
	{"patch-presence", [3]int{0, 17, 5}},
	{"batch-messages", [3]int{0, 22, 75}},
}

// Features computed from the init versions map.
type Features map[string]bool

// Negotiate derives the feature set from the SDK versions map.
func Negotiate(versions map[string]string) Features {
	out := Features{}
	core := versions["@instantdb/core"]
	var v [3]int
	if n, _ := fmt.Sscanf(core, "%d.%d.%d", &v[0], &v[1], &v[2]); n == 3 {
		for _, g := range gates {
			out[g.name] = v[0] > g.min[0] ||
				(v[0] == g.min[0] && v[1] > g.min[1]) ||
				(v[0] == g.min[0] && v[1] == g.min[1] && v[2] >= g.min[2])
		}
	}
	return out
}

// Authenticator resolves an init refresh-token to a user record. Implemented
// by internal/authn.Service.
type Authenticator interface {
	VerifyRefreshTokenAsMap(ctx context.Context, appID [16]byte, rawToken string) (map[string]any, bool, error)
}

// Deps are the services a session needs.
type Deps struct {
	DB       *storage.DB
	Catalogs *platform.CatalogCache
	Store    *reactive.Store
	Auth     Authenticator
	Rooms    *RoomHub
	OnCommit func(ctx context.Context, appID string, attrIDs []string, txID int64)
	Logger   *slog.Logger
}

// Session is one live WebSocket connection's state.
type Session struct {
	ID       string
	AppID    string
	Features Features
	Admin    bool
	AuthUser map[string]any
	Subs     map[string]bool // subscription ids owned by this session
	Rooms    map[string]bool
	Send     func(Frame) error // transport-bound writer
	mu       sync.Mutex
}

// Manager creates sessions and dispatches frames. One Manager per process.
type Manager struct {
	Deps Deps
	log  *slog.Logger
	nm   sync.Mutex
}

func NewManager(d Deps) *Manager {
	return &Manager{Deps: d}
}

func (m *Manager) logger() *slog.Logger {
	if m.log != nil {
		return m.log
	}
	return slog.Default()
}

// HandleInit validates the init op and builds the session.
func (m *Manager) HandleInit(ctx context.Context, f Frame) (*Session, Frame, error) {
	appID, _ := f.String("app-id")
	if appID == "" {
		return nil, ErrFrame(400, "missing-app-id", "init requires app-id"), fmt.Errorf("missing app-id")
	}
	_ = ctx // reserved for async auth resolution
	var versions map[string]string
	if raw, ok := f["versions"]; ok {
		_ = json.Unmarshal(raw, &versions)
	}
	adminTok, _ := f.String("__admin-token")
	sess := &Session{
		ID:       newID(&m.nm),
		AppID:    appID,
		Features: Negotiate(versions),
		Subs:     map[string]bool{},
		Rooms:    map[string]bool{},
	}
	if adminTok != "" {
		ok, err := m.Deps.Catalogs.CheckAdminToken(ctx, appID, adminTok)
		if err != nil {
			return nil, ErrFrame(500, "internal", "admin token check failed"), err
		}
		sess.Admin = ok
	}
	var user map[string]any
	if rt, _ := f.String("refresh-token"); rt != "" && m.Deps.Auth != nil {
		appUUID, perr := platform.ScanUUIDErr(appID)
		if perr == nil {
			if u, ok, aerr := m.Deps.Auth.VerifyRefreshTokenAsMap(ctx, appUUID, rt); aerr == nil && ok {
				user = u
				sess.AuthUser = u
			}
		}
	}
	cat, err := m.Deps.Catalogs.For(ctx, appID)
	if err != nil {
		return nil, ErrFrame(404, "unknown-app", "no such app"), err
	}
	// v1 ALWAYS includes attrs in init-ok (session.clj L186); skip-attrs gates
	// whether refresh frames re-send unchanged attrs, not init.
	b, _ := json.Marshal(cat.WireAttrs())
	authObj := map[string]any{
		"app":    map[string]any{"id": appID},
		"user":   user,
		"admin?": sess.Admin,
	}
	reply := Frame{
		"op":         json.RawMessage(`"init-ok"`),
		"session-id": json.RawMessage(mustJSON(sess.ID)),
		"attrs":      b,
		"auth":       json.RawMessage(mustJSON(authObj)),
		"app-status": json.RawMessage(`{"status":"ok"}`),
	}
	if eid, _ := f.String("client-event-id"); eid != "" {
		reply["client-event-id"] = json.RawMessage(mustJSON(eid))
	}
	return sess, reply, nil
}

// Handle dispatches one authenticated frame for the session, returning the
// direct reply frame(s). Async refresh-ok frames flow through reactive.Emit.
func (m *Manager) Handle(ctx context.Context, sess *Session, f Frame) ([]Frame, error) {
	op, _ := f.GetOp()
	switch op {
	case "add-query":
		return m.handleAddQuery(ctx, sess, f)
	case "remove-query":
		subID, _ := f.String("client-event-id")
		_ = subID
		qid, _ := f.String("q")
		_ = qid
		// v2 keys subscriptions by id echoed in add-query-ok; removal by that id.
		id, _ := f.String("subscription-id")
		if id == "" {
			if raw, ok := f["q"]; ok {
				id = subKey(sess.ID, string(raw))
			}
		}
		m.Deps.Store.Remove(id)
		delete(sess.Subs, id)
		return []Frame{{"op": json.RawMessage(`"remove-query-ok"`)}}, nil
	case "transact":
		return m.handleTransact(ctx, sess, f)
	case "error":
		return nil, nil // client-side reports logged upstream; no reply
	case "join-room":
		return m.Deps.Rooms.Join(ctx, sess, f)
	case "leave-room":
		return m.Deps.Rooms.Leave(ctx, sess, f)
	case "set-presence":
		return m.Deps.Rooms.SetPresence(ctx, sess, f)
	case "refresh-presence":
		// Client-initiated resync: full current room state to requester only.
		return m.Deps.Rooms.RefreshPresence(ctx, sess, f)
	case "client-broadcast":
		return m.Deps.Rooms.ClientBroadcast(ctx, sess, f)
	case "server-broadcast", "start-sync", "remove-sync",
		"refresh-sync-table", "resync-table", "start-stream", "append-stream",
		"subscribe-stream", "unsubscribe-stream":
		// Delta-sync/stream surface arrives in Phase 6; ack so clients don't stall.
		return []Frame{{"op": mustRaw(fmt.Sprintf("%q", op+"-ok"))}}, nil
	default:
		// Unknown ops are logged-and-ignored — the forward-compat lever.
		m.logger().Info("sync: unknown op ignored", "op", op)
		return nil, nil
	}
}

func (m *Manager) handleAddQuery(ctx context.Context, sess *Session, f Frame) ([]Frame, error) {
	rawQ, ok := f["q"]
	if !ok {
		return []Frame{ErrFrame(400, "bad-request", "add-query requires q")}, nil
	}
	key := subKey(sess.ID, string(rawQ))
	eventID, _ := f.String("client-event-id")

	exists := false
	if _, ok := m.Deps.Store.Get(key); ok {
		exists = true
	}
	if exists {
		fr := Frame{"op": json.RawMessage(`"add-query-exists"`)}
		if eventID != "" {
			fr["client-event-id"] = json.RawMessage(mustJSON(eventID))
		}
		return []Frame{fr}, nil
	}

	// Register the subscription; topics come from compiling the query.
	topics, err := m.topicsFor(ctx, sess.AppID, rawQ)
	if err != nil {
		return []Frame{ErrFrame(400, "invalid-query", err.Error())}, nil
	}
	sub := &reactive.Subscription{
		ID: key, AppID: sess.AppID, Query: rawQ, Topics: topics,
		Emit: func(fr reactive.Frame) {
			if sess.Send == nil {
				return
			}
			payload, _ := json.Marshal([]map[string]any{{
				"instaql-query":  json.RawMessage(fr.QueryJSON),
				"instaql-result": json.RawMessage(fr.ResultJSON),
			}})
			_ = sess.Send(Frame{
				"op":              json.RawMessage(`"refresh-ok"`),
				"computations":    payload,
				"processed-tx-id": json.RawMessage(mustJSON(fr.ProcessedTxID)),
			})
		},
	}
	m.Deps.Store.Add(sub)
	sess.mu.Lock()
	sess.Subs[key] = true
	sess.mu.Unlock()

	reply := Frame{"op": json.RawMessage(`"add-query-ok"`)}
	if eventID != "" {
		reply["client-event-id"] = json.RawMessage(mustJSON(eventID))
	}
	return []Frame{reply}, nil
}

func (m *Manager) handleTransact(ctx context.Context, sess *Session, f Frame) ([]Frame, error) {
	rawSteps, ok := f["tx-steps"]
	if !ok {
		return []Frame{ErrFrame(400, "bad-request", "transact requires tx-steps")}, nil
	}
	var steps []json.RawMessage
	if err := json.Unmarshal(rawSteps, &steps); err != nil {
		return []Frame{ErrFrame(400, "bad-request", "tx-steps must be an array")}, nil
	}
	parsed, err := transact.ParseSteps(steps)
	if err != nil {
		return []Frame{ErrFrame(400, "tx-step-validation", err.Error())}, nil
	}
	cat, err := m.Deps.Catalogs.For(ctx, sess.AppID)
	if err != nil {
		return []Frame{ErrFrame(404, "unknown-app", "no such app")}, nil
	}
	appID := parseUUIDOrZero(sess.AppID)
	opts := transact.Options{
		Admin:    sess.Admin,
		AuthUser: sess.AuthUser,
	}
	res, err := transact.Transact(ctx, m.Deps.DB, cat, appID, parsed, opts, nil)
	if err != nil {
		return []Frame{ErrFrame(403, "transact-error", err.Error())}, nil
	}
	if m.Deps.OnCommit != nil {
		var attrIDs []string
		for _, st := range parsed {
			if st.Op == "add-triple" || st.Op == "deep-merge-triple" || st.Op == "retract-triple" {
				if len(st.Args) >= 2 {
					var a string
					if json.Unmarshal(st.Args[1], &a) == nil && a != "" {
						attrIDs = append(attrIDs, a)
					}
				}
			}
		}
		m.Deps.OnCommit(ctx, sess.AppID, attrIDs, res.TxID)
	}
	txID, _ := f.String("client-event-id")
	fr := Frame{
		"op":    json.RawMessage(`"transact-ok"`),
		"tx-id": json.RawMessage(mustJSON(res.TxID)),
	}
	if txID != "" {
		fr["client-event-id"] = json.RawMessage(mustJSON(txID))
	}
	return []Frame{fr}, nil
}

// topicsFor compiles the query to its attr-id topic set.
func (m *Manager) topicsFor(ctx context.Context, appID string, rawQ json.RawMessage) (map[string]bool, error) {
	cat, err := m.Deps.Catalogs.For(ctx, appID)
	if err != nil {
		return nil, err
	}
	var q map[string]any
	if err := json.Unmarshal(rawQ, &q); err != nil {
		return nil, err
	}
	topics := map[string]bool{}
	var walk func(prefixEtype string, node map[string]any) error
	walk = func(etype string, node map[string]any) error {
		attrs := cat.ByEtype(etype)
		for k, v := range node {
			if k == "$" {
				if wm, ok := v.(map[string]any); ok {
					if w, ok := wm["where"].(map[string]any); ok {
						for label := range w {
							if a := cat.FindByEtypeLabel(etype, label); a != nil {
								topics[platform.UUIDToStr(a.ID)] = true
							}
						}
					}
				}
				continue
			}
			_ = attrs
			child, ok := v.(map[string]any)
			if !ok {
				child = map[string]any{}
			}
			// child level invalidates on the link attr of this etype too
			if a := cat.FindByEtypeLabel(etype, k); a != nil {
				topics[platform.UUIDToStr(a.ID)] = true
			}
			if err := walk(k, child); err != nil {
				return err
			}
		}
		// The level itself depends on every attr of its etype (new entities).
		for _, a := range cat.ByEtype(etype) {
			topics[a] = true
		}
		return nil
	}
	if err := walk("", q); err != nil {
		return nil, err
	}
	if len(topics) == 0 {
		// conservative fallback: invalidate on everything of known etypes
		for etype := range q {
			for _, a := range cat.ByEtype(etype) {
				topics[a] = true
			}
		}
	}
	return topics, nil
}

func subKey(sessionID, queryJSON string) string {
	h := fnv32a(sessionID + "\x00" + strings.TrimSpace(queryJSON))
	return fmt.Sprintf("sub-%08x", h)
}

func fnv32a(s string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime32
	}
	return h
}

var _ = perms.Bindings{}
