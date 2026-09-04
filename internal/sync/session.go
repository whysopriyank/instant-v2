// Package sync owns the WebSocket session protocol at /runtime/session.
// Port surface of v1's reactive/session.clj op dispatcher + lib/ring/websocket,
// collapsed onto coder/websocket. The frozen surface (docs/reference/03-protocol.md):
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
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
	"github.com/instant-v2/instant-v2/internal/storage"
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
	// delta-refresh ≥ 0.23.0 — additive v2 wire optimisation (docs/03 §5):
	// negotiating sessions may receive structural refresh patches
	// (refresh-ok-delta) instead of full envelopes when the change set is
	// expressible as per-entity ops. Min version chosen as the next minor
	// AFTER batch-messages (0.22.75), the newest existing gate, so every SDK
	// version in the wild today stays below it and keeps receiving full
	// envelopes with zero behavior change.
	{"delta-refresh", [3]int{0, 23, 0}},
}

// ErrCloseSession instructs the transport to deliver any pending reply
// frames, then tear down the connection/session. Handle wraps it around
// unrecoverable per-session failures (e.g. subscription-cap breaches).
var ErrCloseSession = errors.New("sync: closing session")

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
	OnCommit func(ctx context.Context, appID string, attrIDs []string, txID int64, attrsChanged bool)
	Logger   *slog.Logger
	// Rules resolves the app's permission RuleDoc for transact checks and
	// subscription view gates. nil → default-open (no rules configured).
	Rules func(ctx context.Context, appID string) (*perms.RuleDoc, error)
	// Limiter is the optional per-app traffic gate wired from cmd (the
	// ratelimit package's token buckets). class is "transact" | "ws".
	// Denials surface as 429-shaped protocol frames with Retry-After.
	Limiter interface {
		Allow(appID string, class string) (bool, time.Duration)
	}
	// TransactGate consults overload state (notifier queue depth,
	// docs/reference/09-tier2-architecture.md §T2.1) before executing a transact
	// op. nil means always allow. A *reactive.ShedError denial becomes
	// the 429-shaped error frame carrying RetryAfter as the hint.
	TransactGate func(appID string) error
	// OnCommitChanges is the change-annotated invalidation path
	// (docs/reference/09-tier2-architecture.md §T2.5): when set and every step of a
	// transact resolves to a plain triple write, it is preferred over
	// OnCommit so the incremental engine can splice instead of recompute.
	OnCommitChanges func(ctx context.Context, appID string, changes []reactive.Change, txID int64, attrsChanged bool)
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
	// SendRaw writes pre-encoded frame bytes (shared fan-out payloads);
	// nil falls back to nothing — group dispatch skips such sessions.
	SendRaw func([]byte) error
	// TreeResults selects v1's return-type :tree (admin subscribe-query,
	// session.clj:1395): refresh envelopes carry the bare object tree instead
	// of the join-rows node-list used on the WS path.
	TreeResults bool
	mu          sync.Mutex
}

// Manager creates sessions and dispatches frames. One Manager per process.
type Manager struct {
	Deps Deps
	log  *slog.Logger
	nm   sync.Mutex

	// Query-group registry (docs/archive/08-tier1-hotpath.md §T1.1).
	groupsMu   sync.Mutex
	groups     map[string]*queryGroup
	appMembers map[string]int // appID -> total attached members (cap accounting)

	// flight serializes the synchronous add-query initial answer per group
	// key: the first arriver computes and seeds the group snapshot,
	// concurrent duplicates wait and reuse it (docs/08 §T1.1 single-flight).
	flight sync.Map // group key -> *snapshotFlight
}

func NewManager(d Deps) *Manager {
	return &Manager{
		Deps:       d,
		groups:     map[string]*queryGroup{},
		appMembers: map[string]int{},
	}
}

func (m *Manager) logger() *slog.Logger {
	if m.log != nil {
		return m.log
	}
	return slog.Default()
}

// Security invariant: an unloadable rule doc must never widen access. The
// HTTP planes fail closed (main.go, runtimeapi); the sync plane refuses the
// op with a protocol error instead of degrading to default-open enforcement.
func (m *Manager) rulesFor(ctx context.Context, appID string) (*perms.RuleDoc, error) {
	if m.Deps.Rules == nil {
		return nil, nil
	}
	doc, err := m.Deps.Rules(ctx, appID)
	if err != nil {
		m.logger().Warn("sync: rules load failed; refusing op", "app", appID, "err", err)
		return nil, err
	}
	return doc, nil
}

// HandleInit validates the init op and builds the session.
func (m *Manager) HandleInit(ctx context.Context, f Frame) (*Session, Frame, error) {
	appID, _ := f.String("app-id")
	if appID == "" {
		return nil, ErrFrame(400, "missing-app-id", "init requires app-id"), fmt.Errorf("missing app-id")
	}
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
		// v1 app statuses are :active|:read-only|:disabled (app.clj:341) and the
		// frozen SDK rejects writes unless status === "active" (Reactor._setAppStatus).
		"app-status": json.RawMessage(`{"status":"active"}`),
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
		// Removal resolves the same group key add-query registered.
		id, _ := f.String("subscription-id")
		if id == "" {
			if raw, ok := f["q"]; ok {
				class := wireNodelist
				if sess.TreeResults {
					class = wireTree
				}
				id = groupKey(sess.AppID, class, raw, sess.Admin)
			}
		}
		m.detachMember(sess, id)
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
		// Delta-sync/stream surface is not implemented in this checkpoint. Never
		// acknowledge a placeholder: a successful <op>-ok would make clients
		// believe state was created or changed when nothing happened.
		return []Frame{unsupportedOperationFrame(f, op)}, nil
	default:
		// Unknown ops are logged-and-ignored — the forward-compat lever.
		m.logger().Info("sync: unknown op ignored", "op", op)
		return nil, nil
	}
}

func unsupportedOperationFrame(f Frame, op string) Frame {
	fr := ErrFrame(501, "unsupported", "sync operation is unsupported")
	fr["operation"] = json.RawMessage(mustJSON(op))
	if eid, _ := f.String("client-event-id"); eid != "" {
		fr["client-event-id"] = json.RawMessage(mustJSON(eid))
	}
	return fr
}
