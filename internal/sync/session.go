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
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/metrics"
	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/tracing"
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
	// docs/09-tier2-architecture.md §T2.1) before executing a transact
	// op. nil means always allow. A *reactive.ShedError denial becomes
	// the 429-shaped error frame carrying RetryAfter as the hint.
	TransactGate func(appID string) error
	// OnCommitChanges is the change-annotated invalidation path
	// (docs/09-tier2-architecture.md §T2.5): when set and every step of a
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

	// Query-group registry (docs/08-tier1-hotpath.md §T1.1).
	groupsMu   sync.Mutex
	groups     map[string]*queryGroup
	appMembers map[string]int // appID -> total attached members (cap accounting)
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

// QueryGate is the permission snapshot stored on every subscription at
// attach time (Subscription.AttachCtx). Refresh executors rebuild the same
// visibility the group was admitted under: Rules drives instaql's view gate
// (closed etypes render empty; dynamic ones were rejected pre-attach for
// non-admin callers), Admin bypasses.
type QueryGate struct {
	Rules *perms.RuleDoc
	Admin bool
}

// collectEtypes walks a raw InstaQL body and returns every etype name at any
// nesting level (the "$" options key excluded).
func collectEtypes(rawQ json.RawMessage) []string {
	var q map[string]any
	if err := json.Unmarshal(rawQ, &q); err != nil {
		return nil
	}
	var out []string
	var walk func(map[string]any)
	seen := map[string]bool{}
	walk = func(m map[string]any) {
		for k, v := range m {
			if k == "$" || seen[k] {
				continue
			}
			seen[k] = true
			out = append(out, k)
			if child, ok := v.(map[string]any); ok {
				walk(child)
			}
		}
	}
	walk(q)
	return out
}

// gateQuery enforces view rules for a would-be subscription. Returns:
//   - err non-nil → dynamic view rule and caller is not admin (reject).
//
// Closed etypes are allowed through with an empty-result gate: refreshes
// render [] via instaql's Executor, so live updates never leak denied data.
func gateQuery(doc *perms.RuleDoc, rawQ json.RawMessage) error {
	for _, etype := range collectEtypes(rawQ) {
		if perms.ViewGate(doc, etype) == perms.ViewDynamic {
			return &instaql.ErrRuleFilterUnsupported{Etype: etype}
		}
	}
	return nil
}

func (m *Manager) rulesFor(ctx context.Context, appID string) *perms.RuleDoc {
	if m.Deps.Rules == nil {
		return nil
	}
	doc, err := m.Deps.Rules(ctx, appID)
	if err != nil {
		m.logger().Warn("sync: rules load failed; default-open", "app", appID, "err", err)
		return nil
	}
	return doc
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
		subID, _ := f.String("client-event-id")
		_ = subID
		qid, _ := f.String("q")
		_ = qid
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
	eventID, _ := f.String("client-event-id")

	class := wireNodelist
	if sess.TreeResults {
		class = wireTree
	}
	key := groupKey(sess.AppID, class, rawQ, sess.Admin)

	// Same session re-adding its own query → v1 add-query-exists.
	sess.mu.Lock()
	dup := sess.Subs[key]
	sess.mu.Unlock()
	if dup {
		fr := Frame{"op": json.RawMessage(`"add-query-exists"`)}
		if eventID != "" {
			fr["client-event-id"] = json.RawMessage(mustJSON(eventID))
		}
		return []Frame{fr}, nil
	}

	// Compile topics + catalog BEFORE touching the registry (same order as
	// the pre-group code; failures never create groups).
	topics, err := m.topicsFor(ctx, sess.AppID, rawQ)
	if err != nil {
		return []Frame{ErrFrame(400, "invalid-query", err.Error())}, nil
	}
	cat, err := m.Deps.Catalogs.For(ctx, sess.AppID)
	if err != nil {
		return []Frame{ErrFrame(404, "unknown-app", "no such app")}, nil
	}

	// View-rule gate: dynamic rules cannot be honored on shared subscriptions
	// (no rule-where pushdown yet) — refuse them for non-admin callers rather
	// than leak. Closed rules proceed with an empty-result gate.
	doc := m.rulesFor(ctx, sess.AppID)
	if !sess.Admin {
		if gerr := gateQuery(doc, rawQ); gerr != nil {
			return []Frame{ErrFrame(400, "invalid-query", gerr.Error())}, nil
		}
	}

	// Attach (creates the shared subscription on first use). Cap breaches
	// reject with the exact 429 protocol frames and close the connection.
	if _, aerr := m.attachGroup(sess, rawQ, topics, cat, class, doc); aerr != nil {
		var capErr *reactive.SubLimitError
		if errors.As(aerr, &capErr) {
			return []Frame{ErrFrame(429, "subscription-limit",
					fmt.Sprintf("per-app subscription cap (%d) exceeded", capErr.Max))},
				fmt.Errorf("%w: %v", ErrCloseSession, aerr)
		}
		return []Frame{ErrFrame(500, "internal", aerr.Error())}, aerr
	}

	reply := Frame{"op": json.RawMessage(`"add-query-ok"`)}
	if eventID != "" {
		reply["client-event-id"] = json.RawMessage(mustJSON(eventID))
	}
	return []Frame{reply}, nil
}

func (m *Manager) handleTransact(ctx context.Context, sess *Session, f Frame) ([]Frame, error) {
	// WS frames carry no trace context; each op is a fresh root span. The
	// span covers parse→commit→notify so a waterfall shows the full chain.
	ctx, span := tracing.Tracer.Start(ctx, "transact.ws")
	defer span.End()
	rawSteps, ok := f["tx-steps"]
	if !ok {
		return []Frame{ErrFrame(400, "bad-request", "transact requires tx-steps")}, nil
	}
	// Per-app write budget: shed before parsing/DB work.
	if m.Deps.Limiter != nil {
		if ok2, retry := m.Deps.Limiter.Allow(sess.AppID, "transact"); !ok2 {
			return []Frame{ErrFrame(429, "rate-limited",
				fmt.Sprintf("rate limited; retry after %s", retry.Round(time.Millisecond)))}, nil
		}
	}
	// Shed before doing any work: an overloaded drain loop must shed
	// publishers at the gate, not after they burned a Postgres txn
	// (docs/09-tier2-architecture.md §T2.1).
	if m.Deps.TransactGate != nil {
		if gerr := m.Deps.TransactGate(sess.AppID); gerr != nil {
			var shed *reactive.ShedError
			hint := "server busy"
			if errors.As(gerr, &shed) && shed.RetryAfter > 0 {
				hint = fmt.Sprintf("server busy; retry after %s", shed.RetryAfter)
			}
			return []Frame{ErrFrame(429, "shed", hint)}, nil
		}
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
	doc := m.rulesFor(ctx, sess.AppID)
	opts := transact.Options{
		Admin:    sess.Admin,
		AuthUser: sess.AuthUser,
	}
	started := time.Now()
	res, err := transact.Transact(ctx, m.Deps.DB, cat, appID, parsed, opts, doc)
	metrics.TransactDuration.WithLabelValues("ws").Observe(time.Since(started).Seconds())
	if err != nil {
		return []Frame{ErrFrame(403, "transact-error", err.Error())}, nil
	}
	if res.AttrsChanged && m.Deps.Catalogs != nil {
		m.Deps.Catalogs.Invalidate(sess.AppID)
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
		// Change-routed path (docs/09 §T2.5): fully-resolved plain triple
		// writes carry entity identity so the incremental engine can splice;
		// anything else keeps the topic-wide Notify semantics.
		if m.Deps.OnCommitChanges != nil {
			if triples, ok := transact.ResolveTriples(parsed, cat); ok && len(triples) > 0 {
				changes := make([]reactive.Change, 0, len(triples))
				for _, tt := range triples {
					changes = append(changes, reactive.Change{
						Etype:    tt.Etype,
						EntityID: tt.EntityID,
						AttrIDs:  []string{tt.AttrID},
					})
				}
				m.Deps.OnCommitChanges(ctx, sess.AppID, changes, res.TxID, res.AttrsChanged)
			} else {
				m.Deps.OnCommit(ctx, sess.AppID, attrIDs, res.TxID, res.AttrsChanged)
			}
		} else {
			m.Deps.OnCommit(ctx, sess.AppID, attrIDs, res.TxID, res.AttrsChanged)
		}
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

var _ = perms.Bindings{}
