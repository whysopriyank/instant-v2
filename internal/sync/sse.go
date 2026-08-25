package sync

// SSE fallback transport mirroring v1's /runtime/sse pair
// (runtime/routes.clj L57-69 @ a4d2ef33):
//
//	GET /runtime/sse?app_id=<uuid>
//	  → text/event-stream; first event carries
//	    {op:"init-ok", machine-id, session-id, sse-token}
//	POST /runtime/sse   body {machine_id, session_id, sse_token, messages:[frame…]}
//	  → each message runs the SAME Manager.Handle path as WebSocket frames;
//	    replies are pushed onto the open SSE stream. Auth = sha256(sse_token)
//	    bound to the live connection.
//
// No duplicate session state: sessions are ordinary Manager sessions whose
// Send closure writes SSE events instead of websocket frames.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/ratelimit"
	"github.com/instant-v2/instant-v2/internal/reactive"
)

type sseConn struct {
	sess      *Session
	events    chan Frame
	sessionID string
	// msgMu serializes POST handling per stream: two overlapping POSTs with
	// the same sse_token would otherwise race conn.sess materialization and
	// Session map mutations (concurrent map writes = process-fatal).
	msgMu sync.Mutex
	// initialized flips true after a successful protocol `init` op; every
	// other op is refused before that (mirrors the WS loop guard).
	initialized bool
}

// SSEHandler serves the /runtime/sse pair. Wire Manager/Store/Refresh exactly
// as for WSHandler — same reactive pipeline, no second implementation.
type SSEHandler struct {
	Manager *Manager
	Store   *reactive.Store
	Refresh func(ctx context.Context, sub *reactive.Subscription) (json.RawMessage, error)
	// AdminAuth gates POST /admin/subscribe-query; cmd injects
	// CatalogCache.CheckAdminToken. nil disables the endpoint.
	AdminAuth func(ctx context.Context, appID, token string) bool
	// MaxConns caps concurrent SSE streams; <= 0 means 10000. Excess GETs
	// answer 503 before any handshake state is allocated.
	MaxConns int
	// MaxConnsPerIP caps streams per client IP; <= 0 means 100. The global
	// cap alone let one host squat every slot indefinitely (audit H5).
	MaxConnsPerIP int
	// HeartbeatEvery is the SSE comment-ping cadence; <= 0 means 20s. Each
	// write carries a deadline of one heartbeat interval, so a dead reader
	// frees its slot within ~1 interval instead of forever.
	HeartbeatEvery time.Duration
	// Limiter charges per-message chatter on the POST batch path (same ws
	// class as the WS loop); nil disables charging (tests).
	Limiter *ratelimit.Limiter

	mu     sync.Mutex
	conns  map[string]*sseConn // sha256hex(sseToken) → conn
	perIP  map[string]int      // client IP → live stream count
	bootID string
}

func (h *SSEHandler) maxConns() int {
	if h.MaxConns > 0 {
		return h.MaxConns
	}
	return 10000
}

func (h *SSEHandler) maxConnsPerIP() int {
	if h.MaxConnsPerIP > 0 {
		return h.MaxConnsPerIP
	}
	return 100
}

func (h *SSEHandler) heartbeatEvery() time.Duration {
	if h.HeartbeatEvery > 0 {
		return h.HeartbeatEvery
	}
	return 20 * time.Second
}

func (h *SSEHandler) conn(tokenHash string) (*sseConn, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	c, ok := h.conns[tokenHash]
	return c, ok
}

// ConnCount reports live SSE connections (scrape-time gauge source).
func (h *SSEHandler) ConnCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.conns)
}

func tokenHashOf(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

// ServeHTTP routes both methods.
func (h *SSEHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.getSSE(w, r)
	case http.MethodPost:
		h.postSSE(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *SSEHandler) getSSE(w http.ResponseWriter, r *http.Request) {
	appID := r.URL.Query().Get("app_id")
	if appID == "" {
		appID = r.URL.Query().Get("app-id")
	}
	if _, err := platform.ScanUUIDErr(appID); err != nil {
		http.Error(w, "missing app_id", 400)
		return
	}
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", 500)
		return
	}
	if h.ConnCount() >= h.maxConns() {
		http.Error(w, "sse connection limit reached", http.StatusServiceUnavailable)
		return
	}
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		ip = r.RemoteAddr
	}
	// Per-IP cap (audit H5): one host must not be able to squat every slot.
	h.mu.Lock()
	if h.perIP == nil {
		h.perIP = map[string]int{}
	}
	if h.perIP[ip] >= h.maxConnsPerIP() {
		h.mu.Unlock()
		http.Error(w, "sse connection limit reached for client", http.StatusServiceUnavailable)
		return
	}
	h.perIP[ip]++
	sseToken := randSSEToken()
	sessionID := newSSESessID()
	hash := tokenHashOf(sseToken)
	conn := &sseConn{
		sessionID: sessionID,
		events:    make(chan Frame, 128),
	}
	if h.conns == nil {
		h.conns = map[string]*sseConn{}
	}
	h.conns[hash] = conn
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		delete(h.conns, hash)
		h.perIP[ip]--
		if h.perIP[ip] <= 0 {
			delete(h.perIP, ip)
		}
		h.mu.Unlock()
		if conn.sess != nil {
			h.Manager.DetachAll(conn.sess)
			h.Manager.Deps.Rooms.LeaveAll(conn.sess)
		}
	}()

	rc := http.NewResponseController(w)
	writeDeadline := func() bool {
		_ = rc.SetWriteDeadline(time.Now().Add(h.heartbeatEvery()))
		return true
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	writeDeadline()
	w.WriteHeader(http.StatusOK)

	flushEvent := func(b []byte) bool {
		if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
			return false
		}
		fl.Flush()
		return true
	}
	writeEvent := func(f Frame) bool {
		b, err := f.Encode()
		if err != nil {
			return true // skip malformed; keep stream alive
		}
		return flushEvent(b)
	}

	// First event mirrors v1's handle-sse-init!: transport handshake only —
	// the protocol-level `init` op still arrives via POST messages.
	writeDeadline()
	_ = writeEvent(Frame{
		"op":         json.RawMessage(`"init-ok"`),
		"machine-id": json.RawMessage(mustJSON(h.machineID())),
		"session-id": json.RawMessage(mustJSON(sessionID)),
		"sse-token":  json.RawMessage(mustJSON(sseToken)),
	})

	// Heartbeat comments (audit H5): dead readers trip the write deadline
	// within one interval, freeing the slot; live proxies stay warm.
	hb := time.NewTicker(h.heartbeatEvery())
	defer hb.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-hb.C:
			writeDeadline()
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
			fl.Flush()
		case f := <-conn.events:
			if raw, isRaw := f["__raw"]; isRaw {
				writeDeadline()
				if _, err := fmt.Fprintf(w, "data: %s\n\n", raw); err != nil {
					return
				}
				fl.Flush()
				continue
			}
			writeDeadline()
			if !writeEvent(f) {
				return
			}
		}
	}
}

func (h *SSEHandler) postSSE(w http.ResponseWriter, r *http.Request) {
	var req struct {
		MachineID string            `json:"machine_id"`
		AppID     string            `json:"app_id"`
		SessionID string            `json:"session_id"`
		SSEToken  string            `json:"sse_token"`
		Messages  []json.RawMessage `json:"messages"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.SSEToken == "" {
		http.Error(w, "bad request", 400)
		return
	}
	conn, ok := h.conn(tokenHashOf(req.SSEToken))
	if !ok || (req.SessionID != "" && req.SessionID != conn.sessionID) {
		http.Error(w, "unknown sse session", http.StatusUnauthorized)
		return
	}
	// One writer at a time per stream (see sseConn.msgMu).
	conn.msgMu.Lock()
	defer conn.msgMu.Unlock()
	if conn.sess == nil {
		// Lazily materialize a Session bound to this stream; it carries the
		// Send closures until the protocol `init` op replaces it wholesale
		// and marks the stream initialized. Ops before init are refused.
		conn.sess = &Session{
			ID:       conn.sessionID,
			AppID:    req.AppID,
			Features: Features{},
			Subs:     map[string]bool{},
			Rooms:    map[string]bool{},
			Send: func(f Frame) error {
				select {
				case conn.events <- f:
					return nil
				default:
					return errSSEBackpressure
				}
			},
			SendRaw: func(b []byte) error {
				select {
				case conn.events <- Frame{"__raw": json.RawMessage(b)}:
					return nil
				default:
					return errSSEBackpressure
				}
			},
		}
	}
	for _, raw := range req.Messages {
		f, err := ParseFrame(raw)
		if err != nil {
			pushReply(conn.events, ErrFrame(400, "bad-frame", err.Error()))
			continue
		}
		op, _ := f.GetOp()
		// Protocol guard (audit M5): mirror the WS loop — every op except
		// init is refused until the session is initialized.
		if op != "init" && !conn.initialized {
			pushReply(conn.events, ErrFrame(401, "not-initialized", "send init first"))
			continue
		}
		if op == "init" {
			sess, reply, ierr := h.Manager.HandleInit(r.Context(), f)
			if ierr != nil {
				pushReply(conn.events, reply)
				continue
			}
			sess.Send = conn.sess.Send
			conn.sess = sess
			conn.initialized = true
			pushReply(conn.events, reply)
			continue
		}
		// Per-message chatter budget (audit H5): the WS loop charges the
		// ws class for non-transact ops; the batch POST path must not be a
		// limiter bypass. Transacts stay self-limited in handleTransact.
		if h.Limiter != nil && op != "transact" {
			if aerr := h.Limiter.Acquire(r.Context(), conn.sess.AppID, ratelimit.ClassWS); aerr != nil {
				pushReply(conn.events, ErrFrame(429, "rate-limited", "rate limited"))
				break
			}
		}
		replies, herr := h.Manager.Handle(r.Context(), conn.sess, f)
		for _, rf := range replies {
			pushReply(conn.events, rf)
		}
		if errors.Is(herr, ErrCloseSession) {
			h.closeTearDown(tokenHashOf(req.SSEToken), conn)
			break
		}
		// add-query initial snapshot rides the same direct-refresh path
		// as the WS handler.
		if op == "add-query" && h.Refresh != nil {
			h.snapshot(r.Context(), conn.sess, f)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{})
}

func (h *SSEHandler) snapshot(ctx context.Context, sess *Session, f Frame) {
	rawQ, ok := f["q"]
	if !ok {
		return
	}
	class := wireNodelist
	if sess.TreeResults {
		class = wireTree
	}
	key := groupKey(sess.AppID, class, rawQ, sess.Admin)
	sub, ok := h.Store.Get(key)
	if !ok {
		return
	}
	result, err := h.Refresh(ctx, sub)
	if err != nil {
		return
	}
	// Baseline for delta-refresh diffs is the flat envelope; SSE init-query
	// answers with v1 :tree semantics (session.clj:1395) — a bare object
	// tree with NO "data" wrapper key.
	sub.SetSnapshot(result)
	tree, terr := UnwrapTree(result)
	if terr != nil {
		return
	}
	payload := computationEntry(
		[2]json.RawMessage{keyInstaqlQuery, json.RawMessage(rawQ)},
		[2]json.RawMessage{keyInstaqlResult, tree},
	)
	_ = sess.Send(Frame{
		"op":              json.RawMessage(`"refresh-ok"`),
		"computations":    payload,
		"processed-tx-id": json.RawMessage(mustJSON(0)),
	})
}

// closeTearDown drops the SSE registration and tears down the session's
// subscriptions and rooms (ErrCloseSession path).
func (h *SSEHandler) closeTearDown(tokenHash string, c *sseConn) {
	h.mu.Lock()
	delete(h.conns, tokenHash)
	h.mu.Unlock()
	if c.sess != nil {
		h.Manager.DetachAll(c.sess)
		h.Manager.Deps.Rooms.LeaveAll(c.sess)
	}
}

var errSSEBackpressure = fmt.Errorf("sse: event buffer full")

func pushReply(events chan Frame, f Frame) {
	select {
	case events <- f:
	default:
	}
}

func randSSEToken() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return formatHex16(b)
}

func newSSESessID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return "sess-" + formatHex16(b)
}

func formatHex16(u [16]byte) string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}

// machineID is a process-lifetime identifier surfaced in the SSE handshake
// (v1 sends @config/hostname; v2 uses a boot uuid).
func (h *SSEHandler) machineID() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.bootID == "" {
		h.bootID = randSSEToken()
	}
	return h.bootID
}
