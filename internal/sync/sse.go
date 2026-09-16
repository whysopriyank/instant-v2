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
	events    chan sseEvent
	sessionID string
	// overflow is signaled (non-blocking, capacity 1) whenever a reply
	// cannot fit the event buffer. The GET writer ends the stream on
	// receipt: an SSE client reconnects and re-initializes, whereas a
	// silently dropped frame strands it stale until an unrelated event
	// happens to re-dirty its subscriptions (audit backlog B1).
	overflow chan struct{}
	// msgMu serializes POST handling per stream: two overlapping POSTs with
	// the same sse_token would otherwise race conn.sess materialization and
	// Session map mutations (concurrent map writes = process-fatal).
	msgMu sync.Mutex
	// initialized flips true after a successful protocol `init` op; every
	// other op is refused before that (mirrors the WS loop guard).
	initialized bool
}

// sseEvent is one queued SSE frame plus its optional generation guard
// (RT-001f): fan-out envelopes and initial answers carry the stamped epoch
// and exact subscription; the GET writer holds a generation lease through
// the bounded write and flush, dropping superseded or cancelled events.
// Control replies (errors, handshake) carry no guard and always send.
type sseEvent struct {
	frame  Frame
	hasGen bool
	gen    uint64
	sub    *reactive.Subscription
}

var errSSEMissingSubscription = errors.New("sync: guarded SSE event missing subscription")

const sseWriteTimeout = 10 * time.Second

// writeSSEEvent renders and writes one SSE data event. Frame encoding happens
// before taking a subscription's delivery lease, while the bounded network
// write and flush stay inside it. A gate swap therefore cannot overtake a
// write that was admitted for the current generation.
func writeSSEEvent(w http.ResponseWriter, ev sseEvent) error {
	var payload []byte
	if raw, ok := ev.frame["__raw"]; ok {
		payload = raw
	} else {
		var err error
		payload, err = ev.frame.Encode()
		if err != nil {
			return err
		}
	}

	rc := http.NewResponseController(w)
	write := func(requireDeadline bool) error {
		if err := rc.SetWriteDeadline(time.Now().Add(sseWriteTimeout)); err != nil && requireDeadline {
			return err
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
			return err
		}
		return rc.Flush()
	}

	if !ev.hasGen {
		// Control replies retain compatibility with writers without the
		// optional deadline interface. Guarded data writes fail closed.
		return write(false)
	}
	if ev.sub == nil {
		return errSSEMissingSubscription
	}
	current, err := ev.sub.WithCurrentGeneration(ev.gen, func() error {
		return write(true)
	})
	if err != nil {
		return err
	}
	if !current {
		return errGenerationSuperseded
	}
	return nil
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
	// HeartbeatEvery is the SSE comment-ping cadence; <= 0 means 20s. Pings
	// carry that interval as their deadline; data events use sseWriteTimeout.
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
		events:    make(chan sseEvent, 128),
		overflow:  make(chan struct{}, 1),
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

	writeEvent := func(f Frame) bool {
		return writeSSEEvent(w, sseEvent{frame: f}) == nil
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
		case <-conn.overflow:
			// The event buffer overflowed: this stream can no longer keep
			// up with its reply pressure. End it — EventSource clients
			// reconnect with backoff and re-initialize; silently dropping
			// frames would leave them stale with no signal at all.
			return
		case ev := <-conn.events:
			if err := writeSSEEvent(w, ev); err != nil {
				// A stale/cancelled queued generation is intentionally dropped;
				// all other write failures end the stream and trigger teardown.
				if errors.Is(err, errGenerationSuperseded) {
					continue
				}
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
				case conn.events <- sseEvent{frame: f}:
					return nil
				default:
					signalOverflow(conn)
					return errSSEBackpressure
				}
			},
			SendRaw: func(b []byte) error {
				select {
				case conn.events <- sseEvent{frame: Frame{"__raw": json.RawMessage(b)}}:
					return nil
				default:
					signalOverflow(conn)
					return errSSEBackpressure
				}
			},
			SendRawGen: func(b []byte, gen uint64, sub *reactive.Subscription) error {
				select {
				case conn.events <- sseEvent{frame: Frame{"__raw": json.RawMessage(b)}, hasGen: true, gen: gen, sub: sub}:
					return nil
				default:
					signalOverflow(conn)
					return errSSEBackpressure
				}
			},
			SendGen: func(f Frame, gen uint64, sub *reactive.Subscription) error {
				select {
				case conn.events <- sseEvent{frame: f, hasGen: true, gen: gen, sub: sub}:
					return nil
				default:
					signalOverflow(conn)
					return errSSEBackpressure
				}
			},
		}
	}
	for _, raw := range req.Messages {
		f, err := ParseFrame(raw)
		if err != nil {
			pushReply(conn, ErrFrame(400, "bad-frame", err.Error()))
			continue
		}
		op, _ := f.GetOp()
		// Protocol guard (audit M5): mirror the WS loop — every op except
		// init is refused until the session is initialized.
		if op != "init" && !conn.initialized {
			pushReply(conn, ErrFrame(401, "not-initialized", "send init first"))
			continue
		}
		if op == "init" {
			sess, reply, ierr := h.Manager.HandleInit(r.Context(), f)
			if ierr != nil {
				pushReply(conn, reply)
				continue
			}
			// Audit F6 parity with the WS loop: detach the replaced session.
			if conn.sess != nil && sess != conn.sess {
				h.Manager.DetachAll(conn.sess)
				h.Manager.Deps.Rooms.LeaveAll(conn.sess)
			}
			sess.Send = conn.sess.Send
			sess.SendRaw = conn.sess.SendRaw
			sess.SendRawGen = conn.sess.SendRawGen
			sess.SendGen = conn.sess.SendGen
			conn.sess = sess
			conn.initialized = true
			pushReply(conn, reply)
			continue
		}
		// Per-message chatter budget (audit H5): the WS loop charges the
		// ws class for non-transact ops; the batch POST path must not be a
		// limiter bypass. Transacts stay self-limited in handleTransact.
		if h.Limiter != nil && op != "transact" {
			if aerr := h.Limiter.Acquire(r.Context(), conn.sess.AppID, ratelimit.ClassWS); aerr != nil {
				pushReply(conn, ErrFrame(429, "rate-limited", "rate limited"))
				break
			}
		}
		replies, herr := h.Manager.Handle(r.Context(), conn.sess, f)
		for _, rf := range replies {
			pushReply(conn, rf)
		}
		if errors.Is(herr, ErrCloseSession) {
			h.closeTearDown(tokenHashOf(req.SSEToken), conn)
			break
		}
		// add-query initial snapshot rides the same direct-refresh path
		// as the WS handler. A render failure ends the stream (after a
		// 500 reply) so the client reconnects instead of idling
		// subscribed with no state (RT-002b).
		if op == "add-query" && h.Refresh != nil {
			if serr := h.snapshot(r.Context(), conn.sess, f); serr != nil {
				pushReply(conn, ErrFrame(500, "internal", platform.ClientMessage(serr)))
				signalOverflow(conn)
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{})
}

func (h *SSEHandler) snapshot(ctx context.Context, sess *Session, f Frame) error {
	rawQ, ok := f["q"]
	if !ok {
		return nil
	}
	class := wireNodelist
	if sess.TreeResults {
		class = wireTree
	}
	key := groupKey(sess.AppID, class, rawQ, sess.Admin)
	sub, ok := h.Store.Get(key)
	if !ok {
		return nil
	}
	_, err := h.Manager.snapshotOrRefresh(ctx, h.Refresh, sub)
	if err != nil {
		// A failed initial refresh (including a rule-loader failure)
		// must end the stream via the caller's 500 path — never leave
		// the subscriber connected with no initial state.
		return err
	}
	// Post-render pre-send guard (RT-001d): a re-gate landing after the
	// flight but before this answer hits the queue must not be served as
	// an authorized new answer. Capture the epoch after the flight, render,
	// then recheck before enqueue; on mismatch fail so the caller ends the
	// stream and the client reconnects under the new gate.
	genAfter := sub.Gen.Load()
	// Baseline for delta-refresh diffs is the flat envelope; SSE init-query
	// answers with v1 :tree semantics (session.clj:1395) — a bare object
	// tree with NO "data" wrapper key. snapshotOrRefresh seeds the baseline
	// on the refresh path and reuses it on the duplicate path.
	// RT-002a/RT-002d: answer with the served pair so the watermark matches
	// the snapshot even when siblings kept the group alive.
	// Fail-closed (H-01d S1): success guarantees non-nil snapshot; nil means a
	// concurrent re-gate cleared after publish — return superseded to end the
	// stream rather than serve stale result as a new answer.
	snap, snapTx := sub.SnapshotPair()
	if snap == nil {
		return errGenerationSuperseded
	}
	tree, terr := UnwrapTree(snap)
	if terr != nil {
		// RT-002b: a joiner that cannot be rendered must not be left
		// subscribed with no state on a live stream. The caller ends the
		// stream so the client reconnects from a full snapshot.
		return terr
	}
	if sub.Gen.Load() != genAfter {
		return errGenerationSuperseded
	}
	payload := computationEntry(
		[2]json.RawMessage{keyInstaqlQuery, json.RawMessage(rawQ)},
		[2]json.RawMessage{keyInstaqlResult, tree},
	)
	fr := Frame{
		"op":              json.RawMessage(`"refresh-ok"`),
		"computations":    payload,
		"processed-tx-id": json.RawMessage(mustJSON(snapTx)),
	}
	if sess.SendGen != nil {
		return sess.SendGen(fr, genAfter, sub)
	}
	return sess.Send(fr)
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

// signalOverflow flags the stream as overloaded (idempotent — capacity 1,
// non-blocking). The GET writer observes it and closes the stream.
func signalOverflow(conn *sseConn) {
	select {
	case conn.overflow <- struct{}{}:
	default:
	}
}

// pushReply queues one reply frame; when the event buffer is full it
// signals stream overflow instead of silently dropping the frame — the
// GET writer ends the stream so the client reconnects rather than
// silently missing deliveries (audit backlog B1).
func pushReply(conn *sseConn, f Frame) {
	select {
	case conn.events <- sseEvent{frame: f}:
	default:
		signalOverflow(conn)
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
