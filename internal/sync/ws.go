package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/instant-v2/instant-v2/internal/platform"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/metrics"
	"github.com/instant-v2/instant-v2/internal/reactive"
)

// wsWriteTimeout bounds one fan-out/direct socket write (H-01g): a wedged peer must not stall its group or delay revocation healing indefinitely. Dispatch holds no process-wide lock across I/O; on timeout failMember detaches+closes so siblings and other groups progress. 10s covers large refresh envelopes on slow links without masking wedged peers as healthy.
//
// Since RT-004 every session also owns a bounded outbound queue drained by
// one writer: a wedged peer fills its own queue (shedding via overflow)
// instead of ever occupying the shared fan-out loop, so this timeout now
// fires only for a genuinely stalled socket, never as group head-of-line
// blocking.
const wsWriteTimeout = 10 * time.Second

// wsSendQueueCap bounds one session's outbound queue (RT-004). It reuses the
// SSE event-buffer capacity (sse.go getSSE, admin_sse.go AdminSubscribe):
// the same backpressure envelope across transports, so a WS session holds at
// most 128 unsent frames — bounded memory per slow reader — before the
// overflow policy sheds it. Proven by the SSE overflow tests; no second
// tuning knob.
const wsSendQueueCap = 128

// errWSBackpressure reports a full per-session outbound queue (RT-004). Group
// dispatch treats it exactly like SSE backpressure: detach-and-continue so
// siblings certify the generation while the slow member reconnects.
var errWSBackpressure = errors.New("sync: ws outbound queue full")

// wsEvent is one queued outbound frame plus its optional generation guard
// (RT-004, mirroring sseEvent): fan-out envelopes and guarded answers carry
// the stamped epoch and exact subscription; the single writer holds a
// generation lease through the bounded socket write, dropping superseded or
// cancelled events (RT-001f). Control replies (errors, handshake) carry no
// guard and always send. Payloads are pre-encoded at enqueue time so an
// encode failure surfaces synchronously to the enqueuer (RT-002b) and the
// writer never emits partial or empty bytes (RT-002e).
type wsEvent struct {
	raw    []byte
	hasGen bool
	gen    uint64
	sub    *reactive.Subscription
	// flushed, when set, marks a flush barrier: the writer closes it once
	// every event enqueued before it has been written (or dropped).
	flushed chan struct{}
}

// runWSWriter drains queue with exactly one socket writer for the session
// (RT-004): every frame for the connection — replies, errors, refresh, delta —
// passes through here, so per-session frame order is the enqueue order.
// Guarded events validate their generation lease through the bounded write
// and flush, dropping superseded envelopes exactly like writeSSEEvent.
//
// It returns (exits) when ctx ends, when the session overflows (after issuing
// the explicit 1013 shed), or when a socket write fails (after breaking the
// read loop so deferred teardown runs). The queue is never closed: in-flight
// dispatches may enqueue after ServeHTTP returns, and a send on a closed
// channel would panic — late events are simply never read.
func runWSWriter(
	ctx context.Context,
	conn *websocket.Conn,
	writeMu *sync.Mutex,
	queue <-chan wsEvent,
	overflow <-chan struct{},
	cancelRead context.CancelFunc,
	exited chan<- struct{},
) {
	defer close(exited)
	write := func(b []byte) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		// Bounded write (RT-001g/H-01g): keep the bound on the actual
		// socket write. No process-wide lock is held across this I/O
		// (dispatch stays outside groupsMu); on timeout failMember
		// detaches+closes so siblings and other groups progress.
		wctx, cancel := context.WithTimeout(ctx, wsWriteTimeout)
		defer cancel()
		return conn.Write(wctx, websocket.MessageText, b)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-overflow:
			// R3: explicit shed — the client reconnects from a full
			// snapshot. failMember concurrently issues the same status
			// through sess.Close, so either path observes 1013.
			_ = conn.Close(websocket.StatusTryAgainLater, "try again later")
			return
		case ev := <-queue:
			if ev.flushed != nil {
				close(ev.flushed)
				continue
			}
			if ev.hasGen {
				if ev.sub == nil {
					// Programmer error: fail closed, end the session.
					cancelRead()
					return
				}
				// RT-001f: a superseded/cancelled generation is dropped
				// inside WithCurrentGeneration; keep draining either way.
				if _, err := ev.sub.WithCurrentGeneration(ev.gen, func() error {
					return write(ev.raw)
				}); err != nil {
					// RT-002c: break the read loop so deferred
					// teardown runs and the client reconnects from a
					// full snapshot.
					cancelRead()
					return
				}
				continue
			}
			if err := write(ev.raw); err != nil {
				cancelRead()
				return
			}
		}
	}
}

// WSHandler upgrades and serves /runtime/session connections.
type WSHandler struct {
	Manager *Manager
	Store   *reactive.Store
	// Refresh compiles+runs a subscription query; wired from instaql in assembly.
	Refresh func(ctx context.Context, sub *reactive.Subscription) (json.RawMessage, error)
	// Compression selects permessage-deflate handling
	// (docs/reference/09-tier2-architecture.md §T2.2): "" | "disabled" |
	// "no-context-takeover" | "context-takeover". Empty/disabled keeps the
	// historical byte-exact behavior; takeover mode persists the LZ77 window
	// across frames, which is where large refresh envelopes win.
	Compression string
	// MaxConns caps live websocket connections; <= 0 means 20000. Excess
	// connections are refused with 1013 (Try Again Later) before any
	// session work happens.
	MaxConns int
	// PingInterval is the keepalive cadence; <= 0 means 30s. A ping that
	// fails — or hangs — for this long marks the connection dead and tears
	// the session down through the normal read-error path.
	PingInterval time.Duration
	// ReadLimit bounds one inbound frame in bytes; <= 0 means 4 MiB. The
	// historical 64 MiB let any unauthenticated session stream 64 MiB
	// payloads at the per-app chatter rate — memory/CPU exhaustion by math,
	// not cleverness. Refresh envelopes ride the WRITE direction and are
	// unaffected by this bound.
	ReadLimit int64
	// AllowedOrigins are websocket Accept OriginPatterns; nil/empty or a
	// lone "*" accepts every origin (v1 parity). Set explicit origins via
	// INSTANT_V2_WS_ALLOWED_ORIGINS to harden against cross-site WebSocket
	// hijacking, especially once cookie auth exists.
	AllowedOrigins []string

	live connRegistry // live conns for graceful drain
}

func (h *WSHandler) originPatterns() []string {
	pats := make([]string, 0, len(h.AllowedOrigins))
	for _, p := range h.AllowedOrigins {
		if p = strings.TrimSpace(p); p != "" {
			pats = append(pats, p)
		}
	}
	if len(pats) == 0 {
		return []string{"*"} // v1 parity default
	}
	return pats
}

func (h *WSHandler) maxConns() int {
	if h.MaxConns > 0 {
		return h.MaxConns
	}
	return 20000
}

func (h *WSHandler) pingInterval() time.Duration {
	if h.PingInterval > 0 {
		return h.PingInterval
	}
	return 30 * time.Second
}

// runKeepalive pings until ctx ends, invoking onDead exactly once when a
// ping fails or hangs longer than one interval. Extracted from ServeHTTP so
// the failure modes it exists for — write error, hung write path against a
// half-open peer — are unit-testable without fragile network simulation.
func (h *WSHandler) runKeepalive(ctx context.Context, write func(context.Context) error, onDead func()) {
	interval := h.pingInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		wctx, cancel := context.WithTimeout(ctx, interval)
		err := write(wctx)
		cancel()
		if err != nil {
			onDead()
			return
		}
	}
}

func compressionMode(cfg string) websocket.CompressionMode {
	switch cfg {
	case "no-context-takeover":
		return websocket.CompressionNoContextTakeover
	case "context-takeover":
		return websocket.CompressionContextTakeover
	default:
		return websocket.CompressionDisabled
	}
}

// ConnCount reports live websocket connections (scrape-time gauge source).
func (h *WSHandler) ConnCount() int { return h.live.len() }

func (h *WSHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.live.len() >= h.maxConns() {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: h.originPatterns()})
		if err == nil {
			_ = conn.Close(websocket.StatusTryAgainLater, "connection limit reached")
		}
		return
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns:  h.originPatterns(),
		CompressionMode: compressionMode(h.Compression),
	})
	if err != nil {
		return
	}
	// Inbound frames are bounded (audit H3): init/add-query/transact
	// payloads are KBs; refresh-ok results ride the write direction.
	// Configurable via INSTANT_V2_MAX_FRAME_BYTES.
	readLimit := h.ReadLimit
	if readLimit <= 0 {
		readLimit = 4 << 20
	}
	conn.SetReadLimit(readLimit)
	h.live.add(conn)
	defer h.live.remove(conn)
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx := r.Context()
	var (
		sess    *Session
		writeMu sync.Mutex
		sessID  atomic.Value // string; set once at init, read on overflow
	)
	// Per-session outbound queue (RT-004, mirroring SSE): fan-out never
	// blocks on a slow reader — it enqueues into this bounded channel and
	// the single writer below drains it. A full queue sheds the session
	// (overflow) instead of stalling its group.
	queue := make(chan wsEvent, wsSendQueueCap)
	overflow := make(chan struct{}, 1)
	var overflowOnce sync.Once
	signalOverflow := func() {
		overflowOnce.Do(func() {
			sid, _ := sessID.Load().(string)
			if sid == "" {
				sid = "pre-init"
			}
			h.Manager.logger().Warn("sync: ws outbound queue overflow; closing session", "session", sid)
			metrics.WSOverflowCloses.Inc()
			select {
			case overflow <- struct{}{}:
			default:
			}
		})
	}
	enqueue := func(ev wsEvent) error {
		select {
		case queue <- ev:
			return nil
		default:
			// R3: do not block the fan-out — shed this session so the
			// client reconnects from a full snapshot.
			signalOverflow()
			return errWSBackpressure
		}
	}
	// Session teardown must happen on EVERY exit path once a session exists
	// (send failures after add-query attached groups, close-session,
	// enrichment errors), not only on read errors — leaked memberships keep
	// subscriptions refreshing, presence ghosts alive, and app-cap slots
	// consumed forever. The deferred hook below is idempotent-safe: DetachAll
	// and LeaveAll are no-ops on empty state.
	defer func() {
		if sess != nil {
			h.Manager.DetachAll(sess)
			h.Manager.Deps.Rooms.LeaveAll(sess)
		}
	}()

	// Keepalive: half-open TCP connections (laptop sleep, NAT drop, SIGKILLed
	// client) never surface as Read errors, so sessions would otherwise
	// linger as ghosts until process restart. Ping periodically; a failed
	// or hung ping cancels the read context, which surfaces as a Read error
	// and runs the normal teardown path.
	pingCtx, cancelPing := context.WithCancel(ctx)
	defer cancelPing()
	readCtx, cancelRead := context.WithCancel(ctx)
	defer cancelRead()
	writerExited := make(chan struct{})
	go runWSWriter(ctx, conn, &writeMu, queue, overflow, cancelRead, writerExited)
	// Ping deliberately does not take writeMu: coder/websocket serializes
	// frame writes itself, and Ping blocks until a Read processes the pong.
	// Holding writeMu across that wait deadlocked a session whose read loop
	// was waiting on writeMu, and stalled the notifier's fan-out behind it
	// (QR-001 soak goroutine dump, 2026-09-26).
	go h.runKeepalive(pingCtx, conn.Ping, cancelRead)
	send := func(f Frame) error {
		b, err := f.Encode()
		if err != nil {
			return err
		}
		return enqueue(wsEvent{raw: b})
	}
	sendErr := func(status int, typ, msg string) {
		_ = send(ErrFrame(status, typ, msg))
	}
	// flushOutbound waits (bounded by wsWriteTimeout) for the single writer
	// to drain already-enqueued frames. Terminal reply paths close right
	// after enqueueing the client's last frame; without this the deferred
	// conn.Close could win the race and drop it. A wedged socket bounds the
	// wait exactly like the old synchronous write; an overflow shed or a
	// cancelled context ends it immediately (that close is already on its
	// way).
	flushOutbound := func() {
		// A queue that looks empty may still have its last frame in the
		// writer's hands, so wait for a barrier the writer releases only
		// after everything enqueued before it was written or dropped.
		// Bounded exactly like the old synchronous write.
		done := make(chan struct{})
		if enqueue(wsEvent{flushed: done}) != nil {
			return // overflow shed: that close is already on its way
		}
		t := time.NewTimer(wsWriteTimeout)
		defer t.Stop()
		select {
		case <-done:
		case <-writerExited:
		case <-ctx.Done():
		case <-t.C:
		}
	}
	// sendErrClose delivers a terminal error frame before tearing down,
	// preserving the old synchronous guarantee that the client observes its
	// last frame ahead of the close.
	sendErrClose := func(status int, typ, msg string) {
		sendErr(status, typ, msg)
		flushOutbound()
	}

	for {
		_, data, err := conn.Read(readCtx)
		if err != nil {
			// Teardown (memberships + rooms) runs via the deferred hook.
			return
		}
		f, err := ParseFrame(data)
		if err != nil {
			sendErr(400, "bad-frame", err.Error())
			continue
		}
		op, _ := f.GetOp()
		if op == "init" {
			s, reply, err := h.Manager.HandleInit(ctx, f)
			if err != nil {
				_ = send(reply)
				continue
			}
			// Audit F6: a re-init replaces the live session. Detach the old
			// one NOW so its subscriptions/rooms don't linger until socket
			// close — the deferred teardown only sees the latest pointer.
			if sess != nil && s != sess {
				h.Manager.DetachAll(sess)
				h.Manager.Deps.Rooms.LeaveAll(sess)
			}
			sess = s

			sess.Send = send
			sess.SendRaw = func(b []byte) error {
				return enqueue(wsEvent{raw: b})
			}
			// Generation-aware fan-out writer (RT-001f/RT-004): the epoch
			// travels with the bytes so the single writer drops superseded
			// envelopes instead of serving them post-revoke — the same
			// dequeue-drop contract SSE already has. Dispatch treats a full
			// queue exactly like SSE backpressure (detach-and-continue).
			sess.SendRawGen = func(b []byte, gen uint64, sub *reactive.Subscription) error {
				return enqueue(wsEvent{raw: b, hasGen: true, gen: gen, sub: sub})
			}
			// Generation-aware Frame writer for the guarded initial answer
			// (RT-001d): same dequeue-drop contract as SendRawGen.
			sess.SendGen = func(f Frame, gen uint64, sub *reactive.Subscription) error {
				b, err := f.Encode()
				if err != nil {
					return err
				}
				return enqueue(wsEvent{raw: b, hasGen: true, gen: gen, sub: sub})
			}
			// RT-002c/R3: group dispatch calls this after detaching a member
			// whose send failed, so the client reconnects and re-establishes
			// from a full snapshot. 1013 (Try Again Later) is the explicit
			// shed signal — the same status as the MaxConns refusal and the
			// writer's own overflow close, so every shed path observes it.
			sess.Close = func() {
				_ = conn.Close(websocket.StatusTryAgainLater, "try again later")
			}
			sessID.Store(s.ID)
			if err := send(reply); err != nil {
				return
			}
			continue
		}
		if sess == nil {
			sendErr(401, "not-initialized", "send init first")
			continue
		}
		// Per-app frame budget (session chatter): shed before dispatch.
		// Per-app frame budget; transacts are budgeted inside handleTransact
		// with its own retry hint instead.
		if h.Manager.Deps.Limiter != nil && op != "transact" {
			if ok2, retry := h.Manager.Deps.Limiter.Allow(sess.AppID, "ws"); !ok2 {
				sendErr(429, "rate-limited", fmt.Sprintf("rate limited; retry after %s", retry.Round(time.Millisecond)))
				continue
			}
		}

		replies, err := h.Manager.Handle(ctx, sess, f)
		closing := errors.Is(err, ErrCloseSession)
		if err != nil && !closing && len(replies) == 0 {
			sendErr(500, "internal", platform.ClientMessage(err))
			continue
		}
		// add-query: v1 rides the INITIAL ANSWER on the add-query-ok ack itself
		// (session.clj:264-270 sends q/result/result-meta/processed-*), with no
		// follow-up refresh-ok. Enrich the ack in place before sending.
		if op == "add-query" && len(replies) > 0 {
			if rop, _ := replies[0].GetOp(); rop == "add-query-ok" {
				rawQ, _ := f["q"]
				class := wireNodelist
				if sess.TreeResults {
					class = wireTree
				}
				key := groupKey(sess.AppID, class, rawQ, sess.Admin)
				sub, ok := h.Store.Get(key)
				if !ok || h.Refresh == nil {
					sendErrClose(500, "internal", "subscription missing after add-query")
					return
				}
				_, rerr := h.Manager.snapshotOrRefresh(ctx, h.Refresh, sub)
				if rerr != nil {
					sendErrClose(400, "invalid-query", platform.ClientMessage(rerr))
					return
				}
				// Post-render pre-send guard (RT-001d): a re-gate landing after the flight but before this ack hits the wire must not be served as an authorized new answer. Capture the epoch after the flight; recheck after render before enriching/sending.
				genAfter := sub.Gen.Load()
				// Baseline for delta-refresh diffs is always the flat envelope;
				// the wire gets the v1 node-list. snapshotOrRefresh seeded the
				// baseline on the refresh path; on the reuse path it is already
				// current and must NOT be overwritten with a stale-local copy.
				// RT-002a/RT-002d: the ack carries the served snapshot and
				// its watermark as one pair, so a (re)subscriber
				// establishes continuity from exactly this state —
				// including when siblings kept the group (and its
				// snapshot) alive across the reconnect.
				// Fail-closed (H-01d S1): a successful flight guarantees a
				// non-nil snapshot; nil here means a concurrent re-gate cleared
				// it after the flight published (swap landed between publish
				// and genAfter). Serving result (stale-allow) would be a new
				// authorized answer after observed deny — close to force
				// reconnect under the new gate instead.
				snap, snapTx := sub.SnapshotPair()
				if snap == nil {
					sendErrClose(400, "invalid-query", "subscription re-gated during subscribe")
					return
				}
				nodes, nerr := nodelistFor(ctx, h.Manager.Deps.Catalogs, sess.AppID, snap)
				if nerr != nil {
					sendErrClose(500, "internal", nerr.Error())
					return
				}
				if sub.Gen.Load() != genAfter {
					// Revoked during render: fail closed (close forces reconnect under the new gate) rather than serve superseded bytes as a new answer.
					sendErrClose(400, "invalid-query", "subscription re-gated during subscribe")
					return
				}
				replies[0]["q"] = json.RawMessage(rawQ)
				replies[0]["result"] = nodes
				replies[0]["result-meta"] = resultMetaOf(snap)
				replies[0]["processed-tx-id"] = json.RawMessage(mustJSON(snapTx))
				replies[0]["processed-isn"] = json.RawMessage(mustJSON(0))
				// RT-001d/RT-004: the ack is a NEW authorized answer, so it
				// rides the guarded writer — a re-gate landing between the
				// check above and the socket write drops it instead of
				// serving stale-allow bytes (mirrors SSE SendGen). Ack only;
				// no follow-up refresh-ok.
				if serr := sess.SendGen(replies[0], genAfter, sub); serr != nil {
					return
				}
				replies = replies[:0]
			}
		}
		for _, rf := range replies {
			if serr := send(rf); serr != nil {
				return
			}
		}
		if closing {
			// e.g. subscription-cap breach: the error frame above went out;
			// flush it before tearing the connection down (deferred Close
			// runs on return) so the client observes its last frame.
			flushOutbound()
			return
		}
	}
}

var _ = fmt.Sprintf
var _ = instaql.Executor{}
