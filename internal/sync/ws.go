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
	"time"

	"github.com/coder/websocket"

	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/reactive"
)

// WSHandler upgrades and serves /runtime/session connections.
type WSHandler struct {
	Manager *Manager
	Store   *reactive.Store
	// Refresh compiles+runs a subscription query; wired from instaql in assembly.
	Refresh func(ctx context.Context, sub *reactive.Subscription) (json.RawMessage, error)
	// Compression selects permessage-deflate handling
	// (docs/09-tier2-architecture.md §T2.2): "" | "disabled" |
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
	)
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
	go h.runKeepalive(pingCtx, func(wctx context.Context) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return conn.Ping(wctx)
	}, cancelRead)
	send := func(f Frame) error {
		b, err := f.Encode()
		if err != nil {
			return err
		}
		writeMu.Lock()
		defer writeMu.Unlock()
		return conn.Write(ctx, websocket.MessageText, b)
	}
	sendErr := func(status int, typ, msg string) {
		_ = send(ErrFrame(status, typ, msg))
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
				writeMu.Lock()
				defer writeMu.Unlock()
				return conn.Write(ctx, websocket.MessageText, b)
			}
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
					sendErr(500, "internal", "subscription missing after add-query")
					return
				}
				result, rerr := h.Manager.snapshotOrRefresh(ctx, h.Refresh, sub)
				if rerr != nil {
					sendErr(400, "invalid-query", platform.ClientMessage(rerr))
					return
				}
				// Baseline for delta-refresh diffs is always the flat envelope;
				// the wire gets the v1 node-list. snapshotOrRefresh seeded the
				// baseline on the refresh path; on the reuse path it is already
				// current and must NOT be overwritten with a stale-local copy.
				nodes, nerr := nodelistFor(ctx, h.Manager.Deps.Catalogs, sess.AppID, result)
				if nerr != nil {
					sendErr(500, "internal", nerr.Error())
					return
				}
				replies[0]["q"] = json.RawMessage(rawQ)
				replies[0]["result"] = nodes
				replies[0]["result-meta"] = resultMetaOf(result)
				replies[0]["processed-tx-id"] = json.RawMessage(mustJSON(0))
				replies[0]["processed-isn"] = json.RawMessage(mustJSON(0))
				replies = replies[:1] // ack only; no follow-up refresh-ok
			}
		}
		for _, rf := range replies {
			if serr := send(rf); serr != nil {
				return
			}
		}
		if closing {
			// e.g. subscription-cap breach: the error frame above went out;
			// tear the connection down (deferred Close runs on return).
			return
		}
	}
}

var _ = fmt.Sprintf
var _ = instaql.Executor{}
