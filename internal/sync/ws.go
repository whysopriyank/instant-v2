package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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

	live connRegistry // live conns for graceful drain
}

func (h *WSHandler) maxConns() int {
	if h.MaxConns > 0 {
		return h.MaxConns
	}
	return 20000
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
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"*"}})
		if err == nil {
			_ = conn.Close(websocket.StatusTryAgainLater, "connection limit reached")
		}
		return
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns:  []string{"*"},
		CompressionMode: compressionMode(h.Compression),
	})
	if err != nil {
		return
	}
	// refresh-ok carries full InstaQL results; large apps blow past the
	// 32 KiB default read limit before any sane bound. Writes are unbounded.
	conn.SetReadLimit(64 << 20)
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
	// ping cancels the read context, which surfaces as a Read error and runs
	// the normal teardown path.
	pingCtx, cancelPing := context.WithCancel(ctx)
	defer cancelPing()
	readCtx, cancelRead := context.WithCancel(ctx)
	defer cancelRead()
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-pingCtx.Done():
				return
			case <-ticker.C:
				writeMu.Lock()
				err := conn.Ping(pingCtx)
				writeMu.Unlock()
				if err != nil {
					cancelRead()
					return
				}
			}
		}
	}()
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
		if h.Manager.Deps.Limiter != nil {
			class := "ws"
			if op == "transact" {
				class = "transact" // enforced inside handleTransact with its own hint
			} else if ok2, retry := h.Manager.Deps.Limiter.Allow(sess.AppID, class); !ok2 {
				sendErr(429, "rate-limited", fmt.Sprintf("rate limited; retry after %s", retry.Round(time.Millisecond)))
				continue
			}
		}

		replies, err := h.Manager.Handle(ctx, sess, f)
		closing := errors.Is(err, ErrCloseSession)
		if err != nil && !closing && len(replies) == 0 {
			sendErr(500, "internal", err.Error())
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
				result, rerr := h.Refresh(ctx, sub)
				if rerr != nil {
					sendErr(400, "invalid-query", rerr.Error())
					return
				}
				// Baseline for delta-refresh diffs is always the flat envelope;
				// the wire gets the v1 node-list.
				sub.SetSnapshot(result)
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
