package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"

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

	live connRegistry // live conns for graceful drain
}

func (h *WSHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns:  []string{"*"},
		CompressionMode: websocket.CompressionDisabled,
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
		_, data, err := conn.Read(ctx)
		if err != nil {
			// Tear down this session's group memberships and rooms.
			if sess != nil {
				h.Manager.DetachAll(sess)
				h.Manager.Deps.Rooms.LeaveAll(sess)
			}
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
				key := groupKey(sess.AppID, class, rawQ)
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
