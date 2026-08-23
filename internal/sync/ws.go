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
			// Tear down this session's subscriptions and rooms.
			if sess != nil {
				for id := range sess.Subs {
					h.Store.Remove(id)
				}
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
		// add-query: run the initial snapshot immediately.
		if op == "add-query" {
			if rawQ, ok := f["q"]; ok {
				key := subKey(sess.ID, string(rawQ))
				if sub, ok := h.Store.Get(key); ok && h.Refresh != nil {
					result, rerr := h.Refresh(ctx, sub)
					if rerr == nil {
						// Baseline for delta-refresh diffs is always the flat
						// envelope; the wire gets the v1 node-list.
						sub.SetSnapshot(result)
						nodes, nerr := nodelistFor(ctx, h.Manager.Deps.Catalogs, sess.AppID, result)
						if nerr == nil {
							payload, _ := json.Marshal([]map[string]any{{
								"instaql-query":  json.RawMessage(rawQ),
								"instaql-result": nodes,
							}})
							_ = send(Frame{
								"op":              json.RawMessage(`"refresh-ok"`),
								"computations":    payload,
								"processed-tx-id": json.RawMessage(mustJSON(0)),
								"client-event-id": mustJSON(mustString(f, "client-event-id")),
							})
						} else {
							sendErr(500, "internal", nerr.Error())
						}
					} else {
						sendErr(400, "invalid-query", rerr.Error())
					}
				}
			}
		}
	}
}

func mustString(f Frame, key string) string {
	s, _ := f.String(key)
	return s
}

var _ = fmt.Sprintf
var _ = instaql.Executor{}
