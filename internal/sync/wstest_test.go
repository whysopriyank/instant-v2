package sync_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/instant-v2/instant-v2/internal/reactive"
	syncpkg "github.com/instant-v2/instant-v2/internal/sync"
)

// wsEnv bundles a fully-wired Manager + WS handler + httptest server for
// protocol-level tests.
type wsEnv struct {
	Mgr    *syncpkg.Manager
	AppID  string
	IDs    qattr
	Server *httptest.Server
	WS     *syncpkg.WSHandler
	SSE    *syncpkg.SSEHandler
	closed <-chan struct{} // emitted after a WebSocket handler and its teardown return
}

func newWSEnv(t *testing.T) *wsEnv {
	t.Helper()
	mgr, store, _, ex, appID, cats, ids := fakeStack(t)
	handler := &syncpkg.WSHandler{
		Manager: mgr,
		Store:   store,
		Refresh: func(ctx context.Context, sub *reactive.Subscription) (json.RawMessage, error) {
			return runQuery(ex, cats, sub)
		},
	}
	sseHandler := &syncpkg.SSEHandler{
		Manager: mgr,
		Store:   store,
		Refresh: handler.Refresh,
	}
	mux := http.NewServeMux()
	closed := make(chan struct{}, 32)
	mux.HandleFunc("/runtime/session", func(w http.ResponseWriter, r *http.Request) {
		defer func() { closed <- struct{}{} }()
		handler.ServeHTTP(w, r)
	})
	mux.Handle("/runtime/sse", sseHandler)
	env := &wsEnv{
		Mgr:    mgr,
		AppID:  uuidStr(appID),
		IDs:    ids,
		Server: httptest.NewServer(mux),
		WS:     handler,
		SSE:    sseHandler,
		closed: closed,
	}
	t.Cleanup(env.Server.Close)
	return env
}

func (e *wsEnv) waitClosed(t *testing.T) {
	t.Helper()
	select {
	case <-e.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("WebSocket handler did not complete teardown")
	}
}

// dial opens a WebSocket to env's server and starts a reader pump.
func (e *wsEnv) dial(t *testing.T, ctx context.Context) (*websocket.Conn, chan map[string]any) {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(e.Server.URL, "http") + "/runtime/session"
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{CompressionMode: websocket.CompressionDisabled})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(websocket.StatusNormalClosure, "") })
	frames := make(chan map[string]any, 256)
	go func() {
		defer close(frames)
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var f map[string]any
			_ = json.Unmarshal(data, &f)
			select {
			case frames <- f:
			default:
			}
		}
	}()
	return conn, frames
}

func sendFrame(t *testing.T, conn *websocket.Conn, ctx context.Context, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Write(ctx, websocket.MessageText, b); err != nil {
		t.Fatalf("write %v: %v", v, err)
	}
}
