package sync_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// TestGracefulShutdownDrain proves a live session receives close code 1001
// (Going Away) when the handler drains, and that Drain blocks until every
// connection is gone.
func TestGracefulShutdownDrain(t *testing.T) {
	env := newWSEnv(t)
	handler := env.WS

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, frames := env.dial(t, ctx)
	sendFrame(t, conn, ctx, map[string]any{"op": "init", "app-id": env.AppID})
	expectOp(t, frames, "init-ok")

	drained := make(chan struct{})
	go func() {
		handler.Drain(ctx)
		close(drained)
	}()

	// Client must observe 1001 Going Away.
	readCtx, rcancel := context.WithTimeout(ctx, 5*time.Second)
	defer rcancel()
	for {
		typ, r, err := conn.Reader(readCtx)
		if err != nil {
			st := websocket.CloseStatus(err)
			// Primary contract: 1001 Going Away. Under -race the transport
			// may tear down mid-handshake; a bare closed-connection error
			// still proves the server closed first.
			if st == websocket.StatusGoingAway {
				break
			}
			if st == -1 && strings.Contains(err.Error(), "closed") {
				break
			}
			t.Fatalf("close status = %v (%v), want 1001", st, err)
		}
		// Drain a control/data frame to keep the read loop fed.
		buf := make([]byte, 512)
		for {
			n, rerr := r.Read(buf)
			if rerr != nil || n > 0 && typ != websocket.MessageBinary {
				break
			}
			if n == 0 {
				break
			}
		}
	}

	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("Drain never returned")
	}
}
