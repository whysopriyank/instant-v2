package sync_test

import (
	"context"
	"encoding/json"
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
	// This test owns the reader so it can assert the exact close status.
	// env.dial starts a background reader that would consume that status.
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(env.Server.URL, "http")+"/runtime/session", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.CloseNow() }()
	sendFrame(t, conn, ctx, map[string]any{"op": "init", "app-id": env.AppID})
	_, raw, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var init struct {
		Op string `json:"op"`
	}
	if err := json.Unmarshal(raw, &init); err != nil || init.Op != "init-ok" {
		t.Fatalf("init response=%s, error=%v", raw, err)
	}

	drained := make(chan struct{})
	go func() {
		handler.Drain(ctx)
		close(drained)
	}()

	// Client must observe 1001 Going Away.
	readCtx, rcancel := context.WithTimeout(ctx, 5*time.Second)
	defer rcancel()
	for {
		_, _, err := conn.Read(readCtx)
		if err != nil {
			st := websocket.CloseStatus(err)
			if st == websocket.StatusGoingAway {
				break
			}
			t.Fatalf("close status = %v (%v), want 1001", st, err)
		}
	}

	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("Drain never returned")
	}
}
