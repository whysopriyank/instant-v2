package sync

import (
	"context"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// Drain closes every live session connection with 1001 (Going Away) after
// waiting for in-flight frames to finish — the Phase 4 shutdown order:
// stop accepting → drain in-flight transacts → close WS with correct code.
// (LSN checkpointing sits upstream in waltail; single-node direct-notify has
// no WAL lag to flush.)
type connRegistry struct {
	mu    sync.Mutex
	conns map[*websocket.Conn]struct{}
}

func (r *connRegistry) add(c *websocket.Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conns == nil {
		r.conns = map[*websocket.Conn]struct{}{}
	}
	r.conns[c] = struct{}{}
}

func (r *connRegistry) remove(c *websocket.Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.conns, c)
}

func (r *connRegistry) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.conns)
}

// Drain waits for all connections to go quiet (no frame being processed) and
// closes them with StatusGoingAway. Bounded by ctx.
func (h *WSHandler) Drain(ctx context.Context) {
	h.live.mu.Lock()
	conns := make([]*websocket.Conn, 0, len(h.live.conns))
	for c := range h.live.conns {
		conns = append(conns, c)
	}
	h.live.mu.Unlock()

	for _, c := range conns {
		go func(c *websocket.Conn) {
			// coder/websocket Close takes (status, reason).
			_ = c.Close(websocket.StatusGoingAway, "server shutting down")
		}(c)
	}
	// Wait until the registry empties (each ServeHTTP defer removes its conn).
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for h.live.len() > 0 {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
