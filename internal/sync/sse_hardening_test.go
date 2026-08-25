package sync_test

import (
	"bufio"
	"net/http"
	"strings"
	"testing"
	"time"

	syncpkg "github.com/instant-v2/instant-v2/internal/sync"
)

// Audit H5: a per-IP connection cap bounds slot squatting by one host.
func TestSSEPerIPCap(t *testing.T) {
	srv, appID := sseSrv(t, func(h *syncpkg.SSEHandler) { h.MaxConnsPerIP = 2 })

	bodies := []*http.Response{}
	defer func() {
		for _, r := range bodies {
			r.Body.Close()
		}
	}()
	for i := 0; i < 2; i++ {
		resp, err := http.Get(srv.URL + "/runtime/sse?app_id=" + appID)
		if err != nil {
			t.Fatalf("stream %d: %v", i, err)
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			t.Fatalf("stream %d: want 200, got %d", i, resp.StatusCode)
		}
		bodies = append(bodies, resp)
	}
	third, err := http.Get(srv.URL + "/runtime/sse?app_id=" + appID)
	if err != nil {
		t.Fatalf("third stream: %v", err)
	}
	defer third.Body.Close()
	if third.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("third stream from same IP must be 503, got %d", third.StatusCode)
	}

	// Releasing a slot admits the next caller.
	bodies[0].Body.Close()
	bodies = bodies[1:]
	time.Sleep(150 * time.Millisecond)
	fourth, err := http.Get(srv.URL + "/runtime/sse?app_id=" + appID)
	if err != nil {
		t.Fatalf("post-release stream: %v", err)
	}
	defer fourth.Body.Close()
	if fourth.StatusCode != http.StatusOK {
		t.Fatalf("stream after release must be 200, got %d", fourth.StatusCode)
	}
}

// Audit H5: heartbeat comments keep proxies warm and bound dead-reader slots.
func TestSSEHeartbeat(t *testing.T) {
	srv, appID := sseSrv(t, func(h *syncpkg.SSEHandler) { h.HeartbeatEvery = 50 * time.Millisecond })
	resp, err := http.Get(srv.URL + "/runtime/sse?app_id=" + appID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)

	start := time.Now()
	for time.Since(start) < 3*time.Second {
		line := nextLine(t, sc)
		if strings.HasPrefix(line, ": ping") {
			return
		}
	}
	t.Fatal("no : ping heartbeat observed within 3s")
}
