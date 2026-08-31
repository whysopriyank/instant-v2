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
	srv, appID, closed := sseSrv(t, func(h *syncpkg.SSEHandler) { h.MaxConnsPerIP = 2 })
	waitClosed := func() {
		t.Helper()
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatal("SSE handler did not complete teardown")
		}
	}

	bodies := []*http.Response{}
	defer func() {
		for _, r := range bodies {
			_ = r.Body.Close()
		}
	}()
	for i := 0; i < 2; i++ {
		resp, err := http.Get(srv.URL + "/runtime/sse?app_id=" + appID)
		if err != nil {
			t.Fatalf("stream %d: %v", i, err)
		}
		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			t.Fatalf("stream %d: want 200, got %d", i, resp.StatusCode)
		}
		bodies = append(bodies, resp)
	}
	third, err := http.Get(srv.URL + "/runtime/sse?app_id=" + appID)
	if err != nil {
		t.Fatalf("third stream: %v", err)
	}
	defer func() { _ = third.Body.Close() }()
	if third.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("third stream from same IP must be 503, got %d", third.StatusCode)
	}

	// Releasing a slot admits the next caller.
	waitClosed() // the rejected third request has returned
	_ = bodies[0].Body.Close()
	bodies = bodies[1:]
	waitClosed() // the first stream's per-IP slot has been released
	fourth, err := http.Get(srv.URL + "/runtime/sse?app_id=" + appID)
	if err != nil {
		t.Fatalf("post-release stream: %v", err)
	}
	defer func() { _ = fourth.Body.Close() }()
	if fourth.StatusCode != http.StatusOK {
		t.Fatalf("stream after release must be 200, got %d", fourth.StatusCode)
	}
}

// Audit H5: heartbeat comments keep proxies warm and bound dead-reader slots.
func TestSSEHeartbeat(t *testing.T) {
	srv, appID, _ := sseSrv(t, func(h *syncpkg.SSEHandler) { h.HeartbeatEvery = 50 * time.Millisecond })
	resp, err := http.Get(srv.URL + "/runtime/sse?app_id=" + appID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
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
