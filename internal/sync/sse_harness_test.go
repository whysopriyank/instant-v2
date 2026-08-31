package sync_test

// Helpers shared by the SSE security tests live here; tests that reference
// post-fix handler fields (MaxConnsPerIP, HeartbeatEvery) are in this file
// too so they can be excluded wholesale from pre-fix red runs.

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/reactive"
	syncpkg "github.com/instant-v2/instant-v2/internal/sync"
)

func sseSrv(t *testing.T, mutate func(*syncpkg.SSEHandler)) (*httptest.Server, string, <-chan struct{}) {
	t.Helper()
	mgr, store, _, ex, appID, cats, _ := fakeStack(t)
	h := &syncpkg.SSEHandler{
		Manager: mgr,
		Store:   store,
		Refresh: func(ctx context.Context, sub *reactive.Subscription) (json.RawMessage, error) {
			return runQuery(ex, cats, sub)
		},
	}
	if mutate != nil {
		mutate(h)
	}
	closed := make(chan struct{}, 16)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			defer func() { closed <- struct{}{} }()
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, uuidStr(appID), closed
}

func nextLine(t *testing.T, sc *bufio.Scanner) string {
	t.Helper()
	ch := make(chan string, 1)
	go func() {
		if sc.Scan() {
			ch <- sc.Text()
		} else {
			ch <- ""
		}
	}()
	select {
	case line := <-ch:
		return line
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for SSE line")
		return ""
	}
}

func nextEvent(t *testing.T, sc *bufio.Scanner) map[string]any {
	t.Helper()
	for {
		line := nextLine(t, sc)
		if strings.HasPrefix(line, "data: ") {
			var f map[string]any
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &f); err != nil {
				t.Fatalf("bad frame %q: %v", line, err)
			}
			return f
		}
		if line == "" {
			continue
		}
	}
}

func postMsgs(t *testing.T, srvURL, token string, msgs ...map[string]any) {
	t.Helper()
	parts := make([]string, 0, len(msgs))
	for _, m := range msgs {
		b, _ := json.Marshal(m)
		parts = append(parts, string(b))
	}
	body := fmt.Sprintf(`{"sse_token":%q,"messages":[%s]}`, token, strings.Join(parts, ","))
	resp, err := http.Post(srvURL+"/runtime/sse", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	_ = resp.Body.Close()
}
