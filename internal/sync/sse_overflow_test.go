package sync_test

// SSE overflow semantics (audit backlog B1): a stream whose event buffer
// fills must END (client reconnects) — never silently drop frames and keep
// running, which would strand the client stale with no signal.

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestSSEOverflowClosesStream(t *testing.T) {
	env := newWSEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Open the GET stream and read the handshake for the sse-token.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, env.Server.URL+"/runtime/sse?app_id="+env.AppID, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content type %q", ct)
	}
	reader := bufio.NewReader(resp.Body)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("handshake read: %v", err)
	}
	if !strings.HasPrefix(line, "data: ") {
		t.Fatalf("unexpected first event %q", line)
	}
	var handshake map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &handshake); err != nil {
		t.Fatal(err)
	}
	token, _ := handshake["sse-token"].(string)
	if token == "" {
		t.Fatalf("handshake missing sse-token: %v", handshake)
	}

	// Flood replies: every malformed message produces a bad-frame reply.
	// 300 replies against the 128-event buffer must overflow it.
	messages := make([]string, 0, 300)
	for i := 0; i < 300; i++ {
		messages = append(messages, fmt.Sprintf(`"bad-frame-%d"`, i))
	}
	body := fmt.Sprintf(`{"machine_id":"m","app_id":%q,"sse_token":%q,"messages":[%s]}`,
		env.AppID, token, strings.Join(messages, ","))
	pre, precancel := context.WithTimeout(ctx, 10*time.Second)
	defer precancel()
	preq, err := http.NewRequestWithContext(pre, http.MethodPost, env.Server.URL+"/runtime/sse", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	preq.Header.Set("Content-Type", "application/json")
	presp, err := http.DefaultClient.Do(preq)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	presp.Body.Close()

	// The GET stream must terminate on overflow (EOF), not keep running
	// with dropped frames. Heartbeats are 20s apart by default, so an EOF
	// inside this window can only come from the overflow close.
	eof := make(chan error, 1)
	go func() {
		for {
			_, err := reader.ReadString('\n')
			if err != nil {
				eof <- err
				return
			}
		}
	}()
	select {
	case err := <-eof:
		if err != io.EOF && !strings.Contains(err.Error(), "context canceled") {
			t.Fatalf("stream ended with %v, want clean EOF", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stream stayed open after overflow — frames would be silently dropped")
	}
}
