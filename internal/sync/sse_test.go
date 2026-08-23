package sync_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestSSEFallback proves the /runtime/sse transport: GET opens the stream and
// hands out session-id/sse-token; POST messages drive the same Manager path;
// replies ride back as SSE data events.
func TestSSEFallback(t *testing.T) {
	env := newWSEnv(t)
	sse := env.SSE
	if sse == nil {
		t.Skip("sse handler not wired")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET",
		env.Server.URL+"/runtime/sse?app_id="+env.AppID, nil)
	resp, err := env.Server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	scanner := bufio.NewScanner(resp.Body)
	readEvent := func() map[string]any {
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				var f map[string]any
				_ = json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &f)
				return f
			}
		}
		t.Fatal("stream ended")
		return nil
	}

	hello := readEvent()
	sessID, _ := hello["session-id"].(string)
	token, _ := hello["sse-token"].(string)
	if sessID == "" || token == "" {
		t.Fatalf("handshake missing: %v", hello)
	}

	sendMsgs := func(msgs ...map[string]any) {
		b, _ := json.Marshal(map[string]any{
			"machine_id": "m1", "app_id": env.AppID,
			"session_id": sessID, "sse_token": token,
			"messages": msgs,
		})
		r2, err := http.NewRequestWithContext(ctx, "POST",
			env.Server.URL+"/runtime/sse", strings.NewReader(string(b)))
		if err != nil {
			t.Fatal(err)
		}
		r2.Header.Set("Content-Type", "application/json")
		resp2, err := env.Server.Client().Do(r2)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp2.Body.Close()
		if resp2.StatusCode != 200 {
			t.Fatalf("post status %d", resp2.StatusCode)
		}
	}

	// Protocol init over the POST path.
	sendMsgs(map[string]any{"op": "init", "app-id": env.AppID})
	if f := readEvent(); f["op"] != "init-ok" || f["attrs"] == nil {
		t.Fatalf("init reply wrong: %v", f)
	}

	// add-query → ok + snapshot refresh.
	sendMsgs(map[string]any{
		"op": "add-query", "q": map[string]any{"todos": map[string]any{}},
		"client-event-id": "sq1",
	})
	if f := readEvent(); f["op"] != "add-query-ok" {
		t.Fatalf("expected add-query-ok, got %v", f)
	}
	if f := readEvent(); f["op"] != "refresh-ok" {
		t.Fatalf("expected snapshot refresh-ok, got %v", f)
	}
}
