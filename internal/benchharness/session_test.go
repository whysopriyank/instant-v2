package benchharness

import (
	"context"
	"encoding/json"
	"github.com/coder/websocket"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDialSSEAcceptsV1TransportHandshake(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("test server does not support streaming")
		}
		_, _ = w.Write([]byte("data: {\"op\":\"sse-init\",\"machine-id\":\"00000000-0000-0000-0000-000000000002\",\"session-id\":\"00000000-0000-0000-0000-000000000003\",\"sse-token\":\"00000000-0000-0000-0000-000000000004\"}\n\n"))
		flusher.Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()
	app := "00000000-0000-0000-0000-000000000001"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sess, err := DialSSE(ctx, SessionOptions{URL: srv.URL, AppID: app})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sess.Close() }() // Cleanup may observe the peer's normal close status.
	if !strings.Contains(sess.(*SSESession).opts.URL, "app_id=") {
		t.Fatalf("resolved POST URL omitted app_id: %q", sess.(*SSESession).opts.URL)
	}
}

func TestSSESessionMessageEncodingShape(t *testing.T) {
	var posts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("data: {\"op\":\"init-ok\",\"session-id\":\"s\",\"sse-token\":\"t\",\"machine-id\":\"m\"}\n\ndata: {broken\n\n"))
			return
		}
		var body struct {
			AppID     string            `json:"app_id"`
			MachineID string            `json:"machine_id"`
			SessionID string            `json:"session_id"`
			SSEToken  string            `json:"sse_token"`
			Messages  []json.RawMessage `json:"messages"`
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil || body.AppID != "app" || body.MachineID != "m" || body.SessionID != "s" || body.SSEToken != "t" || len(body.Messages) != 1 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		posts.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	s, err := DialSSE(context.Background(), SessionOptions{URL: srv.URL, AppID: "app", EventBuffer: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() // Cleanup may observe the peer's normal close status.
	ack, err := s.Send(context.Background(), SessionMessage{Op: "init", ClientEventID: "e"})
	if err != nil || !ack.Accepted || posts.Load() != 1 {
		t.Fatalf("SSE send %v/%#v posts=%d", err, ack, posts.Load())
	}
	var got []SessionEvent
	for ev := range s.Events() {
		got = append(got, ev)
	}
	if len(got) != 2 || got[0].Op != "init-ok" || got[1].Op != "protocol-error" {
		t.Fatalf("events %#v", got)
	}
}

func TestWebSocketTransactWaitsForCorrelatedAck(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		_, data, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		var req map[string]any
		if json.Unmarshal(data, &req) != nil {
			return
		}
		id, _ := req["client-event-id"].(string)
		body, _ := json.Marshal(map[string]any{"op": "transact-ok", "client-event-id": id, "tx-id": "tx-7", "processed-tx-id": "ptx-7"})
		_ = conn.Write(r.Context(), websocket.MessageText, body)
	}))
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	s, err := DialWebSocket(context.Background(), SessionOptions{URL: wsURL})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() // Cleanup may observe the peer's normal close status.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ack, err := s.Send(ctx, SessionMessage{Op: "transact", ClientEventID: "event-7"})
	if err != nil {
		t.Fatal(err)
	}
	if !ack.Accepted || ack.ServerTransactionID != "tx-7" || ack.ProcessedTransactionID != "ptx-7" {
		t.Fatalf("ack %#v", ack)
	}
}
