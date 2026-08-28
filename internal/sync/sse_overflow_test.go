package sync_test

// SSE overflow semantics (audit backlog B1): a stream whose event buffer
// fills must END (client reconnects) — never silently drop frames and keep
// running, which would strand the client stale with no signal.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSSEOverflowClosesStream(t *testing.T) {
	env := newWSEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Keep the writer blocked after the transport handshake. A client that
	// merely stops reading is not enough: httptest's socket buffer can absorb
	// hundreds of small frames, allowing the GET goroutine to drain events
	// before the producer fills the queue.
	w := newBlockingSSEWriter()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, env.Server.URL+"/runtime/sse?app_id="+env.AppID, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.RemoteAddr = "127.0.0.1:12345"
	streamDone := make(chan struct{})
	go func() {
		defer close(streamDone)
		env.SSE.ServeHTTP(w, req)
	}()
	select {
	case <-w.handshakeWritten:
	case <-ctx.Done():
		t.Fatal("SSE handshake was not written")
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content type %q", ct)
	}
	line, err := bufio.NewReader(bytes.NewReader(w.Bytes())).ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "data: ") {
		t.Fatalf("unexpected handshake %q (%v)", line, err)
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
	preq, err := http.NewRequestWithContext(pre, http.MethodPost, "/runtime/sse", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	preq.Header.Set("Content-Type", "application/json")
	env.SSE.ServeHTTP(httptest.NewRecorder(), preq)

	// The GET writer is now blocked on the first queued reply. The batch has
	// filled its bounded event channel and signalled overflow; allow that
	// writer to resume so it can observe overflow and close the stream.
	select {
	case <-w.replyWriteBlocked:
	case <-ctx.Done():
		t.Fatal("SSE writer did not block on the queued reply")
	}
	close(w.allowReplyWrites)

	select {
	case <-streamDone:
	case <-time.After(5 * time.Second):
		t.Fatal("stream stayed open after an observed queue overflow")
	}
	if got := env.SSE.ConnCount(); got != 0 {
		t.Fatalf("live SSE connections = %d, want 0 after overflow", got)
	}
}

// blockingSSEWriter models a live response whose client has received the
// handshake but whose next event cannot be written. It is a deterministic
// backpressure seam: SSEHandler's real queue and close path remain in use.
type blockingSSEWriter struct {
	header http.Header

	mu                sync.Mutex
	body              bytes.Buffer
	handshakeWritten  chan struct{}
	replyWriteBlocked chan struct{}
	allowReplyWrites  chan struct{}
	handshakeOnce     sync.Once
	replyOnce         sync.Once
}

func newBlockingSSEWriter() *blockingSSEWriter {
	return &blockingSSEWriter{
		header:            make(http.Header),
		handshakeWritten:  make(chan struct{}),
		replyWriteBlocked: make(chan struct{}),
		allowReplyWrites:  make(chan struct{}),
	}
}

func (w *blockingSSEWriter) Header() http.Header { return w.header }

func (*blockingSSEWriter) WriteHeader(int) {}

func (w *blockingSSEWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.body.Write(p)
	w.mu.Unlock()
	if bytes.Contains(p, []byte(`"init-ok"`)) {
		w.handshakeOnce.Do(func() { close(w.handshakeWritten) })
		return len(p), nil
	}
	w.replyOnce.Do(func() { close(w.replyWriteBlocked) })
	<-w.allowReplyWrites
	return len(p), nil
}

func (*blockingSSEWriter) Flush() {}

func (w *blockingSSEWriter) Bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.body.Bytes()...)
}
