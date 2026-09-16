package sync_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
)

// adminOverflowWriter parks the first refresh write so the bounded admin SSE
// queue can be filled deterministically by the production send closures.
type adminOverflowWriter struct {
	header http.Header

	mu   sync.Mutex
	body bytes.Buffer

	addQueryWritten  chan struct{}
	refreshBlocked   chan struct{}
	allowRefresh     chan struct{}
	addQueryOnce     sync.Once
	refreshBlockOnce sync.Once
}

func newAdminOverflowWriter() *adminOverflowWriter {
	return &adminOverflowWriter{
		header:          make(http.Header),
		addQueryWritten: make(chan struct{}),
		refreshBlocked:  make(chan struct{}),
		allowRefresh:    make(chan struct{}),
	}
}

func (w *adminOverflowWriter) Header() http.Header { return w.header }

func (*adminOverflowWriter) WriteHeader(int) {}

func (w *adminOverflowWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	_, _ = w.body.Write(p)
	w.mu.Unlock()
	if bytes.Contains(p, []byte(`"add-query-ok"`)) {
		w.addQueryOnce.Do(func() { close(w.addQueryWritten) })
	}
	if bytes.Contains(p, []byte(`"refresh-ok"`)) {
		w.refreshBlockOnce.Do(func() {
			close(w.refreshBlocked)
			<-w.allowRefresh
		})
	}
	return len(p), nil
}

func (*adminOverflowWriter) Flush() {}

func (*adminOverflowWriter) SetWriteDeadline(time.Time) error { return nil }

func TestAdminSSEOverflowClosesStream(t *testing.T) {
	env := newWSEnv(t)
	env.SSE.AdminAuth = func(context.Context, string, string) bool { return true }

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	w := newAdminOverflowWriter()
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/admin/subscribe-query", bytes.NewReader([]byte(`{"query":{"todos":{}}}`)))
	req.Header.Set("app-id", env.AppID)
	req.Header.Set("Authorization", "Bearer test-token")
	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		env.SSE.AdminSubscribe(w, req)
	}()

	select {
	case <-w.addQueryWritten:
	case <-ctx.Done():
		t.Fatal("admin SSE did not write its initial answer")
	}

	subs := env.Mgr.Deps.Store.SubsForTopics([]string{platform.UUIDToStr(env.IDs.title)})
	if len(subs) != 1 {
		t.Fatalf("admin SSE subscriptions = %d, want one", len(subs))
	}
	sub := subs[0]
	gen := sub.Gen.Load()
	if err := sub.Emit(reactive.Frame{
		SubID:         sub.ID,
		QueryJSON:     sub.Query,
		ResultJSON:    json.RawMessage(`{"data":{"todos":[{"id":"e1"}]}}`),
		ProcessedTxID: 1,
		Gen:           gen,
	}); err != nil {
		t.Fatalf("first refresh enqueue: %v", err)
	}
	select {
	case <-w.refreshBlocked:
	case <-ctx.Done():
		t.Fatal("admin SSE did not reach its first refresh write")
	}

	// One event is being written and the production queue holds 128 more;
	// the next send must take the overflow/disconnect path.
	for i := 0; i < 129; i++ {
		if err := sub.Emit(reactive.Frame{
			SubID:         sub.ID,
			QueryJSON:     sub.Query,
			ResultJSON:    json.RawMessage(`{"data":{"todos":[{"id":"e1"}]}}`),
			ProcessedTxID: int64(i + 2),
			Gen:           gen,
		}); err != nil {
			t.Fatalf("refresh %d enqueue: %v", i, err)
		}
	}

	// Keep an emitter active while the unblocked handler observes overflow and
	// runs deferred teardown. This exercises producer-versus-detach locking,
	// rather than proving teardown only after all producers are quiescent.
	producerStarted := make(chan struct{})
	stopProducer := make(chan struct{})
	producerDone := make(chan error, 1)
	go func() {
		for txID := int64(131); ; txID++ {
			select {
			case <-stopProducer:
				producerDone <- nil
				return
			default:
			}
			if err := sub.Emit(reactive.Frame{
				SubID:         sub.ID,
				QueryJSON:     sub.Query,
				ResultJSON:    json.RawMessage(`{"data":{"todos":[{"id":"e1"}]}}`),
				ProcessedTxID: txID,
				Gen:           gen,
			}); err != nil {
				producerDone <- err
				return
			}
			if txID == 131 {
				close(producerStarted)
			}
		}
	}()
	<-producerStarted
	close(w.allowRefresh)
	select {
	case <-handlerDone:
	case <-ctx.Done():
		t.Fatal("admin SSE stayed open after event-buffer overflow")
	}
	close(stopProducer)
	if err := <-producerDone; err != nil {
		t.Fatalf("concurrent producer: %v", err)
	}
	if got := env.Mgr.Deps.Store.SubsForTopics([]string{platform.UUIDToStr(env.IDs.title)}); len(got) != 0 {
		t.Fatalf("admin SSE subscriptions after overflow teardown = %d, want zero", len(got))
	}
}
