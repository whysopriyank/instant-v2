package sync

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	stdsync "sync"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/reactive"
)

type sseDeliveryWriter struct {
	header http.Header

	mu   stdsync.Mutex
	body bytes.Buffer

	deadlineErr error
	deadline    time.Time
	writeErr    error
	flushErr    error

	flushEntered chan struct{}
	allowFlush   chan struct{}
	flushOnce    stdsync.Once
	writeEntered chan struct{}
	allowWrite   chan struct{}
	writeOnce    stdsync.Once
}

func newSSEDeliveryWriter() *sseDeliveryWriter {
	return &sseDeliveryWriter{header: make(http.Header)}
}

func (w *sseDeliveryWriter) Header() http.Header { return w.header }

func (*sseDeliveryWriter) WriteHeader(int) {}

func (w *sseDeliveryWriter) Write(p []byte) (int, error) {
	if w.writeEntered != nil {
		w.writeOnce.Do(func() { close(w.writeEntered) })
	}
	if w.allowWrite != nil {
		select {
		case <-w.allowWrite:
		case <-time.After(time.Second):
			return 0, errors.New("test write release timed out")
		}
	}
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	w.mu.Lock()
	_, _ = w.body.Write(p)
	w.mu.Unlock()
	return len(p), nil
}

func (w *sseDeliveryWriter) Flush() {}

func (w *sseDeliveryWriter) FlushError() error {
	if w.flushEntered != nil {
		w.flushOnce.Do(func() { close(w.flushEntered) })
	}
	if w.allowFlush != nil {
		select {
		case <-w.allowFlush:
		case <-time.After(time.Second):
			return errors.New("flush barrier timeout")
		}
	}
	return w.flushErr
}

func (w *sseDeliveryWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadline = deadline
	return w.deadlineErr
}

func (w *sseDeliveryWriter) bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.body.Bytes()...)
}

func TestWriteSSEEventDropsStaleGenerationWithoutBytes(t *testing.T) {
	sub := &reactive.Subscription{}
	sub.Gen.Store(2)
	w := newSSEDeliveryWriter()

	err := writeSSEEvent(w, sseEvent{
		frame:  Frame{"op": []byte(`"refresh-ok"`)},
		hasGen: true,
		gen:    1,
		sub:    sub,
	})
	if !errors.Is(err, errGenerationSuperseded) {
		t.Fatalf("stale guarded event error = %v, want generation superseded", err)
	}
	if got := w.bytes(); len(got) != 0 {
		t.Fatalf("stale guarded event wrote %q", got)
	}
}

func TestWriteSSEEventUsesExactSubscriptionPointerAfterCancellationABA(t *testing.T) {
	store := reactive.NewStore()
	old := &reactive.Subscription{ID: "same", AppID: "app"}
	old.Gen.Store(1)
	if _, err := store.Add(old); err != nil {
		t.Fatal(err)
	}
	if !store.Remove(old.ID) {
		t.Fatal("old subscription was not removed")
	}
	replacement := &reactive.Subscription{ID: old.ID, AppID: old.AppID}
	replacement.Gen.Store(1)
	if _, err := store.Add(replacement); err != nil {
		t.Fatal(err)
	}

	w := newSSEDeliveryWriter()
	err := writeSSEEvent(w, sseEvent{
		frame:  Frame{"op": []byte(`"refresh-ok"`)},
		hasGen: true,
		gen:    1,
		sub:    old,
	})
	if !errors.Is(err, errGenerationSuperseded) {
		t.Fatalf("cancelled old pointer error = %v, want generation superseded", err)
	}
	if got := w.bytes(); len(got) != 0 {
		t.Fatalf("cancelled old pointer wrote %q despite replacement", got)
	}
}

func TestWriteSSEEventGuardedWriteRequiresDeadline(t *testing.T) {
	w := newSSEDeliveryWriter()
	w.deadlineErr = errors.New("deadline unsupported")
	sub := &reactive.Subscription{}
	sub.Gen.Store(1)

	err := writeSSEEvent(w, sseEvent{
		frame:  Frame{"op": []byte(`"refresh-ok"`)},
		hasGen: true,
		gen:    1,
		sub:    sub,
	})
	if !errors.Is(err, w.deadlineErr) {
		t.Fatalf("guarded deadline error = %v, want %v", err, w.deadlineErr)
	}
	if got := w.bytes(); len(got) != 0 {
		t.Fatalf("guarded write with unsupported deadline wrote %q", got)
	}
}

func TestWriteSSEEventUsesFiniteTenSecondDeadline(t *testing.T) {
	w := newSSEDeliveryWriter()
	sub := &reactive.Subscription{}
	sub.Gen.Store(1)
	started := time.Now()

	if err := writeSSEEvent(w, sseEvent{frame: Frame{"op": []byte(`"refresh-ok"`)}, hasGen: true, gen: 1, sub: sub}); err != nil {
		t.Fatalf("guarded write: %v", err)
	}
	remaining := w.deadline.Sub(started)
	if remaining < 9*time.Second || remaining > 11*time.Second {
		t.Fatalf("guarded write deadline = %s from start, want about 10s", remaining)
	}
}

func TestWriteSSEEventControlMayIgnoreUnsupportedDeadline(t *testing.T) {
	w := newSSEDeliveryWriter()
	w.deadlineErr = errors.New("deadline unsupported")

	if err := writeSSEEvent(w, sseEvent{frame: Frame{"op": []byte(`"init-ok"`)}}); err != nil {
		t.Fatalf("control write with unsupported deadline: %v", err)
	}
	if got := string(w.bytes()); got != "data: {\"op\":\"init-ok\"}\n\n" {
		t.Fatalf("control wire bytes = %q", got)
	}
}

func TestWriteSSEEventPropagatesWriteAndFlushErrors(t *testing.T) {
	writeErr := errors.New("write failed")
	w := newSSEDeliveryWriter()
	w.writeErr = writeErr
	sub := &reactive.Subscription{}
	sub.Gen.Store(1)
	if err := writeSSEEvent(w, sseEvent{frame: Frame{"op": []byte(`"refresh-ok"`)}, hasGen: true, gen: 1, sub: sub}); !errors.Is(err, writeErr) {
		t.Fatalf("write error = %v, want %v", err, writeErr)
	}
	if got := w.bytes(); len(got) != 0 {
		t.Fatalf("failed write left bytes %q", got)
	}

	flushErr := errors.New("flush failed")
	w = newSSEDeliveryWriter()
	w.flushErr = flushErr
	if err := writeSSEEvent(w, sseEvent{frame: Frame{"op": []byte(`"refresh-ok"`)}, hasGen: true, gen: 1, sub: sub}); !errors.Is(err, flushErr) {
		t.Fatalf("flush error = %v, want %v", err, flushErr)
	}
	if got := w.bytes(); len(got) == 0 {
		t.Fatal("flush failure was not reached after a successful write")
	}
}

func TestWriteSSEEventHoldsDeliveryLeaseThroughFlush(t *testing.T) {
	w := newSSEDeliveryWriter()
	w.flushEntered = make(chan struct{})
	w.allowFlush = make(chan struct{})
	var releaseOnce stdsync.Once
	release := func() { releaseOnce.Do(func() { close(w.allowFlush) }) }
	defer release()
	sub := &reactive.Subscription{}
	sub.Gen.Store(1)

	done := make(chan error, 1)
	go func() {
		done <- writeSSEEvent(w, sseEvent{
			frame:  Frame{"op": []byte(`"refresh-ok"`)},
			hasGen: true,
			gen:    1,
			sub:    sub,
		})
	}()

	select {
	case <-w.flushEntered:
	case <-time.After(time.Second):
		t.Fatal("guarded write did not reach flush")
	}

	swapDone := make(chan struct{})
	go func() {
		sub.LockDelivery()
		close(swapDone)
		sub.UnlockDelivery()
	}()
	select {
	case <-swapDone:
		t.Fatal("gate swap acquired delivery lock before guarded flush completed")
	case <-time.After(25 * time.Millisecond):
	}

	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("guarded write: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("guarded write did not complete")
	}
	select {
	case <-swapDone:
	case <-time.After(time.Second):
		t.Fatal("gate swap stayed blocked after guarded flush completed")
	}
}

func TestWriteSSEEventExcludesRefreshGate(t *testing.T) {
	for _, stage := range []string{"write", "flush"} {
		t.Run(stage, func(t *testing.T) {
			testSSEEventExcludesRefreshGate(t, stage)
		})
	}
}

func testSSEEventExcludesRefreshGate(t *testing.T, stage string) {
	allow := &perms.RuleDoc{Raw: []byte(`{"posts":{"allow":{"view":"true"}}}`)}
	deny := &perms.RuleDoc{Raw: []byte(`{"posts":{"allow":{"view":"false"}}}`)}
	mgr := NewManager(Deps{Rules: func(context.Context, string) (*perms.RuleDoc, error) {
		return deny, nil
	}})
	sub := &reactive.Subscription{AppID: "app", AttachCtx: NewQueryGate(allow, false)}
	sub.SetAuthGate(sub.AttachCtx)
	sub.Gen.Store(1)
	w := newSSEDeliveryWriter()
	entered, allowFinish := make(chan struct{}), make(chan struct{})
	if stage == "write" {
		w.writeEntered, w.allowWrite = entered, allowFinish
	} else {
		w.flushEntered, w.allowFlush = entered, allowFinish
	}
	var releaseOnce stdsync.Once
	release := func() { releaseOnce.Do(func() { close(allowFinish) }) }
	defer release()

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- writeSSEEvent(w, sseEvent{
			frame:  Frame{"op": []byte(`"refresh-ok"`)},
			hasGen: true,
			gen:    1,
			sub:    sub,
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatalf("guarded write did not reach %s", stage)
	}

	type gateResult struct {
		regated bool
		err     error
	}
	gateDone := make(chan gateResult, 1)
	go func() {
		_, regated, err := mgr.RefreshGate(context.Background(), sub)
		gateDone <- gateResult{regated: regated, err: err}
	}()
	select {
	case <-gateDone:
		t.Fatalf("RefreshGate completed before guarded %s completed", stage)
	case <-time.After(25 * time.Millisecond):
	}
	// The blocked subscription must not hold the global registry lock while
	// waiting for this writer: a sibling's actual gate swap must still finish.
	sibling := &reactive.Subscription{AppID: "other", AttachCtx: NewQueryGate(allow, false)}
	siblingDone := make(chan gateResult, 1)
	go func() {
		_, regated, err := mgr.RefreshGate(context.Background(), sibling)
		siblingDone <- gateResult{regated: regated, err: err}
	}()
	select {
	case result := <-siblingDone:
		if result.err != nil || !result.regated {
			t.Fatalf("sibling gate swap: %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked writer stalled sibling gate swap")
	}

	release()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("guarded write: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("guarded write did not complete")
	}
	select {
	case result := <-gateDone:
		if result.err != nil || !result.regated {
			t.Fatalf("RefreshGate result = regated:%v err:%v, want a completed swap", result.regated, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("RefreshGate did not complete after guarded flush")
	}
	if got := sub.Gen.Load(); got != 2 {
		t.Fatalf("RefreshGate generation = %d, want 2", got)
	}
	if gate, _ := sub.AuthGate().(*QueryGate); gate == nil || GateHash(gate.Rules) != GateHash(deny) {
		t.Fatal("completed swap did not install the deny gate")
	}
	late := newSSEDeliveryWriter()
	if err := writeSSEEvent(late, sseEvent{frame: Frame{"op": []byte(`"refresh-ok"`)}, hasGen: true, gen: 1, sub: sub}); !errors.Is(err, errGenerationSuperseded) {
		t.Fatalf("late old-generation write: %v", err)
	}
	if len(late.bytes()) != 0 {
		t.Fatal("old-generation bytes written after completed gate swap")
	}
}

func TestWriteSSEEventNilGuardFailsClosed(t *testing.T) {
	w := newSSEDeliveryWriter()
	err := writeSSEEvent(w, sseEvent{
		frame:  Frame{"op": []byte(`"refresh-ok"`)},
		hasGen: true,
		gen:    1,
	})
	if !errors.Is(err, errSSEMissingSubscription) {
		t.Fatalf("nil guarded subscription error = %v, want %v", err, errSSEMissingSubscription)
	}
	if got := w.bytes(); len(got) != 0 {
		t.Fatalf("nil guarded subscription wrote %q", got)
	}
}
