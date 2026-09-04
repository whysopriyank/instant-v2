package reactive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeRetryClock drives retry callbacks synchronously when Advance reaches a
// deadline. It makes every retry assertion independent of scheduler timing.
type fakeRetryClock struct {
	mu     sync.Mutex
	now    time.Duration
	timers []*fakeRetryTimer
}

type fakeRetryTimer struct {
	clock   *fakeRetryClock
	at      time.Duration
	fn      func()
	stopped bool
	fired   bool
}

func (c *fakeRetryClock) AfterFunc(delay time.Duration, fn func()) retryTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeRetryTimer{clock: c, at: c.now + delay, fn: fn}
	c.timers = append(c.timers, t)
	return t
}

func (t *fakeRetryTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if t.stopped || t.fired {
		return false
	}
	t.stopped = true
	return true
}

func (c *fakeRetryClock) Advance(delta time.Duration) {
	c.mu.Lock()
	c.now += delta
	var callbacks []func()
	for _, t := range c.timers {
		if !t.stopped && !t.fired && t.at <= c.now {
			t.fired = true
			callbacks = append(callbacks, t.fn)
		}
	}
	c.mu.Unlock()
	for _, fn := range callbacks {
		fn()
	}
}

func (c *fakeRetryClock) NextDelay() (time.Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var next time.Duration
	found := false
	for _, t := range c.timers {
		if t.stopped || t.fired {
			continue
		}
		d := t.at - c.now
		if !found || d < next {
			next, found = d, true
		}
	}
	return next, found
}

func (c *fakeRetryClock) ActiveTimers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	active := 0
	for _, t := range c.timers {
		if !t.stopped && !t.fired {
			active++
		}
	}
	return active
}

func newRetrySubscription(t *testing.T, id string) (*Store, *Subscription) {
	t.Helper()
	store := NewStore()
	sub := &Subscription{
		ID:     id,
		AppID:  "app",
		Query:  json.RawMessage(`{"posts":{}}`),
		Topics: map[string]bool{"title": true},
	}
	if _, err := store.Add(sub); err != nil {
		t.Fatal(err)
	}
	return store, sub
}

func zeroRetryJitter(_ string, delay time.Duration) time.Duration { return delay }

func TestRetryBackoffResetAndCap(t *testing.T) {
	clock := &fakeRetryClock{}
	store, sub := newRetrySubscription(t, "retry-sequence")
	var calls atomic.Int32
	var emits atomic.Int32
	var failAfterRecovery atomic.Bool
	n := &Notifier{
		Store:       store,
		clock:       clock,
		retryJitter: zeroRetryJitter,
		Refresh: func(context.Context, *Subscription) (json.RawMessage, error) {
			attempt := calls.Add(1)
			if attempt <= int32(len(retryDelays)) || failAfterRecovery.Swap(false) {
				return nil, errors.New("transient refresh failure")
			}
			return json.RawMessage(`{"v":"latest"}`), nil
		},
	}
	sub.Emit = func(Frame) { emits.Add(1) }

	n.Notify(context.Background(), "app", []string{"title"}, 1)
	if !n.drainPass(context.Background(), slog.Default()) {
		t.Fatal("initial refresh was not scheduled")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("initial attempts = %d, want 1", got)
	}
	if n.drainPass(context.Background(), slog.Default()) {
		t.Fatal("retry ran before its first deadline")
	}

	for i, want := range retryDelays {
		if got, ok := clock.NextDelay(); !ok || got != want {
			t.Fatalf("retry %d delay = %s (ok=%t), want %s", i+1, got, ok, want)
		}
		clock.Advance(want - time.Nanosecond)
		if got := calls.Load(); got != int32(i+1) {
			t.Fatalf("attempts before retry %d deadline = %d, want %d", i+1, got, i+1)
		}
		clock.Advance(time.Nanosecond)
		if got := n.QueueDepth(); got != 1 {
			t.Fatalf("queue depth at retry %d deadline = %d, want 1", i+1, got)
		}
		if !n.drainPass(context.Background(), slog.Default()) {
			t.Fatalf("retry %d was not drained", i+1)
		}
		if got := calls.Load(); got != int32(i+2) {
			t.Fatalf("attempts after retry %d = %d, want %d", i+1, got, i+2)
		}
	}

	if got := sub.TxID.Load(); got != 1 {
		t.Fatalf("successful retry watermark = %d, want 1", got)
	}
	if got := emits.Load(); got != 1 {
		t.Fatalf("successful retry emissions = %d, want 1", got)
	}
	if got := clock.ActiveTimers(); got != 0 {
		t.Fatalf("active timers after success = %d, want 0", got)
	}

	// A failure after a successful recovery starts a fresh sequence rather
	// than inheriting the capped failure count.
	failAfterRecovery.Store(true)
	n.Notify(context.Background(), "app", []string{"title"}, 2)
	if !n.drainPass(context.Background(), slog.Default()) {
		t.Fatal("post-recovery refresh was not scheduled")
	}
	if got, ok := clock.NextDelay(); !ok || got != retryDelays[0] {
		t.Fatalf("post-success retry delay = %s (ok=%t), want %s", got, ok, retryDelays[0])
	}
	store.Remove(sub.ID)
}

func TestRetryLatestWatermarkAndUnknownKnowledgeAreSticky(t *testing.T) {
	clock := &fakeRetryClock{}
	store, sub := newRetrySubscription(t, "retry-latest")
	var calls atomic.Int32
	var gotFrame Frame
	n := &Notifier{
		Store:       store,
		clock:       clock,
		retryJitter: zeroRetryJitter,
		Refresh: func(context.Context, *Subscription) (json.RawMessage, error) {
			if calls.Add(1) == 1 {
				return nil, errors.New("first attempt fails")
			}
			return json.RawMessage(`{"v":"latest"}`), nil
		},
	}
	sub.Emit = func(frame Frame) { gotFrame = frame }

	first := Change{Etype: "posts", EntityID: "p1", AttrIDs: []string{"title"}}
	second := Change{Etype: "posts", EntityID: "p2", AttrIDs: []string{"title"}}
	n.NotifyChanges(context.Background(), "app", []Change{first}, 5)
	if !n.drainPass(context.Background(), slog.Default()) {
		t.Fatal("initial refresh was not scheduled")
	}
	if got := clock.ActiveTimers(); got != 1 {
		t.Fatalf("timers after first failure = %d, want 1", got)
	}

	// A newer known event and then an unknown invalidation update one timer's
	// state. Neither is allowed to trigger an immediate second attempt.
	n.NotifyChanges(context.Background(), "app", []Change{second}, 7)
	n.Notify(context.Background(), "app", []string{"title"}, 8)
	if got := calls.Load(); got != 1 {
		t.Fatalf("attempts before retry deadline = %d, want 1", got)
	}
	n.mu.Lock()
	state := n.retries[sub.ID]
	if state == nil || state.txID != 8 || state.changes != nil {
		n.mu.Unlock()
		t.Fatalf("retry state = %+v, want tx=8 and sticky unknown knowledge", state)
	}
	n.mu.Unlock()
	if got := clock.ActiveTimers(); got != 1 {
		t.Fatalf("timers after coalescing newer invalidations = %d, want 1", got)
	}

	clock.Advance(100 * time.Millisecond)
	if !n.drainPass(context.Background(), slog.Default()) {
		t.Fatal("latest retry was not drained")
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("attempts after latest retry = %d, want 2", got)
	}
	if got := sub.TxID.Load(); got != 8 {
		t.Fatalf("latest retry watermark = %d, want 8", got)
	}
	if string(gotFrame.ResultJSON) != `{"v":"latest"}` || gotFrame.ProcessedTxID != 8 {
		t.Fatalf("recovery frame = %+v, want latest result at tx 8", gotFrame)
	}
	if got := n.drainPass(context.Background(), slog.Default()); got {
		t.Fatal("successful recovery left another immediate refresh")
	}
}

func TestRetryChangeAccumulationFallsBackToUnknownAtCap(t *testing.T) {
	clock := &fakeRetryClock{}
	store, sub := newRetrySubscription(t, "retry-change-cap")
	t.Cleanup(func() { store.Remove(sub.ID) })
	n := &Notifier{
		Store:       store,
		clock:       clock,
		retryJitter: zeroRetryJitter,
		Refresh: func(context.Context, *Subscription) (json.RawMessage, error) {
			return nil, errors.New("persistent failure")
		},
	}
	first := Change{Etype: "posts", EntityID: "p-0", AttrIDs: []string{"title"}}
	n.NotifyChanges(context.Background(), "app", []Change{first}, 1)
	if !n.drainPass(context.Background(), slog.Default()) {
		t.Fatal("initial refresh was not scheduled")
	}
	if got := clock.ActiveTimers(); got != 1 {
		t.Fatalf("timers after first failure = %d, want 1", got)
	}

	// Persistent outages must bound retained change knowledge. Once the
	// cap is crossed, retry falls back to an unknown/full-refresh obligation,
	// and a later known suffix cannot make that obligation known again.
	for i := 1; i <= maxTrackedChanges; i++ {
		change := Change{
			Etype:    "posts",
			EntityID: fmt.Sprintf("p-%d", i),
			AttrIDs:  []string{"title"},
		}
		n.NotifyChanges(context.Background(), "app", []Change{change}, int64(i+1))
	}
	n.NotifyChanges(context.Background(), "app", []Change{{
		Etype: "posts", EntityID: "after-cap", AttrIDs: []string{"title"},
	}}, int64(maxTrackedChanges+2))
	n.mu.Lock()
	state := n.retries[sub.ID]
	if state == nil || state.changes != nil {
		n.mu.Unlock()
		t.Fatalf("retry state after cap = %+v, want sticky unknown knowledge", state)
	}
	n.mu.Unlock()
	if got := clock.ActiveTimers(); got != 1 {
		t.Fatalf("timers after capped change accumulation = %d, want 1", got)
	}
}

func TestRetryBackoffRemainsCappedUntilSuccess(t *testing.T) {
	clock := &fakeRetryClock{}
	store, sub := newRetrySubscription(t, "retry-cap-repeat")
	var calls atomic.Int32
	const failuresBeforeSuccess = 10
	n := &Notifier{
		Store:       store,
		clock:       clock,
		retryJitter: zeroRetryJitter,
		Refresh: func(context.Context, *Subscription) (json.RawMessage, error) {
			if calls.Add(1) <= failuresBeforeSuccess {
				return nil, errors.New("persistent failure")
			}
			return json.RawMessage(`{"v":"recovered"}`), nil
		},
	}
	n.Notify(context.Background(), "app", []string{"title"}, 1)
	if !n.drainPass(context.Background(), slog.Default()) {
		t.Fatal("initial refresh was not scheduled")
	}
	wantDelays := append(append([]time.Duration(nil), retryDelays[:]...), 5*time.Second, 5*time.Second, 5*time.Second)
	for i, want := range wantDelays {
		if got, ok := clock.NextDelay(); !ok || got != want {
			t.Fatalf("retry %d delay = %s (ok=%t), want %s", i+1, got, ok, want)
		}
		clock.Advance(want)
		if !n.drainPass(context.Background(), slog.Default()) {
			t.Fatalf("retry %d was not drained", i+1)
		}
	}
	if got := calls.Load(); got != failuresBeforeSuccess+1 {
		t.Fatalf("attempts through capped recovery = %d, want %d", got, failuresBeforeSuccess+1)
	}
	if got := sub.TxID.Load(); got != 1 {
		t.Fatalf("capped recovery watermark = %d, want 1", got)
	}
	if got := clock.ActiveTimers(); got != 0 {
		t.Fatalf("active timers after capped recovery = %d, want 0", got)
	}
}

func TestRetryFailureCannotMonopolizeSchedulerPass(t *testing.T) {
	previousWorkers := Workers
	Workers = 1
	t.Cleanup(func() { Workers = previousWorkers })
	clock := &fakeRetryClock{}
	store := NewStore()
	var broken, healthy atomic.Int32
	for _, id := range []string{"a-broken", "b-healthy"} {
		if _, err := store.Add(&Subscription{ID: id, AppID: "app", Topics: map[string]bool{"title": true}}); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		store.Remove("a-broken")
		store.Remove("b-healthy")
	})
	n := &Notifier{
		Store:       store,
		clock:       clock,
		retryJitter: zeroRetryJitter,
		Refresh: func(_ context.Context, sub *Subscription) (json.RawMessage, error) {
			switch sub.ID {
			case "a-broken":
				broken.Add(1)
				return nil, errors.New("broken subscription")
			default:
				healthy.Add(1)
				return json.RawMessage(`{"ok":true}`), nil
			}
		},
	}
	n.Notify(context.Background(), "app", []string{"title"}, 1)
	if !n.drainPass(context.Background(), slog.Default()) {
		t.Fatal("initial scheduler pass was idle")
	}
	if broken.Load() != 1 || healthy.Load() != 1 {
		t.Fatalf("one pass attempts = broken:%d healthy:%d, want 1 each", broken.Load(), healthy.Load())
	}
	if n.drainPass(context.Background(), slog.Default()) {
		t.Fatal("broken subscription monopolized a second immediate pass")
	}
}

func TestRetryUnsubscribeStopsPendingTimer(t *testing.T) {
	clock := &fakeRetryClock{}
	store, sub := newRetrySubscription(t, "retry-unsubscribe")
	var calls atomic.Int32
	n := &Notifier{
		Store:       store,
		clock:       clock,
		retryJitter: zeroRetryJitter,
		Refresh: func(context.Context, *Subscription) (json.RawMessage, error) {
			calls.Add(1)
			return nil, errors.New("persistent failure")
		},
	}
	n.Notify(context.Background(), "app", []string{"title"}, 1)
	n.drainPass(context.Background(), slog.Default())
	if got := clock.ActiveTimers(); got != 1 {
		t.Fatalf("timers before unsubscribe = %d, want 1", got)
	}
	if !store.Remove(sub.ID) {
		t.Fatal("unsubscribe failed")
	}
	if got := clock.ActiveTimers(); got != 0 {
		t.Fatalf("timers after unsubscribe = %d, want 0", got)
	}
	clock.Advance(10 * time.Second)
	if got := calls.Load(); got != 1 {
		t.Fatalf("attempts after unsubscribe = %d, want 1", got)
	}
}

func TestRetryCancellationWhileTimerPendingStopsRun(t *testing.T) {
	clock := &fakeRetryClock{}
	store, _ := newRetrySubscription(t, "retry-cancel-timer")
	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	n := &Notifier{
		Store:       store,
		clock:       clock,
		retryJitter: zeroRetryJitter,
		Refresh: func(context.Context, *Subscription) (json.RawMessage, error) {
			calls.Add(1)
			return nil, errors.New("persistent failure")
		},
	}
	n.Notify(ctx, "app", []string{"title"}, 1)
	if !n.drainPass(ctx, slog.Default()) {
		t.Fatal("initial refresh was not scheduled")
	}
	done := make(chan struct{})
	go func() { n.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not join promptly after parent cancellation")
	}
	clock.Advance(10 * time.Second)
	if got := calls.Load(); got != 1 {
		t.Fatalf("attempts after canceled pending timer = %d, want 1", got)
	}
	if got := clock.ActiveTimers(); got != 0 {
		t.Fatalf("active timers after parent cancellation = %d, want 0", got)
	}
}

func TestRetryCancellationWhileRefreshRunningStopsRun(t *testing.T) {
	clock := &fakeRetryClock{}
	store, _ := newRetrySubscription(t, "retry-cancel-running")
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	var calls atomic.Int32
	n := &Notifier{
		Store:       store,
		clock:       clock,
		retryJitter: zeroRetryJitter,
		Refresh: func(ctx context.Context, _ *Subscription) (json.RawMessage, error) {
			calls.Add(1)
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	n.Notify(ctx, "app", []string{"title"}, 1)
	done := make(chan struct{})
	go func() { n.Run(ctx); close(done) }()
	select {
	case <-started:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("refresh did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not join promptly after in-flight cancellation")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("attempts after in-flight cancellation = %d, want 1", got)
	}
	if got := clock.ActiveTimers(); got != 0 {
		t.Fatalf("active timers after in-flight cancellation = %d, want 0", got)
	}
}

func TestStableRetryJitterIsPerSubscriptionAndBounded(t *testing.T) {
	for _, id := range []string{"sub-a", "sub-b", "sub-c"} {
		for _, base := range []time.Duration{100 * time.Millisecond, time.Second, 5 * time.Second} {
			first := stableRetryJitter(id, base)
			if second := stableRetryJitter(id, base); second != first {
				t.Fatalf("jitter for %q/%s changed from %s to %s", id, base, first, second)
			}
			if first < base*4/5 || first > 5*time.Second {
				t.Fatalf("jitter for %q/%s = %s outside [80%% base, 5s]", id, base, first)
			}
			if base < 5*time.Second && first > base*6/5 {
				t.Fatalf("jitter for %q/%s = %s outside +20%% bound", id, base, first)
			}
		}
	}
}
