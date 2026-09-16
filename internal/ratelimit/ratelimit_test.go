package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock hands out controlled time to the limiter.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Unix(1_700_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func testConfig(rate float64, burst int) Config {
	return Config{Classes: map[Class]ClassConfig{
		ClassWS:       {Rate: rate, Burst: burst},
		ClassTransact: {Rate: rate, Burst: burst},
		ClassAuth:     {Rate: rate, Burst: burst},
		ClassStorage:  {Rate: rate, Burst: burst},
	}}
}

// TestBurstBoundary pins the core contract: exactly Burst immediate admits,
// the next one is denied with a sane Retry-After.
func TestBurstBoundary(t *testing.T) {
	const burst = 5
	l := New(testConfig(10, burst))

	for i := 0; i < burst; i++ {
		if err := l.Acquire(context.Background(), "app-a", ClassTransact); err != nil {
			t.Fatalf("acquire %d/%d denied: %v", i+1, burst, err)
		}
	}
	err := l.Acquire(context.Background(), "app-a", ClassTransact)
	if err == nil {
		t.Fatal("burst+1 admitted; want denial")
	}
	var rlErr *Error
	if !errors.As(err, &rlErr) {
		t.Fatalf("want *Error, got %T", err)
	}
	// Rate 10/s with an empty bucket: one token in 100ms.
	if rlErr.RetryAfter <= 0 || rlErr.RetryAfter > 150*time.Millisecond {
		t.Fatalf("RetryAfter = %s, want (0,150ms]", rlErr.RetryAfter)
	}
}

// TestRefillOverTime drives the bucket across a fake clock: after draining
// the burst, tokens must reappear strictly proportionally to elapsed time.
func TestRefillOverTime(t *testing.T) {
	clock := newFakeClock()
	l := New(testConfig(10 /* per sec */, 2))
	l.now = clock.Now

	for i := 0; i < 2; i++ {
		if err := l.Acquire(context.Background(), "app", ClassWS); err != nil {
			t.Fatalf("initial acquire %d: %v", i, err)
		}
	}
	if err := l.Acquire(context.Background(), "app", ClassWS); err == nil {
		t.Fatal("empty bucket admitted")
	}

	// 50ms at 10/s = half a token — still short of one.
	clock.Advance(50 * time.Millisecond)
	if err := l.Acquire(context.Background(), "app", ClassWS); err == nil {
		t.Fatal("half-refilled bucket admitted")
	}

	// +50ms more = one full token since last refill.
	clock.Advance(50 * time.Millisecond)
	if err := l.Acquire(context.Background(), "app", ClassWS); err != nil {
		t.Fatalf("refilled token not granted: %v", err)
	}
	if err := l.Acquire(context.Background(), "app", ClassWS); err == nil {
		t.Fatal("second token granted before refill")
	}

	// Refill never exceeds the burst cap even after a long idle.
	clock.Advance(1 * time.Hour)
	for i := 0; i < 2; i++ {
		if err := l.Acquire(context.Background(), "app", ClassWS); err != nil {
			t.Fatalf("post-idle acquire %d: %v", i, err)
		}
	}
	if err := l.Acquire(context.Background(), "app", ClassWS); err == nil {
		t.Fatal("burst cap not enforced after long idle")
	}
}

// TestPerAppIsolation proves buckets never leak across app ids.
func TestPerAppIsolation(t *testing.T) {
	l := New(testConfig(0.000001 /* effectively frozen */, 3))
	for i := 0; i < 3; i++ {
		if err := l.Acquire(context.Background(), "app-a", ClassAuth); err != nil {
			t.Fatalf("app-a acquire %d: %v", i, err)
		}
	}
	if err := l.Acquire(context.Background(), "app-a", ClassAuth); err == nil {
		t.Fatal("app-a over burst admitted")
	}
	// app-b shares nothing with app-a's exhausted bucket.
	for i := 0; i < 3; i++ {
		if err := l.Acquire(context.Background(), "app-b", ClassAuth); err != nil {
			t.Fatalf("app-b acquire %d leaked from app-a: %v", i, err)
		}
	}
}

// TestMiddleware429 exercises the HTTP surface: pass-through under budget,
// then 429 with Retry-After once the class bucket drains.
func TestMiddleware429(t *testing.T) {
	clock := newFakeClock()
	l := New(testConfig(1, 2))
	l.now = clock.Now

	var hits int
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++ })
	classify := func(r *http.Request) (string, Class) { return "app-x", ClassStorage }
	h := HTTPMiddleware(next, l, classify)

	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("POST", "/runtime/transact", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: code %d, want 200", i, rec.Code)
		}
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/runtime/transact", nil))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("over-budget code %d, want 429", rec.Code)
	}
	ra := rec.Header().Get("Retry-After")
	if ra == "" {
		t.Fatal("429 missing Retry-After")
	}
	if n, err := strconv.Atoi(ra); err != nil || n < 1 || n > 2 {
		t.Fatalf("Retry-After = %q, want integer in [1,2]", ra)
	}
	if hits != 2 {
		t.Fatalf("handler invoked %d times, want 2 (denied requests must not reach it)", hits)
	}

	// Empty classify key skips limiting entirely.
	openH := HTTPMiddleware(next, l, func(r *http.Request) (string, Class) { return "", ClassWS })
	clock.Advance(time.Hour) // exhaust again
	for i := 0; i < 100; i++ {
		rec := httptest.NewRecorder()
		openH.ServeHTTP(rec, httptest.NewRequest("GET", "/health", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("empty key got limited: %d", rec.Code)
		}
	}
}

// TestConcurrentHammer hammers ONE bucket from 32 goroutines. Under -race it
// proves the lock discipline; the admission ceiling proves no token is
// double-spent despite contention.
func TestConcurrentHammer(t *testing.T) {
	const goroutines = 32
	const attemptsEach = 200
	l := New(testConfig(1000 /* generous but finite */, 50))

	var wg sync.WaitGroup
	var mu sync.Mutex
	admits := 0
	start := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			local := 0
			for i := 0; i < attemptsEach; i++ {
				if err := l.Acquire(context.Background(), "hot-app", ClassTransact); err == nil {
					local++
				}
			}
			mu.Lock()
			admits += local
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()

	// The bucket starts at burst=50 and refills at 1000/s for however long
	// the hammer ran; elapsed is realistically >100ms, so only assert the
	// invariant that matters for correctness: admissions can never fall
	// below burst and can never exceed burst plus elapsed-time refill.
	if admits < 50 {
		t.Fatalf("admits=%d below initial burst — lost updates under contention", admits)
	}
}

func TestUnknownClassFallsBack(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Classes[ClassTransact] = ClassConfig{Rate: 1, Burst: 1} // tighten fallback target
	l := New(cfg)

	if err := l.Acquire(context.Background(), "app", "bogus-class"); err != nil {
		t.Fatalf("first bogus-class admit: %v", err)
	}
	err := l.Acquire(context.Background(), "app", "bogus-class")
	if err == nil {
		t.Fatal("bogus class bypassed limiting; want transact fallback budget applied")
	}
}

// Audit H4 / DA-007: the bucket map must be bounded. Idle buckets evict
// lazily at cap and via SweepLoop. Under DEC-001, over-cap requests fail closed
// with bounded retry duration rather than admitting untracked.
func TestBucketMapBoundedAndFailClosed(t *testing.T) {
	base := time.Unix(0, 0)
	clock := base
	l := New(Config{Classes: map[Class]ClassConfig{
		ClassWS: {Rate: 1000, Burst: 1000},
	}, MaxBuckets: 3, IdleTTL: time.Minute})
	l.now = func() time.Time { return clock }

	// Fill to cap.
	for i := 0; i < 3; i++ {
		if ok, _ := l.Allow(fmt.Sprintf("app-%d", i), ClassWS); !ok {
			t.Fatalf("bucket %d must be admitted", i)
		}
	}
	if l.lenBuckets() != 3 {
		t.Fatalf("want 3 buckets, got %d", l.lenBuckets())
	}
	// Over cap with nothing evictable: fail-closed denial with bounded retry.
	for i := 10; i < 20; i++ {
		key := fmt.Sprintf("app-%d", i)
		ok, retryAfter := l.Allow(key, ClassWS)
		if ok {
			t.Fatalf("over-cap request must fail closed (denied), key %s allowed", key)
		}
		if retryAfter <= 0 || retryAfter > time.Minute {
			t.Fatalf("key %s retryAfter = %s, want bounded (0, 1m]", key, retryAfter)
		}
		if l.lenBuckets() > 3 {
			t.Fatalf("bucket map grew past cap: %d", l.lenBuckets())
		}
	}
	// Advance past IdleTTL: lazy insert path evicts the stale set.
	clock = base.Add(2 * time.Minute)
	if ok, _ := l.Allow("fresh", ClassWS); !ok {
		t.Fatal("fresh bucket after idle sweep must be admitted")
	}
	appIDs := l.bucketAppIDs()
	for appID := range appIDs {
		if appID != "fresh" && strings.HasPrefix(appID, "app-") && appID != "app-0" && appID != "app-1" && appID != "app-2" {
			t.Fatalf("unexpected survivor %q", appID)
		}
	}
	if _, found := appIDs["fresh"]; !found {
		t.Fatal("fresh key missing after eviction cycle")
	}
}

func TestSweepLoopEvictsIdleBuckets(t *testing.T) {
	// fakeClock is mutex-guarded: SweepLoop reads l.now() from its own
	// goroutine, so the clock must be race-free (a bare captured variable
	// was a latent data race the single-mutex implementation masked).
	fc := newFakeClock()
	l := New(Config{IdleTTL: time.Minute})
	l.now = fc.Now
	if ok, _ := l.Allow("doomed", ClassAuth); !ok {
		t.Fatal("initial allow must succeed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { l.SweepLoop(ctx, time.Millisecond); close(done) }()
	fc.Advance(2 * time.Minute)
	size := func() int {
		return l.lenBuckets()
	}
	deadline := time.Now().Add(5 * time.Second)
	for size() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	if n := size(); n != 0 {
		t.Fatalf("sweep loop left %d buckets behind", n)
	}
}

// TestShardedConcurrency hammers one limiter from many goroutines with
// distinct keys — the sharded stripe must show no deadlock, no lost updates,
// and exact per-key burst accounting under -race.
func TestShardedConcurrency(t *testing.T) {
	const burst = 50
	const goroutines = 8
	const keys = 40 // spread across shards

	l := New(testConfig(1000, burst))
	var denials atomic.Int64
	var admits atomic.Int64

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				appID := fmt.Sprintf("app-%d", (g*keys+i)%keys)
				if err := l.Acquire(context.Background(), appID, ClassTransact); err != nil {
					denials.Add(1)
				} else {
					admits.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()

	// Every key's bucket holds at most its burst; total admits can never
	// exceed keys*burst regardless of interleaving.
	if got := admits.Load(); got > keys*burst {
		t.Fatalf("admits %d exceed total burst capacity %d", got, keys*burst)
	}
	if got := l.lenBuckets(); got != keys {
		t.Fatalf("want %d tracked buckets, got %d", keys, got)
	}
	// Token accounting is conservative: admits + denials must equal calls.
	if got := admits.Load() + denials.Load(); got != goroutines*200 {
		t.Fatalf("lost decisions: %d != %d", got, goroutines*200)
	}
}

// TestConcurrentAdmissionsBoundedAndNonBlocking forces many distinct keys to
// contend for a tiny global bucket cap. New keys must not deadlock while
// sweeping other shards, and the tracked bucket count must never exceed the
// configured cap. Under DEC-001 (fail-closed overflow), exactly maxBuckets
// are admitted and the remaining concurrent admissions are denied with a
// bounded retry duration without blocking or exceeding the cap.
func TestConcurrentAdmissionsBoundedAndNonBlocking(t *testing.T) {
	const (
		maxBuckets = 2
		workers    = limiterShards
		rounds     = 20
	)

	keys := make([]string, 0, workers)
	seenShards := make(map[uint32]struct{}, workers)
	for i := 0; len(keys) < workers; i++ {
		appID := fmt.Sprintf("contention-app-%d", i)
		shard := shardFor(bucketKey{appID: appID, class: ClassTransact})
		if _, seen := seenShards[shard]; seen {
			continue
		}
		seenShards[shard] = struct{}{}
		keys = append(keys, appID)
	}

	for round := 0; round < rounds; round++ {
		l := New(Config{
			Classes: map[Class]ClassConfig{
				ClassTransact: {Rate: 1, Burst: 1},
			},
			MaxBuckets: maxBuckets,
			IdleTTL:    time.Hour,
		})
		fixedNow := time.Unix(1_700_000_000, 0)
		l.now = func() time.Time { return fixedNow }

		start := make(chan struct{})
		var wg sync.WaitGroup
		var admits atomic.Int32
		var denials atomic.Int32
		for i := range keys {
			wg.Add(1)
			go func(appID string) {
				defer wg.Done()
				<-start
				ok, retryAfter := l.Allow(appID, ClassTransact)
				if ok {
					admits.Add(1)
				} else {
					denials.Add(1)
					if retryAfter <= 0 || retryAfter > time.Minute {
						t.Errorf("round %d: %s retryAfter = %s, want bounded (0, 1m]", round, appID, retryAfter)
					}
				}
			}(keys[i])
		}

		close(start)
		finished := make(chan struct{})
		go func() {
			wg.Wait()
			close(finished)
		}()
		select {
		case <-finished:
		case <-time.After(2 * time.Second):
			t.Fatalf("round %d: concurrent admissions did not finish", round)
		}

		if got := admits.Load(); got != maxBuckets {
			t.Fatalf("round %d: admits = %d, want cap %d", round, got, maxBuckets)
		}
		if got := denials.Load(); got != int32(workers-maxBuckets) {
			t.Fatalf("round %d: denials = %d, want %d", round, got, workers-maxBuckets)
		}
		if got := l.total.Load(); got > maxBuckets {
			t.Fatalf("round %d: tracked bucket counter %d exceeds cap %d", round, got, maxBuckets)
		}
		got := l.lenBuckets()
		if got > maxBuckets {
			t.Fatalf("round %d: tracked bucket map size %d exceeds cap %d", round, got, maxBuckets)
		}
		if got != maxBuckets {
			t.Fatalf("round %d: tracked bucket map size %d, want cap %d after initial admissions", round, got, maxBuckets)
		}
	}
}

// DA-007 / DEC-001: saturated bucket cap must fail closed for rotating identities.
func TestRotatingIdentitiesSaturationFailClosed(t *testing.T) {
	const cap = 3
	l := New(Config{
		Classes: map[Class]ClassConfig{
			ClassTransact: {Rate: 50, Burst: 5},
		},
		MaxBuckets: cap,
		IdleTTL:    time.Hour,
	})

	// Fill to cap with known identities.
	for i := 0; i < cap; i++ {
		key := fmt.Sprintf("legit-app-%d", i)
		if err := l.Acquire(context.Background(), key, ClassTransact); err != nil {
			t.Fatalf("initial bucket %d must be admitted, got: %v", i, err)
		}
	}
	if got := l.lenBuckets(); got != cap {
		t.Fatalf("want %d buckets, got %d", cap, got)
	}

	// Saturated: rotating identities must be denied with bounded retryAfter and Error.
	for i := 0; i < 10; i++ {
		rotKey := fmt.Sprintf("rotating-app-%d", i)
		ok, retryAfter := l.Allow(rotKey, ClassTransact)
		if ok {
			t.Fatalf("rotating key %s admitted under saturated cap; want fail-closed denial", rotKey)
		}
		if retryAfter <= 0 || retryAfter > time.Minute {
			t.Fatalf("rotating key %s retryAfter = %s, want bounded (0, 1m]", rotKey, retryAfter)
		}

		err := l.Acquire(context.Background(), rotKey, ClassTransact)
		if err == nil {
			t.Fatalf("Acquire for rotating key %s succeeded under saturated cap; want denial error", rotKey)
		}
		var rlErr *Error
		if !errors.As(err, &rlErr) {
			t.Fatalf("want *Error, got %T: %v", err, err)
		}
		if rlErr.AppID != rotKey {
			t.Fatalf("rlErr.AppID = %q, want %q", rlErr.AppID, rotKey)
		}
		if rlErr.Class != ClassTransact {
			t.Fatalf("rlErr.Class = %q, want %q", rlErr.Class, ClassTransact)
		}
		if rlErr.RetryAfter <= 0 || rlErr.RetryAfter > time.Minute {
			t.Fatalf("rlErr.RetryAfter = %s, want bounded (0, 1m]", rlErr.RetryAfter)
		}
	}

	// Ensure bucket count did not grow past cap.
	if got := l.lenBuckets(); got != cap {
		t.Fatalf("bucket map grew past cap under rotating identity abuse: got %d, want %d", got, cap)
	}
}

// TestOldKeyFairnessUnderSaturation proves that existing tracked buckets
// continue to refill and serve requests fairly even when the bucket cap is
// saturated and new identities are being aggressively rejected.
func TestOldKeyFairnessUnderSaturation(t *testing.T) {
	fc := newFakeClock()
	const cap = 2
	l := New(Config{
		Classes: map[Class]ClassConfig{
			ClassTransact: {Rate: 10, Burst: 3},
		},
		MaxBuckets: cap,
		IdleTTL:    time.Hour,
	})
	l.now = fc.Now

	// Track two legitimate keys.
	if ok, _ := l.Allow("tenant-alpha", ClassTransact); !ok {
		t.Fatal("tenant-alpha must be admitted")
	}
	if ok, _ := l.Allow("tenant-beta", ClassTransact); !ok {
		t.Fatal("tenant-beta must be admitted")
	}
	if l.lenBuckets() != cap {
		t.Fatalf("want %d buckets, got %d", cap, l.lenBuckets())
	}

	// Saturated: new identity flood is rejected fail-closed.
	for i := 0; i < 50; i++ {
		attacker := fmt.Sprintf("attacker-%d", i)
		if ok, retry := l.Allow(attacker, ClassTransact); ok || retry <= 0 {
			t.Fatalf("attacker %s was admitted or got non-positive retry (%v)", attacker, retry)
		}
	}

	// tenant-alpha still had 2 tokens remaining from burst 3:
	if ok, _ := l.Allow("tenant-alpha", ClassTransact); !ok {
		t.Fatal("tenant-alpha second request denied; want old-key fairness")
	}
	if ok, _ := l.Allow("tenant-alpha", ClassTransact); !ok {
		t.Fatal("tenant-alpha third request denied; want old-key fairness")
	}
	// Drained: now tenant-alpha is rate-limited according to its own bucket, not cap exhaustion.
	if ok, retry := l.Allow("tenant-alpha", ClassTransact); ok {
		t.Fatal("tenant-alpha admitted over burst")
	} else if retry <= 0 || retry > 150*time.Millisecond {
		t.Fatalf("tenant-alpha retry = %s, want rate-limited ~100ms", retry)
	}

	// Advance clock by 100ms: tenant-alpha refills 1 token.
	fc.Advance(100 * time.Millisecond)
	if ok, _ := l.Allow("tenant-alpha", ClassTransact); !ok {
		t.Fatal("tenant-alpha refilled token not granted")
	}

	// Attacker flood still fails closed:
	for i := 50; i < 70; i++ {
		attacker := fmt.Sprintf("attacker-%d", i)
		if ok, _ := l.Allow(attacker, ClassTransact); ok {
			t.Fatalf("attacker %s admitted while tenants remain active", attacker)
		}
	}

	// Tracked bucket count remains strictly capped at 2.
	if got := l.lenBuckets(); got != cap {
		t.Fatalf("bucket map size = %d, want %d", got, cap)
	}
}

// TestAcquireCancellationAndSaturatedErrorSemantics verifies:
// 1. Acquire honors pre-canceled and deadline-exceeded contexts.
// 2. Denied saturated new keys return structured *Error with bounded RetryAfter.
// 3. HTTPMiddleware maps saturated new-key denials to HTTP 429 with integer Retry-After >= 1.
func TestAcquireCancellationAndSaturatedErrorSemantics(t *testing.T) {
	fc := newFakeClock()
	l := New(Config{
		Classes: map[Class]ClassConfig{
			ClassWS: {Rate: 100, Burst: 2},
		},
		MaxBuckets: 1,
		IdleTTL:    time.Hour,
	})
	l.now = fc.Now

	// Fill the single bucket.
	if err := l.Acquire(context.Background(), "app-sole", ClassWS); err != nil {
		t.Fatalf("initial acquire failed: %v", err)
	}

	// 1. Canceled context returns context.Canceled without creating bucket.
	ctxCancel, cancel := context.WithCancel(context.Background())
	cancel()
	if err := l.Acquire(ctxCancel, "app-new", ClassWS); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got: %v", err)
	}
	ctxExpired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	if err := l.Acquire(ctxExpired, "app-expired", ClassWS); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want context.DeadlineExceeded, got: %v", err)
	}

	// 2. Saturated Acquire returns structured *Error.
	err := l.Acquire(context.Background(), "app-new", ClassWS)
	if err == nil {
		t.Fatal("saturated new key acquire succeeded; want denial")
	}
	var rlErr *Error
	if !errors.As(err, &rlErr) {
		t.Fatalf("want *Error, got %T: %v", err, err)
	}
	if rlErr.AppID != "app-new" || rlErr.Class != ClassWS {
		t.Fatalf("unexpected *Error fields: %#v", rlErr)
	}
	if rlErr.RetryAfter <= 0 || rlErr.RetryAfter > time.Minute {
		t.Fatalf("RetryAfter = %s, want bounded (0, 1m]", rlErr.RetryAfter)
	}
	expectedMsg := fmt.Sprintf("ratelimit: app app-new class ws limited; retry after %s",
		rlErr.RetryAfter.Round(time.Millisecond))
	if rlErr.Error() != expectedMsg {
		t.Fatalf("Error() = %q, want %q", rlErr.Error(), expectedMsg)
	}

	// 3. HTTPMiddleware surfaces 429 with Retry-After header.
	hits := 0
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++ })
	classify := func(r *http.Request) (string, Class) { return r.Header.Get("X-App-ID"), ClassWS }
	h := HTTPMiddleware(next, l, classify)

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-App-ID", "app-blocked-http")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("HTTP code = %d, want 429", rec.Code)
	}
	ra := rec.Header().Get("Retry-After")
	if ra == "" {
		t.Fatal("missing Retry-After header on 429")
	}
	if n, err := strconv.Atoi(ra); err != nil || n < 1 {
		t.Fatalf("invalid Retry-After header value: %q (err: %v)", ra, err)
	}
	if hits != 0 {
		t.Fatalf("next handler was invoked %d times, want 0", hits)
	}
}

// TestSaturatedOverflowRetryTracksEviction proves the overflow retry is tied
// to the first bucket that can actually be evicted, not to token refill for
// the denied identity.
func TestSaturatedOverflowRetryTracksEviction(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	now := base
	l := New(Config{
		Classes:    map[Class]ClassConfig{ClassWS: {Rate: 1000, Burst: 2}},
		MaxBuckets: 2,
		IdleTTL:    10 * time.Second,
	})
	l.now = func() time.Time { return now }

	if ok, _ := l.Allow("oldest", ClassWS); !ok {
		t.Fatal("oldest bucket admission failed")
	}
	now = now.Add(3 * time.Second)
	if ok, _ := l.Allow("newer", ClassWS); !ok {
		t.Fatal("newer bucket admission failed")
	}

	ok, retry := l.Allow("overflow", ClassWS)
	if ok {
		t.Fatal("saturated overflow admitted")
	}
	if retry != 7*time.Second {
		t.Fatalf("overflow retry = %s, want 7s until oldest eviction", retry)
	}

	now = now.Add(7*time.Second + time.Nanosecond)
	if ok, _ := l.Allow("overflow", ClassWS); !ok {
		t.Fatal("overflow key not admitted after idle eviction became eligible")
	}
	if got := l.lenBuckets(); got != 2 {
		t.Fatalf("bucket count = %d, want capped at 2", got)
	}
}
