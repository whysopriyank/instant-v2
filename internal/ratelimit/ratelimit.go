// Package ratelimit implements the local-first token-bucket limiter that
// guards instantd's HTTP surface and non-HTTP loops (docs/03 §9: limits are
// enforced per app-id, per traffic class, in-process — no shared store).
//
// Buckets are keyed by (app-id, class) where class ∈ {ws, transact, auth,
// storage}. Each bucket refills continuously at its class rate up to the
// class burst; Acquire either consumes one token or reports the duration
// until one is available (surfaced as Retry-After by HTTPMiddleware).
//
// Defaults: v1's exact numbers were not recoverable for this rewrite
// (V1_REF is a bare commit hash; server/src not vendored), so the defaults
// below are documented defensible picks sized from the v1 semantics we do
// mirror elsewhere ($rateLimits rule-doc entries are greedy token buckets —
// internal/perms/ruledoc.go):
//
//	ws        100 req/s, burst 200  (session chatter: add/remove-query, presence)
//	transact   50 req/s, burst 100  (write path; matches v1-style headroom)
//	auth       10 req/s, burst  20  (magic-code / refresh endpoints are costly)
//	storage    20 req/s, burst  40  (upload/download signed flows)
package ratelimit

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/instant-v2/instant-v2/internal/metrics"
)

// Class enumerates the frozen traffic classes.
type Class string

const (
	ClassWS       Class = "ws"
	ClassTransact Class = "transact"
	ClassAuth     Class = "auth"
	ClassStorage  Class = "storage"
)

// ClassConfig is one class's refill rate (tokens/second) and burst capacity.
type ClassConfig struct {
	Rate  float64 // tokens added per second
	Burst int     // bucket depth; also the instantaneous allowance
}

// Config holds per-class settings. Classes absent from a custom Config fall
// back to their DefaultConfig value, so callers may override one class
// without restating the others.
type Config struct {
	Classes map[Class]ClassConfig
	// MaxBuckets caps the bucket map; zero applies DefaultMaxBuckets. When
	// the cap is hit, idle buckets are evicted first. Under DEC-001 (bounded
	// fail-closed overflow), requests needing a brand-new key when the cap
	// is saturated and no bucket is evictable are denied with a bounded
	// retry duration to prevent untracked rate-limit bypass.
	MaxBuckets int
	// IdleTTL is how long an untouched bucket survives eviction; zero
	// applies DefaultIdleTTL.
	IdleTTL time.Duration
}

const (
	DefaultMaxBuckets = 100_000
	DefaultIdleTTL    = 10 * time.Minute
	maxOverflowRetry  = time.Minute
)

// DefaultConfig returns the documented defaults (see package comment).
func DefaultConfig() Config {
	return Config{Classes: map[Class]ClassConfig{
		ClassWS:       {Rate: 100, Burst: 200},
		ClassTransact: {Rate: 50, Burst: 100},
		ClassAuth:     {Rate: 10, Burst: 20},
		ClassStorage:  {Rate: 20, Burst: 40},
	}}
}

// resolved merges cfg over DefaultConfig.
func (c Config) resolved() map[Class]ClassConfig {
	def := DefaultConfig().Classes
	out := make(map[Class]ClassConfig, len(def))
	for k, v := range def {
		out[k] = v
	}
	for k, v := range c.Classes {
		out[k] = v
	}
	return out
}

// Error is the denial returned by Acquire; RetryAfter is how long until the
// bucket can admit one request.
type Error struct {
	AppID      string
	Class      Class
	RetryAfter time.Duration
}

func (e *Error) Error() string {
	return fmt.Sprintf("ratelimit: app %s class %s limited; retry after %s",
		e.AppID, e.Class, e.RetryAfter.Round(time.Millisecond))
}

type bucketKey struct {
	appID string
	class Class
}

type bucket struct {
	tokens float64
	last   time.Time // last refill timestamp
}

// Limiter is the in-process token-bucket set. Safe for concurrent use.
//
// Locking: buckets are striped across limiterShards shards keyed by a hash of
// (appID, class). The previous single-mutex design serialized every Allow
// call process-wide — one lock contended by every WS frame, SSE message and
// HTTP request across all apps. Existing buckets now lock only their own
// shard; new-bucket admission additionally takes the reservation mutex so
// the global cap remains atomic. The cap counter is read lock-free.
type Limiter struct {
	// admissionMu serializes only new-bucket reservations and global idle
	// eviction. Existing buckets remain protected by their own shard mutex.
	// Every path that acquires a shard while holding admissionMu does so in
	// this direction; no path acquires admissionMu while holding a shard.
	admissionMu sync.Mutex
	cfg         map[Class]ClassConfig
	shards      [limiterShards]limiterShard
	now         func() time.Time // swappable for tests
	maxBuckets  int
	idleTTL     time.Duration
	total       atomic.Int64 // tracked buckets across all shards
}

// limiterShards is the stripe count (power of two, so shardFor masks).
const limiterShards = 64

type limiterShard struct {
	mu      sync.Mutex
	buckets map[bucketKey]*bucket
}

// shardFor hashes a bucket key to its stripe (alloc-free FNV-1a).
func shardFor(key bucketKey) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(key.appID); i++ {
		h ^= uint32(key.appID[i])
		h *= prime32
	}
	h ^= uint32(len(key.appID)) // separator without an allocation
	h *= prime32
	for i := 0; i < len(key.class); i++ {
		h ^= uint32(key.class[i])
		h *= prime32
	}
	return h & (limiterShards - 1)
}

// New builds a Limiter from cfg (missing classes inherit defaults).
func New(cfg Config) *Limiter {
	maxB := cfg.MaxBuckets
	if maxB <= 0 {
		maxB = DefaultMaxBuckets
	}
	ttl := cfg.IdleTTL
	if ttl <= 0 {
		ttl = DefaultIdleTTL
	}
	l := &Limiter{
		cfg:        cfg.resolved(),
		now:        time.Now,
		maxBuckets: maxB,
		idleTTL:    ttl,
	}
	for i := range l.shards {
		l.shards[i].buckets = map[bucketKey]*bucket{}
	}
	return l
}

// SweepLoop evicts idle buckets every `every` until ctx is canceled. Without
// it, eviction still happens lazily when the bucket cap is hit; the loop
// reclaims memory from long quiet periods proactively (audit H4).
func (l *Limiter) SweepLoop(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			l.evictAll(l.now())
		}
	}
}

// evictShardLocked drops buckets idle longer than idleTTL in one shard.
// Caller holds sh.mu.
func (l *Limiter) evictShardLocked(sh *limiterShard, now time.Time) {
	for k, b := range sh.buckets {
		if now.Sub(b.last) > l.idleTTL {
			delete(sh.buckets, k)
			l.total.Add(-1)
		}
	}
}

// evictAll drops idle buckets across every shard. The admission mutex gives
// this operation the same global reservation exclusion as new-bucket paths.
func (l *Limiter) evictAll(now time.Time) {
	l.admissionMu.Lock()
	l.evictAllLocked(now)
	l.admissionMu.Unlock()
}

// evictAllLocked drops idle buckets across every shard. Caller holds
// admissionMu and no shard mutex.
func (l *Limiter) evictAllLocked(now time.Time) {
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.Lock()
		l.evictShardLocked(sh, now)
		sh.mu.Unlock()
	}
}

// nextEvictionRetry returns an advisory delay until the earliest tracked
// bucket can become idle. Caller holds admissionMu; shard locks are acquired
// only here, in the same direction as evictAllLocked. The result is bounded
// because HTTP Retry-After is an advisory value, not a promise that another
// request will arrive exactly at the eviction boundary.
func (l *Limiter) nextEvictionRetry(now time.Time) time.Duration {
	best := l.idleTTL
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.Lock()
		for _, b := range sh.buckets {
			remaining := l.idleTTL - now.Sub(b.last)
			if remaining < best {
				best = remaining
			}
		}
		sh.mu.Unlock()
	}
	if best <= 0 {
		return time.Millisecond
	}
	if best > maxOverflowRetry {
		return maxOverflowRetry
	}
	return best
}

// lenBuckets reports tracked buckets across all shards (test helper).
func (l *Limiter) lenBuckets() int {
	n := 0
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.Lock()
		n += len(sh.buckets)
		sh.mu.Unlock()
	}
	return n
}

// bucketAppIDs lists the app ids of every tracked bucket (test helper).
func (l *Limiter) bucketAppIDs() map[string]struct{} {
	out := map[string]struct{}{}
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.Lock()
		for k := range sh.buckets {
			out[k.appID] = struct{}{}
		}
		sh.mu.Unlock()
	}
	return out
}

// Allow attempts to take one token for (appID, class). It returns the retry
// delay on denial. Unknown classes degrade to the transact budget so a
// misconfigured classifier cannot bypass limiting entirely.
func (l *Limiter) Allow(appID string, class Class) (ok bool, retryAfter time.Duration) {
	cc, known := l.cfg[class]
	if !known {
		cc, _ = l.cfg[ClassTransact]
	}
	if cc.Burst <= 0 || cc.Rate <= 0 {
		// Degenerate config: deny hard rather than divide by zero.
		metrics.RateLimitRejections.WithLabelValues(string(class)).Inc()
		return false, time.Minute
	}

	key := bucketKey{appID: appID, class: class}
	sh := &l.shards[shardFor(key)]
	sh.mu.Lock()
	now := l.now()
	b, okb := sh.buckets[key]
	if !okb {
		// A new key must release its shard before taking admissionMu. This
		// avoids the old cross-shard lock inversion. Recheck after acquiring
		// the reservation because another caller may have created this key.
		sh.mu.Unlock()
		l.admissionMu.Lock()
		sh.mu.Lock()
		now = l.now()
		b, okb = sh.buckets[key]
		if !okb && l.total.Load() >= int64(l.maxBuckets) {
			// evictAllLocked takes shard locks in one direction while no
			// caller holds a shard lock, so concurrent admissions cannot
			// form a cycle here.
			sh.mu.Unlock()
			l.evictAllLocked(now)
			sh.mu.Lock()
			b, okb = sh.buckets[key]
		}
		if !okb {
			if l.total.Load() >= int64(l.maxBuckets) {
				// Cap reached with nothing evictable: fail-closed under DEC-001.
				// Deny with bounded retry duration to prevent untracked new-key bypass.
				sh.mu.Unlock()
				retryAfter := l.nextEvictionRetry(now)
				l.admissionMu.Unlock()
				metrics.RateLimitRejections.WithLabelValues(string(class)).Inc()
				return false, retryAfter
			}
			b = &bucket{tokens: float64(cc.Burst), last: now}
			sh.buckets[key] = b
			l.total.Add(1)
		}
		l.admissionMu.Unlock()
	}
	if okb {
		elapsed := now.Sub(b.last)
		if elapsed > 0 {
			b.tokens = math.Min(float64(cc.Burst), b.tokens+elapsed.Seconds()*cc.Rate)
			b.last = now
		}
	}
	if b.tokens >= 1 {
		b.tokens--
		sh.mu.Unlock()
		return true, 0
	}
	deficit := 1 - b.tokens
	sh.mu.Unlock()
	metrics.RateLimitRejections.WithLabelValues(string(class)).Inc()
	return false, time.Duration(deficit / cc.Rate * float64(time.Second))
}

// Acquire is the non-HTTP entry point (WS transact loop, background jobs).
// It honors ctx cancellation while waiting nothing — the decision is
// instantaneous — so it only propagates a canceled context.
func (l *Limiter) Acquire(ctx context.Context, appID string, class Class) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ok, retryAfter := l.Allow(appID, class)
	if ok {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return &Error{AppID: appID, Class: class, RetryAfter: retryAfter}
}

// HTTPMiddleware wraps next with per-(key,class) limiting. classify maps a
// request to its app-id key and traffic class; returning an empty key skips
// limiting (e.g. unauthenticated health checks). Denials become 429 responses
// carrying Retry-After (whole seconds, minimum 1).
func HTTPMiddleware(next http.Handler, limiter *Limiter, classify func(*http.Request) (key string, class Class)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, class := classify(r)
		if key != "" {
			if err := limiter.Acquire(r.Context(), key, class); err != nil {
				var rlErr *Error
				retrySecs := 1
				if asErr(err, &rlErr) && rlErr.RetryAfter > 0 {
					retrySecs = int(math.Ceil(rlErr.RetryAfter.Seconds()))
					if retrySecs < 1 {
						retrySecs = 1
					}
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", strconv.Itoa(retrySecs))
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = fmt.Fprintf(w, `{"error":"rate limited","class":%q,"retry-after":%d}`+"\n", class, retrySecs)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func asErr(err error, target **Error) bool {
	e, ok := err.(*Error)
	if ok {
		*target = e
	}
	return ok
}
