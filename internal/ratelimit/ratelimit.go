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
}

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
type Limiter struct {
	mu      sync.Mutex
	cfg     map[Class]ClassConfig
	buckets map[bucketKey]*bucket
	now     func() time.Time // swappable for tests
}

// New builds a Limiter from cfg (missing classes inherit defaults).
func New(cfg Config) *Limiter {
	return &Limiter{
		cfg:     cfg.resolved(),
		buckets: map[bucketKey]*bucket{},
		now:     time.Now,
	}
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

	l.mu.Lock()
	defer l.mu.Unlock()
	key := bucketKey{appID: appID, class: class}
	b, okb := l.buckets[key]
	now := l.now()
	if !okb {
		b = &bucket{tokens: float64(cc.Burst), last: now}
		l.buckets[key] = b
	} else {
		elapsed := now.Sub(b.last)
		if elapsed > 0 {
			b.tokens = math.Min(float64(cc.Burst), b.tokens+elapsed.Seconds()*cc.Rate)
			b.last = now
		}
	}
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	deficit := 1 - b.tokens
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
