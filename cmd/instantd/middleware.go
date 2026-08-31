package main

import (
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/instant-v2/instant-v2/internal/config"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/ratelimit"
)

// limiterAdapter adapts the ratelimit token buckets to the sync package's
// structural interface (string classes keep sync decoupled from ratelimit).
type limiterAdapter struct{ l *ratelimit.Limiter }

func (a limiterAdapter) Allow(appID string, class string) (bool, time.Duration) {
	return a.l.Allow(appID, ratelimit.Class(class))
}

// classifyRoute maps a request to its (rate-limit key, class). Admin and
// backup planes are already token-authenticated and skip limiting; runtime
// routes are keyed by app id when present, else by client IP.
func classifyRoute(r *http.Request) (string, ratelimit.Class) {
	path := r.URL.Path
	switch {
	case strings.HasPrefix(path, "/runtime/auth/"):
		return routeKey(r), ratelimit.ClassAuth
	case path == "/runtime/transact":
		return routeKey(r), ratelimit.ClassTransact
	case strings.HasPrefix(path, "/storage/"):
		return routeKey(r), ratelimit.ClassStorage
	case strings.HasPrefix(path, "/runtime/"):
		return routeKey(r), ratelimit.ClassWS
	default:
		return "", "" // admin/backup/health: token-gated or inert
	}
}

func routeKey(r *http.Request) string {
	// Security (audit H4): the key must be server-derived. Honor a client
	// app-id only when it parses as a real UUID — rotating garbage header
	// values must not mint fresh rate-limit buckets. Non-UUID callers share
	// their IP's bucket instead.
	isUUID := func(s string) bool {
		_, err := platform.ScanUUIDErr(s)
		return err == nil
	}
	for _, k := range []string{"app-id", "X-app-id"} {
		if v := r.Header.Get(k); v != "" && isUUID(v) {
			return v
		}
	}
	q := r.URL.Query()
	for _, k := range []string{"app-id", "app_id"} {
		if v := q.Get(k); v != "" && isUUID(v) {
			return v
		}
	}
	host := r.RemoteAddr
	if i := strings.LastIndex(host, ":"); i > 0 {
		host = host[:i]
	}
	return "ip:" + host
}

// bodyLimitFor picks the request-body ceiling for a route. Streaming routes
// (uploads, restores) get their own generous caps; everything else is small.
func bodyLimitFor(path string, cfg config.Config, method string) int64 {
	const (
		mib = 1 << 20
		kib = 1 << 10
	)
	switch {
	case strings.HasPrefix(path, "/backup/") && method == http.MethodPost:
		return cfg.MaxBackupBytes
	case strings.HasPrefix(path, "/storage/upload/") && method == http.MethodPut:
		return cfg.MaxUploadBytes + (1 << 20) // headroom for query metadata
	case path == "/runtime/transact" || path == "/admin/transact":
		return int64(cfg.MaxFrameBytes) // tx batches ride the WS frame limit
	case strings.HasPrefix(path, "/admin/query"):
		return 16 << 20
	case path == "/runtime/sse" && method == http.MethodPost:
		return 4 << 20
	case path == "/storage/signed-upload-url":
		return 64 * kib
	default:
		return 1 << 20
	}
}

// assembleMiddleware layers request-body ceilings and per-app rate limiting
// over the route mux. Order: body limit first so oversized bodies are cut
// before any handler reads them; then rate limiting.
func assembleMiddleware(next http.Handler, cfg config.Config, logger *slog.Logger, limiter *ratelimit.Limiter) http.Handler {
	limited := ratelimit.HTTPMiddleware(next, limiter, func(r *http.Request) (string, ratelimit.Class) {
		return classifyRoute(r)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		limit := bodyLimitFor(r.URL.Path, cfg, r.Method)
		if limit > 0 && r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, limit)
		}
		limited.ServeHTTP(w, r)
	})
}

// loopbackAddr reports whether the configured listen address binds only to
// a loopback host (or is empty, which defaults to all interfaces → false).
func loopbackAddr(addr string) bool {
	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil {
		host = h
	}
	if host == "" {
		return false
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return host == "localhost"
	}
	return ip.IsLoopback()
}
