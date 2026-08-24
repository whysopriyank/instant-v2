// Package bus provides the symmetric Postgres LISTEN/NOTIFY invalidation
// bus for horizontal scale-out (docs/09-tier2-architecture.md §T2.4).
//
// WHY this shape:
//
//   - Postgres NOTIFY payload cap is 8000 bytes (docs/09 §T2.4). We stay
//     under it by capping the encoded JSON at ~7400 bytes and degrading to
//     AttrIDs=nil ("invalidate all local subs of app") when the cap would be
//     exceeded. The truncation path is boring and correct (rare) and avoids
//     silent drops. Margin 600 bytes covers channel-name + TX overhead.
//
//   - Self-echo is intentional and harmless. PublishInvalidation runs
//     alongside the local Notifier.Notify (zero-latency local path); the
//     echo arriving via LISTEN is watermark-deduped by reactive refresh
//     coalescing, so at-most-one refresh per tx is preserved.
//
//   - LISTEN state is per-connection (docs/09 §T2.4; assembly notes). A
//     pgxpool must NOT be used for the listener — returning the connection
//     to the pool drops the LISTEN. Callers must supply a dedicated
//     *pgconn.PgConn (or any Conn) and keep it for the lifetime of Run.
package bus

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/instant-v2/instant-v2/internal/metrics"
	"github.com/jackc/pgx/v5/pgconn"
)

// Channel is the Postgres NOTIFY channel for invalidations.
const Channel = "instant_v2_invalidate"

// Invalidation is the bus payload: which app's subscribers to invalidate,
// which attrs changed (nil == all), and the commit watermark.
//
// AttrIDs == nil is the truncation fallback (payload >7400 bytes); receivers
// must treat it as "invalidate all local subs of app". Empty non-nil slice
// means "no attrs" (no-op) — callers normally use nil for full-refresh.
type Invalidation struct {
	AppID   string   `json:"app_id"`
	AttrIDs []string `json:"attr_ids"`
	TxID    int64    `json:"tx_id"`
	// Changes carries entity-level change records so peers can run the
	// incremental splice instead of a full recompute
	// (docs/09-tier2-architecture.md §T2.5). Optional: older payloads and
	// size-degraded ones omit it, which receivers treat as topic-granular.
	Changes []EntityChange `json:"ch,omitempty"`
}

// EntityChange is one touched entity on the bus. Mirrors reactive.Change's
// wire needs without importing it (bus stays a leaf package).
type EntityChange struct {
	Etype    string   `json:"e,omitempty"`
	EntityID string   `json:"id,omitempty"`
	AttrIDs  []string `json:"a,omitempty"`
}

// Publisher is the write side of the bus. PublishInvalidation must be
// called post-commit; assembly bridges it alongside the local notifier so
// local refreshes stay zero-latency and remote nodes learn via NOTIFY.
type Publisher interface {
	PublishInvalidation(ctx context.Context, inv Invalidation) error
}

// Conn is the minimal Postgres connection surface needed by the bus.
// It is satisfied exactly by *pgconn.PgConn (dedicated connection). Using
// pgxpool is incorrect: LISTEN dies with the borrowed connection when it is
// returned to the pool (see package doc).
type Conn interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	WaitForNotification(ctx context.Context) (*pgconn.Notification, error)
}

// Encode marshals inv to JSON for NOTIFY. Postgres caps NOTIFY payloads at
// 8000 bytes (see package doc); degradation is staged — first drop the
// entity-level Changes (peers fall back to the attr-id projection), then nil
// AttrIDs for full-app invalidation. The event itself is never lost; only
// its granularity degrades. The caller never needs to branch on size.
//
// Encoding is deterministic (json.Marshal) so the receiver can Unmarshal into
// the same Invalidation shape byte-identically across nodes.
func Encode(inv Invalidation) ([]byte, error) {
	b, err := json.Marshal(inv)
	if err != nil {
		return nil, err
	}
	if len(b) <= 7400 {
		return b, nil
	}
	if len(inv.Changes) > 0 {
		// Copy: never mutate the caller's slice header.
		noChanges := inv
		noChanges.Changes = nil
		return Encode(noChanges)
	}
	trunc := inv
	trunc.AttrIDs = nil
	//nolint:wrapcheck // json error is already descriptive
	return json.Marshal(trunc)
}

// Publish sends inv via pg_notify on c. The connection may be any Conn
// (typically a pooled connection or the same PgConn used elsewhere);
// LISTEN affinity is not required for publishing.
func Publish(ctx context.Context, c Conn, inv Invalidation) error {
	payload, err := Encode(inv)
	if err != nil {
		metrics.BusPublishErrors.Inc()
		return err
	}
	// pg_notify(channel, payload) — channel is Channel, payload is JSON text.
	_, err = c.Exec(ctx, "SELECT pg_notify($1,$2)", Channel, string(payload))
	if err != nil {
		metrics.BusPublishErrors.Inc()
	}
	return err
}

// Run subscribes c to Channel and dispatches incoming invalidations to
// onEvent until ctx is done. Malformed payloads are logged and skipped
// (via slog.Default(); callers that need a custom logger should set the
// default or use RunWithLogger). The initial LISTEN is required; failure
// there is returned immediately.
//
// WHY loop shape: WaitForNotification blocks on the dedicated connection's
// socket. Cancellation is via ctx (pgconn respects it). Self-echo is
// included; the reactive layer dedupes by watermark so delivering it is
// correct (at-most-once refresh per tx still holds).
func Run(ctx context.Context, c Conn, onEvent func(Invalidation)) error {
	return RunWithLogger(ctx, c, onEvent, slog.Default())
}

// RunWithLogger is Run with an explicit logger for malformed-payload
// warnings. When logger is nil slog.Default() is used. This exists so
// assembly can wire its structured logger without mutating the global
// default.
func RunWithLogger(ctx context.Context, c Conn, onEvent func(Invalidation), logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	if _, err := c.Exec(ctx, "LISTEN "+Channel); err != nil {
		return err
	}
	for {
		n, err := c.WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		if n == nil {
			continue
		}
		if n.Channel != Channel {
			continue
		}
		var inv Invalidation
		if err := json.Unmarshal([]byte(n.Payload), &inv); err != nil {
			metrics.BusMalformed.Inc()
			logger.Warn("bus: skipping malformed invalidation payload", "error", err, "payload", n.Payload, "channel", n.Channel)
			continue
		}
		metrics.BusEventsReceived.Inc()
		onEvent(inv)
	}
}
