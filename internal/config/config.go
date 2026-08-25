// Package config loads instantd configuration from the environment.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config is the full runtime configuration for instantd.
type Config struct {
	HTTPAddr    string // INSTANT_V2_HTTP_ADDR, default ":8080"
	DatabaseURL string // DATABASE_URL (required from Phase 1; optional while booting without DB)

	// Read plane (docs/09-tier2-architecture.md §T2.3). Empty means reads
	// share the write pool — today's behavior. Set to a replica DSN to
	// route instaql/catalog/runtime queries off the writer.
	ReadDatabaseURL string // INSTANT_V2_READ_URL, default ""

	// Pool sizing (docs/09 §T2.3). Zero keeps the built-in floor (32);
	// with both pools on one instance, combined capacity should not exceed
	// Postgres max_connections.
	WritePoolMaxConns int32 // INSTANT_V2_WRITE_POOL_MAXCONNS, default 0 (=32)
	ReadPoolMaxConns  int32 // INSTANT_V2_READ_POOL_MAXCONNS, default 0 (=32)

	// Backpressure (docs/09 §T2.1). 0 disables shedding: the notifier
	// queue may grow unbounded (pre-T2 behavior).
	MaxQueueDepth int64 // INSTANT_V2_MAX_QUEUE_DEPTH, default 0

	// Wire compression (docs/09 §T2.2): disabled | no-context-takeover |
	// context-takeover. Disabled is byte-identical for every client.
	WSCompression string // INSTANT_V2_WS_COMPRESSION, default "disabled"

	// Scale-out (docs/09 §T2.4). Bus "postgres" makes OnCommit bridges
	// publish invalidations via LISTEN/NOTIFY and apply events from peers;
	// sessions stay node-local, writes are accepted on any node.
	InvalidationBus string // INSTANT_V2_INVALIDATION_BUS: "none"|"postgres", default "none"
	NodeID          string // INSTANT_V2_NODE_ID, default hostname

	// Observability (docs/09 §T3). Prometheus scrape address; empty
	// disables the listener entirely. Loopback default so a bare
	// `instantd` is observable without widening the network surface.
	MetricsAddr string // INSTANT_V2_METRICS_ADDR, default "127.0.0.1:9465"

	// Storage presigning secret (HMAC over op|app-id|id|exp). REQUIRED:
	// startup fails without it unless INSTANT_V2_INSECURE_DEV_SECRETS=1,
	// which restores the legacy DATABASE_URL-derived dev fallback.
	StorageSecret   string // INSTANT_V2_STORAGE_SECRET
	InsecureDevMode bool   // INSTANT_V2_INSECURE_DEV_SECRETS

	// Resource bounds. Zero values below fall back to the documented defaults.
	MaxSubsPerApp int // INSTANT_V2_MAX_SUBS_PER_APP, default 2000
	MaxWSConns    int // INSTANT_V2_MAX_WS_CONNS, default 20000
	// MaxFrameBytes bounds one inbound WS/SSE message and the transact HTTP
	// bodies. Default 4 MiB — refresh-ok envelopes ride the write direction
	// and are unaffected; the historical 64 MiB let unauthenticated clients
	// drive ~6 GiB/s of parse churn per app-id (audit H3).
	MaxFrameBytes    int   // INSTANT_V2_MAX_FRAME_BYTES
	MaxSSEConns      int   // INSTANT_V2_MAX_SSE_CONNS, default 10000
	MaxSSEConnsPerIP int   // INSTANT_V2_MAX_SSE_CONNS_PER_IP, default 100 (audit H5)
	MaxUploadBytes   int64 // INSTANT_V2_MAX_UPLOAD_BYTES, default 512MiB
	MaxBackupBytes   int64 // INSTANT_V2_MAX_BACKUP_BYTES, default 32GiB

	// Postgres per-connection ceilings applied to both pools via
	// RuntimeParams. Zero disables that ceiling. Migrations are exempt
	// (separate connection).
	PGStatementTimeout time.Duration // INSTANT_V2_PG_STATEMENT_TIMEOUT, default 30s
	PGLockTimeout      time.Duration // INSTANT_V2_PG_LOCK_TIMEOUT, default 5s
	PGIdleTxTimeout    time.Duration // INSTANT_V2_PG_IDLE_TX_TIMEOUT, default 30s
}

func (c Config) String() string {
	db := "none"
	if c.DatabaseURL != "" {
		if c.ReadDatabaseURL != "" && c.ReadDatabaseURL != c.DatabaseURL {
			db = "split"
		} else {
			db = "set"
		}
	}
	bus := ""
	if c.InvalidationBus == "postgres" {
		bus = " bus:postgres"
	}
	return fmt.Sprintf("{addr:%s db:%s%s}", c.HTTPAddr, db, bus)
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:        envOr("INSTANT_V2_HTTP_ADDR", ":8080"),
		DatabaseURL:     os.Getenv("DATABASE_URL"),
		ReadDatabaseURL: os.Getenv("INSTANT_V2_READ_URL"),
		WSCompression:   envOr("INSTANT_V2_WS_COMPRESSION", "disabled"),
		InvalidationBus: envOr("INSTANT_V2_INVALIDATION_BUS", "none"),
		NodeID:          os.Getenv("INSTANT_V2_NODE_ID"),
		MetricsAddr:     envOr("INSTANT_V2_METRICS_ADDR", "127.0.0.1:9465"),
	}
	n, err := envInt32("INSTANT_V2_WRITE_POOL_MAXCONNS")
	if err != nil {
		return cfg, err
	}
	cfg.WritePoolMaxConns = n
	if n, err = envInt32("INSTANT_V2_READ_POOL_MAXCONNS"); err != nil {
		return cfg, err
	}
	cfg.ReadPoolMaxConns = n
	if cfg.MaxQueueDepth, err = envInt64("INSTANT_V2_MAX_QUEUE_DEPTH"); err != nil {
		return cfg, err
	}
	switch cfg.WSCompression {
	case "", "disabled", "no-context-takeover", "context-takeover":
	default:
		return cfg, fmt.Errorf("INSTANT_V2_WS_COMPRESSION %q: want disabled|no-context-takeover|context-takeover", cfg.WSCompression)
	}
	switch cfg.InvalidationBus {
	case "", "none", "postgres":
	default:
		return cfg, fmt.Errorf("INSTANT_V2_INVALIDATION_BUS %q: want none|postgres", cfg.InvalidationBus)
	}
	if p := os.Getenv("INSTANT_V2_HTTP_PORT"); p != "" {
		port, perr := strconv.Atoi(p)
		if perr != nil || port < 1 || port > 65535 {
			return cfg, fmt.Errorf("INSTANT_V2_HTTP_PORT %q: %w", p, perr)
		}
		cfg.HTTPAddr = ":" + p
	}
	if cfg.HTTPAddr == "" {
		return cfg, errors.New("INSTANT_V2_HTTP_ADDR must not be empty")
	}

	cfg.StorageSecret = os.Getenv("INSTANT_V2_STORAGE_SECRET")
	switch os.Getenv("INSTANT_V2_INSECURE_DEV_SECRETS") {
	case "", "0", "false":
	default:
		cfg.InsecureDevMode = true
	}
	if cfg.StorageSecret == "" && !cfg.InsecureDevMode {
		return cfg, errors.New("INSTANT_V2_STORAGE_SECRET is required (set INSTANT_V2_INSECURE_DEV_SECRETS=1 only for local development)")
	}
	if cfg.MaxSubsPerApp, err = envInt("INSTANT_V2_MAX_SUBS_PER_APP", 2000); err != nil {
		return cfg, err
	}
	if cfg.MaxWSConns, err = envInt("INSTANT_V2_MAX_WS_CONNS", 20000); err != nil {
		return cfg, err
	}
	if cfg.MaxFrameBytes, err = envInt("INSTANT_V2_MAX_FRAME_BYTES", 4<<20); err != nil {
		return cfg, err
	}
	if cfg.MaxSSEConns, err = envInt("INSTANT_V2_MAX_SSE_CONNS", 10000); err != nil {
		return cfg, err
	}
	if cfg.MaxSSEConnsPerIP, err = envInt("INSTANT_V2_MAX_SSE_CONNS_PER_IP", 100); err != nil {
		return cfg, err
	}
	if cfg.MaxUploadBytes, err = envInt64Default("INSTANT_V2_MAX_UPLOAD_BYTES", 512<<20); err != nil {
		return cfg, err
	}
	if cfg.MaxBackupBytes, err = envInt64Default("INSTANT_V2_MAX_BACKUP_BYTES", 32<<30); err != nil {
		return cfg, err
	}
	// Postgres statement/lock/idle-in-tx ceilings (docs/10 §verification):
	// one pathological query must not pin a pool connection forever. "0"
	// disables a ceiling explicitly; unset takes the default. Migrations run
	// on their own database/sql connection and are never bounded by these.
	if cfg.PGStatementTimeout, err = envDuration("INSTANT_V2_PG_STATEMENT_TIMEOUT", 30*time.Second); err != nil {
		return cfg, err
	}
	if cfg.PGLockTimeout, err = envDuration("INSTANT_V2_PG_LOCK_TIMEOUT", 5*time.Second); err != nil {
		return cfg, err
	}
	if cfg.PGIdleTxTimeout, err = envDuration("INSTANT_V2_PG_IDLE_TX_TIMEOUT", 30*time.Second); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func envInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("%s %q: want positive integer", key, v)
	}
	return n, nil
}

// envDuration parses a Go duration ("30s", "1m") or "0" (explicit disable).
// Empty takes the default. Negative values are rejected.
func envDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	if v == "0" {
		return 0, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("%s %q: want duration (e.g. 30s) or 0", key, v)
	}
	return d, nil
}

func envInt64Default(key string, def int64) (int64, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("%s %q: want positive integer", key, v)
	}
	return n, nil
}

// StorageSecretIsSet reports whether an explicit presigning secret was
// configured. The insecure dev fallback (DATABASE_URL-derived) does NOT
// count — operators must see the difference in diagnostics.
func (c Config) StorageSecretIsSet() bool { return c.StorageSecret != "" }

// NodeName resolves the node id used in logs and health output.
func (c Config) NodeName() string {
	if c.NodeID != "" {
		return c.NodeID
	}
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "unknown"
	}
	return h
}

func envInt32(key string) (int32, error) {
	v := os.Getenv(key)
	if v == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("%s %q: want positive integer", key, v)
	}
	return int32(n), nil
}

func envInt64(key string) (int64, error) {
	v := os.Getenv(key)
	if v == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s %q: want non-negative integer", key, v)
	}
	return n, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
