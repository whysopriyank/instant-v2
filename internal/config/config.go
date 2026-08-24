// Package config loads instantd configuration from the environment.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
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
	return cfg, nil
}

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
