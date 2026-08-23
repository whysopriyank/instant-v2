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
}

func (c Config) String() string {
	if c.DatabaseURL != "" {
		return fmt.Sprintf("{addr:%s db:set}", c.HTTPAddr)
	}
	return fmt.Sprintf("{addr:%s db:none}", c.HTTPAddr)
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:    envOr("INSTANT_V2_HTTP_ADDR", ":8080"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
	}
	if p := os.Getenv("INSTANT_V2_HTTP_PORT"); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 65535 {
			return cfg, fmt.Errorf("INSTANT_V2_HTTP_PORT %q: %w", p, err)
		}
		cfg.HTTPAddr = ":" + p
	}
	if cfg.HTTPAddr == "" {
		return cfg, errors.New("INSTANT_V2_HTTP_ADDR must not be empty")
	}
	return cfg, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
