package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func setEnv(t *testing.T, kv map[string]string) {
	t.Helper()
	for k, v := range kv {
		old, had := os.LookupEnv(k)
		if v == "" {
			_ = os.Unsetenv(k)
		} else {
			_ = os.Setenv(k, v)
		}
		t.Cleanup(func() {
			if had {
				_ = os.Setenv(k, old)
			} else {
				_ = os.Unsetenv(k)
			}
		})
	}
}

func TestLoadDefaults(t *testing.T) {
	setEnv(t, map[string]string{
		"DATABASE_URL":                   "",
		"INSTANT_V2_READ_URL":            "",
		"INSTANT_V2_WS_COMPRESSION":      "",
		"INSTANT_V2_INVALIDATION_BUS":    "",
		"INSTANT_V2_MAX_QUEUE_DEPTH":     "",
		"INSTANT_V2_WRITE_POOL_MAXCONNS": "",
		"INSTANT_V2_READ_POOL_MAXCONNS":  "",
		"INSTANT_V2_STORAGE_SECRET":      "test-secret",
	})
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WSCompression != "disabled" || cfg.InvalidationBus != "none" {
		t.Fatalf("defaults: %+v", cfg)
	}
	if cfg.MaxQueueDepth != 0 || cfg.WritePoolMaxConns != 0 || cfg.ReadPoolMaxConns != 0 {
		t.Fatalf("numeric defaults must be zero-valued: %+v", cfg)
	}
	// Resource-bound defaults.
	if cfg.MaxSubsPerApp != 2000 || cfg.MaxWSConns != 20000 || cfg.MaxSSEConns != 10000 {
		t.Fatalf("conn/sub defaults: %+v", cfg)
	}
	if cfg.MaxUploadBytes != 512<<20 || cfg.MaxBackupBytes != 32<<30 {
		t.Fatalf("byte-cap defaults: %+v", cfg)
	}
	if !cfg.StorageSecretIsSet() {
		t.Fatal("secret must register as set")
	}
}

// TestLoadRequiresStorageSecret pins fail-fast startup: no secret and no
// dev escape hatch → refuse to boot; either escape restores it.
func TestLoadRequiresStorageSecret(t *testing.T) {
	setEnv(t, map[string]string{"DATABASE_URL": "", "INSTANT_V2_STORAGE_SECRET": ""})
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "STORAGE_SECRET") {
		t.Fatalf("missing secret must fail startup, got: %v", err)
	}
	setEnv(t, map[string]string{"DATABASE_URL": "", "INSTANT_V2_STORAGE_SECRET": "", "INSTANT_V2_INSECURE_DEV_SECRETS": "1"})
	cfg, err := Load()
	if err != nil {
		t.Fatalf("dev escape hatch must allow boot: %v", err)
	}
	if cfg.StorageSecretIsSet() {
		t.Fatal("dev fallback must not count as a configured secret")
	}
}

func TestLoadTier2Knobs(t *testing.T) {
	setEnv(t, map[string]string{
		"DATABASE_URL":                   "postgres://w",
		"INSTANT_V2_READ_URL":            "postgres://r",
		"INSTANT_V2_WS_COMPRESSION":      "context-takeover",
		"INSTANT_V2_INVALIDATION_BUS":    "postgres",
		"INSTANT_V2_NODE_ID":             "node-7",
		"INSTANT_V2_MAX_QUEUE_DEPTH":     "5000",
		"INSTANT_V2_WRITE_POOL_MAXCONNS": "16",
		"INSTANT_V2_READ_POOL_MAXCONNS":  "48",
		"INSTANT_V2_STORAGE_SECRET":      "k1",
	})
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ReadDatabaseURL != "postgres://r" ||
		cfg.WSCompression != "context-takeover" ||
		cfg.InvalidationBus != "postgres" ||
		cfg.NodeID != "node-7" ||
		cfg.MaxQueueDepth != 5000 ||
		cfg.WritePoolMaxConns != 16 || cfg.ReadPoolMaxConns != 48 {
		t.Fatalf("knobs: %+v", cfg)
	}
	if cfg.String() != "{addr::8080 db:split bus:postgres}" {
		t.Fatalf("String: %q", cfg.String())
	}
	if cfg.NodeName() != "node-7" {
		t.Fatalf("NodeName: %q", cfg.NodeName())
	}
}

func TestLoadValidation(t *testing.T) {
	cases := map[string]map[string]string{
		"bad compression": {"INSTANT_V2_WS_COMPRESSION": "gzip"},
		"bad bus":         {"INSTANT_V2_INVALIDATION_BUS": "nats"},
		"bad depth":       {"INSTANT_V2_MAX_QUEUE_DEPTH": "-1"},
		"bad pool":        {"INSTANT_V2_WRITE_POOL_MAXCONNS": "0"},
		"bad subs cap":    {"INSTANT_V2_STORAGE_SECRET": "s", "INSTANT_V2_MAX_SUBS_PER_APP": "0"},
		"bad upload cap":  {"INSTANT_V2_STORAGE_SECRET": "s", "INSTANT_V2_MAX_UPLOAD_BYTES": "-5"},
	}
	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			setEnv(t, env)
			if _, err := Load(); err == nil {
				t.Fatalf("%s: expected error", name)
			}
		})
	}
}

func TestNodeNameFallback(t *testing.T) {
	setEnv(t, map[string]string{"INSTANT_V2_NODE_ID": ""})
	var c Config
	if c.NodeName() == "" {
		t.Fatal("NodeName must never be empty")
	}
}

func TestLoadPGTimeouts(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		setEnv(t, map[string]string{"INSTANT_V2_STORAGE_SECRET": "s"})
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.PGStatementTimeout != 30*time.Second || cfg.PGLockTimeout != 5*time.Second ||
			cfg.PGIdleTxTimeout != 30*time.Second {
			t.Fatalf("defaults wrong: %+v", cfg)
		}
	})
	t.Run("override and explicit disable", func(t *testing.T) {
		setEnv(t, map[string]string{
			"INSTANT_V2_STORAGE_SECRET":       "s",
			"INSTANT_V2_PG_STATEMENT_TIMEOUT": "2s",
			"INSTANT_V2_PG_LOCK_TIMEOUT":      "0",
			"INSTANT_V2_PG_IDLE_TX_TIMEOUT":   "1m",
			"INSTANT_V2_MAX_SUBS_PER_APP":     "10",
			"INSTANT_V2_MAX_WS_CONNS":         "10",
			"INSTANT_V2_MAX_SSE_CONNS":        "10",
			"INSTANT_V2_MAX_UPLOAD_BYTES":     "1024",
			"INSTANT_V2_MAX_BACKUP_BYTES":     "2048",
			"INSTANT_V2_WRITE_POOL_MAXCONNS":  "4",
			"INSTANT_V2_READ_POOL_MAXCONNS":   "4",
			"INSTANT_V2_MAX_QUEUE_DEPTH":      "8",
		})
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.PGStatementTimeout != 2*time.Second {
			t.Fatalf("statement timeout = %s, want 2s", cfg.PGStatementTimeout)
		}
		if cfg.PGLockTimeout != 0 {
			t.Fatalf("lock timeout must be explicitly disabled, got %s", cfg.PGLockTimeout)
		}
		if cfg.PGIdleTxTimeout != time.Minute {
			t.Fatalf("idle tx timeout = %s, want 1m", cfg.PGIdleTxTimeout)
		}
	})
	t.Run("invalid value rejected", func(t *testing.T) {
		setEnv(t, map[string]string{
			"INSTANT_V2_STORAGE_SECRET":       "s",
			"INSTANT_V2_PG_STATEMENT_TIMEOUT": "soon",
		})
		if _, err := Load(); err == nil {
			t.Fatal("expected parse error for non-duration value")
		}
	})
}
