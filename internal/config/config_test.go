package config

import (
	"os"
	"testing"
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
