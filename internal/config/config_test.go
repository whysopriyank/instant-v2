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
		"INSTANT_V2_POOL_MINCONNS":       "",
		"INSTANT_V2_STORAGE_SECRET":      "test-secret",
		"INSTANT_V2_STORAGE_ROOT":        t.TempDir(),
	})
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WSCompression != "disabled" || cfg.InvalidationBus != "none" {
		t.Fatalf("defaults: %+v", cfg)
	}
	if cfg.ReadDatabaseURL != "" {
		t.Fatalf("empty read URL must retain empty fallback marker: %q", cfg.ReadDatabaseURL)
	}
	if cfg.MaxQueueDepth != 0 || cfg.WritePoolMaxConns != 0 || cfg.ReadPoolMaxConns != 0 {
		t.Fatalf("numeric defaults must be zero-valued: %+v", cfg)
	}
	if cfg.PoolMinConns != 8 {
		t.Fatalf("pool min default = %d, want 8", cfg.PoolMinConns)
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

func TestLoadMetricsAddressConfiguration(t *testing.T) {
	const key = "INSTANT_V2_METRICS_ADDR"

	cases := []struct {
		name    string
		present bool
		value   string
		want    string
	}{
		{name: "unset uses default", want: "127.0.0.1:9465"},
		{name: "explicit empty disables", present: true, value: "", want: ""},
		{name: "explicit address is honored", present: true, value: "127.0.0.1:19465", want: "127.0.0.1:19465"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			old, had := os.LookupEnv(key)
			if tc.present {
				if err := os.Setenv(key, tc.value); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Unsetenv(key); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if had {
					_ = os.Setenv(key, old)
				} else {
					_ = os.Unsetenv(key)
				}
			})

			setEnv(t, map[string]string{"INSTANT_V2_STORAGE_SECRET": "s", "INSTANT_V2_STORAGE_ROOT": t.TempDir()})
			cfg, err := Load()
			if err != nil {
				t.Fatal(err)
			}
			if cfg.MetricsAddr != tc.want {
				t.Fatalf("MetricsAddr = %q, want %q", cfg.MetricsAddr, tc.want)
			}
		})
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
		"DATABASE_URL":                       "postgres://w",
		"INSTANT_V2_READ_URL":                "postgres://r",
		"INSTANT_V2_WS_COMPRESSION":          "context-takeover",
		"INSTANT_V2_INVALIDATION_BUS":        "postgres",
		"INSTANT_V2_NODE_ID":                 "node-7",
		"INSTANT_V2_MAX_QUEUE_DEPTH":         "5000",
		"INSTANT_V2_WRITE_POOL_MAXCONNS":     "16",
		"INSTANT_V2_READ_POOL_MAXCONNS":      "48",
		"INSTANT_V2_POOL_MINCONNS":           "4",
		"INSTANT_V2_STORAGE_SECRET":          "k1",
		"INSTANT_V2_STORAGE_ROOT":            t.TempDir(),
		"INSTANT_OAUTH_GOOGLE_CLIENT_ID":     "google-id",
		"INSTANT_OAUTH_GOOGLE_CLIENT_SECRET": "google-secret",
		"INSTANT_OAUTH_GITHUB_CLIENT_ID":     "github-id",
		"INSTANT_OAUTH_GITHUB_CLIENT_SECRET": "github-secret",
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
	if cfg.PoolMinConns != 4 {
		t.Fatalf("pool min = %d, want 4", cfg.PoolMinConns)
	}
	if cfg.String() != "{addr::8080 db:split bus:postgres}" {
		t.Fatalf("String: %q", cfg.String())
	}
	if cfg.NodeName() != "node-7" {
		t.Fatalf("NodeName: %q", cfg.NodeName())
	}
}

func TestEnvInt32DefaultBoundaries(t *testing.T) {
	const key = "INSTANT_V2_POOL_MINCONNS"
	const maxInt32 = int64(1<<31 - 1)

	cases := []struct {
		name    string
		raw     string
		want    int32
		wantErr bool
	}{
		{name: "empty uses default", raw: "", want: 8},
		{name: "zero disables", raw: "0", want: 0},
		{name: "negative rejected", raw: "-1", wantErr: true},
		{name: "max int32 accepted", raw: "2147483647", want: int32(maxInt32)},
		{name: "above max int32 rejected", raw: "2147483648", wantErr: true},
		{name: "huge overflow rejected", raw: "999999999999999999999999999999", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setEnv(t, map[string]string{key: tc.raw})
			got, err := envInt32Default(key, 8)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("envInt32Default(%q) = %d, want actionable range error", tc.raw, got)
				}
				if !strings.Contains(err.Error(), key) {
					t.Fatalf("error %q does not identify %s", err, key)
				}
				return
			}
			if err != nil {
				t.Fatalf("envInt32Default(%q): %v", tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("envInt32Default(%q) = %d, want %d", tc.raw, got, tc.want)
			}
		})
	}
}

func TestLoadPoolMinOverflowRejected(t *testing.T) {
	setEnv(t, map[string]string{
		"INSTANT_V2_STORAGE_SECRET": "s",
		"INSTANT_V2_STORAGE_ROOT":   t.TempDir(),
		"INSTANT_V2_POOL_MINCONNS":  "2147483648",
	})
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "INSTANT_V2_POOL_MINCONNS") {
		t.Fatalf("pool min overflow must fail with an actionable error, got: %v", err)
	}
}

func TestLoadPoolMaxOverflowRejected(t *testing.T) {
	setEnv(t, map[string]string{
		"INSTANT_V2_STORAGE_SECRET":      "s",
		"INSTANT_V2_WRITE_POOL_MAXCONNS": "2147483648",
	})
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "INSTANT_V2_WRITE_POOL_MAXCONNS") {
		t.Fatalf("pool max overflow must fail with an actionable error, got: %v", err)
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

func TestLoadRequiresSelectedOAuthCredentialsWithDatabase(t *testing.T) {
	base := map[string]string{
		"DATABASE_URL":                       "postgres://db",
		"INSTANT_V2_STORAGE_SECRET":          "storage-secret",
		"INSTANT_V2_STORAGE_ROOT":            t.TempDir(),
		"INSTANT_OAUTH_GOOGLE_CLIENT_ID":     "google-id",
		"INSTANT_OAUTH_GOOGLE_CLIENT_SECRET": "google-secret",
		"INSTANT_OAUTH_GITHUB_CLIENT_ID":     "github-id",
		"INSTANT_OAUTH_GITHUB_CLIENT_SECRET": "github-secret",
	}
	for _, missing := range []string{
		"INSTANT_OAUTH_GOOGLE_CLIENT_ID",
		"INSTANT_OAUTH_GOOGLE_CLIENT_SECRET",
		"INSTANT_OAUTH_GITHUB_CLIENT_ID",
		"INSTANT_OAUTH_GITHUB_CLIENT_SECRET",
	} {
		t.Run(missing, func(t *testing.T) {
			env := make(map[string]string, len(base))
			for key, value := range base {
				env[key] = value
			}
			env[missing] = ""
			setEnv(t, env)
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), missing) {
				t.Fatalf("missing %s must fail startup, got %v", missing, err)
			}
		})
	}
	setEnv(t, base)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OAuthGoogleClientID != "google-id" || cfg.OAuthGitHubClientID != "github-id" {
		t.Fatalf("OAuth credentials not loaded into runtime config: %+v", cfg)
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
		setEnv(t, map[string]string{"INSTANT_V2_STORAGE_SECRET": "s", "INSTANT_V2_STORAGE_ROOT": t.TempDir()})
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
			"INSTANT_V2_STORAGE_ROOT":         t.TempDir(),
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

func TestLoadStorageRootRequired(t *testing.T) {
	setEnv(t, map[string]string{
		"INSTANT_V2_STORAGE_SECRET": "s",
		"INSTANT_V2_STORAGE_ROOT":   "",
	})
	if _, err := Load(); err == nil {
		t.Fatal("missing INSTANT_V2_STORAGE_ROOT must fail without insecure dev mode")
	} else if !strings.Contains(err.Error(), "INSTANT_V2_STORAGE_ROOT") {
		t.Fatalf("error must name the variable, got: %v", err)
	}
}

func TestLoadStorageRootDevFallback(t *testing.T) {
	setEnv(t, map[string]string{
		"INSTANT_V2_STORAGE_SECRET":       "s",
		"INSTANT_V2_STORAGE_ROOT":         "",
		"INSTANT_V2_INSECURE_DEV_SECRETS": "1",
	})
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.StorageRoot == "" || cfg.StorageRoot == os.TempDir() {
		t.Fatalf("dev fallback must be explicit, never temp: %q", cfg.StorageRoot)
	}
}

func TestLoadStorageRootExplicitKept(t *testing.T) {
	setEnv(t, map[string]string{
		"INSTANT_V2_STORAGE_SECRET": "s",
		"INSTANT_V2_STORAGE_ROOT":   t.TempDir(),
	})
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv("INSTANT_V2_STORAGE_ROOT"); cfg.StorageRoot != got {
		t.Fatalf("StorageRoot = %q, want %q", cfg.StorageRoot, got)
	}
}

func TestStorageFingerprintProfiles(t *testing.T) {
	base := Config{StorageRoot: "/data", MaxUploadBytes: 1 << 20, StorageSecret: "s"}
	if f := base.StorageFingerprint(); len(f) != 64 {
		t.Fatalf("fingerprint = %q; want 64 hex chars", f)
	}
	if a, b := base.StorageFingerprint(), base.StorageFingerprint(); a != b {
		t.Fatal("fingerprint is not deterministic")
	}
	otherRoot := base
	otherRoot.StorageRoot = "/other"
	if otherRoot.StorageFingerprint() == base.StorageFingerprint() {
		t.Fatal("different roots must fingerprint differently")
	}
	otherQuota := base
	otherQuota.MaxUploadBytes++
	if otherQuota.StorageFingerprint() == base.StorageFingerprint() {
		t.Fatal("different quotas must fingerprint differently")
	}
	// Secret VALUE must not feed the hash (log-safe); presence must.
	otherSecret := base
	otherSecret.StorageSecret = "different-secret-same-presence"
	if otherSecret.StorageFingerprint() != base.StorageFingerprint() {
		t.Fatal("secret value leaked into the fingerprint")
	}
	noSecret := base
	noSecret.StorageSecret = ""
	if noSecret.StorageFingerprint() == base.StorageFingerprint() {
		t.Fatal("credential posture change must fingerprint differently")
	}
}
