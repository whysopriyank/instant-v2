package main

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/instant-v2/instant-v2/internal/config"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/ratelimit"
	"github.com/instant-v2/instant-v2/internal/testkit"
)

func TestDatabasePoolConfig(t *testing.T) {
	for _, tc := range []struct {
		name, params               string
		max, min, wantMax, wantMin int32
	}{
		{"default floor", "pool_max_conns=4", 0, 8, 32, 8},
		{"larger DSN maximum", "pool_max_conns=64", 0, 8, 64, 8},
		{"explicit maximum", "pool_max_conns=64", 12, 8, 12, 8},
		{"minimum clamps", "pool_max_conns=64", 4, 8, 4, 4},
		{"zero keeps DSN minimum", "pool_max_conns=64&pool_min_conns=3", 12, 0, 12, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.Config{PoolMinConns: tc.min, PGStatementTimeout: 3 * time.Second, PGLockTimeout: 500 * time.Millisecond, PGIdleTxTimeout: time.Second}
			got, err := databasePoolConfig("postgres://localhost/example?"+tc.params, tc.max, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if got.MaxConns != tc.wantMax || got.MinConns != tc.wantMin {
				t.Fatalf("max/min = %d/%d; want %d/%d", got.MaxConns, got.MinConns, tc.wantMax, tc.wantMin)
			}
			for key, want := range map[string]string{"statement_timeout": "3s", "lock_timeout": "500ms", "idle_in_transaction_session_timeout": "1s"} {
				if value := got.ConnConfig.RuntimeParams[key]; value != want {
					t.Errorf("%s = %q; want %q", key, value, want)
				}
			}
		})
	}
}

func TestRuntimeAssemblyIntegration(t *testing.T) {
	fixture := testkit.NewPostgres(t, testkit.PostgresOptions{})
	db, err := sql.Open("pgx", fixture.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	readPool, err := pgxpool.New(ctx, fixture.DSN)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(readPool.Close)
	t.Cleanup(cancel)
	if err := platform.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{
		MaxSubsPerApp: 17, MaxWSConns: 19, MaxSSEConns: 23, MaxSSEConnsPerIP: 7,
		MaxFrameBytes: 1024, WSCompression: "disabled", WSAllowedOrigins: "example.test",
		StorageSecret: "assembly-test-only", HTTPAddr: "127.0.0.1:0",
	}
	a := newAppRuntime(fixture.Pool, readPool, cfg, slog.Default())
	if a.pool != fixture.Pool || a.readPool != readPool || a.ex.DB != readPool || a.store.MaxSubsPerApp != 17 || a.notifier.Store != a.store {
		t.Fatal("writer/read/subscription service assembly changed")
	}
	mux := http.NewServeMux()
	ws, sse := a.mountRoutes(ctx, db, mux, cfg, ratelimit.New(ratelimit.Config{}))
	if ws.Manager != sse.Manager || ws.Store != a.store || sse.Store != a.store || ws.MaxConns != 19 || sse.MaxConns != 23 || sse.MaxConnsPerIP != 7 || ws.ReadLimit != 1024 {
		t.Fatal("transport shared dependencies or configured limits changed")
	}
	for _, tc := range []struct{ method, path, pattern string }{
		{"POST", "/admin/subscribe-query", "POST /admin/subscribe-query"},
		{"GET", "/runtime/session", "GET /runtime/session"},
		{"GET", "/runtime/sse", "GET /runtime/sse"},
		{"POST", "/runtime/sse", "POST /runtime/sse"},
		{"POST", "/runtime/auth/refresh_tokens", "POST /runtime/auth/refresh_tokens"},
		{"POST", "/runtime/signout", "POST /runtime/signout"},
		{"POST", "/runtime/framework/query", "POST /runtime/framework/query"},
		{"GET", "/runtime/openid-configuration", "GET /runtime/openid-configuration"},
		{"GET", "/runtime/app/.well-known/openid-configuration", "GET /runtime/{app_id}/.well-known/openid-configuration"},
		{"POST", "/runtime/auth/send_magic_code", "POST /runtime/auth/"},
		{"GET", "/admin/schema", "/admin/"},
		{"GET", "/storage/file", "/storage/"},
		{"GET", "/backup/export", "/backup/"},
		{"POST", "/runtime/transact", "POST /runtime/transact"},
		{"GET", "/health", "GET /health"},
	} {
		_, pattern := mux.Handler(httptest.NewRequest(tc.method, tc.path, nil))
		if pattern != tc.pattern {
			t.Errorf("%s %s routes to %q; want %q", tc.method, tc.path, pattern, tc.pattern)
		}
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/health", nil))
	if rec.Code != 200 || rec.Body.String() != "{\"db\":true,\"ok\":true,\"queue-depth\":0}\n" {
		t.Fatalf("live health: %d %s", rec.Code, rec.Body.String())
	}
}

func TestServeHTTPBindFailure(t *testing.T) {
	err := serveHTTP(context.Background(), http.NewServeMux(), config.Config{HTTPAddr: "127.0.0.1:invalid"}, slog.Default(), ratelimit.New(ratelimit.Config{}), nil)
	if err == nil {
		t.Fatal("invalid listener address must return its bind error")
	}
}
