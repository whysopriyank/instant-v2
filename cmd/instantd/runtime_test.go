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
	"github.com/instant-v2/instant-v2/internal/metrics"
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
		StorageRoot: t.TempDir(),
	}
	a := newAppRuntime(fixture.Pool, readPool, cfg, slog.Default())
	if a.pool != fixture.Pool || a.readPool != readPool || a.ex.DB != readPool || a.store.MaxSubsPerApp != 17 || a.notifier.Store != a.store {
		t.Fatal("writer/read/subscription service assembly changed")
	}
	mux := http.NewServeMux()
	ws, sse, err := a.mountRoutes(ctx, db, mux, cfg, ratelimit.New(ratelimit.Config{}))
	if err != nil {
		t.Fatal(err)
	}
	if ws.Manager != sse.Manager || ws.Store != a.store || sse.Store != a.store || ws.MaxConns != 19 || sse.MaxConns != 23 || sse.MaxConnsPerIP != 7 || ws.ReadLimit != 1024 {
		t.Fatal("transport shared dependencies or configured limits changed")
	}
	for _, tc := range assemblyRouteCases {
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

// assemblyRouteCases is the production route table shared by the live-DB
// assembly test above and the hermetic test below: both must resolve every
// entry identically, so a route added to one cannot drift from the other.
var assemblyRouteCases = []struct{ method, path, pattern string }{
	{"POST", "/admin/subscribe-query", "POST /admin/subscribe-query"},
	{"GET", "/runtime/session", "GET /runtime/session"},
	{"GET", "/runtime/sse", "GET /runtime/sse"},
	{"POST", "/runtime/sse", "POST /runtime/sse"},
	{"POST", "/runtime/auth/refresh_tokens", "POST /runtime/auth/refresh_tokens"},
	{"POST", "/runtime/signout", "POST /runtime/signout"},
	{"POST", "/runtime/framework/query", "POST /runtime/framework/query"},
	{"GET", "/runtime/openid-configuration", "GET /runtime/openid-configuration"},
	{"GET", "/runtime/app/.well-known/openid-configuration", "GET /runtime/{app_id}/.well-known/openid-configuration"},
	{"GET", "/runtime/oauth/start", "GET /runtime/oauth/start"},
	{"GET", "/runtime/oauth/callback", "GET /runtime/oauth/callback"},
	{"POST", "/runtime/oauth/token", "POST /runtime/oauth/token"},
	{"POST", "/runtime/oauth/id_token", "POST /runtime/oauth/id_token"},
	{"POST", "/runtime/auth/send_magic_code", "POST /runtime/auth/"},
	{"GET", "/admin/schema", "/admin/"},
	{"GET", "/storage/file", "/storage/"},
	{"GET", "/backup/export", "/backup/"},
	{"POST", "/runtime/transact", "POST /runtime/transact"},
	{"GET", "/health", "GET /health"},
}

// TestMountRoutesHermeticAssembly proves route assembly performs zero
// startup database I/O: both handles point at an unroutable address, so any
// eager query, migration, or ping fails the test (the boot auth-maintenance
// sweep is the one exception — its errors are swallowed by design and the
// connect refusal returns immediately).
func TestMountRoutesHermeticAssembly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const dsn = "postgres://127.0.0.1:1/instant?pool_min_conns=0&connect_timeout=1"
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	cfg := config.Config{
		MaxSubsPerApp: 17, MaxWSConns: 19, MaxSSEConns: 23, MaxSSEConnsPerIP: 7,
		MaxFrameBytes: 1024, WSCompression: "disabled", WSAllowedOrigins: "example.test",
		StorageSecret: "assembly-test-only", HTTPAddr: "127.0.0.1:0",
		StorageRoot: t.TempDir(),
	}
	a := newAppRuntime(pool, pool, cfg, slog.Default())
	mux := http.NewServeMux()
	ws, sse, err := a.mountRoutes(ctx, db, mux, cfg, ratelimit.New(ratelimit.Config{}))
	if err != nil {
		t.Fatal(err)
	}
	if ws == nil || sse == nil || ws.Manager != sse.Manager || ws.Store != a.store || sse.Store != a.store {
		t.Fatal("transport shared dependencies changed")
	}
	if ws.MaxConns != 19 || sse.MaxConns != 23 || sse.MaxConnsPerIP != 7 || ws.ReadLimit != 1024 {
		t.Fatal("configured transport limits changed")
	}
	for _, tc := range assemblyRouteCases {
		_, pattern := mux.Handler(httptest.NewRequest(tc.method, tc.path, nil))
		if pattern != tc.pattern {
			t.Errorf("%s %s routes to %q; want %q", tc.method, tc.path, pattern, tc.pattern)
		}
	}
}

// TestMountRoutesRejectsEmptyStorageRoot locks in diagnosable startup
// refusal: a missing durable root returns an error instead of exiting the
// process, so a misconfigured test fails with a message rather than
// killing the whole test binary.
func TestMountRoutesRejectsEmptyStorageRoot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const dsn = "postgres://127.0.0.1:1/instant?pool_min_conns=0&connect_timeout=1"
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	cfg := config.Config{
		StorageSecret: "storage-root-required-test-only", HTTPAddr: "127.0.0.1:0",
	}
	a := newAppRuntime(pool, pool, cfg, slog.Default())
	ws, sse, err := a.mountRoutes(ctx, db, http.NewServeMux(), cfg, ratelimit.New(ratelimit.Config{}))
	if err == nil {
		t.Fatal("empty storage root must return an error, not assemble routes")
	}
	if ws != nil || sse != nil {
		t.Fatal("rejected assembly must not return handlers")
	}
}

func TestServeHTTPBindFailure(t *testing.T) {
	err := serveHTTP(context.Background(), http.NewServeMux(), config.Config{HTTPAddr: "127.0.0.1:invalid"}, slog.Default(), ratelimit.New(ratelimit.Config{}), nil)
	if err == nil {
		t.Fatal("invalid listener address must return its bind error")
	}
}

func TestRunDatabaseReleasesGaugesIntegration(t *testing.T) {
	fixture := testkit.NewPostgres(t, testkit.PostgresOptions{})
	db, err := sql.Open("pgx", fixture.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg := config.Config{
		DatabaseURL: fixture.DSN, HTTPAddr: "127.0.0.1:invalid",
		StorageSecret: "gauge-lifecycle-test-only", InvalidationBus: "none",
		StorageRoot: t.TempDir(),
	}
	if err := runDatabase(ctx, db, http.NewServeMux(), cfg, slog.Default(), ratelimit.New(ratelimit.Config{})); err == nil {
		t.Fatal("expected listener bind failure")
	}
	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		switch family.GetName() {
		case "instant_notifier_queue_depth", "instant_ws_sessions_active", "instant_db_pool_conns":
			t.Errorf("runtime returned but still owns gauge %s", family.GetName())
		}
	}
}
