package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/instant-v2/instant-v2/internal/adminapi"
	"github.com/instant-v2/instant-v2/internal/authn"
	"github.com/instant-v2/instant-v2/internal/backup"
	"github.com/instant-v2/instant-v2/internal/bus"
	"github.com/instant-v2/instant-v2/internal/config"
	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/metrics"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/ratelimit"
	"github.com/instant-v2/instant-v2/internal/reactive"
	"github.com/instant-v2/instant-v2/internal/runtimeapi"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/storageapi"
	syncpkg "github.com/instant-v2/instant-v2/internal/sync"
	"github.com/instant-v2/instant-v2/internal/tracing"
	"github.com/instant-v2/instant-v2/internal/transact"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"strings"
	"syscall"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
	if err := run(logger); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	logger.Info("starting instantd", "config", cfg.String())
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// OTel export is opt-in via OTEL_EXPORTER_OTLP_ENDPOINT; without it the
	// global provider stays a no-op and spans cost nothing.
	if shutdownTracing, terr := tracing.Init(ctx, tracing.Endpoint(), "instantd", cfg.NodeName()); terr != nil {
		logger.Warn("tracing init failed; continuing untraced", "err", terr)
	} else if shutdownTracing != nil {
		defer func() { _ = shutdownTracing(context.Background()) }()
	}

	var db *sql.DB
	if cfg.DatabaseURL == "" {
		logger.Warn("DATABASE_URL empty; serving /health only")
		db = nil
	} else {
		db, err = sql.Open("pgx", cfg.DatabaseURL)
		if err != nil {
			return err
		}
		defer db.Close()
	}

	mux := http.NewServeMux()

	// Shared per-app traffic budgets (docs/03 §9): the HTTP middleware and
	// the WS/SSE frame gates draw from one token-bucket set.
	limiter := ratelimit.New(ratelimit.Config{})
	// Proactive eviction of idle buckets (audit H4): without this the map
	// only shrinks lazily when the cap is hit under pressure.
	go limiter.SweepLoop(ctx, time.Minute)
	if db == nil {
		mux.HandleFunc("GET /health", health(nil, nil))
	}

	var (
		store    *reactive.Store
		notifier *reactive.Notifier
		ws       *syncpkg.WSHandler
	)
	if db != nil {
		if err := platform.Migrate(ctx, db); err != nil {
			return err
		}
		v, err := platform.CurrentVersion(ctx, db)
		if err != nil {
			return err
		}
		logger.Info("schema ready", "version", v)

		// Read/write plane split (docs/09-tier2-architecture.md §T2.3):
		// writes and identity-sensitive reads (catalog cache) stay on the
		// writer; instaql refreshes route to the read pool so snapshot
		// storms cannot starve transactors of connections. With
		// INSTANT_V2_READ_URL unset this is the same instance — two pools,
		// two budgets, one server.
		writeCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
		if err != nil {
			return err
		}
		if cfg.WritePoolMaxConns > 0 {
			writeCfg.MaxConns = cfg.WritePoolMaxConns
		} else if writeCfg.MaxConns < 32 {
			writeCfg.MaxConns = 32 // reactive refreshes + transacts share the writer budget
		}
		storage.ApplyStatementLimits(writeCfg, cfg.PGStatementTimeout, cfg.PGLockTimeout, cfg.PGIdleTxTimeout)
		writePool, err := pgxpool.NewWithConfig(ctx, writeCfg)
		if err != nil {
			return err
		}
		defer writePool.Close()

		readDSN := cfg.ReadDatabaseURL
		if readDSN == "" {
			readDSN = cfg.DatabaseURL
		}
		readCfg, err := pgxpool.ParseConfig(readDSN)
		if err != nil {
			return err
		}
		if cfg.ReadPoolMaxConns > 0 {
			readCfg.MaxConns = cfg.ReadPoolMaxConns
		} else if readCfg.MaxConns < 32 {
			readCfg.MaxConns = 32
		}
		storage.ApplyStatementLimits(readCfg, cfg.PGStatementTimeout, cfg.PGLockTimeout, cfg.PGIdleTxTimeout)
		readPool, err := pgxpool.NewWithConfig(ctx, readCfg)
		if err != nil {
			return err
		}
		defer readPool.Close()

		pool := writePool // legacy alias for write-plane services below

		st := storage.New(pool)
		cats := platform.NewCatalogCache(pool, pool)
		store = reactive.NewStore()
		store.MaxSubsPerApp = cfg.MaxSubsPerApp
		ex := &instaql.Executor{DB: readPool} // hot read plane (docs/09 §T2.3)
		notifier = &reactive.Notifier{
			Store:   store,
			Refresh: refreshFor(ex, cats),
			Logger:  logger,
			// Incremental result maintenance (docs/09 §T2.5): splices
			// known entity changes into materialized results; bails to
			// full refresh for anything it cannot prove. instaql stays
			// the oracle — probes mirror its SQL and are pinned by the
			// fuzz differential in internal/reactive.
			Inc: &reactive.Incremental{
				Source: &reactive.InstaqlSource{DB: readPool, Catalog: cats.For},
			},
		}
		go notifier.Run(ctx)

		// Shared invalidation entry point for every write plane (WS,
		// HTTP, admin): local notify first (zero added latency), then bus
		// publish so peers refresh their own subscribers
		// (docs/09-tier2-architecture.md §T2.4). Self-echo is harmless —
		// Notify dedupes via the subscription watermark.
		var publisher bus.Publisher // nil unless the postgres bus is on
		var invalidate func(ctx context.Context, appID string, attrIDs []string, txID int64, attrsChanged bool)
		var invalidateChanges func(ctx context.Context, appID string, changes []reactive.Change, txID int64, attrsChanged bool)
		if cfg.InvalidationBus == "postgres" {
			pubConn, perr := dedicatedConn(ctx, writePool)
			if perr != nil {
				return fmt.Errorf("bus publisher conn: %w", perr)
			}
			defer pubConn.Release()
			publisher = &pgPublisher{pg: pubConn.Conn()}

			// Supervised listener: a Postgres restart must not end
			// cross-node invalidation for the process lifetime. The
			// supervisor re-acquires a dedicated conn and re-LISTENs with
			// bounded backoff; each attempt owns its conn's release.
			acquireListener := func(cctx context.Context) (bus.Conn, func(), error) {
				c, cerr := dedicatedConn(cctx, writePool)
				if cerr != nil {
					return nil, nil, cerr
				}
				return c.Conn(), c.Release, nil
			}
			go bus.RunSupervised(ctx, acquireListener, func(inv bus.Invalidation) {
				// APPLY ONLY — never republish. Routing received
				// events through the invalidate closures would echo
				// every event back onto the bus from every node: an
				// infinite amplify loop that starves the listeners
				// (observed live as pgx "conn busy" storms).
				// Entity-annotated events let peers splice too
				// (docs/09 §T2.5); degraded payloads carry attr ids
				// only and take the topic-wide path.
				//
				// Schema propagation: a peer's cached catalog must not
				// outlive another node's attrs.create — drop it so the
				// next query reloads (cross-node half of the staleness
				// fix; the writer's own node invalidates inline).
				if inv.AttrsChanged {
					cats.Invalidate(inv.AppID)
				}
				if len(inv.Changes) > 0 {
					changes := make([]reactive.Change, 0, len(inv.Changes))
					for _, c := range inv.Changes {
						changes = append(changes, reactive.Change{
							Etype: c.Etype, EntityID: c.EntityID, AttrIDs: c.AttrIDs,
						})
					}
					notifier.NotifyChanges(ctx, inv.AppID, changes, inv.TxID)
					return
				}
				notifier.Notify(ctx, inv.AppID, inv.AttrIDs, inv.TxID)
			}, logger)
			logger.Info("invalidation bus enabled", "channel", bus.Channel, "node", cfg.NodeName())
		}
		invalidate = func(ctx context.Context, appID string, attrIDs []string, txID int64, attrsChanged bool) {
			notifier.Notify(ctx, appID, attrIDs, txID)
			if publisher != nil {
				go func() {
					if err := publisher.PublishInvalidation(context.WithoutCancel(ctx),
						bus.Invalidation{AppID: appID, AttrIDs: attrIDs, TxID: txID, AttrsChanged: attrsChanged}); err != nil {
						logger.Warn("bus publish failed", "err", err)
					}
				}()
			}
		}

		// Change-routed variant: entity-annotated events feed the
		// incremental engine locally AND on peers (docs/09 §T2.5). The
		// attr-id projection rides along so Encode's staged size
		// degradation can drop Changes first and keep topic granularity.
		invalidateChanges = func(ctx context.Context, appID string, changes []reactive.Change, txID int64, attrsChanged bool) {
			notifier.NotifyChanges(ctx, appID, changes, txID)
			if publisher != nil {
				attrSet := map[string]bool{}
				entity := make([]bus.EntityChange, 0, len(changes))
				for _, c := range changes {
					for _, a := range c.AttrIDs {
						attrSet[a] = true
					}
					attrs := append([]string(nil), c.AttrIDs...)
					entity = append(entity, bus.EntityChange{
						Etype: c.Etype, EntityID: c.EntityID, AttrIDs: attrs,
					})
				}
				attrs := make([]string, 0, len(attrSet))
				for a := range attrSet {
					attrs = append(attrs, a)
				}
				go func() {
					if err := publisher.PublishInvalidation(context.WithoutCancel(ctx),
						bus.Invalidation{AppID: appID, AttrIDs: attrs, TxID: txID, Changes: entity, AttrsChanged: attrsChanged}); err != nil {
						logger.Warn("bus publish failed", "err", err)
					}
				}()
			}
		}

		authSvc := &authn.Service{
			DB:       st,
			Pool:     pool,
			Catalogs: cats,
			Logger:   logger,
		}
		// Keep auth_throttle bounded (audit follow-up): prune idle rows at
		// boot and hourly; 24h covers lockout + resend horizons. The same
		// tick sweeps TTL-expired transient auth entities ($magicCodes /
		// $oauthRedirects / $oauthCodes): their expiry otherwise applies
		// lazily-at-consume only, letting anonymous send_magic_code traffic
		// grow the triple store forever (2026-08-27 follow-up audit MED-1).
		maintenance := func() {
			authSvc.PruneThrottle(ctx, 24*time.Hour)
			if n, err := authSvc.SweepExpiredAuthEntities(ctx); err != nil {
				logger.Warn("expired auth artifact sweep failed", "err", err)
			} else if n > 0 {
				logger.Info("expired auth artifacts swept", "triples", n)
			}
		}
		maintenance()
		go func() {
			t := time.NewTicker(time.Hour)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					maintenance()
				}
			}
		}()
		// Shared per-app traffic budgets (docs/03 §9): HTTP middleware and the
		// WS/SSE frame gates draw from one token-bucket set.
		mgr := syncpkg.NewManager(syncpkg.Deps{
			DB:              st,
			Catalogs:        cats,
			Store:           store,
			Rooms:           syncpkg.NewRoomHub(),
			Auth:            authSvc,
			Rules:           cats.RuleDocFor,
			Limiter:         limiterAdapter{limiter},
			OnCommit:        invalidate, // set below once notifier + bus exist
			OnCommitChanges: invalidateChanges,
			// Overload gate (docs/09 §T2.1): shed transacts when the
			// refresh queue crosses MaxQueueDepth; off by default.
			TransactGate: gateFor(notifier, cfg.MaxQueueDepth),
			Logger:       logger,
		})
		ws = &syncpkg.WSHandler{
			Manager:        mgr,
			Store:          store,
			Compression:    cfg.WSCompression,
			MaxConns:       cfg.MaxWSConns,
			AllowedOrigins: strings.Split(cfg.WSAllowedOrigins, ","),
			ReadLimit:      int64(cfg.MaxFrameBytes),
			Refresh: func(ctx context.Context, sub *reactive.Subscription) (json.RawMessage, error) {
				q, err := instaql.Coerce(rawToMap(sub.Query))
				if err != nil {
					return nil, err
				}
				cat, err := cats.For(ctx, sub.AppID)
				if err != nil {
					return nil, err
				}
				appID, err := platform.ScanUUIDErr(sub.AppID)
				if err != nil {
					return nil, err
				}
				runner := ex
				if gate, ok := sub.AttachCtx.(*syncpkg.QueryGate); ok && gate != nil {
					// Reproduce the visibility the group was admitted under.
					runner = &instaql.Executor{DB: readPool, Rules: gate.Rules, Admin: gate.Admin}
				}
				res, err := runner.Run(ctx, q, cat, appID)
				if err != nil {
					return nil, err
				}
				return json.Marshal(res)
			},
		}
		sseH := &syncpkg.SSEHandler{
			Manager:        mgr,
			Store:          store,
			Refresh:        refreshFor(ex, cats),
			MaxConns:       cfg.MaxSSEConns,
			MaxConnsPerIP:  cfg.MaxSSEConnsPerIP,
			HeartbeatEvery: 20 * time.Second,
			Limiter:        limiter,
		}
		sseH.AdminAuth = func(ctx context.Context, appID, token string) bool {
			ok, err := cats.CheckAdminToken(ctx, appID, token)
			return err == nil && ok
		}
		mux.HandleFunc("POST /admin/subscribe-query", sseH.AdminSubscribe)
		mux.Handle("GET /runtime/session", ws)
		mux.HandleFunc("GET /runtime/sse", sseH.ServeHTTP)
		mux.HandleFunc("POST /runtime/sse", sseH.ServeHTTP)

		runtimeH := &runtimeapi.Handler{
			Pool:     pool,
			DB:       st,
			Catalogs: cats,
			Auth:     authSvc,
		}
		// Exact-path routes win over the authn prefix below.
		mux.Handle("POST /runtime/auth/refresh_tokens", runtimeH)
		mux.Handle("POST /runtime/signout", runtimeH)
		mux.Handle("POST /runtime/framework/query", runtimeH)
		mux.Handle("GET /runtime/openid-configuration", runtimeH)
		mux.Handle("GET /runtime/{app_id}/.well-known/openid-configuration", runtimeH)
		mux.Handle("POST /runtime/auth/", &authn.Handler{Service: authSvc})

		mux.Handle("/admin/", &adminapi.Handler{
			Pool:     pool,
			DB:       st,
			Catalogs: cats,
			Logger:   logger,
			OnCommit: func(ctx context.Context, appID [16]byte, attrIDs []string, txID int64, attrsChanged bool) {
				// Admin-plane writes must invalidate live subscribers on
				// every node. Detach: the request context dies when
				// handleTransact returns, but refreshes must outlive it.
				go invalidate(context.WithoutCancel(ctx), platform.UUIDToStr(appID), attrIDs, txID, attrsChanged)
			},
			OnCommitChanges: func(ctx context.Context, appID [16]byte, changes []reactive.Change, txID int64, attrsChanged bool) {
				go invalidateChanges(context.WithoutCancel(ctx), platform.UUIDToStr(appID), changes, txID, attrsChanged)
			},
		})

		storeSecret := []byte(cfg.StorageSecret)
		if len(storeSecret) == 0 && cfg.InsecureDevMode {
			// Audit M2: the deterministic dev fallback signs with the DSN
			// itself — which usually contains the DB password. Refuse it
			// on any non-loopback listener so prod can't slide into it.
			if !loopbackAddr(cfg.HTTPAddr) {
				logger.Error("refusing to start: INSTANT_V2_STORAGE_SECRET is required when " +
					"listening on a non-loopback address (insecure dev-secret fallback is loopback-only)")
				os.Exit(1)
			}
			logger.Warn("INSECURE dev mode: storage signatures derive from DATABASE_URL; never expose beyond loopback")
			storeSecret = []byte(cfg.DatabaseURL) // deterministic dev fallback (insecure; dev only)
		}
		mux.Handle("/storage/", &storageapi.Handler{
			Store:           storageapi.NewDiskBackend(os.TempDir()+"/instantv2-files", storeSecret),
			Secret:          storeSecret,
			Triples:         st,
			Catalogs:        cats,
			AdminTokenCheck: cats.CheckAdminToken,
			MaxUploadBytes:  cfg.MaxUploadBytes,
		})

		// Backup/restore surface (docs/07 §backup). Every route re-checks
		// the app's admin token; object routes scope keys under the app id
		// and stay 503 until an S3-compatible store is wired.
		mux.Handle("/backup/", &backup.Handler{
			Pool:            pool,
			AdminTokenCheck: cats.CheckAdminToken,
			Logger:          logger,
		})

		gate := gateFor(notifier, cfg.MaxQueueDepth)
		mux.Handle("POST /runtime/transact", transactHandler(st, cats, invalidate, invalidateChanges, gate, logger))
		mux.HandleFunc("GET /health", health(db, notifier))

		// Scrape-time gauges over live state (no polling goroutines).
		metrics.RegisterGauge("instant_notifier_queue_depth",
			"Pending refreshes awaiting a notifier drain.", nil, nil,
			func() float64 { return float64(notifier.QueueDepth()) })
		metrics.RegisterGauge("instant_ws_sessions_active",
			"Live websocket/SSE sessions.", nil, nil,
			func() float64 { return float64(ws.ConnCount()) + float64(sseH.ConnCount()) })
		for name, p := range map[string]*pgxpool.Pool{"write": writePool, "read": readPool} {
			for state, fn := range map[string]func() int32{
				"acquired": func() int32 { return p.Stat().AcquiredConns() },
				"idle":     func() int32 { return p.Stat().IdleConns() },
				"max":      func() int32 { return p.Stat().MaxConns() },
			} {
				metrics.RegisterGauge("instant_db_pool_conns",
					"pgxpool connections by state.", []string{"pool", "state"},
					[]string{name, state}, func() float64 { return float64(fn()) })
			}
		}
	}

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           assembleMiddleware(mux, cfg, logger, limiter),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	// Prometheus scrape endpoint (docs/09 §T3). Separate listener so the
	// public mux never serves operational internals; loopback default,
	// INSTANT_V2_METRICS_ADDR="" disables.
	if cfg.MetricsAddr != "" {
		metricsSrv := &http.Server{
			Addr:              cfg.MetricsAddr,
			Handler:           metrics.Handler(),
			ReadHeaderTimeout: 5 * time.Second,
			IdleTimeout:       120 * time.Second, // bounded keep-alives; docs/11 §LOW claims this knob
		}
		go func() {
			if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("metrics server failed", "err", err)
			}
		}()
		defer func() { _ = metricsSrv.Close() }()
	}

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
		// Phase 4 order: stop accepting → drain live WS sessions (1001) →
		// then close the HTTP server.
		if ws != nil {
			drainCtx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
			ws.Drain(drainCtx)
			dcancel()
			logger.Info("ws sessions drained")
		}
		shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return srv.Shutdown(shutCtx)
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

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

// writeJSONError emits a safely-encoded {"error": ...} body. Never build
// error JSON by string concatenation — messages contain client input and
// provider strings that must not break the envelope.
func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// transactHandler runs one runtime transaction batch.
func transactHandler(st *storage.DB, cats *platform.CatalogCache,
	invalidate func(ctx context.Context, appID string, attrIDs []string, txID int64, attrsChanged bool),
	invalidateChanges func(ctx context.Context, appID string, changes []reactive.Change, txID int64, attrsChanged bool),
	gate func(appID string) error, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := tracing.Tracer.Start(ctx, "transact.runtime")
		defer span.End()
		var req struct {
			AppID string            `json:"app-id"`
			Steps []json.RawMessage `json:"tx-steps"`
			Rules bool              `json:"-"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"bad json"}`, 400)
			return
		}
		// Overload gate (docs/09 §T2.1): 429 + Retry-After before any
		// Postgres work, mirroring the WS shed frame.
		if gate != nil {
			if gerr := gate(req.AppID); gerr != nil {
				var shed *reactive.ShedError
				retry := time.Second
				if errors.As(gerr, &shed) && shed.RetryAfter > 0 {
					retry = shed.RetryAfter
				}
				w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(retry.Seconds()))))
				http.Error(w, `{"error":"server busy"}`, http.StatusTooManyRequests)
				return
			}
		}
		parsed, err := transact.ParseSteps(req.Steps)
		if err != nil {
			writeJSONError(w, 400, err.Error())
			return
		}
		cat, err := cats.For(r.Context(), req.AppID)
		if err != nil {
			http.Error(w, `{"error":"unknown app"}`, 404)
			return
		}
		appID, err := platform.ScanUUIDErr(req.AppID)
		if err != nil {
			http.Error(w, `{"error":"bad app id"}`, 400)
			return
		}
		doc, derr := cats.RuleDocFor(r.Context(), req.AppID)
		if derr != nil {
			// Fail closed: an unloadable rule doc must never widen access.
			logger.Error("transact: rules load failed", "app", req.AppID, "err", derr)
			writeJSONError(w, http.StatusInternalServerError, "internal error")
			return
		}
		started := time.Now()
		res, err := transact.Transact(ctx, st, cat, appID, parsed, transact.Options{}, doc)
		metrics.TransactDuration.WithLabelValues("runtime").Observe(time.Since(started).Seconds())
		if err != nil {
			writeJSONError(w, http.StatusForbidden, platform.ClientMessage(err))
			return
		}
		if res.AttrsChanged {
			cats.Invalidate(req.AppID)
		}
		// Post-commit invalidation on every node (docs/09 §T2.4); the
		// change-routed variant feeds the incremental engine (§T2.5).
		if invalidateChanges != nil {
			if triples, ok := transact.ResolveTriples(parsed, cat); ok && len(triples) > 0 {
				changes := make([]reactive.Change, 0, len(triples))
				for _, tt := range triples {
					changes = append(changes, reactive.Change{
						Etype: tt.Etype, EntityID: tt.EntityID, AttrIDs: []string{tt.AttrID},
					})
				}
				go invalidateChanges(context.WithoutCancel(r.Context()), req.AppID, changes, res.TxID, res.AttrsChanged)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"tx-id": res.TxID})
				return
			}
		}
		attrsTouched := touchedAttrs(parsed, cat)
		go invalidate(context.WithoutCancel(r.Context()), req.AppID, attrsTouched, res.TxID, res.AttrsChanged)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"tx-id": res.TxID})
	}
}

func touchedAttrs(steps []transact.Step, _ *platform.AttrCatalog) []string {
	var out []string
	for _, s := range steps {
		switch s.Op {
		case "add-triple", "deep-merge-triple", "retract-triple":
			if len(s.Args) >= 2 {
				var attrStr string
				if json.Unmarshal(s.Args[1], &attrStr) == nil {
					out = append(out, attrStr)
				}
			}
		}
	}

	return out
}

// pgPublisher adapts a dedicated connection to bus.Publisher. The listener
// gets its own conn: pgconn is not safe for concurrent Exec +
// WaitForNotification. The mutex serializes publishes because pgx.Conn is
// single-flight — concurrent goroutines publishing invalidations would trip
// "conn busy".
type pgPublisher struct {
	mu sync.Mutex
	pg bus.Conn
}

func (p *pgPublisher) PublishInvalidation(ctx context.Context, inv bus.Invalidation) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return bus.Publish(ctx, p.pg, inv)
}

func refreshFor(ex *instaql.Executor, cats *platform.CatalogCache) func(ctx context.Context, sub *reactive.Subscription) (json.RawMessage, error) {
	return func(ctx context.Context, sub *reactive.Subscription) (json.RawMessage, error) {
		q, err := instaql.Coerce(rawToMap(sub.Query))
		if err != nil {
			return nil, err
		}
		cat, err := cats.For(ctx, sub.AppID)
		if err != nil {
			return nil, err
		}
		appID, err := platform.ScanUUIDErr(sub.AppID)
		if err != nil {
			return nil, err
		}
		runner := ex
		if gate, ok := sub.AttachCtx.(*syncpkg.QueryGate); ok && gate != nil {
			// Reproduce the visibility the group was admitted under: closed
			// view rules render empty; dynamic ones were rejected pre-attach.
			runner = &instaql.Executor{DB: ex.DB, Rules: gate.Rules, Admin: gate.Admin}
		}
		res, err := runner.Run(ctx, q, cat, appID)
		if err != nil {
			return nil, err
		}
		return json.Marshal(res)
	}
}

func rawToMap(raw json.RawMessage) map[string]any {
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	return m
}

// health reports liveness plus the overload gauges operators (and LBs) need:
// notifier queue depth and, when shedding is enabled, its configured ceiling
// (docs/09-tier2-architecture.md §T2.1).
func health(db *sql.DB, n *reactive.Notifier) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if db == nil {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true,"db":false}`))
			return
		}
		if err := db.PingContext(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"ok":false,"db":true}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		// Deliberately minimal: operational internals (node id, queue depth,
		// bus mode) live on the loopback metrics listener, not a public
		// endpoint. Load balancers only need liveness + db reachability.
		body := map[string]any{
			"ok": true,
			"db": true,
		}
		if n != nil {
			body["queue-depth"] = n.QueueDepth()
		}
		_ = json.NewEncoder(w).Encode(body)
	}
}

// gateFor returns the transact shed gate, or nil when shedding is disabled
// (MaxQueueDepth <= 0 preserves pre-T2 unbounded-queue behavior).
func gateFor(n *reactive.Notifier, maxDepth int64) func(appID string) error {
	if n == nil || maxDepth <= 0 {
		return nil
	}
	return n.Gate(maxDepth)
}

// dedicatedConn pins one pool connection for the process lifetime. LISTEN
// state dies with the connection, so the bus must never share pooled conns.
func dedicatedConn(ctx context.Context, pool *pgxpool.Pool) (*pgxpool.Conn, error) {
	return pool.Acquire(ctx)
}

var _ = websocket.Accept
var _ = sync.Mutex{}

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
