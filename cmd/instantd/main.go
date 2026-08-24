package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"syscall"

	"github.com/instant-v2/instant-v2/internal/adminapi"
	"github.com/instant-v2/instant-v2/internal/authn"
	"github.com/instant-v2/instant-v2/internal/bus"
	"github.com/instant-v2/instant-v2/internal/config"
	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
	"github.com/instant-v2/instant-v2/internal/runtimeapi"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/storageapi"
	syncpkg "github.com/instant-v2/instant-v2/internal/sync"
	"github.com/instant-v2/instant-v2/internal/transact"
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
	if db == nil {
		mux.HandleFunc("GET /health", health(nil, nil, cfg))
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
		readPool, err := pgxpool.NewWithConfig(ctx, readCfg)
		if err != nil {
			return err
		}
		defer readPool.Close()

		pool := writePool // legacy alias for write-plane services below

		st := storage.New(pool)
		cats := platform.NewCatalogCache(pool, pool)
		store = reactive.NewStore()
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
		var invalidate func(ctx context.Context, appID string, attrIDs []string, txID int64)
		var invalidateChanges func(ctx context.Context, appID string, changes []reactive.Change, txID int64)
		if cfg.InvalidationBus == "postgres" {
			pubConn, perr := dedicatedConn(ctx, writePool)
			if perr != nil {
				return fmt.Errorf("bus publisher conn: %w", perr)
			}
			defer pubConn.Release()
			publisher = &pgPublisher{pg: pubConn.Conn()}

			listenConn, lerr := dedicatedConn(ctx, writePool)
			if lerr != nil {
				return fmt.Errorf("bus listener conn: %w", lerr)
			}
			defer listenConn.Release()
			go func() {
				err := bus.RunWithLogger(ctx, listenConn.Conn(), func(inv bus.Invalidation) {
					// APPLY ONLY — never republish. Routing received
					// events through the invalidate closures would echo
					// every event back onto the bus from every node: an
					// infinite amplify loop that starves the listeners
					// (observed live as pgx "conn busy" storms).
					// Entity-annotated events let peers splice too
					// (docs/09 §T2.5); degraded payloads carry attr ids
					// only and take the topic-wide path.
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
				if err != nil && ctx.Err() == nil {
					logger.Error("invalidation bus stopped", "err", err)
				}
			}()
			logger.Info("invalidation bus enabled", "channel", bus.Channel, "node", cfg.NodeName())
		}
		invalidate = func(ctx context.Context, appID string, attrIDs []string, txID int64) {
			notifier.Notify(ctx, appID, attrIDs, txID)
			if publisher != nil {
				go func() {
					if err := publisher.PublishInvalidation(context.WithoutCancel(ctx),
						bus.Invalidation{AppID: appID, AttrIDs: attrIDs, TxID: txID}); err != nil {
						logger.Warn("bus publish failed", "err", err)
					}
				}()
			}
		}

		// Change-routed variant: entity-annotated events feed the
		// incremental engine locally AND on peers (docs/09 §T2.5). The
		// attr-id projection rides along so Encode's staged size
		// degradation can drop Changes first and keep topic granularity.
		invalidateChanges = func(ctx context.Context, appID string, changes []reactive.Change, txID int64) {
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
						bus.Invalidation{AppID: appID, AttrIDs: attrs, TxID: txID, Changes: entity}); err != nil {
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
		mgr := syncpkg.NewManager(syncpkg.Deps{
			DB:              st,
			Catalogs:        cats,
			Store:           store,
			Rooms:           syncpkg.NewRoomHub(),
			Auth:            authSvc,
			OnCommit:        invalidate, // set below once notifier + bus exist
			OnCommitChanges: invalidateChanges,
			// Overload gate (docs/09 §T2.1): shed transacts when the
			// refresh queue crosses MaxQueueDepth; off by default.
			TransactGate: gateFor(notifier, cfg.MaxQueueDepth),
			Logger:       logger,
		})
		ws = &syncpkg.WSHandler{
			Manager:     mgr,
			Store:       store,
			Compression: cfg.WSCompression,
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
				res, err := ex.Run(ctx, q, cat, appID)
				if err != nil {
					return nil, err
				}
				return json.Marshal(res)
			},
		}
		sseH := &syncpkg.SSEHandler{
			Manager: mgr,
			Store:   store,
			Refresh: refreshFor(ex, cats),
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
			OnCommit: func(ctx context.Context, appID [16]byte, attrIDs []string, txID int64) {
				// Admin-plane writes must invalidate live subscribers on
				// every node. Detach: the request context dies when
				// handleTransact returns, but refreshes must outlive it.
				go invalidate(context.WithoutCancel(ctx), platform.UUIDToStr(appID), attrIDs, txID)
			},
			OnCommitChanges: func(ctx context.Context, appID [16]byte, changes []reactive.Change, txID int64) {
				go invalidateChanges(context.WithoutCancel(ctx), platform.UUIDToStr(appID), changes, txID)
			},
		})

		storeSecret := []byte(os.Getenv("INSTANT_V2_STORAGE_SECRET"))
		if len(storeSecret) == 0 {
			storeSecret = []byte(cfg.DatabaseURL) // deterministic dev fallback
		}
		mux.Handle("/storage/", &storageapi.Handler{
			Store:    storageapi.NewDiskBackend(os.TempDir()+"/instantv2-files", storeSecret),
			Secret:   storeSecret,
			Triples:  st,
			Catalogs: cats,
		})

		gate := gateFor(notifier, cfg.MaxQueueDepth)
		mux.Handle("POST /runtime/transact", transactHandler(st, cats, invalidate, invalidateChanges, gate))
		mux.HandleFunc("GET /health", health(db, notifier, cfg))
	}

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

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

func transactHandler(st *storage.DB, cats *platform.CatalogCache,
	invalidate func(ctx context.Context, appID string, attrIDs []string, txID int64),
	invalidateChanges func(ctx context.Context, appID string, changes []reactive.Change, txID int64),
	gate func(appID string) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
			http.Error(w, `{"error":"`+err.Error()+`"}`, 400)
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
		res, err := transact.Transact(r.Context(), st, cat, appID, parsed, transact.Options{}, nil)
		if err != nil {
			http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusForbidden)
			return
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
				go invalidateChanges(context.WithoutCancel(r.Context()), req.AppID, changes, res.TxID)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"tx-id": res.TxID})
				return
			}
		}
		attrsTouched := touchedAttrs(parsed, cat)
		go invalidate(context.WithoutCancel(r.Context()), req.AppID, attrsTouched, res.TxID)
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
		res, err := ex.Run(ctx, q, cat, appID)
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
func health(db *sql.DB, n *reactive.Notifier, cfg config.Config) http.HandlerFunc {
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
		body := map[string]any{
			"ok":          true,
			"db":          true,
			"node":        cfg.NodeName(),
			"queue-depth": 0,
			"queue-max":   cfg.MaxQueueDepth,
			"bus":         cfg.InvalidationBus,
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
