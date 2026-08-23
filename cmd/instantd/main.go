package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/instant-v2/instant-v2/internal/adminapi"
	"github.com/instant-v2/instant-v2/internal/authn"
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

// notifierBridge connects storage writes to the reactive invalidator for the
// single-instance deployment: after each committed transact, notify directly.
type notifierBridge struct {
	pool *pgxpool.Pool
	n    *reactive.Notifier
}

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
	mux.HandleFunc("GET /health", health(db))

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

		poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
		if err != nil {
			return err
		}
		if poolCfg.MaxConns < 32 {
			poolCfg.MaxConns = 32 // reactive refreshes + transacts share the pool
		}
		pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
		if err != nil {
			return err
		}
		defer pool.Close()

		st := storage.New(pool)
		cats := platform.NewCatalogCache(pool, pool)
		store = reactive.NewStore()
		ex := &instaql.Executor{DB: pool}
		notifier = &reactive.Notifier{
			Store:   store,
			Refresh: refreshFor(ex, cats),
			Logger:  logger,
		}
		go notifier.Run(ctx)

		authSvc := &authn.Service{
			DB:       st,
			Pool:     pool,
			Catalogs: cats,
			Logger:   logger,
		}
		mgr := syncpkg.NewManager(syncpkg.Deps{
			DB:       st,
			Catalogs: cats,
			Store:    store,
			Rooms:    syncpkg.NewRoomHub(),
			Auth:     authSvc,
			OnCommit: func(ctx context.Context, appID string, attrIDs []string, txID int64) {
				// Single-node direct post-commit invalidation.
				notifier.Notify(ctx, appID, attrIDs, txID)
			},
			Logger: logger,
		})
		ws = &syncpkg.WSHandler{
			Manager: mgr,
			Store:   store,
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
				// Admin-plane writes must invalidate live subscribers too.
				// Detach: the request context dies when handleTransact
				// returns, but refreshes must outlive it.
				go notifier.Notify(context.WithoutCancel(ctx), platform.UUIDToStr(appID), attrIDs, txID)
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

		// Post-commit invalidation bridge: wraps transact via HTTP-level hook.
		bridge := &notifierBridge{pool: pool, n: notifier}
		mux.Handle("POST /runtime/transact", transactHandler(st, cats, bridge))
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

func transactHandler(st *storage.DB, cats *platform.CatalogCache, b *notifierBridge) http.HandlerFunc {
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
		// Direct post-commit notification (single-instance path).
		attrsTouched := touchedAttrs(parsed, cat)
		go b.n.Notify(r.Context(), req.AppID, attrsTouched, res.TxID)
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

func health(db *sql.DB) http.HandlerFunc {
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
		_, _ = w.Write([]byte(`{"ok":true,"db":true}`))
	}
}

var _ = websocket.Accept
var _ = sync.Mutex{}
