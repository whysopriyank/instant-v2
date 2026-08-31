package main

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/instant-v2/instant-v2/internal/bus"
	"github.com/instant-v2/instant-v2/internal/config"
	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/ratelimit"
	"github.com/instant-v2/instant-v2/internal/reactive"
	"github.com/instant-v2/instant-v2/internal/storage"
)

// appRuntime holds the concrete services shared by HTTP transports and the
// invalidation bus. Pool lifetimes remain owned by runDatabase.
type appRuntime struct {
	pool, readPool *pgxpool.Pool
	st             *storage.DB
	cats           *platform.CatalogCache
	store          *reactive.Store
	ex             *instaql.Executor
	notifier       *reactive.Notifier
	logger         *slog.Logger
	publisher      bus.Publisher
}

// runDatabase preserves resource ownership: publisher release precedes read
// and write pool closure; run closes the migration DB and tracing afterward.
func runDatabase(ctx context.Context, db *sql.DB, mux *http.ServeMux, cfg config.Config, logger *slog.Logger, limiter *ratelimit.Limiter) error {
	if err := platform.Migrate(ctx, db); err != nil {
		return err
	}
	v, err := platform.CurrentVersion(ctx, db)
	if err != nil {
		return err
	}
	logger.Info("schema ready", "version", v)

	writeCfg, err := databasePoolConfig(cfg.DatabaseURL, cfg.WritePoolMaxConns, cfg)
	if err != nil {
		return err
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
	readCfg, err := databasePoolConfig(readDSN, cfg.ReadPoolMaxConns, cfg)
	if err != nil {
		return err
	}
	readPool, err := pgxpool.NewWithConfig(ctx, readCfg)
	if err != nil {
		return err
	}
	defer readPool.Close()

	a := newAppRuntime(writePool, readPool, cfg, logger)
	go a.notifier.Run(ctx)
	pubConn, err := a.startInvalidationBus(ctx, cfg)
	if err != nil {
		return err
	}
	if pubConn != nil {
		defer pubConn.Release()
	}

	ws, sse := a.mountRoutes(ctx, db, mux, cfg, limiter)
	a.registerGauges(ws, sse)
	return serveHTTP(ctx, mux, cfg, logger, limiter, ws)
}

// databasePoolConfig applies the same sizing and statement limits to both
// planes. An unset per-plane maximum retains the existing minimum of 32.
func databasePoolConfig(dsn string, maxConns int32, cfg config.Config) (*pgxpool.Config, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if maxConns > 0 {
		poolCfg.MaxConns = maxConns
	} else if poolCfg.MaxConns < 32 {
		poolCfg.MaxConns = 32 // reactive refreshes + transacts share the writer budget
	}
	if cfg.PoolMinConns > 0 {
		poolCfg.MinConns = cfg.PoolMinConns
		if poolCfg.MinConns > poolCfg.MaxConns {
			poolCfg.MinConns = poolCfg.MaxConns
		}
	}
	storage.ApplyStatementLimits(poolCfg, cfg.PGStatementTimeout, cfg.PGLockTimeout, cfg.PGIdleTxTimeout)
	return poolCfg, nil
}

func newAppRuntime(pool, readPool *pgxpool.Pool, cfg config.Config, logger *slog.Logger) *appRuntime {
	st := storage.New(pool)
	cats := platform.NewCatalogCache(pool, pool)
	store := reactive.NewStore()
	store.MaxSubsPerApp = cfg.MaxSubsPerApp
	ex := &instaql.Executor{DB: readPool} // hot read plane (docs/09 §T2.3)
	notifier := &reactive.Notifier{
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

	return &appRuntime{pool: pool, readPool: readPool, st: st, cats: cats,
		store: store, ex: ex, notifier: notifier, logger: logger}
}
