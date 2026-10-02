package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/instant-v2/instant-v2/internal/config"
	"github.com/instant-v2/instant-v2/internal/ratelimit"
	"github.com/instant-v2/instant-v2/internal/tracing"
)

var version = "dev"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
	if err := runCLI(logger, os.Args[1:]); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func runCLI(logger *slog.Logger, args []string) error {
	if len(args) == 0 {
		return run(logger)
	}
	switch args[0] {
	case "healthcheck":
		return healthcheck(args[1:])
	case "version", "--version":
		fmt.Println(version)
		return nil
	default:
		return fmt.Errorf("unknown command %q (available: healthcheck, version)", args[0])
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
		return serveHTTP(ctx, mux, cfg, logger, limiter, nil)
	}
	return runDatabase(ctx, db, mux, cfg, logger, limiter)
}
