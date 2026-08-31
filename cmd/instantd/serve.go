package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/instant-v2/instant-v2/internal/config"
	"github.com/instant-v2/instant-v2/internal/metrics"
	"github.com/instant-v2/instant-v2/internal/ratelimit"
	syncpkg "github.com/instant-v2/instant-v2/internal/sync"
)

func serveHTTP(ctx context.Context, mux *http.ServeMux, cfg config.Config, logger *slog.Logger, limiter *ratelimit.Limiter, ws *syncpkg.WSHandler) error {
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
