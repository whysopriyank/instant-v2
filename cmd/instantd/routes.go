package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/instant-v2/instant-v2/internal/adminapi"
	"github.com/instant-v2/instant-v2/internal/authn"
	"github.com/instant-v2/instant-v2/internal/backup"
	"github.com/instant-v2/instant-v2/internal/config"
	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/ratelimit"
	"github.com/instant-v2/instant-v2/internal/reactive"
	"github.com/instant-v2/instant-v2/internal/runtimeapi"
	"github.com/instant-v2/instant-v2/internal/storageapi"
	syncpkg "github.com/instant-v2/instant-v2/internal/sync"
)

func (a *appRuntime) mountRoutes(ctx context.Context, db *sql.DB, mux *http.ServeMux, cfg config.Config, limiter *ratelimit.Limiter) (*syncpkg.WSHandler, *syncpkg.SSEHandler) {
	authSvc := &authn.Service{
		DB:       a.st,
		Pool:     a.pool,
		Catalogs: a.cats,
		Logger:   a.logger,
	}
	startAuthMaintenance(ctx, authSvc, a.logger)
	// Shared per-app traffic budgets (docs/03 §9): HTTP middleware and the
	// WS/SSE frame gates draw from one token-bucket set.
	mgr := syncpkg.NewManager(syncpkg.Deps{
		DB:              a.st,
		Catalogs:        a.cats,
		Store:           a.store,
		Rooms:           syncpkg.NewRoomHub(),
		Auth:            authSvc,
		Rules:           a.cats.RuleDocFor,
		Limiter:         limiterAdapter{limiter},
		OnCommit:        a.invalidate, // set below once notifier + bus exist
		OnCommitChanges: a.invalidateChanges,
		// Overload gate (docs/09 §T2.1): shed transacts when the
		// refresh queue crosses MaxQueueDepth; off by default.
		TransactGate: gateFor(a.notifier, cfg.MaxQueueDepth),
		Logger:       a.logger,
	})
	ws := &syncpkg.WSHandler{
		Manager:        mgr,
		Store:          a.store,
		Compression:    cfg.WSCompression,
		MaxConns:       cfg.MaxWSConns,
		AllowedOrigins: strings.Split(cfg.WSAllowedOrigins, ","),
		ReadLimit:      int64(cfg.MaxFrameBytes),
		Refresh: func(ctx context.Context, sub *reactive.Subscription) (json.RawMessage, error) {
			q, err := instaql.Coerce(rawToMap(sub.Query))
			if err != nil {
				return nil, err
			}
			cat, err := a.cats.For(ctx, sub.AppID)
			if err != nil {
				return nil, err
			}
			appID, err := platform.ScanUUIDErr(sub.AppID)
			if err != nil {
				return nil, err
			}
			runner := a.ex
			if gate, ok := sub.AttachCtx.(*syncpkg.QueryGate); ok && gate != nil {
				// Reproduce the visibility the group was admitted under.
				runner = &instaql.Executor{DB: a.readPool, Rules: gate.Rules, Admin: gate.Admin}
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
		Store:          a.store,
		Refresh:        refreshFor(a.ex, a.cats),
		MaxConns:       cfg.MaxSSEConns,
		MaxConnsPerIP:  cfg.MaxSSEConnsPerIP,
		HeartbeatEvery: 20 * time.Second,
		Limiter:        limiter,
	}
	sseH.AdminAuth = func(ctx context.Context, appID, token string) bool {
		ok, err := a.cats.CheckAdminToken(ctx, appID, token)
		return err == nil && ok
	}
	mux.HandleFunc("POST /admin/subscribe-query", sseH.AdminSubscribe)
	mux.Handle("GET /runtime/session", ws)
	mux.HandleFunc("GET /runtime/sse", sseH.ServeHTTP)
	mux.HandleFunc("POST /runtime/sse", sseH.ServeHTTP)

	runtimeH := &runtimeapi.Handler{
		Pool:     a.pool,
		DB:       a.st,
		Catalogs: a.cats,
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
		Pool:     a.pool,
		DB:       a.st,
		Catalogs: a.cats,
		Logger:   a.logger,
		OnCommit: func(ctx context.Context, appID [16]byte, attrIDs []string, txID int64, attrsChanged bool) {
			// Admin-plane writes must invalidate live subscribers on
			// every node. Detach: the request context dies when
			// handleTransact returns, but refreshes must outlive it.
			go a.invalidate(context.WithoutCancel(ctx), platform.UUIDToStr(appID), attrIDs, txID, attrsChanged)
		},
		OnCommitChanges: func(ctx context.Context, appID [16]byte, changes []reactive.Change, txID int64, attrsChanged bool) {
			go a.invalidateChanges(context.WithoutCancel(ctx), platform.UUIDToStr(appID), changes, txID, attrsChanged)
		},
	})

	storeSecret := []byte(cfg.StorageSecret)
	if len(storeSecret) == 0 && cfg.InsecureDevMode {
		// Audit M2: the deterministic dev fallback signs with the DSN
		// itself — which usually contains the DB password. Refuse it
		// on any non-loopback listener so prod can't slide into it.
		if !loopbackAddr(cfg.HTTPAddr) {
			a.logger.Error("refusing to start: INSTANT_V2_STORAGE_SECRET is required when " +
				"listening on a non-loopback address (insecure dev-secret fallback is loopback-only)")
			os.Exit(1)
		}
		a.logger.Warn("INSECURE dev mode: storage signatures derive from DATABASE_URL; never expose beyond loopback")
		storeSecret = []byte(cfg.DatabaseURL) // deterministic dev fallback (insecure; dev only)
	}
	mux.Handle("/storage/", &storageapi.Handler{
		Store:           storageapi.NewDiskBackend(os.TempDir()+"/instantv2-files", storeSecret),
		Secret:          storeSecret,
		Triples:         a.st,
		Catalogs:        a.cats,
		AdminTokenCheck: a.cats.CheckAdminToken,
		MaxUploadBytes:  cfg.MaxUploadBytes,
	})

	// Backup/restore surface (docs/07 §backup). Every route re-checks
	// the app's admin token; object routes scope keys under the app id
	// and stay 503 until an S3-compatible store is wired.
	mux.Handle("/backup/", &backup.Handler{
		Pool:            a.pool,
		AdminTokenCheck: a.cats.CheckAdminToken,
		Logger:          a.logger,
	})

	gate := gateFor(a.notifier, cfg.MaxQueueDepth)
	mux.Handle("POST /runtime/transact", transactHandler(a.st, a.cats, a.invalidate, a.invalidateChanges, gate, a.logger))
	mux.HandleFunc("GET /health", health(db, a.notifier))
	return ws, sseH
}

func startAuthMaintenance(ctx context.Context, authSvc *authn.Service, logger *slog.Logger) {
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
}
