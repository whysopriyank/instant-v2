package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
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
	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/ratelimit"
	"github.com/instant-v2/instant-v2/internal/reactive"
	"github.com/instant-v2/instant-v2/internal/runtimeapi"
	"github.com/instant-v2/instant-v2/internal/storageapi"
	syncpkg "github.com/instant-v2/instant-v2/internal/sync"
)

func (a *appRuntime) mountRoutes(ctx context.Context, db *sql.DB, mux *http.ServeMux, cfg config.Config, limiter *ratelimit.Limiter) (*syncpkg.WSHandler, *syncpkg.SSEHandler, error) {
	authSvc := &authn.Service{
		DB:       a.st,
		Pool:     a.pool,
		Catalogs: a.cats,
		Logger:   a.logger,
		// Signup authorization must resolve the persisted per-app rules. Keep
		// this on the same catalog cache used by queries/transacts so an
		// invalidation cannot leave auth with a stale $users.create decision.
		RulesForFn: func(ctx context.Context, appID [16]byte) (*perms.RuleDoc, error) {
			return a.cats.RuleDocFor(ctx, platform.UUIDToStr(appID))
		},
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
			// RT-001: run under the newest LOADED doc; drop the
			// generation when current rules cannot be loaded.
			gate, _, rerr := mgr.RefreshGate(ctx, sub)
			if rerr != nil {
				return nil, rerr
			}
			runner := a.ex
			gated := gate != nil
			if gated {
				runner = &instaql.Executor{DB: a.readPool, Rules: gate.Rules, Admin: gate.Admin}
			}
			res, err := runner.Run(ctx, q, cat, appID)
			if err != nil {
				return nil, err
			}
			if gated {
				// RT-001 publication-time validation: a concurrent
				// re-gate may have landed while the executor ran; drop
				// the result rather than publish stale-authorized
				// state (same guard as refreshFor).
				now, _, rerr := mgr.RefreshGate(ctx, sub)
				if rerr != nil {
					return nil, rerr
				}
				if now == nil || syncpkg.GateHash(now.Rules) != syncpkg.GateHash(gate.Rules) {
					return nil, errRefreshSuperseded
				}
			}
			return json.Marshal(res)
		},
	}
	// RT-001: the steady-state notifier was built before mgr existed
	// (runtime.go); rebind its Refresh now so every generation runs under
	// the newest loaded doc, and hook Revalidate so a re-gated generation
	// skips incremental splicing in favor of full recompute. WS/SSE
	// snapshot paths below use the same Refresh hook.
	a.notifier.Refresh = refreshFor(a.ex, a.cats, mgr.RefreshGate)
	a.notifier.Revalidate = rebindChanged(mgr)
	// RT-001: commit generations under the swap lock so a re-gate
	// landing after fan-out refuses the stale snapshot (see
	// Manager.PublishGeneration). Same late binding as Refresh above.
	a.notifier.Publish = mgr.PublishGeneration
	sseH := &syncpkg.SSEHandler{
		Manager:        mgr,
		Store:          a.store,
		Refresh:        refreshFor(a.ex, a.cats, mgr.RefreshGate),
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
	// OAuth has its own exact paths and methods. Registering these before the
	// auth prefix makes the complete start -> callback -> token lifecycle
	// reachable through the production mux while preserving method-specific
	// 405 handling from net/http.ServeMux.
	authH := &authn.Handler{Service: authSvc}
	mux.Handle("GET /runtime/oauth/start", authH)
	mux.Handle("GET /runtime/oauth/callback", authH)
	mux.Handle("POST /runtime/oauth/token", authH)
	mux.Handle("POST /runtime/oauth/id_token", authH)
	mux.Handle("POST /runtime/auth/", &authn.Handler{Service: authSvc})

	mux.Handle("/admin/", &adminapi.Handler{
		Pool:     a.pool,
		DB:       a.st,
		Catalogs: a.cats,
		Logger:   a.logger,
		Auth:     authSvc,
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
	if cfg.InsecureDevMode && os.Getenv("INSTANT_V2_STORAGE_ROOT") == "" {
		a.logger.Warn("INSECURE dev mode: file storage rooted at explicit dev directory, not durable storage", "root", cfg.StorageRoot)
	}
	// DA-001: the file backend assembles only on the configured durable
	// root. A malformed or unwritable root refuses startup here — never a
	// temp fallback, never first-upload failure.
	store, serr := storageapi.NewDiskBackend(cfg.StorageRoot, storeSecret)
	if serr != nil {
		return nil, nil, fmt.Errorf("refusing to start: invalid file storage root: %w", serr)
	}
	a.logger.Info("file storage assembled",
		"root", cfg.StorageRoot, "fingerprint", cfg.StorageFingerprint())
	mux.Handle("/storage/", &storageapi.Handler{
		Store:           store,
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
	return ws, sseH, nil
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
