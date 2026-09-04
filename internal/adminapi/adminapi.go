// Package adminapi is the admin REST plane keyed by app admin tokens. Port of
// instant.admin.routes (server/src/instant/admin/routes.clj @ a4d2ef33).
//
// Every route authenticates the caller against app_admin_tokens (v1
// req->app-id-authed! restricted to app-admin tokens) and bypasses ALL
// permission checks: admin callers act with :admin? true semantics.
//
// Known deviations from v1 (documented at the affected handlers):
//   - Rules persistence does not exist yet in v2, so *_perms_check routes
//     evaluate against an optional "rules" object from the request body
//     instead of rule-model/get-by-app-id + "rules-override".
//   - transact_perms_check never commits (dry-run only); v1's
//     "dangerously-commit-tx" flag is intentionally ignored.
//   - /admin/rooms/presence is explicitly unsupported until a coordinator-owned
//     read-only presence projection is wired; it never returns a false-success
//     empty object.
package adminapi

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/instant-v2/instant-v2/internal/authn"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
	"github.com/instant-v2/instant-v2/internal/storage"
)

// Handler mounts the /admin/* routes.
type Handler struct {
	Pool     *pgxpool.Pool
	DB       *storage.DB
	Catalogs *platform.CatalogCache
	Logger   *slog.Logger
	// Auth is the optional shared authn.Service. When nil, Handler lazily
	// creates and retains one long-lived service instance.
	Auth *authn.Service
	// OnCommit, when set, fires after every successful /admin/transact so
	// the reactive invalidator learns about admin-plane writes (same hook
	// the WS and /runtime/transact paths use). attrIDs are the triple-step
	// targets; empty means no invalidatable writes.
	OnCommit func(ctx context.Context, appID [16]byte, attrIDs []string, txID int64, attrsChanged bool)
	// OnCommitChanges, when set, is preferred over OnCommit when every
	// step resolves to a plain triple write: entity-annotated events let
	// the incremental engine splice instead of recompute
	// (docs/reference/09-tier2-architecture.md §T2.5).
	OnCommitChanges func(ctx context.Context, appID [16]byte, changes []reactive.Change, txID int64, attrsChanged bool)

	// faultBeforeTokenCommit is a package-private test seam. It is deliberately
	// not exported: failure injection must never be part of the production
	// admin/runtime API surface.
	faultBeforeTokenCommit func() error

	authMu  sync.Mutex
	authSvc *authn.Service
}

func (h *Handler) notifyCommit(ctx context.Context, appID [16]byte, txID int64, attrIDs []string, changes []reactive.Change, attrsChanged bool) {
	if attrsChanged {
		h.Catalogs.Invalidate(platform.UUIDToStr(appID))
	}
	if len(changes) > 0 && h.OnCommitChanges != nil {
		h.OnCommitChanges(ctx, appID, changes, txID, attrsChanged)
	} else if h.OnCommit != nil {
		h.OnCommit(ctx, appID, attrIDs, txID, attrsChanged)
	} else if h.OnCommitChanges != nil {
		// Handlers configured with only OnCommitChanges still receive an
		// invalidation with nil changes (the existing unknown-change convention).
		h.OnCommitChanges(ctx, appID, nil, txID, attrsChanged)
	}
}

// authedReq carries the authenticated request context through routing.
type authedReq struct {
	appID  [16]byte
	appStr string
	cat    *platform.AttrCatalog
	body   map[string]any // decoded JSON body; nil when absent
}

func (h *Handler) logger() *slog.Logger {
	if h.Logger != nil {
		return h.Logger
	}
	return slog.Default()
}

// ServeHTTP authenticates then dispatches by path. Wire spellings are copied
// verbatim from routes.clj (kebab/snake mix included).
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimSuffix(r.URL.Path, "/")
	if !strings.HasPrefix(path, "/admin/") {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	route := path[len("/admin"):]

	a, ok := h.authenticate(w, r)
	if !ok {
		return
	}

	post, get, del := r.Method == http.MethodPost, r.Method == http.MethodGet, r.Method == http.MethodDelete
	switch {
	case post && route == "/query":
		h.handleQuery(w, r, a)
	case post && route == "/transact":
		h.handleTransact(w, r, a)
	case post && route == "/query_perms_check":
		h.handleQueryPermsCheck(w, r, a)
	case post && route == "/transact_perms_check":
		h.handleTransactPermsCheck(w, r, a)
	case post && route == "/sign_out":
		h.handleSignOut(w, r, a)
	case post && route == "/refresh_tokens":
		h.handleRefreshTokens(w, r, a)
	case post && route == "/magic_code", post && route == "/send_magic_code":
		h.handleMagicCode(w, r, a)
	case post && route == "/verify_magic_code":
		h.handleVerifyMagicCode(w, r, a)
	case post && route == "/sign_in_guest":
		h.handleSignInGuest(w, r, a)
	case get && route == "/users":
		h.handleUsersList(w, r, a)
	case del && route == "/users":
		h.handleUsersDelete(w, r, a)
	case get && route == "/schema":
		h.handleSchema(w, r, a)
	case get && route == "/soft_deleted_attrs":
		h.handleSoftDeletedAttrs(w, r, a)
	case get && route == "/rooms/presence":
		h.handlePresence(w, r, a)
	default:
		writeErr(w, http.StatusNotFound, "not found")
	}
}
