// Package storageapi implements the blob-storage REST plane, a port of v1's
// instant.storage.routes + coordinator (server/src/instant/storage/routes.clj
// and coordinator.clj @ a4d2ef33). Wire shapes mirror the v1 runtime routes:
//
//	POST   /storage/signed-upload-url  {app-id, filename|path}      → {"data":{"url","id","expires-at"}}
//	PUT    /storage/upload/{id}?app-id=&filename=&expires=&signature= → {"data":{"id"}}
//	GET    /storage/files/{id}?app-id=&expires=&signature=          → object bytes
//	GET    /storage/signed-download-url?id=&app-id=                 → {"data":{"url"}}
//	DELETE /storage/files?id={id}&app-id=                           → {"data":{"id"}}
//	DELETE /storage/files                     {"app-id","ids":[...]} → {"data":{"ids":[...]}}
//
// v1 presigned URLs point at S3; here presigning produces http URLs served by
// this same Handler (PUT/GET passthrough) and validated with an HMAC-SHA256
// signature over op|app-id|id|filename|expiry using a server secret. Binding
// filename prevents a valid upload URL from being retargeted to another
// metadata path.
//
// Deviations from v1, deliberate:
//   - Object keys are "<app-id>/<file-id>" instead of S3's
//     "app-id/bin/location-id" sharding (no bin prefix; uuids do not collide).
//   - MIME detection uses mime.TypeByExtension (at upload) and net/http
//     content sniffing (at download) — a small subset of v1's Apache Tika.
//   - v1 records $files triples when bytes land (coordinator/upload-file! →
//     model/app_file.clj create!), not at presign time. We mirror that:
//     POST /storage/signed-upload-url touches no triple store; triples are
//     written at PUT-consume time when Triples+Catalogs are wired.
package storageapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
)

// Object is an opened blob ready for streaming.
type Object struct {
	Body io.ReadCloser
	Size int64 // <= 0 when unknown
}

// ErrNotConfigured is returned by backing stores that lack credentials or SDK
// support (see s3.go).
var ErrNotConfigured = errors.New("storageapi: object store not configured")

// ObjectStore abstracts the blob backing store. Presign* return a URL path
// plus query ("/storage/upload/{id}?...") that Handler serves and verifies
// itself; Put/Open/Delete operate on internal keys ("<app-id>/<file-id>").
type ObjectStore interface {
	PresignUpload(key, filename string, ttl time.Duration) (string, error)
	PresignDownload(key string, ttl time.Duration) (string, error)
	Put(key string, r io.Reader) error
	// PutIfAbsent writes a new object without replacing an existing one. It
	// returns ErrObjectExists when the target already belongs to another or
	// earlier request, preserving it for safe upload retries.
	PutIfAbsent(key string, r io.Reader) error
	Open(key string) (*Object, error)
	Delete(keys []string) error
}

// Presigned URL lifetimes. v1 never set expired_at on app_upload_urls rows
// (model/app_upload_url.clj create! omits it), so there is no v1 constant to
// copy; 10 minutes for uploads matches v1's oauth default-expires-at window,
// downloads get a day like long-lived S3 presigned GETs.
const (
	UploadTTL   = 10 * time.Minute
	DownloadTTL = 24 * time.Hour
)

// Handler serves the /storage/* routes. Store and Secret are required;
// Triples and Catalogs are optional — when both are set, a completed upload
// links $files entity triples exactly as v1's app-file-model/create! does.
//
// Authorization model:
//   - POST /storage/signed-upload-url mints write access into an app's
//     $files namespace, so it is ADMIN-ONLY like every control route
//     (ServeHTTP gates it behind requireAdmin; fail-closed at 503 when no
//     checker is wired).
//   - PUT /storage/upload/{id} and GET /storage/files/{id} stay presign-
//     gated: HMAC-SHA256 over op|app-id|id|exp minted by the routes above,
//     bounded by MaxUploadBytes.
type Handler struct {
	Store    ObjectStore
	Secret   []byte
	Triples  *storage.DB
	Catalogs *platform.CatalogCache
	// AdminTokenCheck authorizes destructive/admin operations (wired from
	// CatalogCache.CheckAdminToken in cmd). Required for upload-url,
	// delete, and download-url control routes; nil → those routes 503.
	AdminTokenCheck func(ctx context.Context, appID, token string) (bool, error)
	// MaxUploadBytes bounds one upload body; <= 0 means the handler default
	// (512 MiB) applies.
	MaxUploadBytes int64

	maxUpload atomic.Int64
	// uploadLocks serializes retries for one signed object key so a request
	// that created the object cannot clean it up while another request has
	// adopted the same pre-existing target.
	uploadLocksMu sync.Mutex
	uploadLocks   map[string]*uploadKeyLock
}

type uploadKeyLock struct {
	mu   sync.Mutex
	refs int
}

// ErrFilenameTaken is returned when a completed upload's $files.path
// collides with an existing unique path (duplicate filename).
var ErrFilenameTaken = errors.New("storageapi: a file with this path already exists")

// ErrObjectExists means an idempotent upload target already has bytes. The
// caller may safely continue metadata linking, but must not delete the object
// if that later link fails because this request did not create it.
var ErrObjectExists = errors.New("storageapi: object already exists")

// ErrTooLarge is returned when an upload exceeds MaxUploadBytes.
var ErrTooLarge = errors.New("storageapi: upload exceeds the configured maximum size")

func (h *Handler) uploadLimit() int64 {
	if v := h.maxUpload.Load(); v > 0 {
		return v
	}
	limit := h.MaxUploadBytes
	if limit <= 0 {
		limit = 512 << 20
	}
	h.maxUpload.Store(limit)
	return limit
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/storage")
	switch {
	case path == "/signed-upload-url" && r.Method == http.MethodPost:
		// Presigning mints write access into an app's $files namespace; it
		// requires admin credentials like every other control route.
		appID, ok := h.requireAdmin(w, r)
		if !ok {
			return
		}
		h.signedUploadURL(w, r, appID)
	case strings.HasPrefix(path, "/upload/") && r.Method == http.MethodPut:
		h.uploadPut(w, r, strings.TrimPrefix(path, "/upload/"))
	case strings.HasPrefix(path, "/files/") && r.Method == http.MethodGet:
		h.fileGet(w, r, strings.TrimPrefix(path, "/files/"))
	case path == "/files" && r.Method == http.MethodDelete:
		appID, ok := h.requireAdmin(w, r)
		if !ok {
			return
		}
		h.filesDelete(w, r, appID)
	case path == "/signed-download-url" && r.Method == http.MethodGet:
		appID, ok := h.requireAdmin(w, r)
		if !ok {
			return
		}
		h.signedDownloadURL(w, r, appID)
	default:
		http.NotFound(w, r)
	}
}
