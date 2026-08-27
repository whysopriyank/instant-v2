// Package storageapi implements the blob-storage REST plane, a port of v1's
// instant.storage.routes + coordinator (server/src/instant/storage/routes.clj
// and coordinator.clj @ a4d2ef33). Wire shapes mirror the v1 runtime routes:
//
//	POST   /storage/signed-upload-url  {app-id, filename|path}      → {"data":{"url","id","expires-at"}}
//	PUT    /storage/upload/{id}?app-id=&expires=&signature=         → {"data":{"id"}}
//	GET    /storage/files/{id}?app-id=&expires=&signature=          → object bytes
//	GET    /storage/signed-download-url?id=&app-id=                 → {"data":{"url"}}
//	DELETE /storage/files?id={id}&app-id=                           → {"data":{"id"}}
//	DELETE /storage/files                     {"app-id","ids":[...]} → {"data":{"ids":[...]}}
//
// v1 presigned URLs point at S3; here presigning produces http URLs served by
// this same Handler (PUT/GET passthrough) and validated with an HMAC-SHA256
// signature over op|app-id|id|expiry using a server secret. This keeps the
// presign flow testable without S3 while preserving the wire contract.
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
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/triple"
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
	PresignUpload(key string, ttl time.Duration) (string, error)
	PresignDownload(key string, ttl time.Duration) (string, error)
	Put(key string, r io.Reader) error
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
}

// ErrFilenameTaken is returned when a completed upload's $files.path
// collides with an existing unique path (duplicate filename).
var ErrFilenameTaken = errors.New("storageapi: a file with this path already exists")

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
		if !h.requireAdmin(w, r) {
			return
		}
		h.signedUploadURL(w, r)
	case strings.HasPrefix(path, "/upload/") && r.Method == http.MethodPut:
		h.uploadPut(w, r, strings.TrimPrefix(path, "/upload/"))
	case strings.HasPrefix(path, "/files/") && r.Method == http.MethodGet:
		h.fileGet(w, r, strings.TrimPrefix(path, "/files/"))
	case path == "/files" && r.Method == http.MethodDelete:
		if !h.requireAdmin(w, r) {
			return
		}
		h.filesDelete(w, r)
	case path == "/signed-download-url" && r.Method == http.MethodGet:
		if !h.requireAdmin(w, r) {
			return
		}
		h.signedDownloadURL(w, r)
	default:
		http.NotFound(w, r)
	}
}

// --- HMAC signing -----------------------------------------------------------

// signPayload computes hex(HMAC-SHA256(secret, op|appID|id|exp)).
func signPayload(secret []byte, op, appID, id string, exp int64) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(op + "|" + appID + "|" + id + "|" + strconv.FormatInt(exp, 10)))
	return hex.EncodeToString(mac.Sum(nil))
}

// verifySignature checks the expires/signature query params against
// op/appID/id and enforces non-expiry. Any failure is a 403-class error.
func verifySignature(secret []byte, op, appID, id string, q url.Values) error {
	exp, err := strconv.ParseInt(q.Get("expires"), 10, 64)
	if err != nil || exp < 1 {
		return errors.New("storageapi: missing or malformed expiry")
	}
	if !hmac.Equal([]byte(signPayload(secret, op, appID, id, exp)), []byte(q.Get("signature"))) {
		return errors.New("storageapi: invalid signature")
	}
	if time.Now().Unix() > exp {
		return errors.New("storageapi: signature expired")
	}
	return nil
}

// newFileID mints a random RFC-4122 version-4 uuid string.
func newFileID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return platform.UUIDToStr(b)
}

// --- Wire handlers ----------------------------------------------------------

// signedUploadURL ports create-upload-url-post: body {app-id,
// filename|path} → presigned PUT url + file id + expiry. Like v1, no triple
// rows are created here; linkage happens when bytes arrive.
func (h *Handler) signedUploadURL(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		httpError(w, http.StatusBadRequest, err)
		return
	}
	appID, err := parseAppID(body)
	if err != nil {
		httpError(w, http.StatusBadRequest, err)
		return
	}
	name := str(body, "path")
	if name == "" {
		name = str(body, "filename") // v1 get-some-param [[:path] [:filename]]
	}
	if name == "" {
		httpError(w, http.StatusBadRequest, errors.New("path or filename required"))
		return
	}
	id := newFileID()
	appIDStr := platform.UUIDToStr(appID)
	urlPath, err := h.Store.PresignUpload(appIDStr+"/"+id, UploadTTL)
	if err != nil {
		httpError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"data": map[string]any{
			"url":        absoluteURL(r, urlPath),
			"id":         id,
			"expires-at": time.Now().UTC().Add(UploadTTL).Format(time.RFC3339),
		},
	})
}

// requireAdmin authorizes a destructive/admin storage operation via the
// injected token checker. Fail-closed: no checker → 503; bad token → 401.
// The app id may arrive in the header, query, or JSON body (bulk delete);
// body bytes are buffered and restored for downstream handlers.
func (h *Handler) requireAdmin(w http.ResponseWriter, r *http.Request) bool {
	if h.AdminTokenCheck == nil {
		httpError(w, http.StatusServiceUnavailable, errors.New("storageapi: admin operations not configured"))
		return false
	}
	token := bearerOrXToken(r)
	appIDStr := r.Header.Get("app-id")
	if appIDStr == "" {
		appIDStr = firstQuery(r, "app-id", "app_id")
	}
	if appIDStr == "" && r.Body != nil {
		raw, err := io.ReadAll(r.Body)
		if err == nil {
			r.Body = io.NopCloser(bytes.NewReader(raw))
			var body map[string]any
			if json.Unmarshal(raw, &body) == nil {
				for _, k := range []string{"app-id", "app_id"} {
					if s, ok := body[k].(string); ok && s != "" {
						appIDStr = s
						break
					}
				}
			}
		}
	}
	id, err := platform.ScanUUIDErr(appIDStr)
	if err != nil || token == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"message": "invalid admin credentials"})
		return false
	}
	ok, err := h.AdminTokenCheck(r.Context(), platform.UUIDToStr(id), token)
	if err != nil || !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"message": "invalid admin credentials"})
		return false
	}
	return true
}

// bearerOrXToken extracts the caller credential (Authorization: Bearer or
// X-admin-token), mirroring the admin/backup planes.
func bearerOrXToken(r *http.Request) string {
	authz := r.Header.Get("Authorization")
	if len(authz) >= 7 && strings.EqualFold(authz[:7], "bearer ") {
		if t := strings.TrimSpace(authz[7:]); t != "" {
			return t
		}
	}
	return r.Header.Get("X-admin-token")
}

func firstQuery(r *http.Request, keys ...string) string {
	q := r.URL.Query()
	for _, k := range keys {
		if v := q.Get(k); v != "" {
			return v
		}
	}
	return ""
}

// uploadPut ports consume-upload-url-put: stream the body into the backing
// store at the signed location, then link $files triples if wired (v1 defers
// record creation until the bytes actually land).
func (h *Handler) uploadPut(w http.ResponseWriter, r *http.Request, id string) {
	appIDStr := r.URL.Query().Get("app-id")
	appID, err := platform.ScanUUIDErr(appIDStr)
	if err != nil {
		httpError(w, http.StatusBadRequest, errors.New("missing or invalid app-id"))
		return
	}
	if err := verifySignature(h.Secret, "upload", appIDStr, id, r.URL.Query()); err != nil {
		httpError(w, http.StatusForbidden, err)
		return
	}
	// Hard cap on one upload. The countingReader tracks exact bytes so an
	// over-cap body is REJECTED (never silently truncated), and the partial
	// object is removed.
	limit := h.uploadLimit()
	cr := &countingReader{r: r.Body}
	putErr := h.Store.Put(appIDStr+"/"+id, cr)
	if putErr == nil && cr.n > limit {
		putErr = ErrTooLarge
		// Roll back the bytes that landed before the cap tripped.
		_ = h.Store.Delete([]string{appIDStr + "/" + id})
	}
	if putErr != nil {
		if errors.Is(putErr, ErrTooLarge) {
			httpError(w, http.StatusRequestEntityTooLarge, putErr)
			return
		}
		httpError(w, http.StatusInternalServerError, putErr)
		return
	}
	// Unsigned advisory params carrying the $files metadata (v1 carried these
	// in its app_upload_urls row; we keep no DB row, so they ride the URL).
	filename := r.URL.Query().Get("filename")
	if h.Triples != nil && h.Catalogs != nil {
		if err := h.linkFileTriple(r.Context(), appID, id, filename, r.Header.Get("Content-Type"), cr.n); err != nil {
			if errors.Is(err, ErrFilenameTaken) {
				httpError(w, http.StatusConflict, err)
				return
			}
			httpError(w, http.StatusInternalServerError, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{"id": id}})
}

// fileGet streams a stored object after validating the download signature.
func (h *Handler) fileGet(w http.ResponseWriter, r *http.Request, id string) {
	appIDStr := r.URL.Query().Get("app-id")
	_, err := platform.ScanUUIDErr(appIDStr)
	if err != nil {
		httpError(w, http.StatusBadRequest, errors.New("missing or invalid app-id"))
		return
	}
	if err := verifySignature(h.Secret, "download", appIDStr, id, r.URL.Query()); err != nil {
		httpError(w, http.StatusForbidden, err)
		return
	}
	obj, err := h.Store.Open(appIDStr + "/" + id)
	if err != nil {
		httpError(w, http.StatusNotFound, errors.New("file not found"))
		return
	}
	defer func() { _ = obj.Body.Close() }()
	if obj.Size > 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	}
	// Stored-content hardening (audit M3): files share the API origin, so
	// attacker-uploaded HTML/JS/SVG must never execute in a browser here.
	// Sniff the type, then force safe types to download; never trust or
	// reflect client-supplied names.
	w.Header().Set("X-Content-Type-Options", "nosniff")
	buf := make([]byte, 512)
	n, _ := io.ReadFull(obj.Body, buf)
	ct := "application/octet-stream"
	if n > 0 {
		ct = http.DetectContentType(buf[:n])
	}
	if !inlineSafeType(ct) {
		ct = "application/octet-stream"
		w.Header().Set("Content-Disposition", "attachment")
	} else if ct == "text/plain" {
		w.Header().Set("Content-Disposition", "inline")
	}
	w.Header().Set("Content-Type", ct)
	if n > 0 {
		_, _ = w.Write(buf[:n])
	}
	_, _ = io.Copy(w, obj.Body)
}

// inlineSafeType reports whether ct may render in-browser. Images/audio/
// video/pdf/plain text are inert enough; anything script-capable (HTML,
// XML, SVG, unknown binaries) downloads instead of executing.
func inlineSafeType(ct string) bool {
	switch {
	case strings.HasPrefix(ct, "image/"):
		return ct != "image/svg+xml"
	case strings.HasPrefix(ct, "audio/"), strings.HasPrefix(ct, "video/"):
		return true
	case strings.HasPrefix(ct, "text/plain"), ct == "application/pdf":
		return true
	default:
		return false
	}
}

// signedDownloadURL ports signed-download-url-get for id-addressed files:
// ?id=&app-id= → fresh presigned GET url ({data:{url}} envelope like v1).
func (h *Handler) signedDownloadURL(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	appIDStr := q.Get("app-id")
	id := q.Get("id")
	if _, err := platform.ScanUUIDErr(appIDStr); err != nil || id == "" {
		httpError(w, http.StatusBadRequest, errors.New("id and app-id required"))
		return
	}
	urlPath, err := h.Store.PresignDownload(appIDStr+"/"+id, DownloadTTL)
	if err != nil {
		httpError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{"url": absoluteURL(r, urlPath)}})
}

// filesDelete ports file-delete: single delete via ?id=&app-id= (v1 shape:
// DELETE /storage/files with identifying params) and bulk via JSON body
// {"app-id","ids":[...]} mirroring coordinator/delete-files! ({:ids [...]});
// keyed by id instead of path because keys are derivable without a lookup.
func (h *Handler) filesDelete(w http.ResponseWriter, r *http.Request) {
	if id := r.URL.Query().Get("id"); id != "" {
		appIDStr := r.URL.Query().Get("app-id")
		appID, err := platform.ScanUUIDErr(appIDStr)
		if err != nil {
			httpError(w, http.StatusBadRequest, errors.New("missing or invalid app-id"))
			return
		}
		if err := h.Store.Delete([]string{platform.UUIDToStr(appID) + "/" + id}); err != nil {
			httpError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{"id": id}})
		return
	}
	body, err := readBody(r)
	if err != nil {
		httpError(w, http.StatusBadRequest, err)
		return
	}
	appID, err := parseAppID(body)
	if err != nil {
		httpError(w, http.StatusBadRequest, err)
		return
	}
	rawIDs, _ := body["ids"].([]any)
	prefix := platform.UUIDToStr(appID) + "/"
	keys := make([]string, 0, len(rawIDs))
	ids := make([]string, 0, len(rawIDs))
	for _, raw := range rawIDs {
		s, _ := raw.(string)
		if s == "" {
			continue
		}
		if _, err := platform.ScanUUIDErr(s); err != nil {
			continue
		}
		keys = append(keys, prefix+s)
		ids = append(ids, s)
	}
	if len(keys) == 0 {
		httpError(w, http.StatusBadRequest, errors.New("ids required"))
		return
	}
	if err := h.Store.Delete(keys); err != nil {
		httpError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{"ids": ids}})
}

// --- $files triple linkage --------------------------------------------------

// fileAttrs lists the $files attrs v1 writes in model/app_file.clj create!;
// unique/indexed flags follow lookup-ref semantics (path is the lookup ref).
var fileAttrs = []struct {
	label   string
	value   func(m fileMeta) any
	unique  bool
	indexed bool
}{
	{"path", func(m fileMeta) any { return m.path }, true, true},
	{"id", func(m fileMeta) any { return m.id }, false, true},
	{"size", func(m fileMeta) any { return m.size }, false, false},
	{"content-type", func(m fileMeta) any { return m.contentType }, false, false},
	{"location-id", func(m fileMeta) any { return m.id }, false, false},
	{"key-version", func(fileMeta) any { return int64(1) }, false, false},
}

type fileMeta struct {
	id, path, contentType string
	size                  int64
}

// linkFileTriple mirrors v1 app-file-model/create!: one transaction creating
// the $files attrs (idempotently) and the entity's triples, cardinality-one
// so re-upload overwrites. The catalog cache is invalidated because fresh
// attrs may have been created.
func (h *Handler) linkFileTriple(ctx context.Context, appID [16]byte, id, filename, contentType string, size int64) error {
	if filename == "" {
		filename = id
	}
	meta := fileMeta{id: id, path: filename, contentType: contentType, size: size}
	entity, err := platform.ScanUUIDErr(id)
	if err != nil {
		return fmt.Errorf("storageapi: bad file id: %w", err)
	}
	err = h.Triples.WithTx(ctx, func(tx pgx.Tx) error {
		attrIDs := make(map[string][16]byte, len(fileAttrs))
		for _, fa := range fileAttrs {
			a, err := platform.GetOrCreateAttr(ctx, tx, appID, "$files", fa.label, "blob", "one", fa.unique, fa.indexed)
			if err != nil {
				return err
			}
			attrIDs[fa.label] = a.ID
		}
		cat, err := platform.LoadAttrCatalog(ctx, tx, appID)
		if err != nil {
			return err
		}
		ts := make([]triple.Triple, 0, len(fileAttrs))
		for _, fa := range fileAttrs {
			ts = append(ts, triple.Triple{E: entity, A: attrIDs[fa.label], V: fa.value(meta)})
		}
		return h.Triples.SetTx(ctx, tx, appID, cat, ts, true)
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			// Unique $files.path collision (same filename uploaded twice).
			return ErrFilenameTaken
		}
		return err
	}
	h.Catalogs.Invalidate(platform.UUIDToStr(appID))
	return nil
}

// --- plumbing helpers -------------------------------------------------------

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func readBody(r *http.Request) (map[string]any, error) {
	var m map[string]any
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	if err := dec.Decode(&m); err != nil {
		return nil, errors.New("invalid JSON body")
	}
	return m, nil
}

func str(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

// parseAppID accepts both "app-id" and legacy "app_id" keys (v1
// ex/get-some-param behaviour on the app-file routes).
func parseAppID(m map[string]any) ([16]byte, error) {
	s := str(m, "app-id")
	if s == "" {
		s = str(m, "app_id")
	}
	if s == "" {
		return [16]byte{}, errors.New("missing app-id")
	}
	return platform.ScanUUIDErr(s)
}

// absoluteURL turns a presigned path+query into an absolute URL for clients,
// mirroring v1's config/server-origin prefix.
func absoluteURL(r *http.Request, pathAndQuery string) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	// Audit L3: reflect the Host only when it's structurally sane (same
	// sanitization the discovery endpoint applies) — behind naive proxies a
	// hostile Host header otherwise poisons returned download URLs.
	host := r.Host
	if _, ok := platform.OriginOf("http://" + host); !ok {
		host = ""
	}
	return scheme + "://" + host + pathAndQuery
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]any{"message": err.Error()})
}
