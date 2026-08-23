// HTTP surface for backup/restore. Auth is delegated to the injected
// AdminTokenCheck func (mirrors adminapi: Authorization Bearer or
// x-admin-token header); this package never imports platform.CatalogCache.
//
// Routes:
//
//	GET  /backup/{app_id}              → streams the v2 NDJSON dump (see
//	                                     package docs). Query param
//	                                     ?from-offset=N skips the first N
//	                                     logical records. Responds with
//	                                     Content-Type application/x-ndjson
//	                                     and Accept-Ranges: records.
//	POST /backup/{app_id}/restore      → consumes a v2 NDJSON dump body;
//	                                     responds {"counts": {...}}.
//	POST /backup/{app_id}/restore-v1zip→ consumes a raw v1 export ZIP body;
//	                                     same response. The zip is buffered
//	                                     to a temp file (archive/zip needs
//	                                     ReaderAt).
//
// S3 backends (when Handler.S3 is wired):
//
//	PUT  /backup/{app_id}/object?key=K  → streams an export dump into the
//	                                     object store under key K; responds
//	                                     {"counts": {...}, "key": K}.
//	POST /backup/{app_id}/restore-object?key=K → streams object K through
//	                                     Import; responds {"counts": ...}.
package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// Handler serves the backup/restore routes.
type Handler struct {
	Pool *pgxpool.Pool
	// AdminTokenCheck reports whether token authorizes appID. Required.
	AdminTokenCheck func(ctx context.Context, appID, token string) (bool, error)
	Logger          *slog.Logger
	// S3 optionally enables the /object routes. When nil they answer 503.
	S3 ObjectStore
}

func (h *Handler) logger() *slog.Logger {
	if h.Logger != nil {
		return h.Logger
	}
	return slog.Default()
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	appStr, action, ok := parseBackupRoute(r.URL.Path)
	if !ok {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	if _, err := platform.ScanUUIDErr(appStr); err != nil {
		writeErr(w, http.StatusNotFound, "bad app id")
		return
	}
	if !routeMethodOK(r.Method, action) {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	token := bearerOrAdminToken(r)
	okTok, err := h.AdminTokenCheck(r.Context(), appStr, token)
	if err != nil {
		h.logger().Error("backup: token check", "err", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}
	if !okTok {
		writeErr(w, http.StatusUnauthorized, "Invalid admin token")
		return
	}
	appID, _ := platform.ScanUUIDErr(appStr)

	ctx := r.Context()
	switch action {
	case "":
		h.handleExport(w, r, appStr, appID)
	case "restore":
		counts, ierr := Import(ctx, h.Pool, r.Body)
		if ierr != nil {
			h.writeImportError(w, ierr)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"counts": counts})
	case "restore-v1zip":
		counts, zerr := h.importV1ZipBody(ctx, r, appID)
		if zerr != nil {
			h.writeImportError(w, zerr)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"counts": counts})
	case "object":
		h.handlePutObject(w, r, appID)
	case "restore-object":
		h.handleRestoreObject(w, r, appID)
	}
}

// handleGetObject streams a stored dump back to the caller.
func (h *Handler) handleGetObject(w http.ResponseWriter, r *http.Request, key string) {
	rc, err := h.S3.Get(r.Context(), key)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			writeErr(w, http.StatusNotFound, err.Error())
			return
		}
		h.logger().Error("backup: s3 get", "err", err)
		writeErr(w, http.StatusBadGateway, "object store get failed")
		return
	}
	defer func() { _ = rc.Close() }()
	w.Header().Set("Content-Type", "application/x-ndjson")
	_, _ = io.Copy(w, rc)
}

// handlePutObject exports straight into the object store: no temp file, the
// hashWriter pipeline writes once to the network stream.
func (h *Handler) handlePutObject(w http.ResponseWriter, r *http.Request, appID [16]byte) {
	if h.S3 == nil {
		writeErr(w, http.StatusServiceUnavailable, "no object store wired")
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		writeErr(w, http.StatusBadRequest, "query param key is required")
		return
	}
	if r.Method == http.MethodGet {
		h.handleGetObject(w, r, key)
		return
	}
	pr, pw := io.Pipe()
	go func() {
		_, err := Export(r.Context(), pw, h.Pool, appID, ExportOptions{})
		pw.CloseWithError(err)
	}()
	putErr := h.S3.Put(r.Context(), key, pr, -1)
	// Release the export goroutine no matter how Put ended; otherwise a
	// short put leaves Export blocked on the pipe forever.
	pr.CloseWithError(putErr)
	if putErr != nil {
		h.logger().Error("backup: s3 put", "err", putErr)
		writeErr(w, http.StatusBadGateway, "object store put failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"key": key})
}

func (h *Handler) handleRestoreObject(w http.ResponseWriter, r *http.Request, appID [16]byte) {
	if h.S3 == nil {
		writeErr(w, http.StatusServiceUnavailable, "no object store wired")
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		writeErr(w, http.StatusBadRequest, "query param key is required")
		return
	}
	rc, err := h.S3.Get(r.Context(), key)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			writeErr(w, http.StatusNotFound, err.Error())
			return
		}
		h.logger().Error("backup: s3 get", "err", err)
		writeErr(w, http.StatusBadGateway, "object store get failed")
		return
	}
	defer func() { _ = rc.Close() }()
	counts, ierr := Import(r.Context(), h.Pool, rc)
	if ierr != nil {
		h.writeImportError(w, ierr)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"counts": counts})
}

// parseBackupRoute splits /backup/{app_id}[/{action}].
func parseBackupRoute(path string) (appID, action string, ok bool) {
	p := strings.TrimSuffix(path, "/")
	const prefix = "/backup/"
	if !strings.HasPrefix(p, prefix) {
		return "", "", false
	}
	rest := p[len(prefix):]
	if i := strings.LastIndex(rest, "/"); i >= 0 {
		return rest[:i], rest[i+1:], true
	}
	return rest, "", true
}

// routeMethodOK validates /backup/{app}[/{action}] verb pairs.
func routeMethodOK(method, action string) bool {
	switch action {
	case "":
		return method == http.MethodGet
	case "restore", "restore-v1zip", "restore-object":
		return method == http.MethodPost
	case "object":
		return method == http.MethodPut || method == http.MethodGet
	default:
		return false
	}
}

func (h *Handler) handleExport(w http.ResponseWriter, r *http.Request, appStr string, appID [16]byte) {
	ctx := r.Context()
	offset := 0
	if v := r.URL.Query().Get("from-offset"); v != "" {
		n, perr := strconv.Atoi(v)
		if perr != nil || n < 0 {
			writeErr(w, http.StatusBadRequest, "from-offset must be a non-negative integer")
			return
		}
		offset = n
	}
	// Existence pre-check so errors can still set a status before streaming.
	var one int
	err := h.Pool.QueryRow(ctx, `SELECT 1 FROM apps WHERE id=$1::uuid`, appStr).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		writeErr(w, http.StatusNotFound, "unknown app")
		return
	}
	if err != nil {
		h.logger().Error("backup: app lookup", "err", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Accept-Ranges", "records")
	w.Header().Set("X-Backup-Format", dumpFormat)
	w.WriteHeader(http.StatusOK)
	if _, err := Export(ctx, w, h.Pool, appID, ExportOptions{FromOffset: offset}); err != nil && !errors.Is(err, context.Canceled) {
		h.logger().Error("backup: export stream failed", "err", err)
	}
}

func (h *Handler) writeImportError(w http.ResponseWriter, err error) {
	h.logger().Error("backup: import failed", "err", err)
	status := http.StatusBadRequest
	msg := err.Error()
	if errors.Is(err, ErrAppNotFound) {
		status = http.StatusNotFound
	}
	writeErr(w, status, msg)
}

func (h *Handler) importV1ZipBody(ctx context.Context, r *http.Request, appID [16]byte) (Counts, error) {
	tmp, err := os.CreateTemp("", "instant-v1-backup-*.zip")
	if err != nil {
		return Counts{}, fmt.Errorf("backup: temp file: %w", err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }() //nolint:errcheck // best-effort cleanup
	if _, err := io.Copy(tmp, r.Body); err != nil {
		tmp.Close()
		return Counts{}, fmt.Errorf("backup: read upload: %v", err)
	}
	size, err := tmp.Seek(0, io.SeekEnd)
	if err != nil {
		tmp.Close()
		return Counts{}, err
	}
	counts, err := RestoreV1Zip(ctx, h.Pool, tmp, size, appID)
	cerr := tmp.Close()
	if err != nil {
		return counts, err
	}
	if cerr != nil {
		return counts, cerr
	}
	return counts, nil
}

// bearerOrAdminToken mirrors internal/adminapi token extraction.
func bearerOrAdminToken(r *http.Request) string {
	authz := r.Header.Get("Authorization")
	if len(authz) >= 7 && strings.EqualFold(authz[:7], "bearer ") {
		if t := strings.TrimSpace(authz[7:]); t != "" {
			return t
		}
	}
	return r.Header.Get("X-admin-token")
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"message": msg})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	b, _ := json.Marshal(v)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(b)
}
