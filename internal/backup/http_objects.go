package backup

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
)

const (
	stagingObjectPrefix = ".instant-backup-staging/"
	stagingCleanupLimit = 5 * time.Second
)

// scopeObjectKey namespaces every object under the authenticated app id.
// Without this, a token for app A could read/overwrite app B's dumps by
// constructing the key — the token check authorizes the app, not the bucket.
func scopeObjectKey(appID [16]byte, key string) (string, error) {
	if key == "" {
		return "", errors.New("backup: query param key is required")
	}
	if strings.Contains(key, "..") || strings.HasPrefix(key, "/") {
		return "", errors.New("backup: invalid object key")
	}
	if strings.HasPrefix(key, stagingObjectPrefix) {
		return "", errors.New("backup: staging object key is private")
	}
	return platform.UUIDToStr(appID) + "/" + key, nil
}

// handleGetObject streams a stored dump back to the caller.
func (h *Handler) handleGetObject(w http.ResponseWriter, r *http.Request, appID [16]byte, key string) {
	key, kerr := scopeObjectKey(appID, key)
	if kerr != nil {
		writeErr(w, http.StatusBadRequest, kerr.Error())
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
	w.Header().Set("Content-Type", "application/x-ndjson")
	_, _ = io.Copy(w, rc)
}

// handlePutObject streams an export to a private staging key and publishes it
// only after both the upload and Export complete successfully.
func (h *Handler) handlePutObject(w http.ResponseWriter, r *http.Request, appID [16]byte) {
	if h.S3 == nil {
		writeErr(w, http.StatusServiceUnavailable, "no object store wired")
		return
	}
	key, kerr := scopeObjectKey(appID, r.URL.Query().Get("key"))
	if kerr != nil {
		writeErr(w, http.StatusBadRequest, kerr.Error())
		return
	}
	if r.Method == http.MethodGet {
		h.handleGetObject(w, r, appID, r.URL.Query().Get("key"))
		return
	}
	tempKey, err := newStagingObjectKey(appID)
	if err != nil {
		h.logger().Error("backup: staging key", "err", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}
	cleanup := func() error {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), stagingCleanupLimit)
		defer cancel()
		return h.S3.Delete(ctx, tempKey)
	}
	type exportResult struct {
		counts Counts
		err    error
	}
	pr, pw := io.Pipe()
	resCh := make(chan exportResult, 1)
	go func() {
		counts, err := Export(r.Context(), pw, h.Pool, appID, ExportOptions{})
		resCh <- exportResult{counts: counts, err: err}
		pw.CloseWithError(err)
	}()
	putErr := h.S3.Put(r.Context(), tempKey, pr, -1)
	// Release the export goroutine no matter how Put ended; otherwise a
	// short put leaves Export blocked on the pipe forever.
	pr.CloseWithError(putErr)
	expRes := <-resCh
	if putErr != nil {
		if err := cleanup(); err != nil {
			h.logger().Error("backup: staging cleanup after put failure", "err", err)
		}
		if errors.Is(putErr, ErrAppNotFound) || errors.Is(expRes.err, ErrAppNotFound) {
			writeErr(w, http.StatusNotFound, "unknown app")
			return
		}
		h.logger().Error("backup: s3 put", "err", putErr)
		writeErr(w, http.StatusBadGateway, "object store put failed")
		return
	}
	if expRes.err != nil {
		if err := cleanup(); err != nil {
			h.logger().Error("backup: staging cleanup after export failure", "err", err)
		}
		if errors.Is(expRes.err, ErrAppNotFound) {
			writeErr(w, http.StatusNotFound, "unknown app")
			return
		}
		h.logger().Error("backup: export to object store failed", "err", expRes.err)
		writeErr(w, http.StatusBadGateway, "object store put failed")
		return
	}
	if err := h.S3.Promote(r.Context(), tempKey, key); err != nil {
		if cleanupErr := cleanup(); cleanupErr != nil {
			h.logger().Error("backup: staging cleanup after promotion failure", "err", cleanupErr)
		}
		h.logger().Error("backup: object promotion failed; publication status unknown", "err", err)
		writeErr(w, http.StatusBadGateway, "object promotion failed; publication status unknown")
		return
	}
	if err := cleanup(); err != nil {
		h.logger().Error("backup: object published but staging cleanup failed", "err", err)
	}
	writeJSON(w, http.StatusOK, map[string]any{"counts": expRes.counts, "key": key})
}

func newStagingObjectKey(appID [16]byte) (string, error) {
	var suffix [16]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", fmt.Errorf("backup: random staging key: %w", err)
	}
	return stagingObjectPrefix + platform.UUIDToStr(appID) + "/" + hex.EncodeToString(suffix[:]), nil
}

func (h *Handler) handleRestoreObject(w http.ResponseWriter, r *http.Request, appID [16]byte) {
	if h.S3 == nil {
		writeErr(w, http.StatusServiceUnavailable, "no object store wired")
		return
	}
	key, kerr := scopeObjectKey(appID, r.URL.Query().Get("key"))
	if kerr != nil {
		writeErr(w, http.StatusBadRequest, kerr.Error())
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
	counts, ierr := Import(r.Context(), h.Pool, rc, appID)
	if ierr != nil {
		h.writeImportError(w, ierr)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"counts": counts})
}
