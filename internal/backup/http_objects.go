package backup

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/instant-v2/instant-v2/internal/platform"
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

// handlePutObject exports straight into the object store: no temp file, the
// hashWriter pipeline writes once to the network stream.
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
