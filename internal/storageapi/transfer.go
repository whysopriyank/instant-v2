package storageapi

import (
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/instant-v2/instant-v2/internal/platform"
)

func (h *Handler) lockUploadKey(key string) func() {
	h.uploadLocksMu.Lock()
	if h.uploadLocks == nil {
		h.uploadLocks = make(map[string]*uploadKeyLock)
	}
	lock := h.uploadLocks[key]
	if lock == nil {
		lock = &uploadKeyLock{}
		h.uploadLocks[key] = lock
	}
	lock.refs++
	h.uploadLocksMu.Unlock()

	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		h.uploadLocksMu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(h.uploadLocks, key)
		}
		h.uploadLocksMu.Unlock()
	}
}

func (h *Handler) lockUploadKeys(keys []string) func() {
	ordered := append([]string(nil), keys...)
	sort.Strings(ordered)
	locks := make([]func(), 0, len(ordered))
	var previous string
	for _, key := range ordered {
		if key == previous {
			continue
		}
		locks = append(locks, h.lockUploadKey(key))
		previous = key
	}
	return func() {
		for i := len(locks) - 1; i >= 0; i-- {
			locks[i]()
		}
	}
}

// ErrUploadCleanup means a failed object creation or metadata update could not
// durably remove its new object. Callers must require reconciliation, not retry
// under the assumption that the target is unchanged.
var ErrUploadCleanup = errors.New("storageapi: upload cleanup failed")

// cleanupAfterMetadataFailure removes only an object created by this upload.
// A retry that found an existing object leaves it untouched; a delete failure
// is joined with the link error so the caller can reconcile the object.
func cleanupAfterMetadataFailure(store ObjectStore, key string, created bool, linkErr error) error {
	if !created {
		return linkErr
	}
	if cleanupErr := store.Delete([]string{key}); cleanupErr != nil {
		return errors.Join(linkErr, ErrUploadCleanup, cleanupErr)
	}
	return linkErr
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
	key := appIDStr + "/" + id
	unlock := h.lockUploadKey(key)
	defer unlock()
	// Hard cap on one upload. The countingReader tracks exact bytes so an
	// over-cap body is REJECTED (never silently truncated), and the partial
	// object is removed.
	limit := h.uploadLimit()
	cr := &countingReader{r: r.Body}
	created := true
	putErr := h.Store.PutIfAbsent(key, cr)
	if errors.Is(putErr, ErrObjectExists) {
		// A retry of a previously completed signed upload must not replace
		// or later delete the object owned by that earlier request.
		putErr = nil
		created = false
		obj, openErr := h.Store.Open(key)
		if openErr != nil {
			putErr = openErr
		} else {
			cr.n = obj.Size
			_ = obj.Body.Close()
		}
	}
	if putErr == nil && cr.n > limit {
		putErr = ErrTooLarge
		// Roll back the bytes that landed before the cap tripped.
		if created {
			putErr = cleanupAfterMetadataFailure(h.Store, key, true, putErr)
		}
	}
	if putErr != nil {
		if errors.Is(putErr, ErrTooLarge) && !errors.Is(putErr, ErrUploadCleanup) {
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
			finalErr := cleanupAfterMetadataFailure(h.Store, key, created, err)
			if errors.Is(finalErr, ErrUploadCleanup) {
				httpError(w, http.StatusInternalServerError, finalErr)
				return
			}
			if errors.Is(finalErr, ErrFilenameTaken) {
				httpError(w, http.StatusConflict, err)
				return
			}
			httpError(w, http.StatusInternalServerError, finalErr)
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

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
