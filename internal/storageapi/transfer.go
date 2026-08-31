package storageapi

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/instant-v2/instant-v2/internal/platform"
)

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

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
