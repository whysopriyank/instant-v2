package storageapi

import (
	"errors"
	"net/http"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// signedUploadURL ports create-upload-url-post: body {app-id,
// filename|path} → presigned PUT url + file id + expiry. Like v1, no triple
// rows are created here; linkage happens when bytes arrive.
func (h *Handler) signedUploadURL(w http.ResponseWriter, r *http.Request, authorizedAppID [16]byte) {
	req, err := readUploadRequest(r)
	if err != nil {
		httpError(w, http.StatusBadRequest, err)
		return
	}
	if req.AppID != authorizedAppID {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"message": "invalid admin credentials"})
		return
	}
	id := newFileID()
	appIDStr := platform.UUIDToStr(req.AppID)
	urlPath, err := h.Store.PresignUpload(appIDStr+"/"+id, req.Filename, UploadTTL)
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

// signedDownloadURL ports signed-download-url-get for id-addressed files:
// ?id=&app-id= → fresh presigned GET url ({data:{url}} envelope like v1).
func (h *Handler) signedDownloadURL(w http.ResponseWriter, r *http.Request, authorizedAppID [16]byte) {
	q := r.URL.Query()
	appIDStr := q.Get("app-id")
	id := q.Get("id")
	appID, err := platform.ScanUUIDErr(appIDStr)
	if err != nil || id == "" {
		httpError(w, http.StatusBadRequest, errors.New("id and app-id required"))
		return
	}
	if appID != authorizedAppID {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"message": "invalid admin credentials"})
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
func (h *Handler) filesDelete(w http.ResponseWriter, r *http.Request, authorizedAppID [16]byte) {
	if id := r.URL.Query().Get("id"); id != "" {
		appIDStr := r.URL.Query().Get("app-id")
		appID, err := platform.ScanUUIDErr(appIDStr)
		if err != nil {
			httpError(w, http.StatusBadRequest, errors.New("missing or invalid app-id"))
			return
		}
		if appID != authorizedAppID {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"message": "invalid admin credentials"})
			return
		}
		key := platform.UUIDToStr(appID) + "/" + id
		unlock := h.lockUploadKey(key)
		defer unlock()
		if err := h.Store.Delete([]string{key}); err != nil {
			httpError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{"id": id}})
		return
	}
	req, err := readDeleteRequest(r)
	if err != nil {
		httpError(w, http.StatusBadRequest, err)
		return
	}
	if req.AppID != authorizedAppID {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"message": "invalid admin credentials"})
		return
	}
	prefix := platform.UUIDToStr(req.AppID) + "/"
	keys := make([]string, 0, len(req.IDs))
	for _, id := range req.IDs {
		keys = append(keys, prefix+id)
	}
	unlock := h.lockUploadKeys(keys)
	defer unlock()
	if err := h.Store.Delete(keys); err != nil {
		httpError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{"ids": req.IDs}})
}
