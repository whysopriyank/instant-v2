package storageapi

import (
	"errors"
	"io"
	"net/http"

	"github.com/instant-v2/instant-v2/internal/httpjson"
	"github.com/instant-v2/instant-v2/internal/platform"
)

func readBody(r *http.Request) (map[string]any, error) {
	if r.Body == nil {
		return nil, errors.New("invalid JSON body")
	}
	// Read one byte beyond the existing cap to distinguish a real EOF from
	// LimitReader's synthetic EOF. Otherwise a padded prefix hides trailing JSON.
	limited := &io.LimitedReader{R: r.Body, N: (1 << 20) + 1}
	m, err := httpjson.DecodeObject(limited)
	if err != nil || limited.N == 0 {
		return nil, errors.New("invalid JSON body")
	}
	return m, nil
}

type uploadRequest struct {
	AppID    [16]byte
	Filename string
}

func readUploadRequest(r *http.Request) (uploadRequest, error) {
	var req uploadRequest
	body, err := readBody(r)
	if err != nil {
		return req, err
	}
	req.AppID, err = parseAppID(body)
	if err != nil {
		return req, err
	}
	req.Filename = str(body, "path")
	if req.Filename == "" {
		req.Filename = str(body, "filename")
	}
	if req.Filename == "" {
		return req, errors.New("path or filename required")
	}
	return req, nil
}

type deleteRequest struct {
	AppID [16]byte
	IDs   []string
}

func readDeleteRequest(r *http.Request) (deleteRequest, error) {
	var req deleteRequest
	body, err := readBody(r)
	if err != nil {
		return req, err
	}
	req.AppID, err = parseAppID(body)
	if err != nil {
		return req, err
	}
	rawIDs, _ := body["ids"].([]any)
	req.IDs = make([]string, 0, len(rawIDs))
	for _, raw := range rawIDs {
		id, _ := raw.(string)
		if _, err := platform.ScanUUIDErr(id); err == nil {
			req.IDs = append(req.IDs, id)
		}
	}
	if len(req.IDs) == 0 {
		return req, errors.New("ids required")
	}
	return req, nil
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
	// Storage responses retain HTML escaping and Encoder's final newline.
	_ = httpjson.Write(w, status, v, true)
}

func httpError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]any{"message": err.Error()})
}
