package runtimeapi

import (
	"errors"
	"net/http"

	"github.com/instant-v2/instant-v2/internal/httpjson"
	"github.com/instant-v2/instant-v2/internal/platform"
)

func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"message": "method not allowed"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	// The runtime plane preserves literal HTML characters and a final newline.
	_ = httpjson.Write(w, status, v, false)
}

func readBody(r *http.Request) (map[string]any, error) {
	m, err := httpjson.DecodeObject(r.Body)
	if err != nil {
		return nil, errors.New("invalid JSON body")
	}
	return m, nil
}

// Project the exact wire keys into route-specific requests only after the
// complete object is decoded. Unknown fields remain accepted; unlike decoding
// straight into a Go struct this does not add case-insensitive field aliases.
type refreshTokensRequest struct {
	AppID  [16]byte
	Tokens []string
}

func readRefreshTokens(r *http.Request) (refreshTokensRequest, error) {
	var req refreshTokensRequest
	m, err := readBody(r)
	if err != nil {
		return req, err
	}
	appID, ok := parseAppID(m)
	raws, _ := m["refresh_tokens"].([]any)
	if !ok || len(raws) == 0 {
		return req, errors.New("refresh_tokens and app-id are required")
	}
	req.AppID = appID
	req.Tokens = make([]string, 0, len(raws))
	for _, raw := range raws {
		token, _ := raw.(string)
		if token == "" {
			return req, errors.New("refresh_tokens must be a non-empty list of uuids")
		}
		req.Tokens = append(req.Tokens, token)
	}
	return req, nil
}

type signoutRequest struct {
	AppID [16]byte
	Token string
}

func readSignout(r *http.Request) (signoutRequest, error) {
	var req signoutRequest
	m, err := readBody(r)
	if err != nil {
		return req, err
	}
	req.Token = str(m, "refresh_token")
	if req.Token == "" {
		req.Token = str(m, "refresh-token")
	}
	var ok bool
	req.AppID, ok = parseAppID(m)
	if !ok || req.Token == "" {
		return req, errors.New("refresh_token and app_id are required")
	}
	return req, nil
}

type frameworkQueryRequest struct {
	Query map[string]any // InstaQL expressions and user fields are dynamic.
	Token string
}

func readFrameworkQuery(r *http.Request) (frameworkQueryRequest, error) {
	var req frameworkQueryRequest
	m, err := readBody(r)
	if err != nil {
		return req, err
	}
	req.Query, _ = m["query"].(map[string]any)
	if req.Query == nil {
		return req, errors.New("query is required")
	}
	req.Token = str(m, "refresh-token")
	return req, nil
}

func str(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

// parseAppID accepts both "app-id" and legacy "app_id" keys.
func parseAppID(m map[string]any) ([16]byte, bool) {
	for _, k := range []string{"app-id", "app_id"} {
		if s := str(m, k); s != "" {
			if id, err := platform.ScanUUIDErr(s); err == nil {
				return id, true
			}
		}
	}
	return [16]byte{}, false
}
