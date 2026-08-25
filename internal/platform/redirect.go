package platform

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
)

// parseURL, lower, and hasControlOrSpace keep redirect.go free of extra
// surface while staying testable.

func parseURL(raw string) (*url.URL, bool) {
	u, err := url.Parse(raw)
	if err != nil || u == nil {
		return nil, false
	}
	return u, true
}

func lower(s string) string { return strings.ToLower(s) }

// hasControlOrSpace rejects control characters and whitespace in host
// material before it is ever echoed into a Location header.
func hasControlOrSpace(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c <= ' ' || c == 0x7f {
			return true
		}
	}
	return false
}

// RedirectOrigins returns the app's registered OAuth redirect origins
// ("scheme://host", lowercase). Apps without a configured list return an
// empty slice — callers must treat that as "no redirects permitted".
func RedirectOrigins(ctx context.Context, q RowQueryer, appID [16]byte) ([]string, error) {
	rows, err := q.Query(ctx,
		`SELECT redirect_origins FROM apps WHERE id=$1::uuid`, appID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var raw []byte
	for rows.Next() {
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return []string{}, nil
	}
	var out []string
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// OriginOf extracts the lowercase "scheme://host" origin of a URL. Only
// http(s) with a host is accepted; everything else is invalid for OAuth.
func OriginOf(raw string) (string, bool) {
	u, ok := parseURL(raw)
	if !ok {
		return "", false
	}
	scheme := lower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", false
	}
	host := u.Host
	if host == "" || hasControlOrSpace(host) {
		return "", false
	}
	return scheme + "://" + lower(host), true
}
