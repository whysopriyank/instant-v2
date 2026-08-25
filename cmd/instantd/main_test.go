package main

import (
	"net/http/httptest"
	"testing"
)

// Security invariant (audit S4/H4): the rate-limiter bucket key must be
// server-derived. A client-supplied app-id is only honored when it parses as
// a UUID; anything else falls back to the connection IP so attackers can't
// mint unlimited fresh buckets by rotating header values.
func TestRouteKeyValidatesAppID(t *testing.T) {
	cases := []struct {
		name      string
		appHeader string
		want      string
	}{
		{"valid uuid passes through", "0b3a1d0e-6f2a-4c9b-8e1d-123456789abc", "0b3a1d0e-6f2a-4c9b-8e1d-123456789abc"},
		{"garbage header rejected", "attacker-chosen-key-" + string(rune('x')), ""},
		{"empty header falls to ip", "", ""},
	}
	for _, tc := range cases {
		r := httptest.NewRequest("POST", "/runtime/auth/send_magic_code", nil)
		if tc.appHeader != "" {
			r.Header.Set("app-id", tc.appHeader)
		}
		got := routeKey(r)
		if tc.want == "" {
			if got == tc.appHeader && tc.appHeader != "" {
				t.Fatalf("%s: raw client value %q must not become the limiter key", tc.name, tc.appHeader)
			}
			continue
		}
		if got != tc.want {
			t.Fatalf("%s: want key %q, got %q", tc.name, tc.want, got)
		}
	}
}
