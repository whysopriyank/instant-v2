package main

import (
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/reactive"
)

func TestTransactPreflightOrder(t *testing.T) {
	for _, tc := range []struct {
		name, body, want string
		status           int
		gateCalls        int
	}{
		{"malformed before gate", "{", "{\"error\":\"bad json\"}\n", 400, 0},
		{"gate before app lookup", `{"app-id":"not-a-uuid","tx-steps":[]}`, "{\"error\":\"server busy\"}\n", 429, 1},
		{"gate before step parse", `{"app-id":"not-a-uuid","tx-steps":[false]}`, "{\"error\":\"server busy\"}\n", 429, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			gate := func(appID string) error {
				calls++
				if appID != "not-a-uuid" {
					t.Fatalf("gate app id = %q", appID)
				}
				return &reactive.ShedError{RetryAfter: 1500 * time.Millisecond}
			}
			h := transactHandler(nil, nil, nil, nil, gate, slog.Default())
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest("POST", "/runtime/transact", strings.NewReader(tc.body)))
			if rec.Code != tc.status || rec.Body.String() != tc.want || calls != tc.gateCalls {
				t.Fatalf("got %d %q calls=%d; want %d %q calls=%d", rec.Code, rec.Body.String(), calls, tc.status, tc.want, tc.gateCalls)
			}
			if tc.status == 429 && rec.Header().Get("Retry-After") != "2" {
				t.Fatalf("retry header = %q", rec.Header().Get("Retry-After"))
			}
		})
	}
}
