package main

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/instant-v2/instant-v2/internal/config"
	"github.com/instant-v2/instant-v2/internal/ratelimit"
)

func TestRouteClassificationContract(t *testing.T) {
	for _, tc := range []struct {
		path  string
		class ratelimit.Class
	}{
		{"/runtime/auth/send_magic_code", ratelimit.ClassAuth},
		{"/runtime/transact", ratelimit.ClassTransact},
		{"/runtime/session", ratelimit.ClassWS},
		{"/runtime/sse", ratelimit.ClassWS},
		{"/storage/upload/id", ratelimit.ClassStorage},
		{"/admin/transact", ""},
		{"/backup/export", ""},
		{"/health", ""},
	} {
		t.Run(tc.path, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, tc.path, nil)
			r.RemoteAddr = "192.0.2.1:1234"
			key, class := classifyRoute(r)
			wantKey := ""
			if tc.class != "" {
				wantKey = "ip:192.0.2.1"
			}
			if key != wantKey || class != tc.class {
				t.Fatalf("got (%q,%q); want (%q,%q)", key, class, wantKey, tc.class)
			}
		})
	}
}

func TestBodyLimitContract(t *testing.T) {
	cfg := config.Config{MaxBackupBytes: 99, MaxUploadBytes: 123, MaxFrameBytes: 321}
	for _, tc := range []struct {
		method, path string
		want         int64
	}{
		{"POST", "/backup/import", 99},
		{"GET", "/backup/export", 1 << 20},
		{"PUT", "/storage/upload/id", 123 + (1 << 20)},
		{"POST", "/runtime/transact", 321},
		{"POST", "/admin/transact", 321},
		{"POST", "/admin/query", 16 << 20},
		{"POST", "/admin/query_perms_check", 16 << 20},
		{"POST", "/runtime/sse", 4 << 20},
		{"GET", "/runtime/sse", 1 << 20},
		{"POST", "/storage/signed-upload-url", 64 << 10},
		{"POST", "/unknown", 1 << 20},
	} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			if got := bodyLimitFor(tc.path, cfg, tc.method); got != tc.want {
				t.Fatalf("body limit = %d; want %d", got, tc.want)
			}
		})
	}
}

func TestMiddlewareBodyLimit(t *testing.T) {
	for _, tc := range []struct {
		body string
		code int
	}{{"1234", 204}, {"12345", 413}} {
		t.Run(tc.body, func(t *testing.T) {
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, err := io.ReadAll(r.Body)
				var limitErr *http.MaxBytesError
				if errors.As(err, &limitErr) {
					w.WriteHeader(413)
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				w.WriteHeader(204)
			})
			h := assembleMiddleware(next, config.Config{MaxFrameBytes: 4}, slog.Default(), ratelimit.New(ratelimit.Config{}))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest("POST", "/admin/transact", strings.NewReader(tc.body)))
			if rec.Code != tc.code {
				t.Fatalf("status = %d; want %d", rec.Code, tc.code)
			}
		})
	}
}

func TestLoopbackAddrContract(t *testing.T) {
	for addr, want := range map[string]bool{
		"127.0.0.1:8080": true, "[::1]:8080": true, "localhost:8080": true,
		":8080": false, "0.0.0.0:8080": false, "[::]:8080": false, "": false,
		"example.test:8080": false,
	} {
		if got := loopbackAddr(addr); got != want {
			t.Errorf("loopbackAddr(%q) = %v; want %v", addr, got, want)
		}
	}
}
