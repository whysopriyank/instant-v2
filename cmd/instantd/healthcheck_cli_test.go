package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestHealthcheckCLIReady(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Errorf("probe path = %q", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"ok":true,"db":true}`))
	}))
	defer server.Close()
	bin := da001BuildInstantd(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "healthcheck", "--url", server.URL+"/health")
	cmd.Env = append(os.Environ(), "DATABASE_URL=", "INSTANT_V2_STORAGE_SECRET=", "INSTANT_V2_STORAGE_ROOT=", "INSTANT_V2_INSECURE_DEV_SECRETS=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("healthcheck must probe without server configuration: %v\n%s", err, out)
	}
}

func TestHealthcheckRejectsUnreadyResponses(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{"no database", 200, `{"ok":true,"db":false}`, "not ready"},
		{"not okay", 200, `{"ok":false,"db":true}`, "not ready"},
		{"unhealthy", 503, `{"ok":false,"db":true}`, "status 503"},
		{"malformed", 200, `oops`, "invalid response"},
		{"redirect", 302, `{"ok":true,"db":true}`, "status 302"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			if err := healthcheck([]string{"--url", server.URL}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("probe error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestHealthcheckUsesDaemonAddressWithoutSecrets(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true,"db":true}`))
	}))
	defer server.Close()
	t.Setenv("INSTANT_V2_HTTP_ADDR", strings.TrimPrefix(server.URL, "http://"))
	t.Setenv("INSTANT_V2_HTTP_PORT", "")
	t.Setenv("INSTANT_V2_STORAGE_SECRET", "")
	if err := healthcheck(nil); err != nil {
		t.Fatal(err)
	}
}

func TestHealthcheckTLSRejectsUntrustedCertificate(t *testing.T) {
	ready := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true,"db":true}`))
	}))
	defer ready.Close()
	untrusted := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	defer untrusted.Close()
	if err := healthcheck([]string{"--url", ready.URL, "--tls-url", untrusted.URL}); err == nil || !strings.Contains(err.Error(), "outbound TLS") {
		t.Fatalf("untrusted TLS error = %v", err)
	}
	if err := healthcheck([]string{"--url", ready.URL, "--tls-url", ready.URL}); err == nil || !strings.Contains(err.Error(), "HTTPS") {
		t.Fatalf("plaintext TLS probe error = %v", err)
	}
}
