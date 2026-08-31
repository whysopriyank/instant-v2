package sync_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/websocket"
)

// T2.2 (docs/reference/09-tier2-architecture.md): permessage-deflate is negotiated only
// when the handler's Compression knob enables it; the disabled default never
// accepts the extension, keeping bytes on the wire identical for existing
// clients/proxies.

func wsCompressionServer(t *testing.T, mode string) *httptest.Server {
	t.Helper()
	env := newWSEnv(t)
	env.WS.Compression = mode
	return env.Server
}

func negotiate(t *testing.T, url, _ string) http.Header {
	t.Helper()
	ctx := context.Background()
	conn, resp, err := websocket.Dial(ctx,
		"ws"+strings.TrimPrefix(url, "http")+"/runtime/session",
		&websocket.DialOptions{CompressionMode: websocket.CompressionContextTakeover})
	if conn != nil {
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return resp.Header
}

func TestWSCompressionNegotiated(t *testing.T) {
	srv := wsCompressionServer(t, "context-takeover")
	hdr := negotiate(t, srv.URL, "context-takeover")
	ext := hdr.Get("Sec-Websocket-Extensions")
	if !strings.Contains(ext, "permessage-deflate") {
		t.Fatalf("expected permessage-deflate accepted, got %q", ext)
	}
}

func TestWSCompressionDisabledDefault(t *testing.T) {
	for _, mode := range []string{"", "disabled"} {
		srv := wsCompressionServer(t, mode)
		hdr := negotiate(t, srv.URL, mode)
		if ext := hdr.Get("Sec-Websocket-Extensions"); strings.Contains(ext, "permessage-deflate") {
			t.Fatalf("mode %q: extension must not be negotiated, got %q", mode, ext)
		}
	}
}

func TestWSCompressionNoContextTakeover(t *testing.T) {
	srv := wsCompressionServer(t, "no-context-takeover")
	hdr := negotiate(t, srv.URL, "no-context-takeover")
	ext := hdr.Get("Sec-Websocket-Extensions")
	if !strings.Contains(ext, "permessage-deflate") {
		t.Fatalf("expected permessage-deflate accepted, got %q", ext)
	}
	if !strings.Contains(ext, "server_no_context_takeover") {
		t.Fatalf("expected server_no_context_takeover param, got %q", ext)
	}
}
