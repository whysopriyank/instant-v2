package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// TestCF003AssembledAdminPresenceExclusion closes the CF-003 room/admin-
// presence remainder as an enforced exclusion over the production-mounted
// routes: GET /admin/rooms/presence never returns a successful (possibly
// empty) presence snapshot. It returns the stable 501 unsupported envelope
// to an authorized caller, still 501 with live WS room presence behind it,
// and denies missing/foreign credentials without disclosing presence state.
func TestCF003AssembledAdminPresenceExclusion(t *testing.T) {
	mux, appID, adminToken := cf003PostgresMux(t)
	app := platform.UUIDToStr(appID)
	presencePath := "/admin/rooms/presence?app-id=" + url.QueryEscape(app)

	// Missing credentials are rejected before any presence logic runs.
	status, body, _ := cf003Serve(mux, http.MethodGet, presencePath, "", nil)
	if status != http.StatusUnauthorized || body != "{\"message\":\"missing admin token\"}\n" {
		t.Fatalf("presence missing-token status/body = %d %q; want 401 exact denial", status, body)
	}

	// A foreign admin token discloses nothing, not even an empty snapshot.
	status, body, _ = cf003Serve(mux, http.MethodGet, presencePath, "",
		map[string]string{"X-admin-token": "00000000-0000-4000-8000-000000000099"})
	if status != http.StatusUnauthorized || body != "{\"message\":\"Invalid admin token\"}\n" {
		t.Fatalf("presence foreign-token status/body = %d %q; want 401 exact denial", status, body)
	}

	// A valid token for an unknown app resolves no presence either.
	status, body, _ = cf003Serve(mux, http.MethodGet,
		"/admin/rooms/presence?app-id="+url.QueryEscape("00000000-0000-4000-8000-000000000098"),
		"", map[string]string{"X-admin-token": adminToken})
	if status != http.StatusNotFound || body != "{\"message\":\"unknown app\"}\n" {
		t.Fatalf("presence unknown-app status/body = %d %q; want 404 exact denial", status, body)
	}

	// The authorized call is explicitly unsupported, not an empty room.
	status, body, headers := cf003Serve(mux, http.MethodGet, presencePath, "",
		map[string]string{"X-admin-token": adminToken})
	const wantUnsupported = "{\"message\":\"admin presence is unsupported\",\"type\":\"unsupported\"}\n"
	if status != http.StatusNotImplemented || body != wantUnsupported {
		t.Fatalf("presence authorized status/body = %d %q; want 501 %q", status, body, wantUnsupported)
	}
	if got := headers.Get("Content-Type"); got != "application/json" {
		t.Fatalf("presence authorized content-type = %q; want application/json", got)
	}

	// The exclusion is not vacuous: with live WS room presence behind it,
	// the admin projection still refuses instead of returning a snapshot.
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	client := cf003OpenWS(t, ctx, server, app)
	client.send(t, ctx, map[string]any{
		"op": "join-room", "room-id": "cf003-room", "peer-id": "peer-exclusion",
		"data": map[string]any{"mood": "present"}, "client-event-id": "join-exclusion",
	})
	cf003ExactWS(t, client.nextOp(t, "join-room-ok"), map[string]any{
		"op": "join-room-ok", "room-id": "cf003-room", "client-event-id": "join-exclusion",
	})

	status, body, _ = cf003Serve(mux, http.MethodGet, presencePath, "",
		map[string]string{"X-admin-token": adminToken})
	if status != http.StatusNotImplemented || body != wantUnsupported {
		t.Fatalf("presence with live room status/body = %d %q; want 501 %q", status, body, wantUnsupported)
	}
}
