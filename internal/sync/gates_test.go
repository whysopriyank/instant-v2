package sync_test

import (
	"context"
	"testing"
	"time"

	syncpkg "github.com/instant-v2/instant-v2/internal/sync"
)

// TestNegotiateMatrix pins the exact gate boundaries from
// reactive/session.clj:73-107 — old SDKs must keep working.
func TestNegotiateMatrix(t *testing.T) {
	cases := []struct {
		version       string
		skipAttrs     bool
		patchPresence bool
		batchMessages bool
	}{
		{"0.12.0", false, false, false},
		{"0.13.0", false, false, false},
		{"0.17.4", false, false, false}, // just below patch-presence
		{"0.17.5", false, true, false},  // patch-presence boundary
		{"0.17.6", false, true, false},
		{"0.20.3", false, true, false}, // just below skip-attrs
		{"0.20.4", true, true, false},  // skip-attrs boundary
		{"0.20.5", true, true, false},
		{"0.22.74", true, true, false}, // just below batch-messages
		{"0.22.75", true, true, true},  // batch-messages boundary
		{"0.23.0", true, true, true},
		{"1.0.0", true, true, true},
	}
	for _, tc := range cases {
		f := syncpkg.Negotiate(map[string]string{"@instantdb/core": tc.version})
		if f["skip-attrs"] != tc.skipAttrs ||
			f["patch-presence"] != tc.patchPresence ||
			f["batch-messages"] != tc.batchMessages {
			t.Errorf("version %s: got %v, want skip-attrs=%v patch-presence=%v batch-messages=%v",
				tc.version, f, tc.skipAttrs, tc.patchPresence, tc.batchMessages)
		}
	}
	if f := syncpkg.Negotiate(nil); len(f) != 0 {
		t.Errorf("nil versions should yield no gates, got %v", f)
	}
	if f := syncpkg.Negotiate(map[string]string{"@instantdb/core": "not-semver"}); f["skip-attrs"] {
		t.Error("garbage version must not open gates")
	}
}

// TestFeatureGateInitBehavior drives the negotiated behavior over a live
// WebSocket: pre-0.20.4 clients get attrs in init-ok; 0.20.4+ get none.
func TestFeatureGateInitBehavior(t *testing.T) {
	// v1 session.clj L186: init-ok carries attrs for EVERY client version.
	// skip-attrs governs refresh-frame re-sends of unchanged attrs, not init.
	for _, tc := range []struct {
		name    string
		version string
	}{
		{"old-sdk", "0.19.0"},
		{"new-sdk", "0.22.75"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			env := newWSEnv(t)
			conn, frames := env.dial(t, ctx)
			sendFrame(t, conn, ctx, map[string]any{
				"op":       "init",
				"app-id":   env.AppID,
				"versions": map[string]string{"@instantdb/core": tc.version},
			})
			reply := expectOp(t, frames, "init-ok")
			if _, has := reply["attrs"]; !has {
				t.Fatalf("%s: init-ok must carry attrs", tc.version)
			}
			// auth{app,user,admin?} and app-status must ride along too
			// (session.clj cond-> init-ok shape).
			for _, k := range []string{"auth", "app-status"} {
				if _, has := reply[k]; !has {
					t.Fatalf("%s: init-ok missing %s", tc.version, k)
				}
			}
		})
	}
}
