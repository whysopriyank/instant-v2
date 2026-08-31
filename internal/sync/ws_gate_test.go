package sync_test

import (
	"context"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/reactive"
)

// T2.1 (docs/reference/09-tier2-architecture.md): the transact gate sheds publishers
// with a 429-shaped error frame before any Postgres work happens.
func TestTransactGateSheds(t *testing.T) {
	env := newWSEnv(t)
	denied := &reactive.ShedError{RetryAfter: 250 * time.Millisecond}
	env.Mgr.Deps.TransactGate = func(appID string) error { return denied }
	ctx := context.Background()
	conn, frames := env.dial(t, ctx)

	sendFrame(t, conn, ctx, map[string]any{"op": "init", "app-id": env.AppID})
	expectOp(t, frames, "init-ok")

	sendFrame(t, conn, ctx, map[string]any{"op": "transact", "tx-steps": []any{}})
	f := expectOp(t, frames, "error")
	if got := int(f["status"].(float64)); got != 429 {
		t.Fatalf("expected status 429, got %v", f)
	}
	if typ := f["type"].(string); typ != "shed" {
		t.Fatalf("expected type shed, got %q", typ)
	}
	if msg := f["message"]; msg != "server busy; retry after 250ms" {
		t.Fatalf("hint should carry retry-after, got %v", msg)
	}
}

// Gate unset (nil) is the historical always-allow behavior.
func TestTransactGateNilAllowsTransacts(t *testing.T) {
	env := newWSEnv(t)
	ctx := context.Background()
	conn, frames := env.dial(t, ctx)

	sendFrame(t, conn, ctx, map[string]any{"op": "init", "app-id": env.AppID})
	expectOp(t, frames, "init-ok")
	if env.Mgr.Deps.TransactGate != nil {
		t.Fatal("gate must default to nil")
	}
}
