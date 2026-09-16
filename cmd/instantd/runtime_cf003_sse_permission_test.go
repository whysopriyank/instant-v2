package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/ratelimit"
	"github.com/instant-v2/instant-v2/internal/testkit"
)

func TestCF003AssembledSSEPermissionLifecycle(t *testing.T) {
	a, pool, mux, appID, adminToken := cf003SSEPermissionEnv(t)
	app := platform.UUIDToStr(appID)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	// Seed through the real admin route before the SSE subscription exists.
	cf003TransactTitle(t, mux, app, adminToken, "sse-permission-allowed")

	client := cf003OpenSSE(t, ctx, server, app)
	defer client.resp.Body.Close()
	client.post(t, ctx, map[string]any{"op": "init", "app-id": app})
	idAttr, titleAttr := cf003AssertSSEInit(t, cf003ReadSSE(t, client.scanner), app)
	client.post(t, ctx, map[string]any{
		"op": "add-query", "q": map[string]any{"todos": map[string]any{}},
		"client-event-id": "cf003-permission-query",
	})
	cf003AssertSSEAddQuery(t, cf003ReadSSE(t, client.scanner), "cf003-permission-query")
	cf003AssertSSETodos(t, cf003ReadSSE(t, client.scanner), "sse-permission-allowed", true)

	cf003PersistSSEPermissionRules(t, ctx, pool, appID, `{"todos":{"allow":{"view":"false"}}}`, 1)
	a.cats.Invalidate(app)
	denyTx := cf003TransactTitle(t, mux, app, adminToken, "sse-permission-denied-boundary")
	cf003AssertSSEDenied(t, cf003ReadSSE(t, client.scanner), denyTx)

	cf003PersistSSEPermissionRules(t, ctx, pool, appID, `{"todos":{"allow":{"view":"true"}}}`, 2)
	a.cats.Invalidate(app)
	recoverTx := cf003TransactTitle(t, mux, app, adminToken, "sse-permission-recovered")
	if recoverTx <= denyTx {
		t.Fatalf("recovery tx-id = %d; want greater than deny tx-id %d", recoverTx, denyTx)
	}
	cf003AssertSSERefreshTodo(t, cf003ReadSSE(t, client.scanner), idAttr, titleAttr, "sse-permission-recovered", recoverTx)
}

func cf003SSEPermissionEnv(t *testing.T) (*appRuntime, *pgxpool.Pool, *http.ServeMux, [16]byte, string) {
	t.Helper()
	fixture := testkit.NewPostgres(t, testkit.PostgresOptions{})
	db, err := sql.Open("pgx", fixture.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := platform.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	appID, err := platform.ScanUUIDErr(cf003AppID)
	if err != nil {
		t.Fatal(err)
	}
	creatorID, err := platform.ScanUUIDErr(cf003CreatorID)
	if err != nil {
		t.Fatal(err)
	}
	adminID, err := platform.ScanUUIDErr(cf003AdminToken)
	if err != nil {
		t.Fatal(err)
	}
	if err := func() error {
		tx, err := fixture.Pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx) //nolint:errcheck // commit below owns the transaction
		if _, err := tx.Exec(ctx, `INSERT INTO instant_users (id,email) VALUES ($1,$2)`, creatorID, "cf003-sse-permissions@example.test"); err != nil {
			return err
		}
		if err := platform.CreateApp(ctx, tx, creatorID, appID, "cf003-sse-permissions"); err != nil {
			return err
		}
		if err := platform.SetAdminToken(ctx, tx, appID, adminID); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO rules (app_id, code, version) VALUES ($1, $2::jsonb, 0)`, appID, `{"todos":{"allow":{"view":"true"}}}`)
		if err != nil {
			return err
		}
		return tx.Commit(ctx)
	}(); err != nil {
		t.Fatal(err)
	}

	runCtx, stop := context.WithCancel(context.Background())
	root := t.TempDir()
	a := newAppRuntime(fixture.Pool, fixture.Pool, cf003Config(root), slog.Default())
	notifierDone := make(chan struct{})
	go func() {
		defer close(notifierDone)
		a.notifier.Run(runCtx)
	}()
	t.Cleanup(func() {
		stop()
		select {
		case <-notifierDone:
		case <-time.After(5 * time.Second):
			t.Error("CF-003 SSE permission notifier did not stop")
		}
	})

	mux := http.NewServeMux()
	if _, _, err := a.mountRoutes(runCtx, db, mux, cf003Config(root), ratelimit.New(ratelimit.Config{})); err != nil {
		t.Fatal(err)
	}
	return a, fixture.Pool, mux, appID, cf003AdminToken
}

func cf003PersistSSEPermissionRules(t *testing.T, ctx context.Context, pool *pgxpool.Pool, appID [16]byte, rules string, version int) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO rules (app_id, code, version) VALUES ($1, $2::jsonb, $3)
		ON CONFLICT (app_id) DO UPDATE SET code=EXCLUDED.code, version=EXCLUDED.version`, appID, rules, version); err != nil {
		t.Fatal(err)
	}
	var stored string
	var storedVersion int
	if err := pool.QueryRow(ctx, `SELECT code::text, version FROM rules WHERE app_id=$1`, appID).Scan(&stored, &storedVersion); err != nil {
		t.Fatal(err)
	}
	var want bytes.Buffer
	if err := json.Compact(&want, []byte(rules)); err != nil {
		t.Fatal(err)
	}
	var got bytes.Buffer
	if err := json.Compact(&got, []byte(stored)); err != nil {
		t.Fatal(err)
	}
	if got.String() != want.String() {
		t.Fatalf("persisted SSE rules = %q version=%d; want %q version=%d", stored, storedVersion, rules, version)
	}
	if storedVersion != version {
		t.Fatalf("persisted SSE rule version = %d; want %d", storedVersion, version)
	}
}

func cf003AssertSSEDenied(t *testing.T, frame map[string]any, wantTxID int64) {
	t.Helper()
	txID, ok := frame["processed-tx-id"].(float64)
	if !ok || int64(txID) != wantTxID {
		t.Fatalf("denied SSE processed tx id = %#v; want %d", frame["processed-tx-id"], wantTxID)
	}
	want := map[string]any{
		"op": "refresh-ok", "processed-tx-id": txID,
		"computations": []any{map[string]any{
			"instaql-query": map[string]any{"todos": map[string]any{}},
			"instaql-result": []any{map[string]any{
				"child-nodes": []any{},
				"data": map[string]any{"datalog-result": map[string]any{
					"join-rows": []any{[]any{}},
				}},
			}},
		}},
	}
	if !reflect.DeepEqual(frame, want) {
		t.Fatalf("denied SSE refresh = %#v; want %#v", frame, want)
	}
}
