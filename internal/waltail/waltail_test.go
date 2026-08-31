package waltail_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/testkit"
	"github.com/instant-v2/instant-v2/internal/triple"
	"github.com/instant-v2/instant-v2/internal/waltail"
)

// TestTailerDeliversTriplesChanges proves the pgoutput pipeline end-to-end:
// insert a triple via storage, receive the decoded Record from the tailer.
func TestTailerDeliversTriplesChanges(t *testing.T) {
	fixture := testkit.NewPostgres(t, testkit.PostgresOptions{LogicalWAL: true})
	waltail.SetDSN(fixture.DSN)
	t.Cleanup(func() { waltail.SetDSN("") })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := fixture.Pool
	sqlDB, err := sql.Open("pgx", fixture.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := platform.Migrate(ctx, sqlDB); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	st := storage.New(pool)
	appID, entity := randUUID(), randUUID()
	var titleAttr [16]byte
	err = st.WithTx(ctx, func(tx pgx.Tx) error {
		creator := randUUID()
		if _, e := tx.Exec(ctx, `INSERT INTO instant_users (id,email) VALUES ($1,$2)`, creator, "w@test"); e != nil {
			return e
		}
		if e := platform.CreateApp(ctx, tx, creator, appID, "w"); e != nil {
			return e
		}
		row := tx.QueryRow(ctx, `
			INSERT INTO attrs (id, app_id, etype, label, value_type, cardinality,
			                   is_unique, is_indexed, forward_ident)
			VALUES ($1,$2,'todos','title','blob','one',false,true,$1)
			RETURNING id`, randUUID(), appID)
		return row.Scan(&titleAttr)
	})
	if err != nil {
		t.Fatal(err)
	}

	tlogger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	tailer := &waltail.Tailer{Pool: pool, Slot: fixture.Name, Logger: tlogger}
	if err := tailer.CheckWalLevel(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tailer.EnsurePublication(ctx); err != nil {
		t.Fatalf("EnsurePublication: %v", err)
	}
	// Slot names are cluster-wide, unlike publications. Cleanup is restricted
	// to the generated name AND this fixture's database, after Run stops.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := pool.Exec(cleanupCtx,
			`SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1 AND database=$2`,
			fixture.Name, fixture.Name); err != nil {
			t.Errorf("drop owned replication slot: %v", err)
		}
	})
	var slotDatabase string
	if err := pool.QueryRow(ctx, `SELECT database FROM pg_replication_slots WHERE slot_name=$1`, fixture.Name).Scan(&slotDatabase); err != nil {
		t.Fatalf("configured replication slot was not created: %v", err)
	}
	if slotDatabase != fixture.Name {
		t.Fatalf("slot database = %q, want owned database %q", slotDatabase, fixture.Name)
	}
	if err := tailer.EnsurePublication(ctx); err != nil {
		t.Fatalf("EnsurePublication must reuse the configured slot: %v", err)
	}
	cp, err := waltail.OpenCheckpoint(sqlDB)
	if err != nil {
		t.Fatal(err)
	}

	deliver := make(chan waltail.Record, 16)
	handler := func(ctx context.Context, rec waltail.Record) error {
		select {
		case deliver <- rec:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	var runErr error
	stopped := make(chan struct{})
	go func() {
		runErr = tailer.Run(ctx, cp, handler)
		close(stopped)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			t.Error("tailer did not stop before slot cleanup")
		}
	})

	// The first-boot tailer starts at the current WAL position. Write only
	// after PostgreSQL confirms this slot's connection is streaming.
	readyCtx, readyCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readyCancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var streaming bool
		if err := pool.QueryRow(readyCtx, `SELECT EXISTS (
			SELECT 1 FROM pg_replication_slots s JOIN pg_stat_replication r ON r.pid=s.active_pid
			WHERE s.slot_name=$1 AND s.database=current_database() AND r.state='streaming')`,
			fixture.Name).Scan(&streaming); err != nil {
			t.Fatalf("observe replication readiness: %v", err)
		}
		if streaming {
			break
		}
		select {
		case <-stopped:
			t.Fatalf("tailer stopped before streaming: %v", runErr)
		case <-readyCtx.Done():
			t.Fatal("configured replication slot did not become streaming")
		case <-ticker.C:
		}
	}

	cat, err := platform.LoadAttrCatalog(ctx, pool, appID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: entity, A: titleAttr, V: "from-wal"},
	}, false); err != nil {
		t.Fatal(err)
	}

	select {
	case rec := <-deliver:
		b, _ := json.Marshal(rec)
		t.Logf("record: %s", b)
		if rec.AppID != uuidText(appID) || rec.EntityID != uuidText(entity) || rec.AttrID != uuidText(titleAttr) || rec.Op != "insert" || rec.LSN == 0 {
			t.Fatalf("expected inserted entity's title record with an LSN, got %+v", rec)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no WAL record delivered after replication became ready")
	}
}
