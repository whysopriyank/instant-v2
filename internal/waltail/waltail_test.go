package waltail_test

import (
	"context"
	"log/slog"

	"database/sql"
	"encoding/json"
	"github.com/instant-v2/instant-v2/internal/triple"
	"github.com/jackc/pgx/v5"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/waltail"
)

func dsn(t *testing.T) string {
	t.Helper()
	d := os.Getenv("DATABASE_URL")
	if d == "" {
		t.Skip("DATABASE_URL not set")
	}
	return d
}

// TestTailerDeliversTriplesChanges proves the pgoutput pipeline end-to-end:
// insert a triple via storage, receive the decoded Record from the tailer.
func TestTailerDeliversTriplesChanges(t *testing.T) {
	d := dsn(t)
	waltail.SetDSN(d)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	sqlDB, err := sql.Open("pgx", d)
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	if _, err := pool.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public;`); err != nil {
		t.Fatal(err)
	}
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
	tailer := &waltail.Tailer{Pool: pool, Logger: tlogger}
	if err := tailer.CheckWalLevel(ctx); err != nil {
		t.Skip(err.Error())
	}
	// Fresh slot too: a slot left over from earlier runs (or other suites
	// sharing the cluster) can lag gigabytes of WAL behind; the tailer would
	// spend its whole timeout streaming backlog before current records.
	if _, err := sqlDB.Exec(
		`SELECT pg_drop_replication_slot('instant_v2_tail')
		 WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='instant_v2_tail')`); err != nil {
		t.Fatal(err)
	}
	if err := tailer.EnsurePublication(ctx); err != nil {
		t.Fatalf("EnsurePublication: %v", err)
	}
	// Fresh checkpoint namespace: sibling tests persist synthetic LSNs into
	// tail_state; starting replication from those starves the tailer.
	if _, err := sqlDB.Exec(`DROP TABLE IF EXISTS tail_state`); err != nil {
		t.Fatal(err)
	}
	cp, err := waltail.OpenCheckpoint(sqlDB)
	if err != nil {
		t.Fatal(err)
	}

	var (
		mu      sync.Mutex
		got     []waltail.Record
		deliver = make(chan struct{}, 16)
	)
	handler := func(_ context.Context, rec waltail.Record) error {
		mu.Lock()
		got = append(got, rec)
		mu.Unlock()
		deliver <- struct{}{}
		return nil
	}
	//nolint:errcheck // background stream; errors logged by tailer
	go tailer.Run(ctx, cp, handler)

	// Give the replication connection a moment to attach before writing.
	time.Sleep(500 * time.Millisecond)

	cat, _ := platform.LoadAttrCatalog(ctx, pool, appID)
	if _, err := st.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: entity, A: titleAttr, V: "from-wal"},
	}, false); err != nil {
		t.Fatal(err)
	}

	select {
	case <-deliver:
	case <-time.After(10 * time.Second):
		mu.Lock()
		defer mu.Unlock()
		t.Fatalf("no WAL records delivered; got %+v", got)
	}
	mu.Lock()
	defer mu.Unlock()
	found := false
	for _, r := range got {
		b, _ := json.Marshal(r)
		t.Logf("record: %s", b)
		if r.AttrID == uuidText(titleAttr) && r.Op == "insert" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected an insert record for the title attr")
	}
}
