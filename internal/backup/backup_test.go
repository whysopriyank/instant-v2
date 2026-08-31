package backup_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/instant-v2/instant-v2/internal/backup"
	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/testkit"
	"github.com/instant-v2/instant-v2/internal/triple"
)

// newDatabase migrates a fresh test-owned database; restore tests use a second
// instance rather than resetting a schema shared with other packages.
func newDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	fixture := testkit.NewPostgres(t, testkit.PostgresOptions{})
	sqldb, err := sql.Open("pgx", fixture.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqldb.Close() })
	if err := platform.Migrate(context.Background(), sqldb); err != nil {
		t.Fatal(err)
	}
	return fixture.Pool
}

// env seeds one user and app inside an isolated database.
func env(t *testing.T) (*pgxpool.Pool, [16]byte, func()) {
	t.Helper()
	pool := newDatabase(t)
	ctx := context.Background()
	appID := newUUID()
	creator := newUUID()
	if _, err := pool.Exec(ctx,
		`INSERT INTO instant_users (id,email) VALUES ($1,$2)`, creator, "backup@test"); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := platform.CreateApp(ctx, tx, creator, appID, "backup-test"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	cleanup := pool.Close
	return pool, appID, cleanup
}

func newUUID() [16]byte {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return b
}

func uuidStr(u [16]byte) string { return platform.UUIDToStr(u) }

// seedTodoApp creates attrs and inserts 5 triples per entity:
// text (blob one), done (blob one), priority number 40+i (blob one), tags
// "urgent"+"home" (blob many).
func seedTodoApp(t *testing.T, ctx context.Context, pool *pgxpool.Pool, appID [16]byte, n int) {
	t.Helper()
	var attrs []platform.Attr
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, def := range []struct {
		label, card string
		uniq, idx   bool
	}{
		{"text", "one", false, true},
		{"done", "one", false, false},
		{"priority", "one", false, true},
		{"tags", "many", false, false},
	} {
		a, aerr := platform.GetOrCreateAttr(ctx, tx, appID, "todo", def.label, "blob", def.card, def.uniq, def.idx)
		if aerr != nil {
			t.Fatal(aerr)
		}
		attrs = append(attrs, a)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	cat, err := platform.LoadAttrCatalog(ctx, pool, appID)
	if err != nil {
		t.Fatal(err)
	}
	st := storage.New(pool)
	for i := 0; i < n; i++ {
		e := newUUID()
		ts := []triple.Triple{
			{E: e, A: attrs[0].ID, V: fmt.Sprintf("todo number %d", i)},
			{E: e, A: attrs[1].ID, V: i%2 == 0},
			{E: e, A: attrs[2].ID, V: float64(40 + i)}, // checksum-corruption target
			{E: e, A: attrs[3].ID, V: "urgent"},
			{E: e, A: attrs[3].ID, V: "home"},
		}
		if _, err := st.InsertTriples(ctx, appID, cat, ts, false); err != nil {
			t.Fatal(err)
		}
	}
	// One journal row so the export covers the transactions section.
	if err := st.WithTx(ctx, func(tx pgx.Tx) error {
		_, err := storage.RecordTransaction(ctx, tx, appID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func runInstaql(t *testing.T, ctx context.Context, pool *pgxpool.Pool, appID [16]byte) map[string]json.RawMessage {
	t.Helper()
	cat, err := platform.LoadAttrCatalog(ctx, pool, appID)
	if err != nil {
		t.Fatal(err)
	}
	q, err := instaql.Coerce(map[string]any{"todo": map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	x := &instaql.Executor{DB: pool}
	res, err := x.Run(ctx, q, cat, appID)
	if err != nil {
		t.Fatal(err)
	}
	return res.Data
}

func compactJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func exportApp(t *testing.T, ctx context.Context, pool *pgxpool.Pool, appID [16]byte, opts backup.ExportOptions) ([]byte, backup.Counts) {
	t.Helper()
	var buf bytes.Buffer
	counts, err := backup.Export(ctx, &buf, pool, appID, opts)
	if err != nil {
		t.Fatal(err)
	}
	return buf.Bytes(), counts
}

func creatorID(t *testing.T, ctx context.Context, pool *pgxpool.Pool) [16]byte {
	t.Helper()
	var id [16]byte
	if err := pool.QueryRow(ctx, `SELECT id FROM instant_users LIMIT 1`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}
