package sync_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/testkit"
)

func rand16() [16]byte {
	var u [16]byte
	if _, err := randRead(u[:]); err != nil {
		panic(err)
	}
	u[6] = (u[6] & 0x0f) | 0x40
	u[8] = (u[8] & 0x3f) | 0x80
	return u
}

func randRead(b []byte) (int, error) {
	f, err := os.Open("/dev/urandom")
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return f.Read(b)
}

func uuidStr(u [16]byte) string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", be32(u[0:4]), be16(u[4:6]), be16(u[6:8]), be16(u[8:10]), u[10:16])
}

func be32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}
func be16(b []byte) uint16 { return uint16(b[0])<<8 | uint16(b[1]) }

func strptr(s string) *string { return &s }

func rawMap(raw json.RawMessage) map[string]any {
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	return m
}

func newPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	fixture := testkit.NewPostgres(t, testkit.PostgresOptions{})
	migrate(t, fixture.DSN)
	return fixture.Pool
}

// Stop background work before the isolated database is closed and removed.
func runNotifier(t *testing.T, notifier *reactive.Notifier) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); notifier.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("notifier did not stop")
		}
	})
}

func migrate(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := platform.Migrate(context.Background(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
}

func seedApp(t *testing.T, st *storage.DB, appID [16]byte) {
	t.Helper()
	creator := rand16()
	err := st.WithTx(context.Background(), func(tx pgx.Tx) error {
		if _, err := tx.Exec(context.Background(),
			`INSERT INTO instant_users (id,email) VALUES ($1,$2)`, creator, "s@test"); err != nil {
			return err
		}
		return platform.CreateApp(context.Background(), tx, creator, appID, "sync-test")
	})
	if err != nil {
		t.Fatal(err)
	}
}
