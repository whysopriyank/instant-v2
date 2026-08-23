package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
)

func uid() [16]byte {
	var b [16]byte
	rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return b
}

func main() {
	ctx := context.Background()
	dsn := os.Getenv("DATABASE_URL")
	sqldb, _ := sql.Open("pgx", dsn)
	if _, err := sqldb.ExecContext(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public;`); err != nil {
		panic(err)
	}
	if err := platform.Migrate(ctx, sqldb); err != nil {
		panic(err)
	}
	sqldb.Close()
	pool, _ := pgxpool.New(ctx, dsn)
	defer pool.Close()
	st := storage.New(pool)
	appID, creator, title := uid(), uid(), uid()
	err := st.WithTx(ctx, func(tx pgx.Tx) error {
		if _, e := tx.Exec(ctx, `INSERT INTO instant_users (id,email) VALUES ($1,$2)`, creator, "soak@test"); e != nil {
			return e
		}
		if e := platform.CreateApp(ctx, tx, creator, appID, "soak"); e != nil {
			return e
		}
		at, e := platform.GetOrCreateAttr(ctx, tx, appID, "todos", "title", "blob", "one", false, true)
		title = at.ID
		return e
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("APP=%s\nATTR=%s\n", format(appID), format(title))
}
func format(u [16]byte) string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}
