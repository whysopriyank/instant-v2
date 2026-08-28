package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/instant-v2/instant-v2/internal/benchharness"
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
	dsn := flag.String("database-url", os.Getenv("DATABASE_URL"), "disposable benchmark PostgreSQL URL")
	marker := flag.String("marker", os.Getenv("BENCHMARK_MARKER"), "explicit benchmark marker")
	reset := flag.Bool("reset", false, "reset only a previously marked disposable benchmark database")
	flag.Parse()
	if err := benchharness.ValidateDatabaseURL(*dsn); err != nil {
		fatal(err)
	}
	databaseName, err := benchharness.DatabaseName(*dsn)
	if err != nil {
		fatal(err)
	}
	sqldb, err := sql.Open("pgx", *dsn)
	if err != nil {
		fatal(err)
	}
	defer sqldb.Close()
	if err := sqldb.PingContext(ctx); err != nil {
		fatal(err)
	}
	if err := benchharness.VerifyDatabaseIdentity(ctx, sqldb, databaseName); err != nil {
		fatal(err)
	}
	guard := benchharness.ResetGuard{Marker: strings.TrimSpace(*marker)}
	if guard.Marker == "" && !*reset {
		guard.Marker = "soak-" + time.Now().UTC().Format("20060102T150405.000000000Z")
	}
	if *reset {
		if err := guard.VerifyMarker(ctx, sqldb, databaseName); err != nil {
			fatal(err)
		}
		if _, err := sqldb.ExecContext(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public;`); err != nil {
			fatal(err)
		}
	}
	if err := platform.Migrate(ctx, sqldb); err != nil {
		fatal(err)
	}
	if err := benchharness.EnsureMarker(ctx, sqldb, guard.Marker); err != nil {
		fatal(err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, mustPoolConfig(*dsn))
	if err != nil {
		fatal(err)
	}
	defer pool.Close()
	st := storage.New(pool)
	appID, creator, title := uid(), uid(), uid()
	err = st.WithTx(ctx, func(tx pgx.Tx) error {
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
		fatal(err)
	}
	fmt.Printf("APP=%s\nATTR=%s\n", format(appID), format(title))
}

func mustPoolConfig(dsn string) *pgxpool.Config {
	c, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		fatal(err)
	}
	return c
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "soaksetup:", err)
	os.Exit(2)
}
func format(u [16]byte) string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}
