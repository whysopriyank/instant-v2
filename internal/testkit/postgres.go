// Package testkit supplies isolated integration fixtures. Product code must not
// import it. Callers own migrations and application-specific seeding.
package testkit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresOptions declares capabilities needed by a suite.
type PostgresOptions struct {
	LogicalWAL bool
}

// Postgres is a private, initially empty database. Name is also a valid unique
// replication-slot identifier. Register resource cleanup after NewPostgres so
// it runs before the fixture's database cleanup.
type Postgres struct {
	DSN  string
	Name string
	Pool *pgxpool.Pool
}

// NewPostgres creates one database for t (or a parent suite containing subtests).
// It skips without connecting in unit mode. Integration mode requires
// INSTANT_TEST_INTEGRATION=1 and DATABASE_URL for a role with CREATEDB privilege.
// Cleanup closes Pool and drops only this fixture's randomly named database.
func NewPostgres(t testing.TB, opts PostgresOptions) *Postgres {
	t.Helper()
	enabled, err := integrationEnabled(os.Getenv("INSTANT_TEST_INTEGRATION"))
	if err != nil {
		t.Fatal(err)
	}
	if !enabled {
		t.Skip("integration test: set INSTANT_TEST_INTEGRATION=1 and DATABASE_URL")
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Fatal("integration requires DATABASE_URL pointing to a PostgreSQL role with CREATEDB privilege")
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal("integration DATABASE_URL is not a valid PostgreSQL connection string")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		// Connection errors may embed a password from the supplied URL.
		t.Fatal("integration PostgreSQL connection failed; check DATABASE_URL and server readiness")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := admin.Close(ctx); err != nil {
			t.Errorf("close fixture admin connection: %v", err)
		}
	})
	if opts.LogicalWAL {
		var level string
		var replication bool
		if err := admin.QueryRow(ctx, `SELECT current_setting('wal_level'), rolsuper OR rolreplication FROM pg_roles WHERE rolname = current_user`).Scan(&level, &replication); err != nil {
			t.Fatalf("check integration logical-WAL capability: %v", err)
		}
		if level != "logical" || !replication {
			t.Fatal("integration requires wal_level=logical and a replication-capable PostgreSQL role")
		}
	}
	name := databaseName()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()+" TEMPLATE template0"); err != nil {
		t.Fatalf("create isolated integration database (role requires CREATEDB): %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Errorf("drop owned integration database %s: %v", name, err)
		}
	})
	privateDSN := databaseDSN(dsn, name)
	poolConfig, err := pgxpool.ParseConfig(privateDSN)
	if err != nil {
		t.Fatal("integration DATABASE_URL is not a valid PostgreSQL pool connection string")
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatal("open isolated integration database pool")
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatal("ping isolated integration database failed")
	}
	return &Postgres{DSN: privateDSN, Name: name, Pool: pool}
}

func integrationEnabled(mode string) (bool, error) {
	switch mode {
	case "", "0":
		return false, nil
	case "1":
		return true, nil
	default:
		return false, fmt.Errorf("INSTANT_TEST_INTEGRATION must be 0 or 1, got %q", mode)
	}
}

func databaseName() string {
	var suffix [12]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		panic(err)
	}
	return "instant_test_" + hex.EncodeToString(suffix[:])
}

// databaseDSN receives an already validated DSN and a generated identifier.
// Reparse the changed string so both pool configuration and exposed DSN point
// to the private database (ConnConfig.ConnString retains the original input).
func databaseDSN(dsn, name string) string {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, _ := url.Parse(dsn)
		u.Path = "/" + name
		u.RawPath = ""
		q := u.Query()
		q.Del("dbname")
		q.Del("database")
		u.RawQuery = q.Encode()
		return u.String()
	}
	return dsn + " dbname=" + name
}
