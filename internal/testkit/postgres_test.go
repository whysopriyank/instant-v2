package testkit

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestIntegrationEnabled(t *testing.T) {
	for _, mode := range []string{"", "0", "1", "true", "2"} {
		got, err := integrationEnabled(mode)
		if got != (mode == "1") || (err != nil) != (mode == "true" || mode == "2") {
			t.Fatalf("mode=%q enabled=%t err=%v", mode, got, err)
		}
	}
}

func TestDatabaseDSN(t *testing.T) {
	for _, dsn := range []string{
		"postgres://user:password@localhost:5432/shared?sslmode=disable",
		"postgresql://user@localhost/shared?dbname=override&application_name=test",
		"postgres://user@localhost/shared?database=override",
		"host=localhost user=user dbname=shared sslmode=disable",
	} {
		config, err := pgx.ParseConfig(databaseDSN(dsn, "instant_test_private"))
		if err != nil || config.Database != "instant_test_private" {
			t.Fatalf("private DSN database mismatch: config=%v error=%v", config, err)
		}
	}
}

func TestDatabaseName(t *testing.T) {
	seen := make(map[string]bool)
	for range 100 {
		name := databaseName()
		if !strings.HasPrefix(name, "instant_test_") || len(name) != 37 || seen[name] {
			t.Fatalf("invalid or repeated database name %q", name)
		}
		seen[name] = true
	}
}

// A subprocess establishes actual testing.T skip/fail behavior without a mock
// TB or any database prerequisite in the ordinary unit lane.
func TestPostgresModeSelection(t *testing.T) {
	if os.Getenv("INSTANT_TESTKIT_MODE_PROBE") == "1" {
		NewPostgres(t, PostgresOptions{})
		t.Fatal("probe unexpectedly opened a database")
	}
	for _, mode := range []string{"0", "1"} {
		cmd := exec.Command(os.Args[0], "-test.run=^TestPostgresModeSelection$", "-test.v")
		cmd.Env = append(withoutFixtureEnv(os.Environ()), "INSTANT_TESTKIT_MODE_PROBE=1", "INSTANT_TEST_INTEGRATION="+mode, "DATABASE_URL=")
		output, err := cmd.CombinedOutput()
		if mode == "0" {
			if err != nil || !strings.Contains(string(output), "--- SKIP") {
				t.Fatalf("unit mode did not skip without database: %s, %v", output, err)
			}
		} else if err == nil || !strings.Contains(string(output), "integration requires DATABASE_URL") {
			t.Fatalf("required integration did not fail clearly: %s, %v", output, err)
		}
	}
}

func withoutFixtureEnv(env []string) []string {
	var clean []string
	for _, item := range env {
		if !strings.HasPrefix(item, "INSTANT_TEST_INTEGRATION=") && !strings.HasPrefix(item, "DATABASE_URL=") && !strings.HasPrefix(item, "INSTANT_TESTKIT_MODE_PROBE=") {
			clean = append(clean, item)
		}
	}
	return clean
}

func TestPostgresIsolationIntegration(t *testing.T) {
	first := NewPostgres(t, PostgresOptions{LogicalWAL: true})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var firstDatabase string
	if err := first.Pool.QueryRow(ctx, "SELECT current_database()").Scan(&firstDatabase); err != nil || firstDatabase != first.Name {
		t.Fatalf("pool database mismatch: got=%q want=%q err=%v", firstDatabase, first.Name, err)
	}
	if _, err := first.Pool.Exec(ctx, "CREATE TABLE fixture_isolation (value text); INSERT INTO fixture_isolation VALUES ('owned')"); err != nil {
		t.Fatal(err)
	}
	var secondName string
	t.Run("second", func(t *testing.T) {
		second := NewPostgres(t, PostgresOptions{})
		secondName = second.Name
		conn, err := pgx.Connect(ctx, second.DSN)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := conn.Close(ctx); err != nil {
				t.Error(err)
			}
		}()
		var database string
		var exists bool
		if err := conn.QueryRow(ctx, "SELECT current_database(), to_regclass('public.fixture_isolation') IS NOT NULL").Scan(&database, &exists); err != nil {
			t.Fatal(err)
		}
		if database != second.Name || first.Name == second.Name || exists {
			t.Fatalf("private DSN isolation failed: db=%s first=%s second=%s table=%t", database, first.Name, second.Name, exists)
		}
		if _, err := conn.Exec(ctx, "CREATE TABLE fixture_isolation (value text); INSERT INTO fixture_isolation VALUES ('second')"); err != nil {
			t.Fatal(err)
		}
	})
	var value string
	if err := first.Pool.QueryRow(ctx, "SELECT value FROM fixture_isolation").Scan(&value); err != nil || value != "owned" {
		t.Fatalf("first fixture changed: value=%q err=%v", value, err)
	}
	var exists bool
	if err := first.Pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", secondName).Scan(&exists); err != nil || exists {
		t.Fatalf("second database not cleaned up: exists=%t err=%v", exists, err)
	}
}
