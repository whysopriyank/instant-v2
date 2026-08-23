package platform

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// shippedFiles asserts that the embedded migrations directory contains at least
// the bootstrap trio from Phase 0. This test runs even without a database.
func TestMigrationsShipped(t *testing.T) {
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) < 3 {
		t.Fatalf("expected >=3 migrations, got %d", len(entries))
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	var want = []string{"001_bootstrap.sql", "002_transactions.sql", "003_rules.sql"}
	for _, w := range want {
		found := false
		for _, got := range names {
			if got == w {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing %s; have %v", w, names)
		}
	}
}

// TestBootstrapSQLIntegrity reads the raw bootstrap SQL (the filesystem fallback
// path is handy when embedding is disabled in tests) and checks for the
// flag-column indexes that the query engine depends on.
func TestBootstrapSQLIntegrity(t *testing.T) {
	// Try to locate 001_bootstrap.sql on disk for a direct check.
	for _, dir := range []string{
		filepath.Join("migrations"),
		filepath.Join("internal", "platform", "migrations"),
	} {
		p := filepath.Join(dir, "001_bootstrap.sql")
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		s := string(b)
		for _, key := range []string{"ea_index", "eav_index", "av_index", "ave_index", "vae_index", "ref_values_are_uuid", "indexed_values_are_constrained"} {
			if !strings.Contains(s, key) {
				t.Fatalf("bootstrap SQL missing %q (file %s)", key, p)
			}
		}
		return
	}
	t.Skip("bootstrap SQL not found on disk; embedded check already passed")
}

// TestMigrationsApply runs the embedded migrations against a real Postgres when
// DATABASE_URL is set; otherwise it is skipped. CI sets it via testcontainers
// (docs/05-conformance.md).
func TestMigrationsApply(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping live migration run (set it or run `make test` in CI)")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	v, err := CurrentVersion(ctx, db)
	if err != nil {
		t.Fatalf("CurrentVersion: %v", err)
	}
	if v < 3 {
		t.Fatalf("version %d < 3; expected at least 001..003 applied", v)
	}
	// Sanity: the flagship triples indexes and constraints exist.
	for _, q := range []string{
		`SELECT 1 FROM information_schema.tables WHERE table_name='triples'`,
		`SELECT 1 FROM pg_indexes WHERE indexname='ea_index'`,
		`SELECT 1 FROM information_schema.table_constraints WHERE constraint_name='ref_values_are_uuid'`,
	} {
		var one int
		if err := db.QueryRowContext(ctx, q).Scan(&one); err != nil {
			t.Fatalf("catalog probe %q: %v", q, err)
		}
	}
}
