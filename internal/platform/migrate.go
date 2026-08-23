// Package platform owns apps/attrs catalog access and boot-time schema management.
package platform

import (
	"context"
	"database/sql"
	"embed"
	"fmt"

	"github.com/pressly/goose/v3"
)

//go:embed all:migrations
var migrationsFS embed.FS

// Migrate applies all pending embedded migrations under an advisory lock so that
// concurrent instantd instances do not race DDL.
func Migrate(ctx context.Context, db *sql.DB) error {
	g, err := goose.NewProvider(goose.DialectPostgres, db, migrationsFS)
	if err != nil {
		return fmt.Errorf("goose provider: %w", err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock(727272)`); err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}
	defer func() {
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock(727272)`)
	}()
	if _, err := g.Up(ctx); err != nil {
		return err
	}
	return nil
}

// CurrentVersion reports the highest applied migration version, or 0 when none ran.
func CurrentVersion(ctx context.Context, db *sql.DB) (int64, error) {
	const q = `SELECT COALESCE(MAX(version_id),0) FROM goose_db_version`
	var v int64
	if err := db.QueryRowContext(ctx, q).Scan(&v); err != nil {
		return 0, err
	}
	return v, nil
}
