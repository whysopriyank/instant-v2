package backup

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var ErrTargetNotEmpty = errors.New("backup: restore target is non-empty")

// ErrCommitOutcomeUnknown requires operator reconciliation before retry. SQL
// might have committed even though its acknowledgement was lost; never delete
// restored files on this path.
var ErrCommitOutcomeUnknown = errors.New("backup: commit outcome unknown; reconcile target before retry")

func requireEmptyTarget(ctx context.Context, tx pgx.Tx, appID [16]byte) error {
	// ponytail: restore blocks writes globally; per-app writer coordination if
	// online restore throughput matters. An advisory lock alone cannot exclude
	// ordinary writers, which do not acquire that lock.
	if _, err := tx.Exec(ctx, `LOCK TABLE apps, attrs, triples, rules, transactions IN SHARE ROW EXCLUSIVE MODE`); err != nil {
		return fmt.Errorf("backup: lock restore target: %w", err)
	}
	var populated bool
	if err := tx.QueryRow(ctx, `SELECT
		EXISTS(SELECT 1 FROM attrs WHERE app_id=$1) OR
		EXISTS(SELECT 1 FROM triples WHERE app_id=$1) OR
		EXISTS(SELECT 1 FROM rules WHERE app_id=$1) OR
		EXISTS(SELECT 1 FROM transactions WHERE app_id=$1)`, appID).Scan(&populated); err != nil {
		return fmt.Errorf("backup: inspect restore target: %w", err)
	}
	if populated {
		return ErrTargetNotEmpty
	}
	return nil
}

func restoreCommitError(err error) error {
	var pgErr *pgconn.PgError
	if errors.Is(err, pgx.ErrTxCommitRollback) || (errors.As(err, &pgErr) && pgErr.Severity == "ERROR") {
		return fmt.Errorf("backup: commit rejected: %w", err)
	}
	return errors.Join(ErrCommitOutcomeUnknown, err)
}
