package transact

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
)

// Options control one Transact call (admin bypass, mode defaults, etc.).
type Options struct {
	Admin      bool
	AuthUser   map[string]any // {id: uuid string, email: string, type: string}
	RuleParams map[string]any
	OverwriteT bool
	Request    perms.RequestInfo
}

// Result of one successful transact.
type Result struct {
	TxID int64
	EIDs [][]byte
	// AttrsChanged reports whether the batch mutated attribute metadata
	// (add/update/delete/restore-attr). Callers must drop their cached
	// catalogs when set — the in-tx overlay dies with the transaction.
	AttrsChanged bool
}

// maxTxSteps bounds one transaction's step count. Permission probes cost
// O(steps) point queries inside the write tx, so an unbounded batch pinned
// a pooled writer connection for the full statement timeout (audit L1).
const maxTxSteps = 10000

// Transact executes one atomic batch: journal first, prepare the local catalog,
// resolve lookups, authorize writes, dispatch operation groups in first-appearance
// order, then check required-field postconditions before committing.
func Transact(
	ctx context.Context,
	db *storage.DB,
	catalog *platform.AttrCatalog,
	appID [16]byte,
	steps []Step,
	opts Options,
	ruleDoc *perms.RuleDoc,
) (Result, error) {
	if len(steps) > maxTxSteps {
		return Result{}, fmt.Errorf("transact: too many steps (%d > %d)", len(steps), maxTxSteps)
	}
	var res Result
	for _, st := range steps {
		switch st.Op {
		case "add-attr", "update-attr", "delete-attr", "restore-attr":
			res.AttrsChanged = true
		}
	}
	err := db.WithTx(ctx, func(tx pgx.Tx) error {
		txID, err := storage.RecordTransaction(ctx, tx, appID)
		if err != nil {
			return fmt.Errorf("transact: journal: %w", err)
		}
		res.TxID = txID

		txCat, err := prepareCatalog(ctx, tx, appID, catalog, steps, opts, ruleDoc)
		if err != nil {
			return err
		}

		// Resolve any lookup-ref eids/values against committed state so far.
		if err := resolveEntityIDs(ctx, tx, appID, steps, txCat); err != nil {
			return err
		}
		if err := resolveValues(ctx, tx, appID, steps, txCat); err != nil {
			return err
		}

		// Permission gate: every mutating step is checked against the app's
		// RuleDoc with real data/newData/auth bindings BEFORE execution.
		// Fail-closed — compile or eval errors deny the whole batch. Admin
		// callers bypass (v1 :admin? true semantics).
		if ruleDoc != nil && !opts.Admin {
			if err := enforcePerms(ctx, tx, appID, txCat, steps, opts, ruleDoc, storage.FetchTx); err != nil {
				return err
			}
		}

		effects, err := applyOperations(ctx, tx, db, appID, txCat, steps, opts, ruleDoc)
		if err != nil {
			return err
		}

		touched := collectTouchedEntities(steps)
		touched = appendDistinctEntities(touched, effects.cascadeTouched...)
		if txCat != nil && len(touched) > 0 {
			// Validate against the transaction-local catalog so required attrs
			// created earlier in this batch are enforced before commit.
			if err := validateRequired(ctx, tx, appID, txCat, touched); err != nil {
				return err
			}
		}
		if err := validateUpdatedRequired(ctx, tx, appID, effects.requiredUpdates); err != nil {
			return err
		}
		return nil
	})
	return res, err
}
