package transact

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
)

// appliedEffects carries the extra postconditions discovered while applying
// operations: metadata changes and referrers touched by delete cascades.
type appliedEffects struct {
	requiredUpdates []platform.Attr
	cascadeTouched  [][16]byte
}

type indexedStep struct {
	index int
	step  Step
}

func applyOperations(ctx context.Context, tx pgx.Tx, db *storage.DB, appID [16]byte, txCat *platform.AttrCatalog, steps []Step, opts Options, ruleDoc *perms.RuleDoc) (appliedEffects, error) {
	var effects appliedEffects

	for _, op := range orderSteps(steps) {
		batch := filterSteps(steps, op)
		switch op {
		case "rule-params":
			// Kept as a no-op for compatibility: permission gates use
			// Options.RuleParams, not params embedded in individual steps.

		case "add-attr":
			// Handled in the pre-pass above (attrs must exist before
			// same-batch triples resolve); nothing left to do here.

		case "update-attr":
			updates, err := applyRequiredAttrUpdates(ctx, tx, appID, txCat, filterIndexedSteps(steps, op), opts, ruleDoc, nil)
			if err != nil {
				return appliedEffects{}, err
			}
			effects.requiredUpdates = append(effects.requiredUpdates, updates...)

		case "delete-attr":
			if !opts.Admin {
				return appliedEffects{}, deleteAttrDenied()
			}

		case "add-triple":
			if err := applyAddTriples(ctx, tx, db, appID, txCat, batch, opts.OverwriteT); err != nil {
				return appliedEffects{}, err
			}

		case "retract-triple":
			if err := applyRetract(ctx, tx, db, appID, txCat, batch); err != nil {
				return appliedEffects{}, err
			}

		case "deep-merge-triple":
			if err := applyDeepMerge(ctx, tx, db, appID, txCat, batch, opts); err != nil {
				return appliedEffects{}, err
			}

		case "delete-entity":
			refs, err := applyDeleteEntity(ctx, tx, appID, batch)
			if err != nil {
				return appliedEffects{}, err
			}
			effects.cascadeTouched = append(effects.cascadeTouched, refs...)

		default:
			// forward-compat: unknown ops no-op
		}
	}

	return effects, nil
}

func evaluateAttributeOperations(ctx context.Context, tx pgx.Tx, appID [16]byte, txCat *platform.AttrCatalog, steps []Step, opts Options, ruleDoc *perms.RuleDoc, evaluation *PermissionEvaluation) error {
	for _, op := range orderSteps(steps) {
		batch := filterIndexedSteps(steps, op)
		switch op {
		case "update-attr":
			if _, err := applyRequiredAttrUpdates(ctx, tx, appID, txCat, batch, opts, ruleDoc, evaluation); err != nil {
				return err
			}
		case "delete-attr":
			if !opts.Admin && len(batch) > 0 {
				err := deleteAttrDenied()
				recordPermissionCheck(evaluation, batch[0].index, "attrs", "delete", false, err, permBindings(opts, nil, nil))
				return err
			}
		}
	}
	return nil
}

func deleteAttrDenied() error {
	return fmt.Errorf("transact: attrs.delete denied (admin only in Phase 2)")
}

func orderSteps(steps []Step) []string {
	seen := make(map[string]struct{})
	var order []string
	for _, s := range steps {
		if _, ok := seen[s.Op]; !ok {
			seen[s.Op] = struct{}{}
			order = append(order, s.Op)
		}
	}
	return order
}

func filterSteps(steps []Step, op string) []Step {
	var out []Step
	for _, s := range steps {
		if s.Op == op {
			out = append(out, s)
		}
	}
	return out
}

func filterIndexedSteps(steps []Step, op string) []indexedStep {
	var out []indexedStep
	for index, step := range steps {
		if step.Op == op {
			out = append(out, indexedStep{index: index, step: step})
		}
	}
	return out
}
