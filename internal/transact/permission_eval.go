package transact

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// PermissionCheck is one ordered runtime-equivalent permission decision.
// Err is populated for compile/evaluation failures; Allowed is false for both
// errors and explicit denials.
type PermissionCheck struct {
	StepIndex int
	Etype     string
	Action    string
	Allowed   bool
	Err       error
	Bindings  perms.Bindings
}

// PermissionEvaluation is the read-only result of EvaluatePermissions.
type PermissionEvaluation struct {
	Checks     []PermissionCheck
	AllAllowed bool
}

// EvaluatePermissions runs the same lookup and permission projection stages
// as Transact in a transaction that is always rolled back. It deliberately
// does not record a journal row, commit, or invalidate any cache. Caller-owned
// steps and catalogs are copied before those stages run.
func EvaluatePermissions(
	ctx context.Context,
	db *storage.DB,
	catalog *platform.AttrCatalog,
	appID [16]byte,
	steps []Step,
	opts Options,
	doc *perms.RuleDoc,
) (PermissionEvaluation, error) {
	var evaluation PermissionEvaluation
	if db == nil || db.Pool == nil {
		return evaluation, fmt.Errorf("transact: permission evaluation requires a database")
	}
	if catalog == nil {
		return evaluation, fmt.Errorf("transact: permission evaluation requires a catalog")
	}
	if doc == nil && !opts.Admin {
		return evaluation, fmt.Errorf("transact: permission rules are required")
	}
	if len(steps) > maxTxSteps {
		return evaluation, fmt.Errorf("transact: too many steps (%d > %d)", len(steps), maxTxSteps)
	}

	localSteps := clonePermissionSteps(steps)
	localCatalog := catalog.Clone()
	tx, err := db.Pool.Begin(ctx)
	if err != nil {
		return evaluation, err
	}
	rolledBack := false
	rollback := func() error {
		err := tx.Rollback(context.WithoutCancel(ctx))
		rolledBack = true
		return err
	}
	returnWithRollback := func(cause error) error {
		if rollbackErr := rollback(); rollbackErr != nil {
			return errors.Join(cause, fmt.Errorf("transact: permission evaluation rollback: %w", rollbackErr))
		}
		return cause
	}
	defer func() {
		if !rolledBack {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()

	evaluation.AllAllowed = true
	txCat, err := prepareCatalog(ctx, tx, appID, localCatalog, localSteps, opts, doc, &evaluation)
	if err != nil {
		if !evaluation.AllAllowed && len(evaluation.Checks) > 0 {
			if rollbackErr := rollback(); rollbackErr != nil {
				return evaluation, fmt.Errorf("transact: permission evaluation rollback: %w", rollbackErr)
			}
			return evaluation, nil
		}
		return evaluation, returnWithRollback(err)
	}
	if err := resolveEntityIDs(ctx, tx, appID, localSteps, txCat); err != nil {
		return evaluation, returnWithRollback(err)
	}
	if err := resolveValues(ctx, tx, appID, localSteps, txCat); err != nil {
		return evaluation, returnWithRollback(err)
	}
	permissionSteps, err := expandUntypedDeleteEntities(ctx, tx, appID, localSteps)
	if err != nil {
		return evaluation, returnWithRollback(err)
	}

	if !opts.Admin {
		if err := enforcePerms(ctx, tx, appID, txCat, permissionSteps, opts, doc, storage.FetchTx, &evaluation); err != nil {
			if !evaluation.AllAllowed && len(evaluation.Checks) > 0 {
				if rollbackErr := rollback(); rollbackErr != nil {
					return evaluation, fmt.Errorf("transact: permission evaluation rollback: %w", rollbackErr)
				}
				return evaluation, nil
			}
			return evaluation, returnWithRollback(err)
		}
	}

	if err := evaluateAttributeOperations(ctx, tx, appID, txCat, localSteps, opts, doc, &evaluation); err != nil {
		if !evaluation.AllAllowed && len(evaluation.Checks) > 0 {
			if rollbackErr := rollback(); rollbackErr != nil {
				return evaluation, fmt.Errorf("transact: permission evaluation rollback: %w", rollbackErr)
			}
			return evaluation, nil
		}
		return evaluation, returnWithRollback(err)
	}

	if err := rollback(); err != nil {
		return evaluation, fmt.Errorf("transact: permission evaluation rollback: %w", err)
	}
	return evaluation, nil
}

func clonePermissionSteps(steps []Step) []Step {
	out := make([]Step, len(steps))
	for i, step := range steps {
		out[i] = step
		out[i].Raw = append(json.RawMessage(nil), step.Raw...)
		out[i].Args = make([]json.RawMessage, len(step.Args))
		for j, arg := range step.Args {
			out[i].Args[j] = append(json.RawMessage(nil), arg...)
		}
	}
	return out
}

func recordPermissionCheck(evaluation *PermissionEvaluation, stepIndex int, etype, action string, allowed bool, err error, bindings perms.Bindings) {
	if evaluation == nil {
		return
	}
	if err != nil || !allowed {
		evaluation.AllAllowed = false
	}
	evaluation.Checks = append(evaluation.Checks, PermissionCheck{
		StepIndex: stepIndex,
		Etype:     etype,
		Action:    action,
		Allowed:   allowed && err == nil,
		Err:       err,
		Bindings:  snapshotBindings(bindings),
	})
}

func snapshotBindings(bindings perms.Bindings) perms.Bindings {
	out := bindings
	out.Data = snapshotMap(bindings.Data)
	out.NewData = snapshotMap(bindings.NewData)
	out.Auth = snapshotMap(bindings.Auth)
	out.RuleParams = snapshotMap(bindings.RuleParams)
	out.LinkedData = snapshotMap(bindings.LinkedData)
	out.Actions = snapshotMap(bindings.Actions)
	out.Request.ModifiedFields = append([]string(nil), bindings.Request.ModifiedFields...)
	if bindings.Request.Time != nil {
		out.Request.Time = proto.Clone(bindings.Request.Time).(*timestamppb.Timestamp)
	}
	return out
}

func snapshotMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	out := make(map[string]any, len(src))
	for key, value := range src {
		out[key] = snapshotValue(value)
	}
	return out
}

func snapshotValue(value any) any {
	if value == nil {
		return nil
	}
	return snapshotReflect(reflect.ValueOf(value)).Interface()
}

func snapshotReflect(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		out := reflect.New(value.Type()).Elem()
		out.Set(snapshotReflect(value.Elem()))
		return out
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		out := reflect.MakeMapWithSize(value.Type(), value.Len())
		iter := value.MapRange()
		for iter.Next() {
			out.SetMapIndex(iter.Key(), snapshotReflect(iter.Value()))
		}
		return out
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		out := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for i := 0; i < value.Len(); i++ {
			out.Index(i).Set(snapshotReflect(value.Index(i)))
		}
		return out
	case reflect.Array:
		out := reflect.New(value.Type()).Elem()
		for i := 0; i < value.Len(); i++ {
			out.Index(i).Set(snapshotReflect(value.Index(i)))
		}
		return out
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		out := reflect.New(value.Type().Elem())
		out.Elem().Set(snapshotReflect(value.Elem()))
		return out
	default:
		return value
	}
}
