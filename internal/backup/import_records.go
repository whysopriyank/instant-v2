package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// ensureAppRow creates the placeholder creator user (if any) and upserts the
// apps row — the FK anchor every other imported row needs.
func ensureAppRow(ctx context.Context, tx pgx.Tx, appID, creator [16]byte, title string) error {
	email := "restore+" + platform.UUIDToStr(creator) + "@backup.local"
	if _, err := tx.Exec(ctx, `
		INSERT INTO instant_users (id, email) VALUES ($1, $2)
		ON CONFLICT (id) DO NOTHING`, creator, email); err != nil {
		return fmt.Errorf("backup: ensure creator user: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO apps (id, creator_id, title) VALUES ($1,$2,$3)
		ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title`,
		appID, creator, title); err != nil {
		return fmt.Errorf("backup: upsert app row: %w", err)
	}
	return nil
}

func importAttr(ctx context.Context, tx pgx.Tx, appID [16]byte, body json.RawMessage, strictRequired bool) error {
	var a struct {
		ID               string  `json:"id"`
		Etype            *string `json:"etype"`
		Label            *string `json:"label"`
		ReverseEtype     *string `json:"reverse_etype"`
		ReverseLabel     *string `json:"reverse_label"`
		ValueType        string  `json:"value_type"`
		Cardinality      string  `json:"cardinality"`
		IsUnique         bool    `json:"is_unique"`
		IsIndexed        bool    `json:"is_indexed"`
		ForwardIdent     string  `json:"forward_ident"`
		ReverseIdent     *string `json:"reverse_ident"`
		CheckedDataType  *string `json:"checked_data_type"`
		CheckingDataType *bool   `json:"checking_data_type"`
		IsRequired       *bool   `json:"is_required"`
		DeletionMarkedAt *string `json:"deletion_marked_at"`
	}
	if err := strictUnmarshal(body, &a); err != nil {
		return err
	}
	if strictRequired && a.IsRequired == nil {
		return errors.New(`is_required is required for v2 attr records and must be a boolean`)
	}
	required := false
	if a.IsRequired != nil {
		required = *a.IsRequired
	}
	// Every entity has an implicit id attribute. Normalize hand-authored and
	// legacy dumps so the invariant survives round trips even if an old dump
	// omitted the flag or marked id optional.
	if a.Label != nil && *a.Label == "id" {
		required = true
	}
	id, err := platform.ScanUUIDErr(a.ID)
	if err != nil {
		return fmt.Errorf("bad attr id %q: %v", a.ID, err)
	}
	// Tenant guard: an attr uuid may already exist under ANOTHER app (attr ids
	// are public — clients see them in init-ok). Never let a dump mutate or
	// re-scope rows it does not own.
	var owner [16]byte
	rowErr := tx.QueryRow(ctx, `SELECT app_id FROM attrs WHERE id=$1`, id).Scan(&owner)
	if rowErr == nil && owner != appID {
		return fmt.Errorf("attr %s belongs to a different app", a.ID)
	} else if rowErr != nil && !errors.Is(rowErr, pgx.ErrNoRows) {
		return rowErr
	}
	fwd, err := platform.ScanUUIDErr(a.ForwardIdent)
	if err != nil {
		return fmt.Errorf("bad forward_ident %q: %v", a.ForwardIdent, err)
	}
	var rev *[16]byte
	if a.ReverseIdent != nil {
		u, uerr := platform.ScanUUIDErr(*a.ReverseIdent)
		if uerr != nil {
			return fmt.Errorf("bad reverse_ident %q: %v", *a.ReverseIdent, uerr)
		}
		rev = &u
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO attrs (id, app_id, etype, label, reverse_etype, reverse_label,
		                   value_type, cardinality, is_unique, is_indexed,
		                   forward_ident, reverse_ident,
			checked_data_type, checking_data_type, is_required, deletion_marked_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
		ON CONFLICT (id) DO UPDATE SET
			etype=EXCLUDED.etype, label=EXCLUDED.label,
			reverse_etype=EXCLUDED.reverse_etype, reverse_label=EXCLUDED.reverse_label,
			value_type=EXCLUDED.value_type, cardinality=EXCLUDED.cardinality,
			is_unique=EXCLUDED.is_unique, is_indexed=EXCLUDED.is_indexed,
			forward_ident=EXCLUDED.forward_ident, reverse_ident=EXCLUDED.reverse_ident,
			checked_data_type=EXCLUDED.checked_data_type,
			checking_data_type=EXCLUDED.checking_data_type,
			is_required=EXCLUDED.is_required,
			deletion_marked_at=EXCLUDED.deletion_marked_at`,
		id, appID, a.Etype, a.Label, a.ReverseEtype, a.ReverseLabel,
		a.ValueType, a.Cardinality, a.IsUnique, a.IsIndexed,
		fwd, rev, a.CheckedDataType, a.CheckingDataType, required, a.DeletionMarkedAt); err != nil {
		return err
	}
	// Mirror the idents row like platform.GetOrCreateAttr does on creation.
	if a.Etype != nil && a.Label != nil {
		if _, err := tx.Exec(ctx, `
			INSERT INTO idents (id, app_id, attr_id, etype, label)
			VALUES ($1,$2,$3,$4,$5)
			ON CONFLICT DO NOTHING`, fwd, appID, id, *a.Etype, *a.Label); err != nil {
			return err
		}
	}
	return nil
}

func importRule(ctx context.Context, tx pgx.Tx, appID [16]byte, body json.RawMessage) error {
	var ru struct {
		Code    json.RawMessage `json:"code"`
		Version *int            `json:"version"`
	}
	if err := strictUnmarshal(body, &ru); err != nil {
		return err
	}
	if len(ru.Code) == 0 {
		return errors.New("rule missing code")
	}
	version := 0
	if ru.Version != nil {
		version = *ru.Version
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO rules (app_id, code, version) VALUES ($1,$2,$3)
		ON CONFLICT (app_id) DO UPDATE SET code=EXCLUDED.code, version=EXCLUDED.version`,
		appID, []byte(ru.Code), version)
	return err
}

func importTransaction(ctx context.Context, tx pgx.Tx, appID [16]byte, body json.RawMessage) error {
	var tr struct {
		ID        int64   `json:"id"`
		CreatedAt *string `json:"created_at"`
	}
	if err := strictUnmarshal(body, &tr); err != nil {
		return err
	}
	if tr.CreatedAt == nil {
		_, err := tx.Exec(ctx, `
			INSERT INTO transactions (id, app_id) OVERRIDING SYSTEM VALUE VALUES ($1,$2)
			ON CONFLICT (id) DO NOTHING`, tr.ID, appID)
		return err
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO transactions (id, app_id, created_at) OVERRIDING SYSTEM VALUE VALUES ($1,$2,$3)
		ON CONFLICT (id) DO NOTHING`, tr.ID, appID, *tr.CreatedAt)
	return err
}
