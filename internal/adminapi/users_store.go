package adminapi

import (
	"context"
	"crypto/rand"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/instant-v2/instant-v2/internal/authn"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/triple"
)

// userByEmail resolves a $users entity id by its email triple.
func (h *Handler) userByEmail(ctx context.Context, a *authedReq, email string) (string, error) {
	attr := a.cat.FindByEtypeLabel("$users", "email")
	if attr == nil {
		return "", nil // no auth attrs provisioned yet → no users
	}
	var eid string
	err := h.Pool.QueryRow(ctx,
		`SELECT entity_id FROM triples WHERE app_id=$1 AND attr_id=$2 AND value=to_jsonb($3::text) LIMIT 1`,
		a.appID, attr.ID, email).Scan(&eid)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return eid, err
}

// deleteUserTokens deletes every token entity whose $user link points at uid
// in a single transaction journaled via RecordTransaction and notifies OnCommit.
// Mutation topics are derived directly from DELETE ... RETURNING attr_id.
func (h *Handler) deleteUserTokens(ctx context.Context, a *authedReq, userID [16]byte) error {
	link := a.cat.FindByEtypeLabel("$userRefreshTokens", "$user")
	if link == nil {
		return nil
	}
	var txID int64
	mutatedAttrs := make(map[[16]byte]bool)
	err := h.DB.WithTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT entity_id FROM triples WHERE app_id=$1 AND attr_id=$2 AND value=to_jsonb($3::text)`,
			a.appID, link.ID, platform.UUIDToStr(userID))
		if err != nil {
			return err
		}
		defer rows.Close()
		var ids [][16]byte
		for rows.Next() {
			var id [16]byte
			if err := rows.Scan(&id); err != nil {
				return err
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		txID, err = storage.RecordTransaction(ctx, tx, a.appID)
		if err != nil {
			return err
		}
		delRows, err := tx.Query(ctx,
			`DELETE FROM triples WHERE app_id=$1 AND entity_id = ANY($2::uuid[]) RETURNING attr_id`,
			a.appID, ids)
		if err != nil {
			return err
		}
		for delRows.Next() {
			var aID [16]byte
			if err := delRows.Scan(&aID); err != nil {
				delRows.Close()
				return err
			}
			mutatedAttrs[aID] = true
		}
		delRows.Close()
		return delRows.Err()
	})
	if err != nil {
		return err
	}
	if txID > 0 {
		attrIDs := make([]string, 0, len(mutatedAttrs))
		for aID := range mutatedAttrs {
			attrIDs = append(attrIDs, platform.UUIDToStr(aID))
		}
		h.notifyCommit(ctx, a.appID, txID, attrIDs, nil, false)
	}
	return nil
}

func (h *Handler) userExists(ctx context.Context, a *authedReq, uid [16]byte) bool {
	var one int
	err := h.Pool.QueryRow(ctx,
		`SELECT 1 FROM triples WHERE app_id=$1 AND entity_id=$2 LIMIT 1`,
		a.appID, uid).Scan(&one)
	return err == nil
}

// createOrFindUserAndMintToken atomically provisions required attrs, journals
// user creation (when created=true) and token creation in ONE transaction via
// RecordTransaction, and fires post-commit invalidation hooks. If token creation
// fails, the entire transaction rolls back cleanly without leaving an orphaned user.
// Concurrent same-email races resolve deterministically by falling back to the
// winner's user.
func (h *Handler) createOrFindUserAndMintToken(ctx context.Context, a *authedReq, userID [16]byte, email string, extra map[string]any, created bool) ([16]byte, string, bool, error) {
	for attempt := 0; attempt < 3; attempt++ {
		token, err := h.execUserAndTokenMutation(ctx, a, userID, email, extra, created)
		if err == nil {
			return userID, token, created, nil
		}
		if !isUniqueViolation(err) {
			return userID, "", created, err
		}
		// On concurrent races (attribute insertion contention or email uniqueness),
		// invalidate local catalog, re-resolve userByEmail, and retry as find.
		h.Catalogs.Invalidate(a.appStr)
		if cat, catErr := h.Catalogs.For(ctx, a.appStr); catErr == nil {
			a.cat = cat
		}
		if email != "" {
			eid, lookupErr := h.userByEmail(ctx, a, email)
			if lookupErr == nil && eid != "" {
				var resolvedID [16]byte
				if platform.ScanUUID(eid, &resolvedID) == nil {
					userID = resolvedID
					created = false
					continue
				}
			}
		}
		return userID, "", created, err
	}
	return userID, "", created, errors.New("concurrent user creation retries exhausted")
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	if pgErr.Code == "P0001" && pgErr.Message == "trigger violation trg_attrs_unique_names" {
		return true
	}
	if pgErr.Code != "23505" {
		return false
	}
	// Only these conflicts are expected while racing another create-or-find
	// request. A random UUID/PK or an unrelated unique index must surface as a
	// real failure instead of being retried and eventually reported as an
	// opaque "retries exhausted" error.
	switch pgErr.ConstraintName {
	case "av_ignore_nulls_index", "attrs_etype_label_unique", "attrs_reverse_etype_label_unique":
		return true
	default:
		return false
	}
}

func (h *Handler) execUserAndTokenMutation(ctx context.Context, a *authedReq, userID [16]byte, email string, extra map[string]any, created bool) (string, error) {
	var txID int64
	var tokenStr string
	var attrsChanged bool
	var attrIDs []string

	err := h.DB.WithTx(ctx, func(tx pgx.Tx) error {
		txCat := a.cat.Clone()
		getAttr := func(etype, label, valueType, cardinality string, unique, indexed bool) (platform.Attr, error) {
			attr, attrCreated, err := platform.GetOrCreateAttrWithStatus(
				ctx, tx, a.appID, etype, label, valueType, cardinality, unique, indexed,
			)
			if err != nil {
				return platform.Attr{}, err
			}
			attrsChanged = attrsChanged || attrCreated
			txCat.Add(attr)
			return attr, nil
		}

		hashedAttr, err := getAttr("$userRefreshTokens", "hashedToken", "blob", "one", true, true)
		if err != nil {
			return err
		}

		linkAttr, err := getAttr("$userRefreshTokens", "$user", "ref", "one", false, false)
		if err != nil {
			return err
		}

		var typeAttr, emailAttr platform.Attr
		var extraAttrs []platform.Attr
		if created {
			typeAttr, err = getAttr("$users", "type", "blob", "one", false, false)
			if err != nil {
				return err
			}

			if email != "" {
				emailAttr, err = getAttr("$users", "email", "blob", "one", true, true)
				if err != nil {
					return err
				}
			}

			for k := range extra {
				if isReservedUserLabel(k) {
					continue
				}
				at, err := getAttr("$users", k, "blob", "one", false, false)
				if err != nil {
					return err
				}
				extraAttrs = append(extraAttrs, at)
			}
		}

		txID, err = storage.RecordTransaction(ctx, tx, a.appID)
		if err != nil {
			return err
		}

		raw := newUUID()
		tokenStr = platform.UUIDToStr(raw)
		tokenEntityID := newUUID()

		var ts []triple.Triple
		if created {
			ts = append(ts, triple.Triple{E: userID, A: typeAttr.ID, V: "user"})
			if email != "" {
				ts = append(ts, triple.Triple{E: userID, A: emailAttr.ID, V: email})
			}
			for _, at := range extraAttrs {
				if label := at.Label; label != nil {
					if v, ok := extra[*label]; ok {
						ts = append(ts, triple.Triple{E: userID, A: at.ID, V: v})
					}
				}
			}
		}
		ts = append(ts,
			triple.Triple{E: tokenEntityID, A: hashedAttr.ID, V: authn.HashToken(tokenStr)},
			triple.Triple{E: tokenEntityID, A: linkAttr.ID, V: platform.UUIDToStr(userID)},
		)

		if h.faultBeforeTokenCommit != nil {
			if ferr := h.faultBeforeTokenCommit(); ferr != nil {
				return ferr
			}
		}

		if err := h.DB.SetTx(ctx, tx, a.appID, txCat, ts, false); err != nil {
			return err
		}

		mutatedAttrs := map[[16]byte]bool{
			hashedAttr.ID: true,
			linkAttr.ID:   true,
		}
		if created {
			mutatedAttrs[typeAttr.ID] = true
			if email != "" {
				mutatedAttrs[emailAttr.ID] = true
			}
			for _, at := range extraAttrs {
				mutatedAttrs[at.ID] = true
			}
		}
		attrIDs = make([]string, 0, len(mutatedAttrs))
		for aID := range mutatedAttrs {
			attrIDs = append(attrIDs, platform.UUIDToStr(aID))
		}

		return nil
	})
	if err != nil {
		return "", err
	}

	if attrsChanged {
		h.Catalogs.Invalidate(a.appStr)
		h.service().InvalidateAttrs(a.appID)
	}
	h.notifyCommit(ctx, a.appID, txID, attrIDs, nil, attrsChanged)
	return tokenStr, nil
}

func isReservedUserLabel(label string) bool {
	switch label {
	case "email", "type", "id":
		return true
	}
	return false
}

func orUUID(preferred, fallback [16]byte) [16]byte {
	if preferred != ([16]byte{}) {
		return preferred
	}
	return fallback
}

// deleteUsers removes users and reverse refs atomically inside a transaction
// journaled via RecordTransaction, and notifies post-commit invalidation hooks.
// The count remains the number of removed triples, including reverse refs, not user entities.
// Mutation topics are derived directly from DELETE ... RETURNING attr_id to match actual effects.
func (h *Handler) deleteUsers(ctx context.Context, a *authedReq, ids [][16]byte) (int64, error) {
	var deleted int64
	var txID int64
	mutatedAttrs := make(map[[16]byte]bool)
	link := a.cat.FindByEtypeLabel("$userRefreshTokens", "$user")
	err := h.DB.WithTx(ctx, func(tx pgx.Tx) error {
		var err error
		txID, err = storage.RecordTransaction(ctx, tx, a.appID)
		if err != nil {
			return err
		}
		// Refresh-token entities carry the user link on one entity and the
		// hashed token on another attr of that same entity. Collect those
		// entity ids before deleting refs so deleting a user cannot leave
		// orphaned bearer-token hashes behind.
		var tokenIDs [][16]byte
		if link != nil {
			for _, userID := range ids {
				rows, err := tx.Query(ctx,
					`SELECT entity_id FROM triples WHERE app_id=$1 AND attr_id=$2 AND value=to_jsonb($3::text)`,
					a.appID, link.ID, platform.UUIDToStr(userID))
				if err != nil {
					return err
				}
				for rows.Next() {
					var id [16]byte
					if err := rows.Scan(&id); err != nil {
						rows.Close()
						return err
					}
					tokenIDs = append(tokenIDs, id)
				}
				if err := rows.Err(); err != nil {
					rows.Close()
					return err
				}
				rows.Close()
			}
		}
		entityIDs := make([][16]byte, 0, len(ids)+len(tokenIDs))
		entityIDs = append(entityIDs, ids...)
		entityIDs = append(entityIDs, tokenIDs...)
		rows, err := tx.Query(ctx,
			`DELETE FROM triples WHERE app_id=$1 AND entity_id = ANY($2::uuid[]) RETURNING attr_id`,
			a.appID, entityIDs)
		if err != nil {
			return err
		}
		for rows.Next() {
			var aID [16]byte
			if err := rows.Scan(&aID); err != nil {
				rows.Close()
				return err
			}
			deleted++
			mutatedAttrs[aID] = true
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, id := range ids {
			delRows, err := tx.Query(ctx,
				`DELETE FROM triples WHERE app_id=$1 AND vae AND value = to_jsonb($2::text) RETURNING attr_id`,
				a.appID, platform.UUIDToStr(id))
			if err != nil {
				return err
			}
			for delRows.Next() {
				var aID [16]byte
				if err := delRows.Scan(&aID); err != nil {
					delRows.Close()
					return err
				}
				deleted++
				mutatedAttrs[aID] = true
			}
			delRows.Close()
			if err := delRows.Err(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	attrIDs := make([]string, 0, len(mutatedAttrs))
	for aID := range mutatedAttrs {
		attrIDs = append(attrIDs, platform.UUIDToStr(aID))
	}
	h.notifyCommit(ctx, a.appID, txID, attrIDs, nil, false)
	return deleted, nil
}

func newUUID() [16]byte {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return b
}
