package adminapi

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/authn"
	"github.com/instant-v2/instant-v2/internal/platform"
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

// deleteUserTokens deletes every token entity whose $user link points at uid.
func (h *Handler) deleteUserTokens(ctx context.Context, a *authedReq, userID [16]byte) error {
	link := a.cat.FindByEtypeLabel("$userRefreshTokens", "$user")
	if link == nil {
		return nil
	}
	rows, err := h.Pool.Query(ctx,
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
	if err := rows.Err(); err != nil || len(ids) == 0 {
		return err
	}
	_, err = h.Pool.Exec(ctx,
		`DELETE FROM triples WHERE app_id=$1 AND entity_id = ANY($2::uuid[])`, a.appID, ids)
	return err
}

func (h *Handler) userExists(ctx context.Context, a *authedReq, uid [16]byte) bool {
	var one int
	err := h.Pool.QueryRow(ctx,
		`SELECT 1 FROM triples WHERE app_id=$1 AND entity_id=$2 LIMIT 1`,
		a.appID, uid).Scan(&one)
	return err == nil
}

// createUser writes $users triples for a fresh admin-created user. Attr ids
// resolve through GetOrCreateAttr — the same deterministic ids authn uses.
func (h *Handler) createUser(ctx context.Context, a *authedReq, userID [16]byte, email string, extra map[string]any) error {
	err := h.DB.WithTx(ctx, func(tx pgx.Tx) error {
		if _, _, err := h.userAttrsTx(ctx, tx, a); err != nil {
			return err
		}
		for k := range extra {
			if isReservedUserLabel(k) {
				continue
			}
			if _, err := platform.GetOrCreateAttr(ctx, tx, a.appID, "$users", k, "blob", "one", false, false); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	h.Catalogs.Invalidate(a.appStr)
	cat, err := h.Catalogs.For(ctx, a.appStr)
	if err != nil {
		return err
	}

	var ts []triple.Triple
	add := func(etype, label string, v any) error {
		at := cat.FindByEtypeLabel(etype, label)
		if at == nil {
			return fmt.Errorf("missing attr %s.%s", etype, label)
		}
		ts = append(ts, triple.Triple{E: userID, A: at.ID, V: v})
		return nil
	}
	if err := add("$users", "type", "user"); err != nil {
		return err
	}
	if email != "" {
		if err := add("$users", "email", email); err != nil {
			return err
		}
	}
	for k, v := range extra {
		if isReservedUserLabel(k) {
			continue
		}
		if err := add("$users", k, v); err != nil {
			return err
		}
	}
	_, err = h.DB.InsertTriples(ctx, a.appID, cat, ts, false)
	return err
}

func isReservedUserLabel(label string) bool {
	switch label {
	case "email", "type", "id":
		return true
	}
	return false
}

// mintRefreshToken creates a $userRefreshTokens entity and returns the raw
// uuid bearer token (storage keeps only sha256(uuid-text), like authn).
func (h *Handler) mintRefreshToken(ctx context.Context, a *authedReq, userID [16]byte) (string, error) {
	var hashedAttr, linkAttr [16]byte
	err := h.DB.WithTx(ctx, func(tx pgx.Tx) error {
		ha, err := platform.GetOrCreateAttr(ctx, tx, a.appID, "$userRefreshTokens", "hashedToken", "blob", "one", true, true)
		if err != nil {
			return err
		}
		la, err := platform.GetOrCreateAttr(ctx, tx, a.appID, "$userRefreshTokens", "$user", "ref", "one", false, false)
		if err != nil {
			return err
		}
		hashedAttr, linkAttr = ha.ID, la.ID
		return nil
	})
	if err != nil {
		return "", err
	}
	h.Catalogs.Invalidate(a.appStr)
	cat, err := h.Catalogs.For(ctx, a.appStr)
	if err != nil {
		return "", err
	}

	raw := newUUID()
	ts := []triple.Triple{
		{E: raw, A: hashedAttr, V: authn.HashToken(platform.UUIDToStr(raw))},
		{E: raw, A: linkAttr, V: platform.UUIDToStr(userID)},
	}
	if _, err := h.DB.InsertTriples(ctx, a.appID, cat, ts, false); err != nil {
		return "", err
	}
	return platform.UUIDToStr(raw), nil
}

// userAttrsTx ensures the $users email/type attrs exist inside tx.
func (h *Handler) userAttrsTx(ctx context.Context, tx pgx.Tx, a *authedReq) ([16]byte, [16]byte, error) {
	ea, err := platform.GetOrCreateAttr(ctx, tx, a.appID, "$users", "email", "blob", "one", true, true)
	if err != nil {
		return [16]byte{}, [16]byte{}, err
	}
	ta, err := platform.GetOrCreateAttr(ctx, tx, a.appID, "$users", "type", "blob", "one", false, false)
	if err != nil {
		return [16]byte{}, [16]byte{}, err
	}
	return ea.ID, ta.ID, nil
}

// ---- small helpers ---------------------------------------------------------

func orUUID(preferred, fallback [16]byte) [16]byte {
	if preferred != ([16]byte{}) {
		return preferred
	}
	return fallback
}

// deleteUsers removes users and reverse refs atomically. The count remains
// the number of removed triples, including reverse refs, not user entities.
func (h *Handler) deleteUsers(ctx context.Context, a *authedReq, ids [][16]byte) (int64, error) {
	var deleted int64
	err := h.DB.WithTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`DELETE FROM triples WHERE app_id=$1 AND entity_id = ANY($2::uuid[])`, a.appID, ids)
		if err != nil {
			return err
		}
		deleted += tag.RowsAffected()
		for _, id := range ids {
			tag, err := tx.Exec(ctx,
				`DELETE FROM triples WHERE app_id=$1 AND vae AND value = to_jsonb($2::text)`,
				a.appID, platform.UUIDToStr(id))
			if err != nil {
				return err
			}
			deleted += tag.RowsAffected()
		}
		return nil
	})
	return deleted, err
}

func newUUID() [16]byte {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return b
}
