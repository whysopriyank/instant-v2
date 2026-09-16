package authn

import (
	"context"
	"errors"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/triple"
	"github.com/jackc/pgx/v5"
)

type oauthAttrs struct {
	state         [16]byte // $oauthRedirects.state (unique)
	cookieHash    [16]byte // $oauthRedirects.cookieHash
	clientID      [16]byte // $oauthRedirects.clientId (string)
	redirectURL   [16]byte // $oauthRedirects.redirectUrl
	codeChallenge [16]byte // $oauthRedirects.codeChallenge
	ccMethod      [16]byte // $oauthRedirects.codeChallengeMethod
	nonceHash     [16]byte // $oauthRedirects.nonceHash (Google only)

	oauthCodeHash [16]byte // $oauthCodes.codeHash
	oauthCC       [16]byte // $oauthCodes.codeChallenge
	oauthCCM      [16]byte // $oauthCodes.codeChallengeMethod
	oauthUserInfo [16]byte // $oauthCodes.userInfo (json blob)
}

// cachedAttr finds an already-known attr id without touching the DB.
func (s *Service) cachedAttr(appID [16]byte, etype, label string) *platform.Attr {
	cat, err := s.Catalogs.For(context.Background(), platform.UUIDToStr(appID))
	if err != nil {
		return nil
	}
	return cat.FindByEtypeLabel(etype, label)
}

// lockOAuthRecord serializes consumers on the persisted unique state/code key.
// A waiter rechecks that key after the winner commits; a rollback leaves it
// available. All record reads stay on the owning transaction connection.
func lockOAuthRecord(ctx context.Context, tx pgx.Tx, appID, attrID [16]byte, value string, invalid error) ([16]byte, map[[16]byte]any, time.Time, error) {
	var eid [16]byte
	var created time.Time
	encoded, err := triple.EncodeValue(value)
	if err != nil {
		return eid, nil, created, err
	}
	err = tx.QueryRow(ctx, `SELECT t.entity_id, t.created_at FROM triples t
		JOIN attrs a ON a.id=t.attr_id AND a.deletion_marked_at IS NULL
		WHERE t.app_id=$1 AND t.attr_id=$2 AND t.value=$3::jsonb FOR UPDATE OF t`,
		appID, attrID, string(encoded)).Scan(&eid, &created)
	if errors.Is(err, pgx.ErrNoRows) {
		return eid, nil, created, invalid
	}
	if err != nil {
		return eid, nil, created, err
	}
	rows, err := storage.FetchTx(ctx, tx, appID, storage.FetchFilter{EntityIDs: [][16]byte{eid}})
	if err != nil {
		return eid, nil, created, err
	}
	out := make(map[[16]byte]any, len(rows))
	for _, r := range rows {
		out[r.Triple.A] = r.Triple.V
	}
	return eid, out, created, nil
}

func (s *Service) oaAttrs(ctx context.Context, appID [16]byte) (oauthAttrs, error) {
	var out oauthAttrs
	get := func(etype, label string, unique bool) ([16]byte, error) {
		a := s.cachedAttr(appID, etype, label)
		if a != nil {
			return a.ID, nil
		}
		var id [16]byte
		err := s.DB.WithTx(ctx, func(tx pgx.Tx) error {
			at, err := platform.GetOrCreateAttr(ctx, tx, appID,
				etype, label, "blob", "one", unique, unique)
			if err != nil {
				return err
			}
			id = at.ID
			s.Catalogs.Invalidate(platform.UUIDToStr(appID))
			return nil
		})
		return id, err
	}
	var e error
	if out.state, e = get("$oauthRedirects", "state", true); e != nil {
		return out, e
	}
	if out.cookieHash, e = get("$oauthRedirects", "cookieHash", false); e != nil {
		return out, e
	}
	if out.clientID, e = get("$oauthRedirects", "clientId", false); e != nil {
		return out, e
	}
	if out.redirectURL, e = get("$oauthRedirects", "redirectUrl", false); e != nil {
		return out, e
	}
	if out.codeChallenge, e = get("$oauthRedirects", "codeChallenge", false); e != nil {
		return out, e
	}
	if out.ccMethod, e = get("$oauthRedirects", "codeChallengeMethod", false); e != nil {
		return out, e
	}
	if out.nonceHash, e = get("$oauthRedirects", "nonceHash", false); e != nil {
		return out, e
	}
	if out.oauthCodeHash, e = get("$oauthCodes", "codeHash", true); e != nil {
		return out, e
	}
	if out.oauthCC, e = get("$oauthCodes", "codeChallenge", false); e != nil {
		return out, e
	}
	if out.oauthCCM, e = get("$oauthCodes", "codeChallengeMethod", false); e != nil {
		return out, e
	}
	if out.oauthUserInfo, e = get("$oauthCodes", "userInfo", false); e != nil {
		return out, e
	}
	return out, nil
}
