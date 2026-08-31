package authn

import (
	"context"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/triple"
	"github.com/jackc/pgx/v5"
	"time"
)

type oauthAttrs struct {
	state         [16]byte // $oauthRedirects.state (unique)
	cookieHash    [16]byte // $oauthRedirects.cookieHash
	clientID      [16]byte // $oauthRedirects.clientId (string)
	redirectURL   [16]byte // $oauthRedirects.redirectUrl
	codeChallenge [16]byte // $oauthRedirects.codeChallenge
	ccMethod      [16]byte // $oauthRedirects.codeChallengeMethod

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

// loadTriplesByEntity projects one entity's triples to attr-id → value.
func (s *Service) loadTriplesByEntity(ctx context.Context, appID [16]byte, e [16]byte) (map[[16]byte]any, error) {
	rows, err := s.DB.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{e}})
	if err != nil {
		return nil, err
	}
	out := make(map[[16]byte]any, len(rows))
	for _, r := range rows {
		out[r.Triple.A] = r.Triple.V
	}
	return out, nil
}

// entityCreatedAt reads triples.created_at for expiry checks.
func (s *Service) entityCreatedAt(ctx context.Context, appID [16]byte, e [16]byte) time.Time {
	var ts time.Time
	err := s.Pool.QueryRow(ctx,
		`SELECT created_at FROM triples WHERE app_id=$1 AND entity_id=$2 LIMIT 1`,
		appID, e).Scan(&ts)
	if err != nil {
		return time.Time{}
	}
	return ts
}

// burnRedirect deletes every triple of a consumed $oauthRedirects record.
func (s *Service) burnRedirect(ctx context.Context, appID [16]byte, e [16]byte) error {
	rec, err := s.loadTriplesByEntity(ctx, appID, e)
	if err != nil {
		return err
	}
	var ts []triple.Triple
	for aid, v := range rec {
		ts = append(ts, triple.Triple{E: e, A: aid, V: v})
	}
	_, err = s.DB.DeleteTriples(ctx, appID, ts)
	return err
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
