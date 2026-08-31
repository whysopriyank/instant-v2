package authn

import (
	"context"
	"github.com/instant-v2/instant-v2/internal/triple"
)

// VerifyMagicCodeTrusted creates-or-loads the user for an externally verified
// identity and mints a refresh token (the tail of v1's upsert-oauth-link!).
func (s *Service) VerifyMagicCodeTrusted(ctx context.Context, appID [16]byte,
	email string, extra map[string]any,
) (map[string]any, error) {
	a, err := s.attrs(ctx, appID)
	if err != nil {
		return nil, err
	}
	existing, err := s.userByEmail(ctx, appID, email, a)
	if err != nil {
		return nil, err
	}
	created := existing == nil
	var userID [16]byte
	if existing != nil {
		userID, err = parseUUID(existing.ID)
		if err != nil {
			return nil, err
		}
	} else {
		userID = newRandUUID()
	}
	cat, err := s.catalog(ctx, appID)
	if err != nil {
		return nil, err
	}
	if created {
		// Audit F2b: the OAuth/id_token path must enforce the SAME signup
		// gate as the magic-code path — apps denying open signup via
		// $users.create=false must not get accounts minted here.
		if err := s.checkCreatePerm(ctx, appID, email); err != nil {
			return nil, err
		}
		ts := []triple.Triple{
			{E: userID, A: a.userID, V: formatUUID(userID)},
			{E: userID, A: a.userEmail, V: email},
			{E: userID, A: a.userType, V: "user"},
		}
		if imageURL := strFrom(extra, "picture", "imageURL", "avatar_url"); imageURL != "" {
			if at := cat.FindByEtypeLabel("$users", "imageURL"); at != nil {
				ts = append(ts, triple.Triple{E: userID, A: at.ID, V: imageURL})
			}
		}
		if _, err := s.DB.InsertTriples(ctx, appID, cat, ts, false); err != nil {
			return nil, err
		}
	}
	refreshToken := randToken()
	tokenEntity, _ := parseUUID(refreshToken)
	if _, err := s.DB.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: tokenEntity, A: a.tokenHashedToken, V: HashToken(refreshToken)},
		{E: tokenEntity, A: a.tokenUser, V: formatUUID(userID)},
	}, false); err != nil {
		return nil, err
	}
	user, err := s.loadUser(ctx, appID, userID, a)
	if err != nil {
		return nil, err
	}
	return user.Full(refreshToken, created), nil
}

func strFrom(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := m[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
