// Package authn ports v1's app-level authentication surface
// (runtime/magic_code_auth.clj, model/app_user.clj, model/app_user_magic_code.clj,
// model/app_user_refresh_token.clj). Wire facts pinned from v1 @ a4d2ef33:
//
//   - Refresh tokens are opaque UUIDs. The client keeps the raw uuid; storage
//     holds sha256(uuid-text) hex under $userRefreshTokens.hashedToken plus a
//     $user ref link. No JWT is issued; validity = triple presence.
//   - Magic codes are 6-digit numeric strings; only sha256(code) hex is stored
//     under $magicCodes {codeHash, email}. Default TTL 1440 minutes (24h).
//     Consume = lookup by {codeHash, email} then delete (one-time burn).
//   - Users live as $users triples {id, email, type: "user"|"guest", ...extra}.
//
// Everything lives in the same triples storage so InstaQL visibility and WAL
// invalidation match v1 exactly.
package authn

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/triple"
)

// DefaultMagicCodeTTL mirrors flags/default-magic-code-expiry-minutes
// (flags.clj L593): 24 hours.
const DefaultMagicCodeTTL = 24 * time.Hour

var (
	ErrInvalidCode  = errors.New("authn: invalid magic code")
	ErrExpiredCode  = errors.New("authn: expired magic code")
	ErrBadToken     = errors.New("authn: unknown refresh token")
	ErrSignupDenied = errors.New("authn: signup denied by permissions")
	// ErrLocked is returned while an (app,email) pair is cooling down after
	// repeated failed magic-code verifications. Callers surface it as 429.
	ErrLocked = errors.New("authn: too many failed attempts; try later")
)

// Magic-code brute-force resistance. Codes are 6 digits with a 24h TTL, so
// unlimited verification attempts would be trivially grindable. Failed
// verifications per (app,email) accrue; crossing the threshold locks the
// pair for the cooldown window. State is process-local (single-node honest;
// multi-node deployments get per-node limits until shared state lands).
const (
	maxMagicCodeFails = 5
	magicLockout      = 15 * time.Minute
	minResendInterval = 60 * time.Second
)

type attemptRecord struct {
	fails       int
	lockedUntil time.Time
	lastSent    time.Time
}

// User is an app-level user record ($users entity projection).
type User struct {
	ID    string
	Email string
	Type  string
	Extra map[string]any
}

// Full renders user + refresh_token the way verify! returns it
// (magic_code_auth.clj L300: assoc user :refresh_token :created), i.e.
// {"user": {..., "refresh_token": ...}, "created": bool}.
func (u User) Full(refreshToken string, created bool) map[string]any {
	out := map[string]any{"id": u.ID}
	if u.Email != "" {
		out["email"] = u.Email
	}
	if u.Type != "" {
		out["type"] = u.Type
	}
	for k, v := range u.Extra {
		out[k] = v
	}
	out["refresh_token"] = refreshToken
	return map[string]any{"user": out, "created": created}
}

// RulesFunc optionally resolves the app's permission RuleDoc for the $users
// create gate. Nil or unresolvable rules → v1 default-open behavior.
type RulesFunc func(ctx context.Context, appID [16]byte) (*perms.RuleDoc, error)

// Service is one process's authn facade.
type Service struct {
	DB       *storage.DB
	Pool     *pgxpool.Pool
	Catalogs *platform.CatalogCache
	Mailer   Mailer // nil → codes logged only (dev/self-host default)
	Logger   *slog.Logger

	// CodeTTL overrides DefaultMagicCodeTTL (tests); per-app override pending
	// apps.magic_code_expiry_minutes column (Phase 5).
	CodeTTL    time.Duration
	RulesForFn func(ctx context.Context, appID [16]byte) (*perms.RuleDoc, error)

	mu       sync.Mutex
	attrByID map[[16]byte]systemAttrs // keyed by appID
	attempts map[string]*attemptRecord
	nowFn    func() time.Time // swappable for tests

	// Providers overrides the builtin oauth registry (tests; custom OIDC
	// clients land with apps.rules persistence in Phase 5).
	Providers map[string]*ResolvedProvider
	// Apple signs client-secret assertions when configured (.p8 material).
	Apple *AppleSigner

	// NowFunc overrides time.Now for lockout/throttle state (tests). nil →
	// time.Now.
	NowFunc func() time.Time

	jwks jwksCache
}

// Mailer delivers magic codes; self-hosted installs may no-op.
type Mailer interface {
	SendMagicCode(ctx context.Context, email, code string) error
}

// systemAttrs resolves (and memoizes) the fixed auth attr ids for an app.
type systemAttrs struct {
	tokenHashedToken [16]byte // $userRefreshTokens.hashedToken (unique+indexed)
	tokenUser        [16]byte // $userRefreshTokens.$user (ref, one)
	magicCodeHash    [16]byte // $magicCodes.codeHash
	magicCodeEmail   [16]byte // $magicCodes.email
	userEmail        [16]byte // $users.email (unique+indexed)
	userType         [16]byte // $users.type
	userID           [16]byte // $users.id
	labels           map[[16]byte]string
}

func (s *Service) attrs(ctx context.Context, appID [16]byte) (systemAttrs, error) {
	s.mu.Lock()
	if a, ok := s.attrByID[appID]; ok {
		s.mu.Unlock()
		return a, nil
	}
	s.mu.Unlock()

	var out systemAttrs
	err := s.DB.WithTx(ctx, func(tx pgx.Tx) error {
		get := func(etype, label string, unique, indexed bool) ([16]byte, error) {
			at, err := platform.GetOrCreateAttr(ctx, tx, appID,
				etype, label, "blob", "one", unique, indexed)
			if err != nil {
				return [16]byte{}, err
			}
			return at.ID, nil
		}
		var e error
		if out.tokenHashedToken, e = get("$userRefreshTokens", "hashedToken", true, true); e != nil {
			return e
		}
		refAt, e := platform.GetOrCreateAttr(ctx, tx, appID,
			"$userRefreshTokens", "$user", "ref", "one", false, false)
		if e != nil {
			return e
		}
		out.tokenUser = refAt.ID
		if out.magicCodeHash, e = get("$magicCodes", "codeHash", false, false); e != nil {
			return e
		}
		if out.magicCodeEmail, e = get("$magicCodes", "email", false, true); e != nil {
			return e
		}
		if out.userEmail, e = get("$users", "email", true, true); e != nil {
			return e
		}
		if out.userType, e = get("$users", "type", false, false); e != nil {
			return e
		}
		if out.userID, e = get("$users", "id", false, false); e != nil {
			return e
		}
		return nil
	})
	if err != nil {
		return systemAttrs{}, err
	}
	// Attr creation may have raced the shared catalog cache; drop any stale
	// entry and load post-commit state.
	s.Catalogs.Invalidate(platform.UUIDToStr(appID))
	cat, err := s.Catalogs.For(ctx, platform.UUIDToStr(appID))
	if err != nil {
		return systemAttrs{}, err
	}
	out.labels = make(map[[16]byte]string, cat.Len())
	for _, at := range cat.Attrs() {
		if at.Label != nil {
			out.labels[at.ID] = *at.Label
		}
	}
	s.mu.Lock()
	if s.attrByID == nil {
		s.attrByID = map[[16]byte]systemAttrs{}
	}
	s.attrByID[appID] = out
	s.mu.Unlock()
	return out, nil
}

func (s *Service) catalog(ctx context.Context, appID [16]byte) (*platform.AttrCatalog, error) {
	return s.Catalogs.For(ctx, platform.UUIDToStr(appID))
}

func (s *Service) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}

func (s *Service) now() time.Time {
	if s.NowFunc != nil {
		return s.NowFunc()
	}
	return time.Now()
}

// attemptKey namespaces brute-force state per app and email.
func attemptKey(appID [16]byte, email string) string {
	return platform.UUIDToStr(appID) + "\x00" + strings.ToLower(strings.TrimSpace(email))
}

func (s *Service) recordLocked(appID [16]byte, email string) bool {
	key := attemptKey(appID, email)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attempts == nil {
		s.attempts = map[string]*attemptRecord{}
	}
	rec := s.attempts[key]
	return rec != nil && s.now().Before(rec.lockedUntil)
}

// noteFailure bumps the failure counter and locks on threshold.
func (s *Service) noteFailure(appID [16]byte, email string) {
	key := attemptKey(appID, email)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attempts == nil {
		s.attempts = map[string]*attemptRecord{}
	}
	rec := s.attempts[key]
	if rec == nil {
		rec = &attemptRecord{}
		s.attempts[key] = rec
	}
	rec.fails++
	if rec.fails >= maxMagicCodeFails {
		rec.lockedUntil = s.now().Add(magicLockout)
		rec.fails = 0 // fresh count after cooldown
	}
}

// clearFailures resets state after a successful verification.
func (s *Service) clearFailures(appID [16]byte, email string) {
	key := attemptKey(appID, email)
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.attempts, key)
}

// HashToken ports app-user-refresh-token-model/hash-token:
// sha256 of the canonical uuid text, hex-encoded.
func HashToken(tokenUUID string) string {
	h := sha256.Sum256([]byte(tokenUUID))
	return hex.EncodeToString(h[:])
}

// randToken returns a random RFC-4122 v4 uuid string (the raw bearer token).
func randToken() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return formatUUID(b)
}

// randCode ports rand-code: a uniform 6-digit numeric string.
func randCode() (string, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(1000000))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%06d", n.Int64()), nil
}

func formatUUID(u [16]byte) string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		be32(u[0:4]), be16(u[4:6]), be16(u[6:8]), be16(u[8:10]), u[10:16])
}

func be32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}
func be16(b []byte) uint16 { return uint16(b[0])<<8 | uint16(b[1]) }

// SendMagicCode creates a one-time code for email and hands it to the Mailer.
// HTTP contract: POST /runtime/auth/send_magic_code → {sent: true}.
func (s *Service) SendMagicCode(ctx context.Context, appID [16]byte, email string) error {
	// Resend throttle: silently honor requests inside the cooldown window
	// (the previously issued code remains valid) so clients see the same
	// contract while mail-bombing stays impossible. No enumeration signal.
	key := attemptKey(appID, email)
	s.mu.Lock()
	if s.attempts == nil {
		s.attempts = map[string]*attemptRecord{}
	}
	rec := s.attempts[key]
	now := s.now()
	if rec != nil && now.Sub(rec.lastSent) < minResendInterval {
		s.mu.Unlock()
		return nil
	}
	if rec == nil {
		rec = &attemptRecord{}
		s.attempts[key] = rec
	}
	rec.lastSent = now
	s.mu.Unlock()

	a, err := s.attrs(ctx, appID)
	if err != nil {
		return err
	}
	code, err := randCode()
	if err != nil {
		return err
	}
	cat, err := s.catalog(ctx, appID)
	if err != nil {
		return err
	}
	var codeEntity [16]byte
	_, _ = rand.Read(codeEntity[:])
	_, err = s.DB.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: codeEntity, A: a.magicCodeHash, V: HashToken(code)},
		{E: codeEntity, A: a.magicCodeEmail, V: email},
	}, false)
	if err != nil {
		return err
	}
	if s.Mailer != nil {
		return s.Mailer.SendMagicCode(ctx, email, code)
	}
	s.logger().Info("authn: magic code generated (no mailer configured)",
		"app-id", platform.UUIDToStr(appID), "email", email)
	return nil
}

// VerifyRefreshToken ports app-user-model/get-by-refresh-token: hash the raw
// token, find the $userRefreshTokens entity via hashedToken, resolve its $user
// link, and project the user triples. Returns ErrBadToken when unknown.
func (s *Service) VerifyRefreshToken(ctx context.Context, appID [16]byte, rawToken string) (*User, error) {
	a, err := s.attrs(ctx, appID)
	if err != nil {
		return nil, err
	}
	rows, err := s.DB.FetchTriples(ctx, appID, storage.FetchFilter{
		AttrIDs: [][16]byte{a.tokenHashedToken},
		Value:   HashToken(rawToken),
	})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrBadToken
	}
	tokenEntity := rows[0].Triple.E
	linkRows, err := s.DB.FetchTriples(ctx, appID, storage.FetchFilter{
		EntityIDs: [][16]byte{tokenEntity},
		AttrIDs:   [][16]byte{a.tokenUser},
	})
	if err != nil {
		return nil, err
	}
	if len(linkRows) == 0 {
		return nil, ErrBadToken
	}
	userIDStr, ok := linkRows[0].Triple.V.(string)
	if !ok {
		return nil, ErrBadToken
	}
	var userID [16]byte
	copy(userID[:], zeroUUID[:]) // ensure deterministic zeroing below
	parsed, err := parseUUID(userIDStr)
	if err != nil {
		return nil, ErrBadToken
	}
	userID = parsed
	return s.loadUser(ctx, appID, userID, a)
}

var zeroUUID [16]byte

func parseUUID(s string) ([16]byte, error) {
	var u [16]byte
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return u, errors.New("bad uuid shape")
	}
	hexStr := s[0:8] + s[9:13] + s[14:18] + s[19:23] + s[24:36]
	raw, err := hex.DecodeString(hexStr)
	if err != nil {
		return u, err
	}
	copy(u[:], raw)
	return u, nil
}

func (s *Service) loadUser(ctx context.Context, appID [16]byte, userID [16]byte, a systemAttrs) (*User, error) {
	rows, err := s.DB.FetchTriples(ctx, appID, storage.FetchFilter{
		EntityIDs: [][16]byte{userID},
	})
	if err != nil {
		return nil, err
	}
	u := &User{ID: formatUUID(userID), Extra: map[string]any{}}
	for _, r := range rows {
		switch r.Triple.A {
		case a.userEmail:
			u.Email, _ = r.Triple.V.(string)
		case a.userType:
			u.Type, _ = r.Triple.V.(string)
		default:
			if label := a.labels[r.Triple.A]; label != "" && !isSystemAuthLabel(label) {
				u.Extra[label] = r.Triple.V
			}
		}
	}
	return u, nil
}

func isSystemAuthLabel(label string) bool {
	switch label {
	case "email", "type", "id":
		return true
	}
	return false
}

// SignOut deletes the token entity's triples (sign-out = row delete in v1;
// delete-by-id! works even when the app is read-only — same here, direct SQL).
func (s *Service) SignOut(ctx context.Context, appID [16]byte, rawToken string) error {
	a, err := s.attrs(ctx, appID)
	if err != nil {
		return err
	}
	rows, err := s.DB.FetchTriples(ctx, appID, storage.FetchFilter{
		AttrIDs: [][16]byte{a.tokenHashedToken},
		Value:   HashToken(rawToken),
	})
	if err != nil || len(rows) == 0 {
		return err
	}
	var ts []triple.Triple
	for _, r := range rows {
		ts = append(ts, r.Triple)
	}
	_, err = s.DB.DeleteTriples(ctx, appID, ts)
	return err
}

// VerifyMagicCode ports magic-code-auth/verify!: run the signup permission
// gate BEFORE burning the code (v1 ordering guarantee), consume the code,
// upsert the $users record (guest upgrade in place), mint a fresh refresh
// token, and return {user{...,refresh_token}, created}.
func (s *Service) VerifyMagicCode(ctx context.Context, appID [16]byte,
	email, code string, guestRefreshToken string, extraFields map[string]any,
	admin bool,
) (map[string]any, error) {
	// Brute-force gate: locked pairs are refused before any code check.
	if s.recordLocked(appID, email) {
		return nil, ErrLocked
	}
	a, err := s.attrs(ctx, appID)
	if err != nil {
		return nil, err
	}

	// Resolve optional guest before anything burns.
	guestIsGuest := false
	var guestID [16]byte
	if guestRefreshToken != "" {
		g, err := s.VerifyRefreshToken(ctx, appID, guestRefreshToken)
		if err != nil {
			return nil, err
		}
		if g.Type == "guest" {
			guestIsGuest = true
			guestID, err = parseUUID(g.ID)
			if err != nil {
				return nil, err
			}
		}
	}

	existing, err := s.userByEmail(ctx, appID, email, a)
	if err != nil {
		return nil, err
	}
	created := existing == nil

	// Permission gate runs before consuming the code so a failed check
	// doesn't burn the one-time code (v1 assert-signup! placement).
	if created {
		if !admin {
			if err := s.checkCreatePerm(ctx, appID, email); err != nil {
				return nil, err
			}
		}
		if err := s.consumeCode(ctx, appID, a, email, code); err != nil {
			s.noteFailure(appID, email)
			return nil, err
		}
	} else if err := s.consumeCode(ctx, appID, a, email, code); err != nil {
		s.noteFailure(appID, email)
		return nil, err
	}
	s.clearFailures(appID, email)

	var userID [16]byte
	switch {
	case existing != nil:
		userID, err = parseUUID(existing.ID)
	case guestIsGuest:
		userID = guestID // upgrade guest in place
		err = nil
	default:
		userID = newRandUUID()
	}
	if err != nil {
		return nil, err
	}

	cat, err := s.catalog(ctx, appID)
	if err != nil {
		return nil, err
	}
	if created {
		ts := []triple.Triple{
			{E: userID, A: a.userID, V: formatUUID(userID)},
			{E: userID, A: a.userEmail, V: email},
			{E: userID, A: a.userType, V: "user"},
		}
		for k, v := range extraFields {
			if at := cat.FindByEtypeLabel("$users", k); at != nil {
				ts = append(ts, triple.Triple{E: userID, A: at.ID, V: v})
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

// SignInGuest ports sign-in-guest-post: type=guest user + refresh token.
func (s *Service) SignInGuest(ctx context.Context, appID [16]byte, extraFields map[string]any) (map[string]any, error) {
	a, err := s.attrs(ctx, appID)
	if err != nil {
		return nil, err
	}
	if err := s.checkCreatePerm(ctx, appID, ""); err != nil {
		return nil, err
	}
	userID := newRandUUID()
	cat, err := s.catalog(ctx, appID)
	if err != nil {
		return nil, err
	}
	ts := []triple.Triple{
		{E: userID, A: a.userID, V: formatUUID(userID)},
		{E: userID, A: a.userType, V: "guest"},
	}
	for k, v := range extraFields {
		if at := cat.FindByEtypeLabel("$users", k); at != nil {
			ts = append(ts, triple.Triple{E: userID, A: at.ID, V: v})
		}
	}
	if _, err := s.DB.InsertTriples(ctx, appID, cat, ts, false); err != nil {
		return nil, err
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
	return user.Full(refreshToken, true), nil
}

// checkCreatePerm runs the $users create rule through the CEL engine with an
// auth binding (assert-signup!). No rules configured → allowed (v1
// default-open). Resolution or evaluation failures FAIL CLOSED — a broken
// rule must never widen signup access.
func (s *Service) checkCreatePerm(ctx context.Context, appID [16]byte, email string) error {
	if s.RulesForFn == nil {
		return nil
	}
	doc, err := s.RulesForFn(ctx, appID)
	if err != nil {
		return fmt.Errorf("authn: signup rules unavailable: %w", err)
	}
	if doc == nil {
		return nil // no rules configured → default open
	}
	auth := map[string]any{}
	if email != "" {
		auth["email"] = email
	}
	allow, err := perms.Check("$users", "create", doc, perms.Bindings{Auth: auth})
	if err != nil {
		return fmt.Errorf("authn: signup rule eval: %w", err)
	}
	if !allow {
		return ErrSignupDenied
	}
	return nil
}

func (s *Service) userByEmail(ctx context.Context, appID [16]byte, email string, a systemAttrs) (*User, error) {
	rows, err := s.DB.FetchTriples(ctx, appID, storage.FetchFilter{
		AttrIDs: [][16]byte{a.userEmail},
		Value:   email,
	})
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return s.loadUser(ctx, appID, rows[0].Triple.E, a)
}

// consumeCode ports app-user-magic-code-model/consume!: find the entity
// holding BOTH {codeHash=sha256(code)} and {email}, enforce TTL on
// triples.created_at, then burn (delete) its code triples.
func (s *Service) consumeCode(ctx context.Context, appID [16]byte, a systemAttrs, email, code string) error {
	hash := HashToken(code)
	rows, err := s.Pool.Query(ctx, `
		SELECT h.entity_id, h.created_at
		  FROM triples h
		  JOIN triples e
		    ON e.app_id = h.app_id AND e.entity_id = h.entity_id
		   AND e.attr_id = $4 AND e.value = $5::jsonb
		 WHERE h.app_id = $1 AND h.attr_id = $3 AND h.value = $2::jsonb`,
		appID, mustJSON(hash), a.magicCodeHash, a.magicCodeEmail, mustJSON(email))
	if err != nil {
		return err
	}
	type hit struct {
		e       [16]byte
		created time.Time
	}
	var hits []hit
	for rows.Next() {
		var h hit
		if err := rows.Scan(&h.e, &h.created); err != nil {
			rows.Close()
			return err
		}
		hits = append(hits, h)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	ttl := s.CodeTTL
	if ttl <= 0 {
		ttl = DefaultMagicCodeTTL
	}
	burn := func(e [16]byte) error {
		_, err := s.DB.DeleteTriples(ctx, appID, []triple.Triple{
			{E: e, A: a.magicCodeHash, V: hash},
			{E: e, A: a.magicCodeEmail, V: email},
		})
		return err
	}
	if len(hits) == 0 {
		return ErrInvalidCode
	}
	h := hits[0]
	// v1 order: delete first, THEN expired? throws — the burn happens
	// regardless so an expired code can't be retried.
	if err := burn(h.e); err != nil {
		return err
	}
	if time.Since(h.created) > ttl {
		return ErrExpiredCode
	}
	return nil
}

func mustJSON(v any) string {
	b, err := triple.EncodeValue(v)
	if err != nil {
		return ""
	}
	return string(b)
}

func newRandUUID() [16]byte {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return b
}

// VerifyRefreshTokenAsMap adapts VerifyRefreshToken to sync.Authenticator:
// (user map incl. id/email/type/extra, ok, err).
func (s *Service) VerifyRefreshTokenAsMap(ctx context.Context, appID [16]byte, rawToken string) (map[string]any, bool, error) {
	u, err := s.VerifyRefreshToken(ctx, appID, rawToken)
	if err == ErrBadToken {
		return nil, false, nil
	}
	if err != nil || u == nil {
		return nil, false, err
	}
	out := map[string]any{"id": u.ID}
	if u.Email != "" {
		out["email"] = u.Email
	}
	if u.Type != "" {
		out["type"] = u.Type
	}
	for k, v := range u.Extra {
		out[k] = v
	}
	return out, true, nil
}
