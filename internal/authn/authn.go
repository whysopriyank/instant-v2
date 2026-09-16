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
	// ErrMagicCodeDeliveryUnavailable means no delivery adapter is configured.
	// It is returned before any code is generated or persisted.
	ErrMagicCodeDeliveryUnavailable = errors.New("authn: magic code delivery unavailable")
	// ErrMagicCodeThrottleUnavailable means the shared resend lease could not
	// be checked. Delivery must not proceed without that abuse-control gate.
	ErrMagicCodeThrottleUnavailable = errors.New("authn: magic code throttle unavailable")
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
	Mailer   Mailer // nil → delivery unavailable; codes are neither generated nor persisted
	Logger   *slog.Logger

	// CodeTTL overrides DefaultMagicCodeTTL (tests); per-app override pending
	// apps.magic_code_expiry_minutes column (Phase 5).
	CodeTTL    time.Duration
	RulesForFn func(ctx context.Context, appID [16]byte) (*perms.RuleDoc, error)

	mu          sync.Mutex
	attrByID    map[[16]byte]systemAttrs // keyed by appID
	attrVersion map[[16]byte]uint64      // invalidation generation keyed by appID

	// Providers overrides the builtin oauth registry (tests; custom OIDC
	// clients land with apps.rules persistence in Phase 5).
	Providers          map[string]*ResolvedProvider
	GoogleClientID     string
	GoogleClientSecret string
	GitHubClientID     string
	GitHubClientSecret string
	// Apple signs client-secret assertions when configured (.p8 material).
	Apple *AppleSigner

	// NowFunc overrides time.Now for lockout/throttle and OAuth expiry (tests). nil →
	// time.Now.
	NowFunc func() time.Time

	jwks jwksCache
}

// Mailer delivers magic codes. A nil Mailer is an unavailable delivery
// configuration, not a successful no-op.
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

// InvalidateAttrs drops the cached systemAttrs and label metadata for appID,
// forcing the next auth operation to reload the latest catalog and label map.
func (s *Service) InvalidateAttrs(appID [16]byte) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attrVersion == nil {
		s.attrVersion = map[[16]byte]uint64{}
	}
	s.attrVersion[appID]++
	if s.attrByID != nil {
		delete(s.attrByID, appID)
	}
}

func (s *Service) attrs(ctx context.Context, appID [16]byte) (systemAttrs, error) {
	return s.attrsWithLoader(ctx, appID, s.loadAttrs)
}

// attrsWithLoader contains the generation-aware cache loop. The loader
// argument keeps the ordering contract directly testable without adding a
// production callback or replacing the real storage path; attrs always uses
// loadAttrs above.
func (s *Service) attrsWithLoader(
	ctx context.Context,
	appID [16]byte,
	load func(context.Context, [16]byte) (systemAttrs, error),
) (systemAttrs, error) {
	for {
		s.mu.Lock()
		if a, ok := s.attrByID[appID]; ok {
			s.mu.Unlock()
			return a, nil
		}
		version := s.attrVersion[appID]
		s.mu.Unlock()

		out, err := load(ctx, appID)
		if err != nil {
			return systemAttrs{}, err
		}
		if err := validateSystemAttrs(out); err != nil {
			return systemAttrs{}, err
		}
		if s.cacheAttrsIfCurrent(appID, version, out) {
			return out, nil
		}
		if err := ctx.Err(); err != nil {
			return systemAttrs{}, err
		}
	}
}

func (s *Service) loadAttrs(ctx context.Context, appID [16]byte) (systemAttrs, error) {
	var out systemAttrs
	err := s.DB.WithTx(ctx, func(tx pgx.Tx) error {
		get := func(etype, label string, unique, indexed bool) ([16]byte, error) {
			at, err := platform.GetOrCreateAttr(ctx, tx, appID,
				etype, label, "blob", "one", unique, indexed)
			if err != nil {
				return [16]byte{}, err
			}
			if err := validateSystemAttr(appID, at, systemAttrSpec{
				etype: etype, label: label, valueType: "blob", cardinality: "one",
				unique: unique, indexed: indexed,
			}); err != nil {
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
		if e = validateSystemAttr(appID, refAt, systemAttrSpec{
			etype: "$userRefreshTokens", label: "$user", valueType: "ref", cardinality: "one",
		}); e != nil {
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
	return out, nil
}

type systemAttrSpec struct {
	etype, label    string
	valueType       string
	cardinality     string
	unique, indexed bool
}

// validateSystemAttr prevents a pre-existing attr with the right identity but
// incompatible storage metadata from being adopted by an auth flow. Such an
// attr would otherwise make lookups and writes silently use the wrong
// cardinality, value type, or uniqueness/index semantics.
func validateSystemAttr(appID [16]byte, at platform.Attr, spec systemAttrSpec) error {
	if at.ID == ([16]byte{}) || at.AppID != appID || at.Etype == nil || at.Label == nil ||
		*at.Etype != spec.etype || *at.Label != spec.label || at.ValueType != spec.valueType ||
		at.Cardinality != spec.cardinality || at.IsUnique != spec.unique || at.IsIndexed != spec.indexed {
		return fmt.Errorf("authn: system attribute %s/%s has incompatible schema", spec.etype, spec.label)
	}
	return nil
}

// validateSystemAttrs is the cache admission gate. A loader must return every
// required auth id and its corresponding label map entry before the snapshot
// can be published. This keeps nil-error partial results from poisoning the
// app cache and rejects duplicate ids that would alias two system fields.
func validateSystemAttrs(a systemAttrs) error {
	ids := []struct {
		name  string
		id    [16]byte
		label string
	}{
		{"$userRefreshTokens.hashedToken", a.tokenHashedToken, "hashedToken"},
		{"$userRefreshTokens.$user", a.tokenUser, "$user"},
		{"$magicCodes.codeHash", a.magicCodeHash, "codeHash"},
		{"$magicCodes.email", a.magicCodeEmail, "email"},
		{"$users.email", a.userEmail, "email"},
		{"$users.type", a.userType, "type"},
		{"$users.id", a.userID, "id"},
	}
	if a.labels == nil {
		return errors.New("authn: system attribute labels are missing")
	}
	seen := make(map[[16]byte]string, len(ids))
	for _, item := range ids {
		if item.id == ([16]byte{}) {
			return fmt.Errorf("authn: system attribute %s is missing", item.name)
		}
		if previous, ok := seen[item.id]; ok {
			return fmt.Errorf("authn: system attributes %s and %s alias id", previous, item.name)
		}
		seen[item.id] = item.name
		if label, ok := a.labels[item.id]; !ok || label != item.label {
			return fmt.Errorf("authn: system attribute %s has missing or mismatched label", item.name)
		}
	}
	return nil
}

func (s *Service) cacheAttrsIfCurrent(appID [16]byte, version uint64, out systemAttrs) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attrVersion[appID] != version {
		return false
	}
	if s.attrByID == nil {
		s.attrByID = map[[16]byte]systemAttrs{}
	}
	s.attrByID[appID] = out
	return true
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

// emailKey normalizes the identity for auth_throttle rows. (The app scope
// lives in its own uuid column; Go-map NUL separators are invalid PG text.)
func emailKey(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

// Throttle state lives in Postgres (audit F4/M4): every node sharing the DB
// enforces ONE lockout/resend budget per (app,email). All statements below
// are single-statement atomic upserts — no read-modify-write races.

func (s *Service) recordLocked(ctx context.Context, appID [16]byte, email string) bool {
	var locked bool
	err := s.Pool.QueryRow(ctx, `
		SELECT locked_until > $3 FROM auth_throttle
		 WHERE app_id = $1 AND key = $2`,
		appID, emailKey(email), s.now()).Scan(&locked)
	return err == nil && locked
}

// noteFailure bumps the failure counter and locks on threshold in one
// statement; the CASE keeps concurrent verifiers from overshooting the
// reset-on-lockout semantics.
func (s *Service) noteFailure(ctx context.Context, appID [16]byte, email string) {
	if _, err := s.Pool.Exec(ctx, `
		INSERT INTO auth_throttle(app_id, key, fails)
		VALUES ($1, $2, 1)
		ON CONFLICT (app_id, key) DO UPDATE SET
		  fails = CASE
		    WHEN auth_throttle.fails + 1 >= $3 THEN 0
		    ELSE auth_throttle.fails + 1 END,
		  locked_until = CASE
		    WHEN auth_throttle.fails + 1 >= $3 THEN $5::timestamptz + make_interval(secs => $4)
		    ELSE auth_throttle.locked_until END`,
		appID, emailKey(email), maxMagicCodeFails, int(magicLockout.Seconds()), s.now()); err != nil {
		s.logger().Error("authn: noteFailure", "err", err)
	}
}

func (s *Service) clearFailures(ctx context.Context, appID [16]byte, email string) {
	// Keep last_sent intact: a successful verification must not erase a
	// concurrent sender's resend lease. Only reset the verification-failure
	// state owned by this path.
	if _, err := s.Pool.Exec(ctx, `
		UPDATE auth_throttle
		   SET fails = 0, locked_until = to_timestamp(0)
		 WHERE app_id = $1 AND key = $2`, appID, emailKey(email)); err != nil {
		s.logger().Error("authn: clear failures", "err", err)
	}
}

// resendBlocked atomically acquires the resend lease when no recent attempt
// owns it. A blocked attempt does not refresh last_sent, allowing the owner to
// release its exact timestamp after delivery failure without weakening the
// concurrent-attempt guard.
func (s *Service) resendBlocked(ctx context.Context, appID [16]byte, email string, now time.Time) (bool, error) {
	var acquired int
	err := s.Pool.QueryRow(ctx, `
		INSERT INTO auth_throttle(app_id, key, last_sent)
		VALUES ($1, $2, $3)
		ON CONFLICT (app_id, key) DO UPDATE SET last_sent = EXCLUDED.last_sent
		WHERE auth_throttle.last_sent <= EXCLUDED.last_sent - make_interval(secs => $4)
		RETURNING 1`,
		appID, emailKey(email), now, int(minResendInterval.Seconds())).Scan(&acquired)
	if errors.Is(err, pgx.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		s.logger().Error("authn: resend throttle", "err", err)
		return false, ErrMagicCodeThrottleUnavailable
	}
	return acquired != 1, nil
}

// releaseResendLease permits a retry when an attempt never delivered or
// persisted a code. The timestamp predicate prevents a failed older request
// from clearing the lease of a newer successful attempt.
func (s *Service) releaseResendLease(ctx context.Context, appID [16]byte, email string, attemptAt time.Time) {
	if _, err := s.Pool.Exec(ctx, `
		UPDATE auth_throttle
		   SET last_sent = to_timestamp(0)
		 WHERE app_id = $1 AND key = $2 AND last_sent = $3`,
		appID, emailKey(email), attemptAt); err != nil {
		s.logger().Error("authn: release resend lease", "err", err)
	}
}

// PruneThrottle deletes throttle rows idle beyond ttl (no sends, no active
// lockout). Called at boot and periodically; keeps auth_throttle bounded so
// the shared-state fix doesn't just relocate unbounded growth into Postgres
// (adversarial-review defect 3).
func (s *Service) PruneThrottle(ctx context.Context, ttl time.Duration) {
	_, _ = s.Pool.Exec(ctx, `
		DELETE FROM auth_throttle
		 WHERE last_sent < now() - make_interval(secs => $1)
		   AND locked_until < now()`,
		int(ttl.Seconds()))
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

// SendMagicCode creates a one-time code for email and hands it to the configured Mailer.
// HTTP contract: POST /runtime/auth/send_magic_code → {sent: true}.
func (s *Service) SendMagicCode(ctx context.Context, appID [16]byte, email string) error {
	return s.SendMagicCodeWithMailer(ctx, appID, email, s.Mailer)
}

// SendMagicCodeWithMailer creates a one-time code for email and hands it to the supplied
// mailer override without mutating the receiver's configured Mailer.
func (s *Service) SendMagicCodeWithMailer(ctx context.Context, appID [16]byte, email string, mailer Mailer) error {
	// A nil mailer cannot deliver the code. Fail before touching throttle state,
	// generating a code, or persisting a one-time credential. This prevents a
	// successful-looking response from leaving an unverifiable code behind.
	if mailer == nil {
		return ErrMagicCodeDeliveryUnavailable
	}
	email = emailKey(email)
	a, err := s.attrs(ctx, appID)
	if err != nil {
		return err
	}
	cat, err := s.catalog(ctx, appID)
	if err != nil {
		return err
	}
	if err := validateMagicCodeCatalog(appID, a, cat); err != nil {
		return err
	}
	attemptAt := s.now()
	// Resend throttle: silently honor requests inside the cooldown window
	// (the previously issued code remains valid) so clients see the same
	// contract while mail-bombing stays impossible. No enumeration signal.
	// State is shared across nodes via auth_throttle (audit F4/M4).
	blocked, err := s.resendBlocked(ctx, appID, email, attemptAt)
	if err != nil {
		return err
	}
	if blocked {
		return nil
	}
	code, err := randCode()
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		s.releaseResendLease(cleanupCtx, appID, email, attemptAt)
		return err
	}
	// Do not persist or expose a usable code until delivery succeeds. Cleanup
	// after a failed provider call is only hygiene; this ordering is the
	// security boundary against a concurrent verifier and retry race.
	if err := mailer.SendMagicCode(ctx, email, code); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		s.releaseResendLease(cleanupCtx, appID, email, attemptAt)
		return err
	}
	var codeEntity [16]byte
	_, _ = rand.Read(codeEntity[:])
	codeTriples := []triple.Triple{
		{E: codeEntity, A: a.magicCodeHash, V: HashToken(code)},
		{E: codeEntity, A: a.magicCodeEmail, V: email},
	}
	_, err = s.DB.InsertTriples(ctx, appID, cat, codeTriples, false)
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		s.releaseResendLease(cleanupCtx, appID, email, attemptAt)
		return err
	}
	return nil
}

func validateMagicCodeCatalog(appID [16]byte, a systemAttrs, cat *platform.AttrCatalog) error {
	if cat == nil || cat.AppID != appID {
		return errors.New("authn: magic-code attribute catalog has incompatible schema")
	}
	checks := []struct {
		id   [16]byte
		spec systemAttrSpec
	}{
		{a.magicCodeHash, systemAttrSpec{etype: "$magicCodes", label: "codeHash", valueType: "blob", cardinality: "one"}},
		{a.magicCodeEmail, systemAttrSpec{etype: "$magicCodes", label: "email", valueType: "blob", cardinality: "one", indexed: true}},
	}
	for _, check := range checks {
		at, ok := cat.ByID(check.id)
		if !ok {
			return errors.New("authn: magic-code attribute catalog has incompatible schema")
		}
		if err := validateSystemAttr(appID, at, check.spec); err != nil {
			return err
		}
	}
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
	return s.DB.WithTx(ctx, func(tx pgx.Tx) error {
		var tokenEntity [16]byte
		err := tx.QueryRow(ctx, `
			SELECT t.entity_id
			  FROM triples t
			  JOIN attrs a ON a.id = t.attr_id AND a.deletion_marked_at IS NULL
			 WHERE t.app_id = $1 AND t.attr_id = $2 AND t.value = $3::jsonb
			 ORDER BY t.entity_id
			 LIMIT 1
			 FOR UPDATE`,
			appID, a.tokenHashedToken, mustJSON(HashToken(rawToken))).Scan(&tokenEntity)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			DELETE FROM triples
			 WHERE app_id = $1 AND entity_id = $2`, appID, tokenEntity)
		return err
	})
}

// VerifyMagicCode ports magic-code-auth/verify!: run the signup permission
// gate BEFORE burning the code (v1 ordering guarantee), consume the code,
// upsert the $users record (guest upgrade in place), mint a fresh refresh
// token, and return {user{...,refresh_token}, created}.
func (s *Service) VerifyMagicCode(ctx context.Context, appID [16]byte,
	email, code string, guestRefreshToken string, extraFields map[string]any,
	admin bool,
) (map[string]any, error) {
	email = emailKey(email)
	// Brute-force gate: locked pairs are refused before any code check.
	if s.recordLocked(ctx, appID, email) {
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
			s.noteFailure(ctx, appID, email)
			return nil, err
		}
	} else if err := s.consumeCode(ctx, appID, a, email, code); err != nil {
		s.noteFailure(ctx, appID, email)
		return nil, err
	}
	s.clearFailures(ctx, appID, email)

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
	if err != nil {
		return nil, err
	}
	if len(rows) != 0 {
		return s.loadUser(ctx, appID, rows[0].Triple.E, a)
	}
	// Compatibility for pre-normalization users. New writes are lowercase, but
	// an existing mixed-case identity must win over creating a duplicate.
	var entity [16]byte
	err = s.Pool.QueryRow(ctx, `
		SELECT entity_id
		  FROM triples
		 WHERE app_id=$1 AND attr_id=$2
		   AND lower(value #>> '{}')=$3
		 ORDER BY created_at, entity_id
		 LIMIT 1`, appID, a.userEmail, emailKey(email)).Scan(&entity)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return s.loadUser(ctx, appID, entity, a)
}

// consumeCode ports app-user-magic-code-model/consume!: find the entity
// holding BOTH {codeHash=sha256(code)} and {email}, enforce TTL on
// triples.created_at, then burn (delete) its code triples.
//
// The claim is a single atomic DELETE ... RETURNING (audit F3): two nodes
// racing the same code cannot both observe the pre-delete state, so exactly
// one verification wins. TTL is enforced from the claimed row's created_at,
// preserving v1's delete-then-expire ordering (an expired code stays burnt).
func (s *Service) consumeCode(ctx context.Context, appID [16]byte, a systemAttrs, email, code string) error {
	hash := HashToken(code)
	ttl := s.CodeTTL
	if ttl <= 0 {
		ttl = DefaultMagicCodeTTL
	}
	var (
		claimed     [16]byte
		codeCreated time.Time
	)
	err := s.Pool.QueryRow(ctx, `
		WITH claimed AS (
		  DELETE FROM triples t
		   USING triples h
		   WHERE t.app_id = $1
		     AND h.app_id = $1
		     AND h.attr_id = $3 AND h.value = $2::jsonb
		     AND t.entity_id = h.entity_id
			     AND EXISTS (
		       SELECT 1 FROM triples e
		        WHERE e.app_id = $1 AND e.entity_id = h.entity_id
		          AND e.attr_id = $4 AND lower(e.value #>> '{}') = $5)
		     AND (t.attr_id = $3 OR t.attr_id = $4)
		  RETURNING t.entity_id, t.attr_id, t.created_at
		)
		SELECT entity_id,
		       max(created_at) FILTER (WHERE attr_id = $3) AS code_created
		  FROM claimed
		 GROUP BY entity_id
		 ORDER BY code_created DESC
		 LIMIT 1`,
		appID, mustJSON(hash), a.magicCodeHash, a.magicCodeEmail, emailKey(email),
	).Scan(&claimed, &codeCreated)
	if err == pgx.ErrNoRows {
		return ErrInvalidCode
	}
	if err != nil {
		return err
	}
	if !s.now().Before(codeCreated.Add(ttl)) {
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
