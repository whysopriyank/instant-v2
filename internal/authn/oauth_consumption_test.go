package authn_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/authn"
	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/triple"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestQualityOAuthConsumptionExpiry(t *testing.T) {
	svc, _, appID, cleanup := env(t)
	defer cleanup()
	ctx := context.Background()
	var calls atomic.Int32
	provider := oauthTestProvider(t, &calls, nil)
	svc.Providers = map[string]*authn.ResolvedProvider{"github": provider}
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, kind := range []string{"callback", "token"} {
		for _, boundary := range []struct {
			name  string
			age   time.Duration
			valid bool
		}{
			{"before", 10*time.Minute - time.Microsecond, true},
			{"at", 10 * time.Minute, false},
			{"after", 10*time.Minute + time.Microsecond, false},
		} {
			t.Run(kind+"/"+boundary.name, func(t *testing.T) {
				name := kind + boundary.name
				cookie, eid := oauthStartRecord(t, svc, appID, name)
				if kind == "token" {
					eid = oauthCodeFixture(t, svc, appID, name)
				}
				if _, err := svc.Pool.Exec(ctx, `UPDATE triples SET created_at=$3 WHERE app_id=$1 AND entity_id=$2`, appID, eid, created); err != nil {
					t.Fatal(err)
				}
				svc.NowFunc = func() time.Time { return created.Add(boundary.age) }
				before := oauthEntitySnapshot(t, svc, appID, eid)
				callsBefore := calls.Load()
				var err error
				if kind == "callback" {
					_, err = svc.OAuthCallback(ctx, appID, name, cookie, "provider-code")
				} else {
					_, err = svc.OAuthToken(ctx, appID, name, "verifier")
				}
				if boundary.valid {
					if err != nil {
						t.Fatalf("unexpired %s: %v", kind, err)
					}
					return
				}
				wantErr := authn.ErrOAuthCode
				if kind == "callback" {
					wantErr = authn.ErrOAuthState
				}
				if !errors.Is(err, wantErr) {
					t.Errorf("expired %s must be rejected: %v", kind, err)
				}
				if calls.Load() != callsBefore {
					t.Error("expired callback reached provider")
				}
				if after := oauthEntitySnapshot(t, svc, appID, eid); !reflect.DeepEqual(before, after) {
					t.Error("expired record changed")
				}
			})
		}
	}
}

func TestQualityOAuthConsumptionBindingsAndRetry(t *testing.T) {
	svc, _, appID, cleanup := env(t)
	defer cleanup()
	ctx := context.Background()
	var calls atomic.Int32
	var fail atomic.Bool
	svc.Providers = map[string]*authn.ResolvedProvider{"github": oauthTestProvider(t, &calls, &fail)}
	cookie, eid := oauthStartRecord(t, svc, appID, "bindings")
	before := oauthEntitySnapshot(t, svc, appID, eid)
	for _, tc := range []struct {
		name, state, cookie string
		app                 [16]byte
	}{
		{"cookie", "bindings", "wrong", appID},
		{"state", "wrong", cookie, appID},
		{"app", "bindings", cookie, newUUID()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := svc.OAuthCallback(ctx, tc.app, tc.state, tc.cookie, "provider-code"); err == nil {
				t.Fatal("invalid binding accepted")
			}
			if calls.Load() != 0 {
				t.Error("invalid binding reached provider")
			}
			if !reflect.DeepEqual(before, oauthEntitySnapshot(t, svc, appID, eid)) {
				t.Error("invalid binding changed state")
			}
		})
	}
	if _, err := svc.Pool.Exec(ctx, `UPDATE apps SET redirect_origins='[]'::jsonb WHERE id=$1`, appID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.OAuthCallback(ctx, appID, "bindings", cookie, "provider-code"); err == nil {
		t.Fatal("revoked redirect accepted")
	}
	if calls.Load() != 0 || !reflect.DeepEqual(before, oauthEntitySnapshot(t, svc, appID, eid)) {
		t.Fatal("revoked redirect reached provider or changed state")
	}
	if _, err := svc.Pool.Exec(ctx, `UPDATE apps SET redirect_origins='["http://app"]'::jsonb WHERE id=$1`, appID); err != nil {
		t.Fatal(err)
	}
	fail.Store(true)
	if _, err := svc.OAuthCallback(ctx, appID, "bindings", cookie, "provider-code"); err == nil {
		t.Fatal("provider failure accepted")
	}
	if got := len(oauthEntitySnapshot(t, svc, appID, eid)); got != 0 {
		t.Fatalf("provider failure must burn local state; %d triples remain", got)
	}
	fail.Store(false)
	if _, err := svc.OAuthCallback(ctx, appID, "bindings", cookie, "provider-code"); !errors.Is(err, authn.ErrOAuthState) {
		t.Fatalf("retry after provider failure must require restart: %v", err)
	}
	if _, err := svc.OAuthCallback(ctx, appID, "bindings", cookie, "provider-code"); !errors.Is(err, authn.ErrOAuthState) {
		t.Fatalf("callback replay: %v", err)
	}
}

func TestQualityOAuthConsumptionConcurrent(t *testing.T) {
	for _, kind := range []string{"callback", "token"} {
		t.Run(kind, func(t *testing.T) {
			svc, _, appID, cleanup := env(t)
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			var calls atomic.Int32
			svc.Providers = map[string]*authn.ResolvedProvider{"github": oauthTestProvider(t, &calls, nil)}
			cookie, eid := oauthStartRecord(t, svc, appID, "concurrent")
			if kind == "token" {
				eid = oauthCodeFixture(t, svc, appID, "concurrent")
				// Existing user avoids an unrelated concurrent-signup uniqueness race.
				if _, err := svc.VerifyMagicCodeTrusted(ctx, appID, "oauth@example.com", nil); err != nil {
					t.Fatal(err)
				}
			}
			tokenCount := func() int {
				cat, err := svc.Catalogs.For(ctx, platform.UUIDToStr(appID))
				if err != nil {
					t.Fatal(err)
				}
				attr := cat.FindByEtypeLabel("$userRefreshTokens", "hashedToken")
				if attr == nil {
					return 0
				}
				rows, err := svc.DB.FetchTriples(ctx, appID, storage.FetchFilter{AttrIDs: [][16]byte{attr.ID}})
				if err != nil {
					t.Fatal(err)
				}
				return len(rows)
			}
			tokensBefore := tokenCount()
			blocker, err := svc.Pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = blocker.Rollback(context.Background()) }()
			// SHARE permits reads and row locks, but blocks deletion. Both real
			// requests must reach a database lock before releasing the barrier.
			if _, err := blocker.Exec(ctx, `LOCK TABLE triples IN SHARE MODE`); err != nil {
				t.Fatal(err)
			}
			type outcome struct {
				target string
				result map[string]any
				err    error
			}
			results := make(chan outcome, 2)
			for range 2 {
				reader := &authn.Service{DB: svc.DB, Pool: svc.Pool, Catalogs: svc.Catalogs, Providers: svc.Providers}
				go func() {
					var out outcome
					if kind == "callback" {
						out.target, out.err = reader.OAuthCallback(ctx, appID, "concurrent", cookie, "provider-code")
					} else {
						out.result, out.err = reader.OAuthToken(ctx, appID, "concurrent", "verifier")
					}
					results <- out
				}()
			}
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				var waiting int
				if err := svc.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND cardinality(pg_blocking_pids(pid)) > 0`).Scan(&waiting); err != nil {
					t.Fatal(err)
				}
				if waiting == 2 {
					break
				}
				select {
				case <-ticker.C:
				case <-ctx.Done():
					t.Fatal("both OAuth requests did not reach persistence barrier")
				}
			}
			if err := blocker.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			var winners []outcome
			for range 2 {
				select {
				case out := <-results:
					if out.err == nil {
						winners = append(winners, out)
					} else {
						want := authn.ErrOAuthCode
						if kind == "callback" {
							want = authn.ErrOAuthState
						}
						if !errors.Is(out.err, want) {
							t.Errorf("loser error: %v", out.err)
						}
					}
				case <-ctx.Done():
					t.Fatal("OAuth requests did not finish")
				}
			}
			if len(winners) != 1 {
				t.Fatalf("persisted single-use record produced %d winners, want exactly one", len(winners))
			}
			winner := winners[0]
			if kind == "callback" {
				if calls.Load() != 2 {
					t.Errorf("single callback must make one token and one userinfo request, got %d", calls.Load())
				}
				if len(oauthEntitySnapshot(t, svc, appID, eid)) != 0 {
					t.Error("consumed redirect remains")
				}
				u, _ := url.Parse(winner.target)
				winner.result, err = svc.OAuthToken(ctx, appID, u.Query().Get("code"), "verifier")
				if err != nil {
					t.Fatal(err)
				}
			} else {
				cat, err := svc.Catalogs.For(ctx, platform.UUIDToStr(appID))
				if err != nil {
					t.Fatal(err)
				}
				if _, exists := oauthEntitySnapshot(t, svc, appID, eid)[cat.FindByEtypeLabel("$oauthCodes", "codeHash").ID]; exists {
					t.Error("consumed code key remains")
				}
			}
			oauthAssertIdentity(t, svc, appID, winner.result)
			if minted := tokenCount() - tokensBefore; minted != 1 {
				t.Errorf("concurrent consumption persisted %d new refresh tokens, want exactly one", minted)
			}
			// A new facade reads the committed burn, independent of caller memory.
			reader := &authn.Service{DB: svc.DB, Pool: svc.Pool, Catalogs: platform.NewCatalogCache(svc.Pool, svc.Pool), Providers: svc.Providers}
			if kind == "callback" {
				_, err = reader.OAuthCallback(ctx, appID, "concurrent", cookie, "provider-code")
			} else {
				_, err = reader.OAuthToken(ctx, appID, "concurrent", "verifier")
			}
			if err == nil {
				t.Error("reopened service accepted consumed record")
			}
		})
	}
}

func TestQualityOAuthConsumptionRollback(t *testing.T) {
	svc, _, appID, cleanup := env(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var calls atomic.Int32
	svc.Providers = map[string]*authn.ResolvedProvider{"github": oauthTestProvider(t, &calls, nil)}
	cookie, eid := oauthStartRecord(t, svc, appID, "rollback")
	cat, err := svc.Catalogs.For(ctx, platform.UUIDToStr(appID))
	if err != nil {
		t.Fatal(err)
	}
	codeAttr := cat.FindByEtypeLabel("$oauthCodes", "codeHash").ID
	// The trigger lives only in this testkit-owned database and fails the
	// actual code insert, after redirect deletion has been attempted.
	if _, err := svc.Pool.Exec(ctx, `CREATE FUNCTION fail_oauth_code() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
		IF NEW.attr_id=TG_ARGV[0]::uuid THEN RAISE EXCEPTION 'oauth fixture insert failure'; END IF;
		RETURN NEW; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Pool.Exec(ctx, fmt.Sprintf(`CREATE TRIGGER fail_oauth_code BEFORE INSERT ON triples FOR EACH ROW EXECUTE FUNCTION fail_oauth_code('%s')`, platform.UUIDToStr(codeAttr))); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.OAuthCallback(ctx, appID, "rollback", cookie, "provider-code"); err == nil {
		t.Fatal("code persistence failure accepted")
	}
	if got := len(oauthEntitySnapshot(t, svc, appID, eid)); got != 0 {
		t.Fatalf("code insert failure must retain burned redirect state; %d triples remain", got)
	}
	rows, err := svc.DB.FetchTriples(ctx, appID, storage.FetchFilter{AttrIDs: [][16]byte{codeAttr}})
	if err != nil || len(rows) != 0 {
		t.Fatalf("failed callback persisted codes: %v %v", rows, err)
	}
	if _, err := svc.Pool.Exec(ctx, `DROP TRIGGER fail_oauth_code ON triples`); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.OAuthCallback(ctx, appID, "rollback", cookie, "fresh-provider-code"); !errors.Is(err, authn.ErrOAuthState) {
		t.Fatalf("code persistence failure must require restart: %v", err)
	}
	_, freshCookie, err := svc.OAuthStart(ctx, authn.OAuthStartParams{
		AppID: appID, ClientName: "github", RedirectURI: "http://app/cb",
		State: "rollback-issuance", CodeChallenge: "verifier", CodeChallengeMethod: "plain",
	})
	if err != nil {
		t.Fatal(err)
	}
	target, err := svc.OAuthCallback(ctx, appID, "rollback-issuance", freshCookie, "fresh-provider-code")
	if err != nil {
		t.Fatalf("fresh callback: %v", err)
	}
	u, _ := url.Parse(target)
	code := u.Query().Get("code")
	rows, err = svc.DB.FetchTriples(ctx, appID, storage.FetchFilter{AttrIDs: [][16]byte{codeAttr}, Value: authn.HashToken(code)})
	if err != nil || len(rows) != 1 {
		t.Fatalf("issued code lookup: %v %v", rows, err)
	}
	codeEntity := rows[0].Triple.E
	codeBefore := oauthEntitySnapshot(t, svc, appID, codeEntity)
	delete(codeBefore, codeAttr)
	svc.RulesForFn = func(context.Context, [16]byte) (*perms.RuleDoc, error) {
		return nil, errors.New("fixture rules unavailable")
	}
	if _, err := svc.OAuthToken(ctx, appID, code, "verifier"); err == nil {
		t.Fatal("issuance failure accepted")
	}
	if !reflect.DeepEqual(codeBefore, oauthEntitySnapshot(t, svc, appID, codeEntity)) {
		t.Fatal("issuance failure must burn only the code key")
	}
	svc.RulesForFn = nil
	if _, err := svc.OAuthToken(ctx, appID, code, "verifier"); !errors.Is(err, authn.ErrOAuthCode) {
		t.Fatalf("issuance failure must not restore code: %v", err)
	}
}

func TestQualityOAuthConsumptionExpiresDuringExchange(t *testing.T) {
	svc, _, appID, cleanup := env(t)
	defer cleanup()
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var clock atomic.Int64
	clock.Store(created.Add(time.Minute).UnixNano())
	svc.NowFunc = func() time.Time { return time.Unix(0, clock.Load()) }
	fixture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			_, _ = w.Write([]byte(`{"access_token":"test-token"}`))
			return
		}
		clock.Store(created.Add(10 * time.Minute).UnixNano())
		_, _ = w.Write([]byte(`{"email":"oauth@example.com"}`))
	}))
	defer fixture.Close()
	svc.Providers = map[string]*authn.ResolvedProvider{"github": {ClientID: "client", ClientSecret: "secret", TokenURL: fixture.URL + "/token", UserInfo: fixture.URL + "/userinfo"}}
	cookie, eid := oauthStartRecord(t, svc, appID, "exchange-expiry")
	if _, err := svc.Pool.Exec(context.Background(), `UPDATE triples SET created_at=$3 WHERE app_id=$1 AND entity_id=$2`, appID, eid, created); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.OAuthCallback(context.Background(), appID, "exchange-expiry", cookie, "code"); err != nil {
		t.Fatalf("state valid at claim must complete despite provider crossing TTL: %v", err)
	}
	if len(oauthEntitySnapshot(t, svc, appID, eid)) != 0 {
		t.Fatal("state remained after successful claim")
	}
}

func TestQualityOAuthConsumptionSingleConnection(t *testing.T) {
	svc, _, appID, cleanup := env(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := svc.Pool.Config()
	cfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	svc = &authn.Service{DB: storage.New(pool), Pool: pool, Catalogs: platform.NewCatalogCache(pool, pool)}
	var calls atomic.Int32
	svc.Providers = map[string]*authn.ResolvedProvider{"github": oauthTestProvider(t, &calls, nil)}
	cookie, _ := oauthStartRecord(t, svc, appID, "one-connection")
	target, err := svc.OAuthCallback(ctx, appID, "one-connection", cookie, "code")
	if err != nil {
		t.Fatalf("callback must not reacquire pool while holding tx: %v", err)
	}
	u, _ := url.Parse(target)
	result, err := svc.OAuthToken(ctx, appID, u.Query().Get("code"), "verifier")
	if err != nil {
		t.Fatalf("token must not reacquire pool while holding tx: %v", err)
	}
	oauthAssertIdentity(t, svc, appID, result)
}

func oauthTestProvider(t *testing.T, calls *atomic.Int32, fail *atomic.Bool) *authn.ResolvedProvider {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if fail != nil && fail.Load() {
			http.Error(w, `{"error":"unavailable"}`, http.StatusServiceUnavailable)
			return
		}
		if r.URL.Path == "/token" {
			_, _ = w.Write([]byte(`{"access_token":"test-token"}`))
			return
		}
		_, _ = w.Write([]byte(`{"email":"oauth@example.com","sub":"subject-1"}`))
	}))
	t.Cleanup(server.Close)
	return &authn.ResolvedProvider{ClientID: "client", ClientSecret: "secret", TokenURL: server.URL + "/token", UserInfo: server.URL + "/userinfo"}
}

func oauthStartRecord(t *testing.T, svc *authn.Service, appID [16]byte, state string) (string, [16]byte) {
	t.Helper()
	_, cookie, err := svc.OAuthStart(context.Background(), authn.OAuthStartParams{
		AppID: appID, ClientName: "github", RedirectURI: "http://app/cb", State: state,
		CodeChallenge: "verifier", CodeChallengeMethod: "plain",
	})
	if err != nil {
		t.Fatal(err)
	}
	cat, err := svc.Catalogs.For(context.Background(), platform.UUIDToStr(appID))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := svc.DB.FetchTriples(context.Background(), appID, storage.FetchFilter{AttrIDs: [][16]byte{cat.FindByEtypeLabel("$oauthRedirects", "state").ID}, Value: state})
	if err != nil || len(rows) != 1 {
		t.Fatalf("state fixture: %v %v", rows, err)
	}
	return cookie, rows[0].Triple.E
}

func oauthCodeFixture(t *testing.T, svc *authn.Service, appID [16]byte, code string) [16]byte {
	t.Helper()
	cat, err := svc.Catalogs.For(context.Background(), platform.UUIDToStr(appID))
	if err != nil {
		t.Fatal(err)
	}
	eid := newUUID()
	var ts []triple.Triple
	for field, value := range map[string]string{"codeHash": authn.HashToken(code), "codeChallenge": "verifier", "codeChallengeMethod": "plain", "userInfo": `{"email":"oauth@example.com","sub":"subject-1"}`} {
		ts = append(ts, triple.Triple{E: eid, A: cat.FindByEtypeLabel("$oauthCodes", field).ID, V: value})
	}
	if _, err := svc.DB.InsertTriples(context.Background(), appID, cat, ts, false); err != nil {
		t.Fatal(err)
	}
	return eid
}

func oauthAssertIdentity(t *testing.T, svc *authn.Service, appID [16]byte, result map[string]any) {
	t.Helper()
	user, ok := result["user"].(map[string]any)
	if !ok || user["email"] != "oauth@example.com" || user["id"] == "" {
		t.Fatalf("wrong issued identity: %v", result)
	}
	token, ok := user["refresh_token"].(string)
	if !ok || token == "" {
		t.Fatal("missing refresh token")
	}
	verified, err := svc.VerifyRefreshToken(context.Background(), appID, token)
	if err != nil || verified.ID != user["id"] || verified.Email != "oauth@example.com" {
		t.Fatalf("persisted token identity: %+v %v", verified, err)
	}
}
