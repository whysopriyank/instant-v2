package authn_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/authn"
	"github.com/instant-v2/instant-v2/internal/platform"
)

// TestSweepExpiredAuthEntities pins the 2026-08-27 MED-1 fix: transient auth
// artifacts whose expiry had been enforced only lazily-at-consume are now
// physically deleted once TTL+grace passes — while live artifacts, users, and
// refresh tokens in the same app stay untouched.
func TestSweepExpiredAuthEntities(t *testing.T) {
	svc, _, appID, cleanup := env(t)
	defer cleanup()
	svc.Mailer = &deliveryMailer{}
	ctx := context.Background()

	// A fresh app has nothing to sweep; the CTE chain must no-op cleanly.
	if n, err := svc.SweepExpiredAuthEntities(ctx); err != nil || n != 0 {
		t.Fatalf("fresh-app sweep: got (%d triples, %v)", n, err)
	}

	// 1. Magic code issued now → {codeHash,email} entity lives immediately
	// (attrs are created lazily on first send; the sweep path must tolerate
	// apps where the etype does not exist yet).
	if err := svc.SendMagicCode(ctx, appID, "sweep-expired@test.example"); err != nil {
		t.Fatalf("send magic code: %v", err)
	}
	if got := countEtypeRows(t, svc, appID, "$magicCodes"); got != 2 {
		t.Fatalf("pre-backdate magic triples = %d, want 2", got)
	}

	// 2. Guest user acts as collateral guard: non-artifact etypes sharing the
	// same app/timestamps must survive the sweeper untouched.
	if _, err := svc.SignInGuest(ctx, appID, map[string]any{}); err != nil {
		t.Fatalf("sign-in guest: %v", err)
	}
	usersPre := countEtypeRows(t, svc, appID, "$users")
	tokensPre := countEtypeRows(t, svc, appID, "$userRefreshTokens")
	if tokensPre < 2 {
		t.Fatalf("refresh-token triples = %d, want >= 2", tokensPre)
	}

	// 3. Simulate expiry-by-age (the historical bug: this row would otherwise
	// persist forever since no client ever consumes it).
	backdateEtype(t, svc, appID, "$magicCodes", 30*time.Hour)

	// 4. Fresh code afterwards must survive; distinct email sidesteps the
	// resend cooldown, which silently skips inserts inside 60s windows.
	if err := svc.SendMagicCode(ctx, appID, "sweep-fresh@test.example"); err != nil {
		t.Fatalf("send second magic code: %v", err)
	}

	// 5. Synthetic OAuth redirect pair through the real attr machinery: one
	// beyond oauthStateTTL+grace, one fresh.
	mkRedirect := func(state string, createdAt time.Time) {
		var stateAttr, cookieAttr [16]byte
		err := svc.DB.WithTx(ctx, func(tx pgx.Tx) error {
			a1, err := platform.GetOrCreateAttr(ctx, tx, appID, "$oauthRedirects", "state", "blob", "one", true, true)
			if err != nil {
				return err
			}
			stateAttr = a1.ID
			a2, err := platform.GetOrCreateAttr(ctx, tx, appID, "$oauthRedirects", "cookieHash", "blob", "one", false, false)
			if err != nil {
				return err
			}
			cookieAttr = a2.ID
			return nil
		})
		if err != nil {
			t.Fatalf("provision $oauthRedirects attrs: %v", err)
		}
		// Mirror oaAttrs' post-provision invalidation so catalog lookups see
		// the freshly minted attr ids.
		svc.Catalogs.Invalidate(platform.UUIDToStr(appID))
		insertRawTriple(t, svc, appID, newUUID(), stateAttr, `"`+state+`"`, createdAt)
		insertRawTriple(t, svc, appID, newUUID(), cookieAttr, `"cookie-hash-`+state+`"`, createdAt)
	}
	mkRedirect("old-state-dead", time.Now().Add(-2*time.Hour)) // 10m TTL + 1h grace ≪ 2h
	mkRedirect("new-state-live", time.Now())
	redirectsPre := countEtypeRows(t, svc, appID, "$oauthRedirects")
	if redirectsPre != 4 {
		t.Fatalf("$oauthRedirects triples pre-sweep = %d, want 4", redirectsPre)
	}

	swept, err := svc.SweepExpiredAuthEntities(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	// Minimum: 2 expired magic triples + 2 dead redirect triples.
	if swept < 4 {
		t.Fatalf("swept %d triple rows, want >= 4", swept)
	}

	if got := countEtypeRows(t, svc, appID, "$magicCodes"); got != 2 {
		t.Fatalf("post-sweep magic triples = %d, want 2 (the fresh pair)", got)
	}
	if got := countEtypeRows(t, svc, appID, "$oauthRedirects"); got != 2 {
		t.Fatalf("post-sweep $oauthRedirects triples = %d, want 2 (the fresh pair)", got)
	}
	if got := countEtypeRows(t, svc, appID, "$oauthCodes"); got != 0 {
		t.Fatalf("post-sweep $oauthCodes triples = %d, want 0", got)
	}
	if got := countEtypeRows(t, svc, appID, "$users"); got != usersPre {
		t.Fatalf("user triples mutated: pre=%d post=%d", usersPre, got)
	}
	if got := countEtypeRows(t, svc, appID, "$userRefreshTokens"); got != tokensPre {
		t.Fatalf("refresh-token triples mutated: pre=%d post=%d", tokensPre, got)
	}
}

func countEtypeRows(t *testing.T, svc *authn.Service, appID [16]byte, etype string) int64 {
	t.Helper()
	var n int64
	err := svc.Pool.QueryRow(context.Background(), `
		SELECT count(*)
		  FROM triples t JOIN attrs a ON a.id = t.attr_id
		 WHERE t.app_id = $1 AND a.etype = $2`,
		appID, etype).Scan(&n)
	if err != nil {
		t.Fatalf("count %s: %v", etype, err)
	}
	return n
}

func backdateEtype(t *testing.T, svc *authn.Service, appID [16]byte, etype string, age time.Duration) {
	t.Helper()
	if _, err := svc.Pool.Exec(context.Background(), `
		UPDATE triples t SET created_at = now() - make_interval(secs => $2::int)
		 WHERE t.app_id = $1
		   AND t.attr_id IN (SELECT id FROM attrs WHERE app_id = $1 AND etype = $3)`,
		appID, int(age.Seconds()), etype); err != nil {
		t.Fatalf("backdate %s: %v", etype, err)
	}
}

// insertRawTriple bypasses InsertTriples because backdating requires setting
// created_at explicitly — exactly what makes this a valid expiry simulation.
// Flag columns derive through platform.FlagsFor so fixtures satisfy the same
// physical constraints (ref_values_are_uuid, av_ignore_nulls_index) real
// writes do.
func insertRawTriple(t *testing.T, svc *authn.Service, appID [16]byte, eid [16]byte, attrID [16]byte, valueJSON string, createdAt time.Time) {
	t.Helper()
	ctx := context.Background()
	cat, err := svc.Catalogs.For(ctx, platform.UUIDToStr(appID))
	if err != nil {
		t.Fatalf("catalog load: %v", err)
	}
	at, ok := cat.ByID(attrID)
	if !ok {
		t.Fatalf("attr %s absent from catalog", platform.UUIDToStr(attrID))
	}
	f := platform.FlagsFor(at)
	if _, err := svc.Pool.Exec(ctx, `
		INSERT INTO triples(app_id, entity_id, attr_id, value, value_md5,
		                    ea, eav, av, ave, vae, created_at)
		VALUES ($1, $2, $3, $4::jsonb, md5($4::text),
		        $6, $7, $8, $9, $10, $5)`,
		appID, eid, attrID, valueJSON, createdAt,
		f.EA, f.EAV, f.AV, f.AVE, f.VAE); err != nil {
		t.Fatalf("insert raw triple: %v", err)
	}
}
