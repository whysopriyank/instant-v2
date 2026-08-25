package authn_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/authn"
	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
)

// Audit M1/F2b: the OAuth/id_token signup path (VerifyMagicCodeTrusted)
// must enforce the same $users.create gate as the magic-code path.
func TestOAuthSignupGateParity(t *testing.T) {
	svc, _, appID, cleanup := env(t)
	defer cleanup()

	doc, err := perms.ParseRuleDoc([]byte(`{"$users":{"allow":{"create":"false"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	svc.RulesForFn = func(context.Context, [16]byte) (*perms.RuleDoc, error) { return doc, nil }

	if _, err := svc.VerifyMagicCodeTrusted(context.Background(), appID, "oauth-sneak@test", nil); err == nil {
		t.Fatal("OAuth-path account creation must respect $users.create=false")
	}
}

// Audit F5/M3: a magic code must be single-use even under concurrent
// verification. The SELECT-then-DELETE burn allowed both racers to win.
func TestMagicCodeAtomicBurn(t *testing.T) {
	svc, _, appID, cleanup := env(t)
	defer cleanup()
	ctx := context.Background()

	for round := 0; round < 8; round++ {
		email := fmt.Sprintf("race%d@test", time.Now().UnixNano())
		mailer := &captureMailer{}
		svc.Mailer = mailer
		svc.NowFunc = func() time.Time { return time.Now().Add(time.Duration(round) * time.Minute) }
		if err := svc.SendMagicCode(ctx, appID, email); err != nil {
			t.Fatalf("round %d: send: %v", round, err)
		}

		var mu sync.Mutex
		var wins int
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, err := svc.VerifyMagicCode(ctx, appID, email, mailer.code, "", nil, false)
				if err == nil {
					mu.Lock()
					wins++
					mu.Unlock()
				}
			}()
		}
		close(start)
		wg.Wait()
		if wins > 1 {
			t.Fatalf("round %d: %d concurrent verifications consumed one code", round, wins)
		}
	}
}

// Audit F4/M4: brute-force lockout state must live in shared storage so N
// nodes enforce ONE budget per (app,email), not N.
func TestLockoutSharedAcrossNodes(t *testing.T) {
	svc, h, appID, cleanup := env(t)
	defer cleanup()
	appStr := platform.UUIDToStr(appID)

	nodeB := &authn.Service{Pool: svc.Pool, DB: svc.DB, Catalogs: svc.Catalogs}

	// Drive node A into lockout with wrong codes.
	for i := 0; i < 5; i++ {
		_, _ = post(t, h, "/runtime/auth/verify_magic_code",
			map[string]any{"email": "victim@test", "code": "000000", "app-id": appStr})
	}

	// Node B (fresh in-process state, same DB) must see the lockout too.
	_, err := nodeB.VerifyMagicCode(context.Background(), appID, "victim@test", "123456", "", nil, false)
	if !errors.Is(err, authn.ErrLocked) {
		t.Fatalf("lockout must be visible to other nodes sharing the DB, got: %v", err)
	}
}
