package authn_test

import (
	"context"
	"errors"
	"testing"

	"github.com/instant-v2/instant-v2/internal/authn"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
)

func TestSignOutRetractsTokenEntityAndDeniesReplay(t *testing.T) {
	svc, _, appID, cleanup := env(t)
	defer cleanup()
	ctx := context.Background()

	first, err := svc.SignInGuest(ctx, appID, nil)
	if err != nil {
		t.Fatal(err)
	}
	firstUser, ok := first["user"].(map[string]any)
	if !ok {
		t.Fatalf("first sign-in user = %#v", first["user"])
	}
	firstToken, ok := firstUser["refresh_token"].(string)
	if !ok || firstToken == "" {
		t.Fatalf("first sign-in refresh token = %#v", first["refresh_token"])
	}
	firstEntity, err := platform.ScanUUIDErr(firstToken)
	if err != nil {
		t.Fatalf("first refresh token entity: %v", err)
	}

	second, err := svc.SignInGuest(ctx, appID, nil)
	if err != nil {
		t.Fatal(err)
	}
	secondUser, ok := second["user"].(map[string]any)
	if !ok {
		t.Fatalf("second sign-in user = %#v", second["user"])
	}
	secondToken, ok := secondUser["refresh_token"].(string)
	if !ok || secondToken == "" {
		t.Fatalf("second sign-in refresh token = %#v", second["refresh_token"])
	}

	rows, err := svc.DB.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{firstEntity}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("first token entity rows before sign-out = %d, want hashed-token and user-link triples", len(rows))
	}

	if err := svc.SignOut(ctx, appID, firstToken); err != nil {
		t.Fatal(err)
	}

	rows, err = svc.DB.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{firstEntity}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("first token entity rows after sign-out = %#v, want none", rows)
	}
	if _, err := svc.VerifyRefreshToken(ctx, appID, firstToken); !errors.Is(err, authn.ErrBadToken) {
		t.Fatalf("signed-out token replay error = %v, want %v", err, authn.ErrBadToken)
	}
	if _, err := svc.VerifyRefreshToken(ctx, appID, secondToken); err != nil {
		t.Fatalf("signing out one token revoked another token: %v", err)
	}
	if err := svc.SignOut(ctx, appID, "00000000-0000-4000-8000-000000000099"); err != nil {
		t.Fatalf("unknown token sign-out = %v, want nil", err)
	}
}
