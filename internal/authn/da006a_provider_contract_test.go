package authn

import (
	"context"
	"testing"
)

func TestDA006AProviderResolutionRejectsUnconfiguredAndUnselectedProviders(t *testing.T) {
	for _, name := range []string{"google", "github"} {
		t.Run(name+" empty credentials", func(t *testing.T) {
			if _, err := (&Service{}).resolveProvider(context.Background(), [16]byte{}, name); err == nil {
				t.Fatal("empty provider credentials were accepted")
			}
		})
	}
	for _, name := range []string{"apple", "custom", "test"} {
		t.Run(name+" unsupported", func(t *testing.T) {
			if _, err := (&Service{Providers: map[string]*ResolvedProvider{
				name: {ClientID: "client", ClientSecret: "secret"},
			}}).resolveProvider(context.Background(), [16]byte{}, name); err == nil {
				t.Fatalf("provider %q was accepted", name)
			}
		})
	}
}

func TestDA006APKCECannotBeDisabled(t *testing.T) {
	for _, tc := range []struct{ method, challenge, verifier string }{
		{method: "plain"},
		{method: "S256"},
		{method: "plain", challenge: "challenge"},
		{method: "plain", verifier: "verifier"},
	} {
		if verifyPKCE(tc.method, tc.challenge, tc.verifier) {
			t.Fatalf("verifyPKCE(%q, %q, %q) accepted an incomplete binding", tc.method, tc.challenge, tc.verifier)
		}
	}
}
