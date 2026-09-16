package authn

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestProviderExchangeUsesOneSharedContextBudget(t *testing.T) {
	var calls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/token" {
			time.Sleep(75 * time.Millisecond)
			_, _ = w.Write([]byte(`{"access_token":"access"}`))
			return
		}
		<-r.Context().Done()
	}))
	defer provider.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := exchangeUserInfo(ctx, &ResolvedProvider{
		ClientID: "client", ClientSecret: "secret",
		TokenURL: provider.URL + "/token", UserInfo: provider.URL + "/userinfo",
	}, "code", "http://app/cb")
	if err == nil {
		t.Fatal("provider exchange outlived its shared context")
	}
	if elapsed := time.Since(started); elapsed < 125*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Fatalf("provider exchange elapsed %v, want one approximately 150ms budget", elapsed)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("provider requests = %d, want token and userinfo", got)
	}
}
