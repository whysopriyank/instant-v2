package benchharness

import (
	"context"
	"testing"
	"time"
)

func TestTargetSessionReconnectReplaysQueriesAndRetainsRawEvidence(t *testing.T) {
	dialer := &scriptedDialer{}
	driver, err := NewTargetDriver(TargetConfig{
		ID: "v2", Kind: TargetV2, Transport: TransportWebSocket,
		SessionURL: "ws://127.0.0.1:1/runtime/session", HealthURL: "http://127.0.0.1:1/health",
		AppID: "00000000-0000-0000-0000-000000000001", Revision: "rev", DatabaseName: "instant_bench_probe", PostgresVersion: "17.11", InvalidationMode: "post-commit",
		Provisioner: func(context.Context) error { return nil }, MetadataProbe: func(context.Context) (TargetMetadata, error) {
			return TargetMetadata{Revision: "rev", DatabaseName: "instant_bench_probe", PostgresVersion: "17.11", InvalidationMode: "post-commit"}, nil
		},
		Dialer: dialer, QueryBuilder: func(Query) (any, error) { return map[string]any{"todos": map[string]any{}}, nil }, TransactionBuilder: func(Mutation) ([]any, error) { return nil, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	session, err := driver.OpenSession(context.Background(), "reconnect-client")
	if err != nil {
		t.Fatal(err)
	}
	query := Query{ID: "q-reconnect", MatchAll: true}
	if _, err := session.Subscribe(context.Background(), query); err != nil {
		t.Fatal(err)
	}
	if got := len(session.RawFrames()); got < 3 {
		t.Fatalf("raw handshake/query evidence missing: %d", got)
	}
	if err := session.Reconnect(context.Background()); err != nil {
		t.Fatal(err)
	}
	dialer.mu.Lock()
	sessions := len(dialer.sessions)
	dialer.mu.Unlock()
	if sessions != 2 {
		t.Fatalf("reconnect did not create replacement transport: %d", sessions)
	}
	if got := len(session.RawFrames()); got < 6 {
		t.Fatalf("reconnect evidence/query replay missing: %d", got)
	}
	_ = session.Close()
}

func TestTargetSessionPauseReadsHonorsDeclaredDuration(t *testing.T) {
	s := &TargetSession{}
	s.PauseReads(25 * time.Millisecond)
	started := time.Now()
	if err := s.waitRead(context.Background()); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 20*time.Millisecond {
		t.Fatalf("pause ended before declared duration: %v", elapsed)
	}
}
