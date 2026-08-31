package benchharness

import (
	"testing"
	"time"
)

func TestTargetConfigReadinessTimeoutUsesCanonicalTwentySecondCap(t *testing.T) {
	config := TargetConfig{
		ID: "target", Kind: TargetV1, Transport: TransportWebSocket,
		SessionURL: "ws://127.0.0.1:80/runtime/session", HealthURL: "http://127.0.0.1:80/health",
		AppID: "00000000-0000-0000-0000-000000000001", Revision: "rev-1",
		DatabaseName: "instant_bench_test", PostgresVersion: "16", InvalidationMode: "logical", OutputPlugin: "wal2json",
	}
	config.Probe.Timeout = 30 * time.Second
	if err := validateTargetConfig(config); err == nil {
		t.Fatal("readiness timeout above canonical 20 seconds was accepted")
	}
	config.Probe.Timeout = 20 * time.Second
	if err := validateTargetConfig(config); err != nil {
		t.Fatalf("canonical 20-second readiness timeout was rejected: %v", err)
	}
	config.Probe.Timeout = 0
	if err := validateTargetConfig(config); err != nil {
		t.Fatalf("zero readiness timeout (default fallback) was rejected: %v", err)
	}
}
