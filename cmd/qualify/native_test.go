package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWriteNativeResultFailClosed(t *testing.T) {
	old := isNativePlatform
	isNativePlatform = func() bool { return true }
	defer func() { isNativePlatform = old }()

	dir := t.TempDir()
	// GREEN: full platform pass, no skips, linux-native pinned.
	if code := writeNativeResult(filepath.Join(dir, "pass.json"), fullPlatformPass(), time.Now().UTC(), time.Now().UTC(), nil); code != 0 {
		t.Fatal("platform GREEN + native must PASS")
	}
	// Any skip → FAIL (gate requires skipped_count == 0).
	skipped := append(fullPlatformPass(), testOutcome{Package: "example/a", Test: "TestUnrelated", Action: "skip"})
	if code := writeNativeResult(filepath.Join(dir, "skip.json"), skipped, time.Now().UTC(), time.Now().UTC(), nil); code == 0 {
		t.Fatal("any skip must FAIL the native lane")
	}
	// Any fail → FAIL.
	failed := fullPlatformPass()
	failed[0].Action = "fail"
	if code := writeNativeResult(filepath.Join(dir, "fail.json"), failed, time.Now().UTC(), time.Now().UTC(), nil); code == 0 {
		t.Fatal("any failure must FAIL the native lane")
	}
	// Non-native platform → FAIL even when tests are GREEN.
	isNativePlatform = func() bool { return false }
	if code := writeNativeResult(filepath.Join(dir, "nonnative.json"), fullPlatformPass(), time.Now().UTC(), time.Now().UTC(), nil); code == 0 {
		t.Fatal("non-Linux/cross-compiled must FAIL with details.native=false")
	}
	raw, err := os.ReadFile(filepath.Join(dir, "nonnative.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"native": false`) && !strings.Contains(string(raw), `"native":false`) {
		t.Fatalf("FAIL lane must record native=false: %s", raw)
	}
}
