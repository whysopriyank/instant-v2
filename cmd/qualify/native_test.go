package main

import (
	"encoding/json"
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
	if code := writeNativeResult(filepath.Join(dir, "pass.json"), fullPlatformPass(), time.Now().UTC(), time.Now().UTC(), 0, 0); code != 0 {
		t.Fatal("platform GREEN + native must PASS")
	}
	// Any skip → FAIL (gate requires skipped_count == 0).
	skipped := append(fullPlatformPass(), testOutcome{Package: "example/a", Test: "TestUnrelated", Action: "skip"})
	if code := writeNativeResult(filepath.Join(dir, "skip.json"), skipped, time.Now().UTC(), time.Now().UTC(), 0, 0); code == 0 {
		t.Fatal("any skip must FAIL the native lane")
	}
	// Any fail → FAIL.
	failed := fullPlatformPass()
	failed[0].Action = "fail"
	if code := writeNativeResult(filepath.Join(dir, "fail.json"), failed, time.Now().UTC(), time.Now().UTC(), 0, 0); code == 0 {
		t.Fatal("any failure must FAIL the native lane")
	}
	// Non-native platform → FAIL even when tests are GREEN.
	isNativePlatform = func() bool { return false }
	if code := writeNativeResult(filepath.Join(dir, "nonnative.json"), fullPlatformPass(), time.Now().UTC(), time.Now().UTC(), 0, 0); code == 0 {
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

func TestNativeHermeticFailureFailsLane(t *testing.T) {
	dir := t.TempDir()
	if code := writeNativeResult(filepath.Join(dir, "herm.json"), fullPlatformPass(), time.Now().UTC(), time.Now().UTC(), 1, 0); code == 0 {
		t.Fatal("hermetic-pass failure accepted")
	}
}

func TestNativePackageFailureFailsLane(t *testing.T) {
	old := isNativePlatform
	isNativePlatform = func() bool { return true }
	defer func() { isNativePlatform = old }()
	var passLog strings.Builder
	for _, outcome := range fullPlatformPass() {
		passLog.Write(marshalPublicTest(t, testEvent{Action: outcome.Action, Package: outcome.Package, Test: outcome.Test}))
		passLog.WriteByte('\n')
	}
	packageFailure := `{"Action":"build-fail","ImportPath":"example/unrelated"}` + "\n" + `{"Action":"fail","Package":"example/unrelated"}` + "\n"
	for _, lane := range []string{"log", "owned-db", "hermetic"} {
		t.Run(lane, func(t *testing.T) {
			dir := t.TempDir()
			out := filepath.Join(dir, "lane.json")
			args := []string{"--out", out}
			if lane == "log" {
				path := filepath.Join(dir, "gotest.json")
				requirePublicTestSuccess(t, os.WriteFile(path, []byte(passLog.String()+packageFailure), 0600))
				args = append(args, "--log", path)
			} else {
				full := passLog.String()
				hermetic := passLog.String()
				if lane == "owned-db" {
					full += packageFailure
				} else {
					hermetic += packageFailure
				}
				fullPath := filepath.Join(dir, "full.json")
				hermPath := filepath.Join(dir, "hermetic.json")
				requirePublicTestSuccess(t, os.WriteFile(fullPath, []byte(full), 0600))
				requirePublicTestSuccess(t, os.WriteFile(hermPath, []byte(hermetic), 0600))
				goScript := `#!/usr/bin/env bash
if [[ " $* " == *" -short "* ]];then cat "$NATIVE_TEST_HERMETIC_LOG";[[ $NATIVE_TEST_FAIL_LANE != hermetic ]];else cat "$NATIVE_TEST_FULL_LOG";[[ $NATIVE_TEST_FAIL_LANE != owned-db ]];fi
`
				requirePublicTestSuccess(t, os.WriteFile(filepath.Join(dir, "go"), []byte(goScript), 0755))
				t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
				t.Setenv("INSTANT_TEST_INTEGRATION", "1")
				t.Setenv("NATIVE_TEST_FULL_LOG", fullPath)
				t.Setenv("NATIVE_TEST_HERMETIC_LOG", hermPath)
				t.Setenv("NATIVE_TEST_FAIL_LANE", lane)
				args = append(args, "--run")
			}
			if code := runNative(args); code == 0 {
				t.Fatal("package failure accepted despite partial named test/platform passes")
			}
			var lr laneResult
			requirePublicTestSuccess(t, json.Unmarshal(readPublicTestFile(t, out), &lr))
			if lr.Result != "FAIL" || lr.SelectedCount != len(fullPlatformPass()) {
				t.Fatalf("package failure must retain named count and FAIL: %+v", lr)
			}
			diagnostic, ok := lr.Details["failure_diagnostic"].(string)
			if !ok {
				t.Fatal("package failure diagnostic missing")
			}
			var counts struct {
				Failed        int `json:"failed"`
				PackageFailed int `json:"package_failed"`
			}
			requirePublicTestSuccess(t, json.Unmarshal([]byte(diagnostic), &counts))
			if counts.Failed != 0 || counts.PackageFailed != 1 {
				t.Fatalf("package failure must not inflate named failure count: %+v", counts)
			}
		})
	}
}
