package main

import (
	"testing"
)

// fullPlatformPass builds one passing outcome per platform group so coverage
// is GREEN.
func fullPlatformPass() []testOutcome {
	return []testOutcome{
		{Package: "example/corpus", Test: "TestCandidateVerifyProcessAlive", Action: "pass"},
		{Package: "example/bench", Test: "TestParseProcRouteAcceptsKernelColumnWidths", Action: "pass"},
		{Package: "example/smoke", Test: "TestStableArtifactReadsUseDescriptorValidation", Action: "pass"},
		{Package: "example/chaos", Test: "TestFreezeAndThawPropagatesSignalFailures", Action: "pass"},
		{Package: "example/store", Test: "TestUploadKeyLockSerializesSameKey", Action: "pass"},
		{Package: "example/store", Test: "TestSnapshotStableDoesNotMixSourceReplacement", Action: "pass"},
		{Package: "example/daemon", Test: "TestDA001DaemonRestartPreservesObjects", Action: "pass"},
	}
}

func TestPlatformCoverageGreen(t *testing.T) {
	res, ok := checkPlatformCoverage(fullPlatformPass())
	if !ok {
		t.Fatalf("want GREEN, missing=%v failed=%v", res.Missing, res.Failed)
	}
	if res.Passed != len(fullPlatformPass()) {
		t.Fatalf("Passed=%d, want %d", res.Passed, len(fullPlatformPass()))
	}
}

func TestPlatformCoverageMissingGroupFails(t *testing.T) {
	outcomes := fullPlatformPass()[:len(fullPlatformPass())-1] // drop cleanup group
	res, ok := checkPlatformCoverage(outcomes)
	if ok {
		t.Fatal("want FAIL when a group has no passing test")
	}
	if len(res.Missing) == 0 {
		t.Fatal("want Missing to name the uncovered group")
	}
}

func TestPlatformCoverageNonPassMatchFails(t *testing.T) {
	outcomes := fullPlatformPass()
	outcomes[3].Action = "skip" // signal test skipped
	outcomes[3].Output = "    x_test.go:1: in short mode\n"
	res, ok := checkPlatformCoverage(outcomes)
	if ok {
		t.Fatal("want FAIL when a matched test did not pass")
	}
	if len(res.Failed) == 0 {
		t.Fatal("want Failed to name the non-passing match")
	}
	// A skip also empties the group only if it was the sole match; here the
	// group still reports Missing because no PASS matched it.
	if len(res.Missing) == 0 {
		t.Fatal("want Missing when the group's only match skipped")
	}
}

func TestPlatformFilePatternMatches(t *testing.T) {
	outcomes := fullPlatformPass()
	// Replace the /proc test with one whose Output proves /proc parsing.
	outcomes[1] = testOutcome{Package: "example/x", Test: "TestSomethingElse", Action: "pass", Output: "reading /proc/self/stat\n"}
	res, ok := checkPlatformCoverage(outcomes)
	if !ok {
		t.Fatalf("file-pattern match must satisfy the /proc group: missing=%v failed=%v", res.Missing, res.Failed)
	}
}
