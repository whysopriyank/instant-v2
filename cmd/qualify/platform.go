package main

import (
	"fmt"
	"strings"
)

// platformPattern is one substring rule. Kind selects what it matches:
// "test" matches the (possibly slash-qualified) test name, "pkg" matches the
// go package path, "file" matches go-test -json Output text (which carries
// "file_test.go:NNN:" prefixes and failure bodies).
type platformPattern struct {
	Kind   string // "test", "pkg", or "file"
	Substr string
}

// platformGroup is one reviewed platform-sensitivity category. The native
// lane requires at least one PASSING test matching at least one pattern in
// every group, and FAILS when any matched test did not pass (a skip is not
// a pass). Patterns are OR-ed inside a group so a renamed test cannot fail
// the lane as long as the category is still exercised; adding a genuinely
// new platform-sensitive area is a reviewed edit to this list.
var platformGroups = []struct {
	Reason   string
	Patterns []platformPattern
}{
	{
		Reason: "process identity (pid/exe/start-instance/binary binding)",
		Patterns: []platformPattern{
			{Kind: "test", Substr: "ProcessIdentity"},
			{Kind: "test", Substr: "ProcessAlive"},
			{Kind: "test", Substr: "VerifyProcess"},
			{Kind: "test", Substr: "ProcessCollector"},
			{Kind: "test", Substr: "ProcessWindow"},
			{Kind: "test", Substr: "ProcessEvidence"},
			{Kind: "test", Substr: "ProcessProvenance"},
		},
	},
	{
		Reason: "/proc evidence parsing",
		Patterns: []platformPattern{
			{Kind: "test", Substr: "Proc"},
			{Kind: "file", Substr: "/proc"},
		},
	},
	{
		Reason: "file-descriptor validation",
		Patterns: []platformPattern{
			{Kind: "test", Substr: "Descriptor"},
			{Kind: "test", Substr: "DirFD"},
		},
	},
	{
		Reason: "signal orchestration",
		Patterns: []platformPattern{
			{Kind: "test", Substr: "Signal"},
			{Kind: "test", Substr: "SIGSTOP"},
			{Kind: "test", Substr: "SIGTERM"},
			{Kind: "test", Substr: "SIGKILL"},
			{Kind: "test", Substr: "FreezeAndThaw"},
		},
	},
	{
		Reason: "advisory locks",
		Patterns: []platformPattern{
			{Kind: "test", Substr: "Lock"},
			{Kind: "test", Substr: "Flock"},
		},
	},
	{
		Reason: "filesystem replacement / atomic write",
		Patterns: []platformPattern{
			{Kind: "test", Substr: "Replacement"},
			{Kind: "test", Substr: "Atomic"},
			{Kind: "test", Substr: "WriteOnce"},
			{Kind: "test", Substr: "WriteEvidence"},
		},
	},
	{
		Reason: "owned-resource cleanup and daemon lifecycle",
		Patterns: []platformPattern{
			{Kind: "test", Substr: "Cleanup"},
			{Kind: "test", Substr: "DaemonRestart"},
			{Kind: "test", Substr: "StartupFailure"},
			{Kind: "test", Substr: "Teardown"},
		},
	},
}

// matchPlatformTest reports whether outcome o matches pattern p.
func matchPlatformTest(o testOutcome, p platformPattern) bool {
	switch p.Kind {
	case "test":
		return strings.Contains(o.Test, p.Substr)
	case "pkg":
		return strings.Contains(o.Package, p.Substr)
	case "file":
		return strings.Contains(o.Output, p.Substr)
	}
	return false
}

// platformCheckResult tallies platform-sensitive coverage over outcomes.
type platformCheckResult struct {
	// Passed is the number of distinct passing tests matching >=1 group.
	Passed int
	// Missing lists group reasons that matched zero passing tests.
	Missing []string
	// Failed lists matched tests whose final action was not pass.
	Failed []string
}

// checkPlatformCoverage applies every group to outcomes. ok is false when
// any group matched no passing test or any matched test did not pass.
func checkPlatformCoverage(outcomes []testOutcome) (res platformCheckResult, ok bool) {
	ok = true
	matched := map[int]bool{} // outcome index matched by any group
	for _, g := range platformGroups {
		groupPass := 0
		for i, o := range outcomes {
			hit := false
			for _, p := range g.Patterns {
				if matchPlatformTest(o, p) {
					hit = true
					break
				}
			}
			if !hit {
				continue
			}
			matched[i] = true
			if o.Action == "pass" {
				groupPass++
			} else {
				res.Failed = append(res.Failed, fmt.Sprintf("%s/%s (%s)", o.Package, o.Test, o.Action))
				ok = false
			}
		}
		if groupPass == 0 {
			res.Missing = append(res.Missing, g.Reason)
			ok = false
		}
	}
	for i := range outcomes {
		if matched[i] && outcomes[i].Action == "pass" {
			res.Passed++
		}
	}
	return res, ok
}
