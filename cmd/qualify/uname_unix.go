package main

import (
	"os"
	"runtime"
	"strings"
)

// unameReleaseSys reports the kernel release without new dependencies:
// on Linux it reads /proc/sys/kernel/osrelease; elsewhere it reports
// "unknown" so the caller records an explicit value rather than guessing.
func unameReleaseSys() (string, bool) {
	if runtime.GOOS != "linux" {
		return "unknown", true
	}
	raw, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil {
		return "unknown", true
	}
	release := strings.TrimSpace(string(raw))
	if release == "" {
		return "unknown", true
	}
	return release, true
}
