//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package main

import (
	"fmt"
	"os"
	"syscall"
)

func pgHelperAvailable() bool { return true }

func maybeRunPGHelper() (bool, error) {
	if os.Getenv("INSTANT_V2_CHAOS_PG_HELPER") != "1" {
		return false, nil
	}
	if len(os.Args) < 2 {
		return true, fmt.Errorf("chaos Postgres helper missing executable")
	}
	if err := syscall.Fchdir(3); err != nil {
		return true, fmt.Errorf("fchdir identity-bound pg-data: %w", err)
	}
	if err := syscall.Exec(os.Args[1], os.Args[1:], os.Environ()); err != nil {
		return true, fmt.Errorf("exec identity-bound Postgres helper: %w", err)
	}
	return true, nil
}
