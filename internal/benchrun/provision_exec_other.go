//go:build !linux

package benchrun

import (
	"errors"
	"os"
)

// Hash-and-execute-by-descriptor is intentionally unavailable where this
// package cannot prove the descriptor execution contract. Live provisioning
// must fail closed rather than fall back to a path-based TOCTOU window.
func openProvisionExecutable(string) (*os.File, string, error) {
	return nil, "", errors.New("descriptor-bound provisioning is unsupported on this platform")
}
