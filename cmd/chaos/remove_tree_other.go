//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd

package main

import (
	"errors"
	"fmt"
	"os"
)

// Platforms without openat/unlinkat support fail closed rather than falling
// back to a path-based recursive delete.
func removeMarkerOwnedPGDataTree(path string, expected os.FileInfo, owner pgDataOwner) error {
	return removePGDataTreeIdentityUnsupported(path, expected, owner)
}

func writePGDataOwnerAt(path string, expected os.FileInfo, owner pgDataOwner) error {
	return removePGDataTreeIdentityUnsupported(path, expected, owner)
}

func pgDataDevIno(os.FileInfo) (uint64, uint64, bool) {
	return 0, 0, false
}

func removePGDataTreeIdentityUnsupported(path string, expected os.FileInfo, owner pgDataOwner) error {
	got, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if expected == nil || !os.SameFile(expected, got) {
		return errors.New("--pg-data changed during validation; identity-safe removal unavailable")
	}
	return fmt.Errorf("identity-safe --pg-data removal is unsupported on this platform")
}
