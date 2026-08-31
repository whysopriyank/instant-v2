//go:build !linux

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// snapshotStable uses one opened descriptor on platforms without Linux's
// O_NOFOLLOW. The caller still performs the Linux namespace launch only on
// Linux; this fallback keeps local contract tests deterministic.
func snapshotStable(source, destination string, mode os.FileMode) (string, error) {
	if err := noSymlinkComponents(source); err != nil {
		return "", err
	}
	if err := noSymlinkComponents(destination); err != nil {
		return "", err
	}
	if _, err := os.Lstat(destination); err == nil {
		return "", fmt.Errorf("snapshot destination already exists: %s", filepath.Base(destination))
	} else if !os.IsNotExist(err) {
		return "", err
	}
	sourceFile, err := os.Open(source)
	if err != nil {
		return "", err
	}
	defer sourceFile.Close()
	before, err := sourceFile.Stat()
	if err != nil {
		return "", err
	}
	if !before.Mode().IsRegular() {
		return "", fmt.Errorf("snapshot source is not a regular file")
	}
	temporary, err := os.CreateTemp(filepath.Dir(destination), ".benchsmoke-snapshot-*")
	if err != nil {
		return "", err
	}
	temporaryName := temporary.Name()
	removeTemporary := true
	defer func() {
		_ = temporary.Close()
		if removeTemporary {
			_ = os.Remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(mode.Perm()); err != nil {
		return "", err
	}
	hasher := sha256.New()
	if _, err := io.Copy(io.MultiWriter(temporary, hasher), sourceFile); err != nil {
		return "", err
	}
	digest := hex.EncodeToString(hasher.Sum(nil))
	after, err := sourceFile.Stat()
	if err != nil {
		return "", err
	}
	if before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return "", fmt.Errorf("snapshot source changed while being copied")
	}
	if err := temporary.Sync(); err != nil {
		return "", err
	}
	if err := temporary.Close(); err != nil {
		return "", err
	}
	if err := os.Link(temporaryName, destination); err != nil {
		return "", fmt.Errorf("publish snapshot: %w", err)
	}
	removeTemporary = false
	if err := os.Remove(temporaryName); err != nil {
		return "", err
	}
	return digest, nil
}
