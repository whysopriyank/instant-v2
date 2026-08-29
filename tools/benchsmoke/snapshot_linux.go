//go:build linux

package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// snapshotStable copies one immutable input through the descriptor opened for
// that input.  In particular, it never reopens the source by pathname after a
// replacement/rename race.  The destination is linked into place (rather than
// renamed over an existing path) so a concurrent creator cannot be clobbered.
func snapshotStable(source, destination string, mode os.FileMode) (string, error) {
	if err := noSymlinkComponents(source); err != nil {
		return "", err
	}
	if err := noSymlinkComponents(destination); err != nil {
		return "", err
	}
	sourceFile, before, err := openStableFile(source)
	if err != nil {
		return "", fmt.Errorf("open snapshot source: %w", err)
	}
	defer sourceFile.Close()

	destinationDir, err := openStableDir(filepath.Dir(filepath.Clean(destination)))
	if err != nil {
		return "", fmt.Errorf("open snapshot destination directory: %w", err)
	}
	defer destinationDir.Close()
	destinationName := filepath.Base(filepath.Clean(destination))
	if err := ensureDestinationAbsent(destinationDir.Fd(), destinationName); err != nil {
		return "", err
	}
	temporaryName, temporary, err := createSnapshotTemporary(destinationDir.Fd())
	if err != nil {
		return "", err
	}
	removeTemporary := true
	defer func() {
		_ = temporary.Close()
		if removeTemporary {
			_ = unix.Unlinkat(int(destinationDir.Fd()), temporaryName, 0)
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
	after, err := stableIdentity(int(sourceFile.Fd()))
	if err != nil {
		return "", err
	}
	if !sameStableIdentity(before, after) {
		return "", errors.New("snapshot source changed while being copied")
	}
	if err := temporary.Sync(); err != nil {
		return "", err
	}
	if err := temporary.Close(); err != nil {
		return "", err
	}
	if err := unix.Linkat(int(destinationDir.Fd()), temporaryName, int(destinationDir.Fd()), destinationName, 0); err != nil {
		return "", fmt.Errorf("publish snapshot: %w", err)
	}
	if err := unix.Unlinkat(int(destinationDir.Fd()), temporaryName, 0); err != nil {
		return "", fmt.Errorf("remove snapshot temporary: %w", err)
	}
	removeTemporary = false
	if err := unix.Fsync(int(destinationDir.Fd())); err != nil {
		return "", fmt.Errorf("sync snapshot directory: %w", err)
	}
	return digest, nil
}

func ensureDestinationAbsent(dirFD uintptr, name string) error {
	var stat unix.Stat_t
	if err := unix.Fstatat(int(dirFD), name, &stat, unix.AT_SYMLINK_NOFOLLOW); err == nil {
		return fmt.Errorf("snapshot destination already exists: %s", name)
	} else if !errors.Is(err, unix.ENOENT) {
		return fmt.Errorf("inspect snapshot destination: %w", err)
	}
	return nil
}

func createSnapshotTemporary(dirFD uintptr) (string, *os.File, error) {
	for attempt := 0; attempt < 16; attempt++ {
		var random [12]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", nil, fmt.Errorf("generate snapshot temporary name: %w", err)
		}
		name := ".benchsmoke-snapshot-" + hex.EncodeToString(random[:])
		fd, err := unix.Openat(int(dirFD), name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
		if errors.Is(err, unix.EEXIST) {
			continue
		}
		if err != nil {
			return "", nil, fmt.Errorf("create snapshot temporary: %w", err)
		}
		file := os.NewFile(uintptr(fd), name)
		if file == nil {
			_ = unix.Close(fd)
			return "", nil, errors.New("create snapshot temporary: invalid descriptor")
		}
		return name, file, nil
	}
	return "", nil, errors.New("could not allocate snapshot temporary")
}
