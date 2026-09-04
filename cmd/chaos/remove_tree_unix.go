//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

// removeMarkerOwnedPGDataTree empties the directory through directory file
// descriptors. The owned root is deliberately retained while its sidecar is
// removed through the parent descriptor, so no final path-based root unlink
// can race with a replacement. The caller supplies the identity and sidecar
// provenance it already validated; the remover never re-adopts a new one.
func removeMarkerOwnedPGDataTree(path string, expected os.FileInfo, owner pgDataOwner) error {
	return removePGDataTreeIdentity(path, expected, owner)
}

func pgDataDevIno(info os.FileInfo) (uint64, uint64, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, false
	}
	return uint64(stat.Dev), uint64(stat.Ino), true
}

func writePGDataOwnerAt(path string, expected os.FileInfo, owner pgDataOwner) error {
	parentFD, err := unix.Open(filepath.Dir(path), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open pg-data parent: %w", err)
	}
	defer func() { _ = unix.Close(parentFD) }()
	targetFD, err := unix.Openat(parentFD, filepath.Base(path), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open pg-data directory: %w", err)
	}
	targetFile := os.NewFile(uintptr(targetFD), path)
	defer func() { _ = targetFile.Close() }()
	info, err := targetFile.Stat()
	if err != nil {
		return fmt.Errorf("stat opened pg-data directory: %w", err)
	}
	if expected == nil || !os.SameFile(expected, info) {
		return errors.New("--pg-data changed before ownership marker")
	}
	if err := validatePGDataOwner(owner, filepath.Base(path), expected); err != nil {
		return err
	}
	sidecarName := chaosPGDataOwnerPrefix + filepath.Base(path)
	sidecarFD, err := unix.Openat(parentFD, sidecarName, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return fmt.Errorf("create ownership sidecar: %w", err)
	}
	sidecar := os.NewFile(uintptr(sidecarFD), sidecarName)
	b, err := json.Marshal(owner)
	if err != nil {
		_ = sidecar.Close()
		_ = unix.Unlinkat(parentFD, sidecarName, 0)
		return fmt.Errorf("encode ownership sidecar: %w", err)
	}
	verified := false
	defer func() {
		_ = sidecar.Close()
		if !verified {
			_ = unix.Unlinkat(parentFD, sidecarName, 0)
		}
	}()
	if _, err := sidecar.Write(b); err != nil {
		return fmt.Errorf("write ownership sidecar: %w", err)
	}
	if err := sidecar.Sync(); err != nil {
		return fmt.Errorf("sync ownership sidecar: %w", err)
	}
	sidecarInfo, err := sidecar.Stat()
	if err != nil {
		return fmt.Errorf("stat ownership sidecar: %w", err)
	}
	owner.Sidecar = sidecarInfo
	if err := sidecar.Close(); err != nil {
		return fmt.Errorf("close ownership sidecar: %w", err)
	}
	if _, err := ownerSidecarAt(parentFD, filepath.Base(path), expected, owner); err != nil {
		return fmt.Errorf("verify ownership sidecar: %w", err)
	}
	verified = true
	return nil
}

func removePGDataTreeIdentity(path string, expected os.FileInfo, owner pgDataOwner) error {
	parentFD, err := unix.Open(filepath.Dir(path), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open pg-data parent: %w", err)
	}
	defer func() { _ = unix.Close(parentFD) }()

	targetName := filepath.Base(path)
	targetFD, err := unix.Openat(parentFD, targetName, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open pg-data directory: %w", err)
	}
	targetFile := os.NewFile(uintptr(targetFD), path)
	defer func() { _ = targetFile.Close() }()
	got, err := targetFile.Stat()
	if err != nil {
		return fmt.Errorf("stat opened pg-data directory: %w", err)
	}
	if expected == nil || !os.SameFile(expected, got) {
		return errors.New("--pg-data changed during validation; refusing removal")
	}
	sidecarInfo, err := ownerSidecarAt(parentFD, targetName, expected, owner)
	if err != nil {
		return err
	}
	if err := removeDirectoryContents(targetFD); err != nil {
		return err
	}

	// The hook is test-only and lets the race regression replace the directory
	// after contents are removed but before the final identity check.
	beforePGDataUnlink(targetName)
	var st unix.Stat_t
	if err := unix.Fstatat(parentFD, targetName, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fmt.Errorf("recheck pg-data path before identity check: %w", err)
	}
	if !sameUnixStat(expected, &st) {
		return errors.New("--pg-data changed before final identity check; refusing removal")
	}
	var sidecarStat unix.Stat_t
	if err := unix.Fstatat(parentFD, chaosPGDataOwnerPrefix+targetName, &sidecarStat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fmt.Errorf("recheck ownership sidecar: %w", err)
	}
	if !sameUnixStat(sidecarInfo, &sidecarStat) {
		return errors.New("ownership sidecar changed before removal")
	}
	if err := unix.Unlinkat(parentFD, chaosPGDataOwnerPrefix+targetName, 0); err != nil {
		return fmt.Errorf("remove ownership sidecar: %w", err)
	}
	return nil
}

func ownerSidecarAt(parentFD int, targetName string, expected os.FileInfo, owner pgDataOwner) (os.FileInfo, error) {
	fd, err := unix.Openat(parentFD, chaosPGDataOwnerPrefix+targetName, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("ownership sidecar missing: %w", err)
	}
	f := os.NewFile(uintptr(fd), chaosPGDataOwnerPrefix+targetName)
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat ownership sidecar: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("ownership sidecar is not a regular file")
	}
	if owner.Sidecar == nil || !os.SameFile(owner.Sidecar, info) {
		return nil, errors.New("ownership sidecar changed during validation")
	}
	b, err := io.ReadAll(io.LimitReader(f, 16<<10))
	if err != nil {
		return nil, fmt.Errorf("read ownership sidecar: %w", err)
	}
	var got pgDataOwner
	if err := json.Unmarshal(b, &got); err != nil {
		return nil, fmt.Errorf("decode ownership sidecar: %w", err)
	}
	if err := validatePGDataOwner(got, targetName, expected); err != nil || got.Token != owner.Token || got.Version != owner.Version {
		return nil, errors.New("ownership sidecar mismatch")
	}
	return info, nil
}

func removeDirectoryContents(dirFD int) error {
	entries, err := readDirectoryNames(dirFD)
	if err != nil {
		return fmt.Errorf("read pg-data directory: %w", err)
	}
	for _, name := range entries {
		if err := removeEntryAt(dirFD, name); err != nil {
			return err
		}
	}
	return nil
}

func readDirectoryNames(fd int) ([]string, error) {
	var names []string
	buf := make([]byte, 32*1024)
	for {
		n, err := unix.ReadDirent(fd, buf)
		if n > 0 {
			_, _, names = unix.ParseDirent(buf[:n], -1, names)
		}
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return names, err
		}
		if n == 0 {
			return names, nil
		}
	}
}

func removeEntryAt(parentFD int, name string) error {
	for {
		childFD, err := unix.Openat(parentFD, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err == nil {
			if err := removeDirectoryContents(childFD); err != nil {
				_ = unix.Close(childFD)
				return err
			}
			_ = unix.Close(childFD)
			if err := unix.Unlinkat(parentFD, name, unix.AT_REMOVEDIR); err != nil {
				if errors.Is(err, unix.ENOENT) {
					return nil
				}
				return fmt.Errorf("remove child directory %q: %w", name, err)
			}
			return nil
		}

		if errors.Is(err, unix.ENOENT) {
			return nil
		}
		if !errors.Is(err, unix.ENOTDIR) && !errors.Is(err, unix.ELOOP) {
			return fmt.Errorf("open child %q: %w", name, err)
		}
		if err := unix.Unlinkat(parentFD, name, 0); err != nil {
			if errors.Is(err, unix.ENOENT) {
				return nil
			}
			if errors.Is(err, unix.EISDIR) {
				// The entry changed from a non-directory to a directory. Reopen
				// with O_NOFOLLOW and handle it through the descriptor path.
				continue
			}
			return fmt.Errorf("remove child %q: %w", name, err)
		}
		return nil
	}
}

func sameUnixStat(info os.FileInfo, st *unix.Stat_t) bool {
	sys, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return false
	}
	return uint64(sys.Dev) == uint64(st.Dev) && uint64(sys.Ino) == uint64(st.Ino)
}
