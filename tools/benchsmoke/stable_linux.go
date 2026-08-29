//go:build linux

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// stableFileIdentity is deliberately taken from the opened descriptor. A
// pathname lstat followed by a pathname open is not sufficient here: a
// concurrent parent replacement can make those refer to different files.
type stableFileIdentity struct {
	dev       uint64
	ino       uint64
	uid       uint32
	size      int64
	mtimeSec  int64
	mtimeNsec int64
}

func stableIdentity(fd int) (stableFileIdentity, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return stableFileIdentity{}, err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG {
		return stableFileIdentity{}, errors.New("bounded input is not a regular file")
	}
	return stableFileIdentity{
		dev: stat.Dev, ino: stat.Ino, uid: stat.Uid, size: stat.Size,
		mtimeSec: stat.Mtim.Sec, mtimeNsec: stat.Mtim.Nsec,
	}, nil
}

func sameStableIdentity(before, after stableFileIdentity) bool {
	return before == after
}

func rootOrTaskOwned(uid uint32) bool {
	return uid == 0 || int(uid) == os.Geteuid()
}

func openStableDir(path string) (*os.File, error) {
	if err := noSymlinkComponents(path); err != nil {
		return nil, err
	}
	clean := filepath.Clean(path)
	rootFD, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open artifact root: %w", err)
	}
	currentFD := rootFD
	closeCurrent := true
	defer func() {
		if closeCurrent {
			_ = unix.Close(currentFD)
		}
	}()
	parts := splitAbsolutePath(clean)
	for _, part := range parts {
		nextFD, openErr := unix.Openat(currentFD, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if openErr != nil {
			return nil, fmt.Errorf("open artifact directory %s: %w", part, openErr)
		}
		_ = unix.Close(currentFD)
		currentFD = nextFD
	}
	file := os.NewFile(uintptr(currentFD), clean)
	if file == nil {
		return nil, errors.New("open artifact directory: invalid descriptor")
	}
	closeCurrent = false
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		_ = file.Close()
		return nil, err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || !rootOrTaskOwned(stat.Uid) {
		_ = file.Close()
		return nil, errors.New("artifact directory is not a root/task-owned directory")
	}
	return file, nil
}

func openStableFile(path string) (*os.File, stableFileIdentity, error) {
	if err := noSymlinkComponents(path); err != nil {
		return nil, stableFileIdentity{}, err
	}
	clean := filepath.Clean(path)
	parts := splitAbsolutePath(clean)
	if len(parts) == 0 {
		return nil, stableFileIdentity{}, errors.New("artifact path must not be filesystem root")
	}
	rootFD, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, stableFileIdentity{}, fmt.Errorf("open artifact root: %w", err)
	}
	currentFD := rootFD
	for _, part := range parts[:len(parts)-1] {
		nextFD, openErr := unix.Openat(currentFD, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		_ = unix.Close(currentFD)
		if openErr != nil {
			return nil, stableFileIdentity{}, fmt.Errorf("open artifact directory %s: %w", part, openErr)
		}
		currentFD = nextFD
	}
	fileFD, err := unix.Openat(currentFD, parts[len(parts)-1], unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	_ = unix.Close(currentFD)
	if err != nil {
		return nil, stableFileIdentity{}, fmt.Errorf("open artifact %s: %w", filepath.Base(clean), err)
	}
	identity, err := stableIdentity(fileFD)
	if err != nil {
		_ = unix.Close(fileFD)
		return nil, stableFileIdentity{}, err
	}
	if !rootOrTaskOwned(identity.uid) {
		_ = unix.Close(fileFD)
		return nil, stableFileIdentity{}, errors.New("bounded input is not root/task owned")
	}
	file := os.NewFile(uintptr(fileFD), clean)
	if file == nil {
		_ = unix.Close(fileFD)
		return nil, stableFileIdentity{}, errors.New("open artifact: invalid descriptor")
	}
	return file, identity, nil
}

func readStablePath(path string, max int64) ([]byte, error) {
	if max < 0 {
		return nil, errors.New("bounded input limit is invalid")
	}
	file, before, err := openStableFile(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return readStableDescriptor(file, before, max)
}

type artifactRoot struct {
	path string
	dir  *os.File
}

func openArtifactRoot(path string) (*artifactRoot, error) {
	dir, err := openStableDir(path)
	if err != nil {
		return nil, err
	}
	return &artifactRoot{path: filepath.Clean(path), dir: dir}, nil
}

func (r *artifactRoot) Close() error { return r.dir.Close() }

func (r *artifactRoot) read(relative string, max int64) ([]byte, error) {
	if max < 0 {
		return nil, errors.New("bounded input limit is invalid")
	}
	file, before, err := openStableRelative(int(r.dir.Fd()), relative)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return readStableDescriptor(file, before, max)
}

func (r *artifactRoot) regular(relative string) error {
	file, _, err := openStableRelative(int(r.dir.Fd()), relative)
	if err != nil {
		return err
	}
	return file.Close()
}

func openStableRelative(rootFD int, relative string) (*os.File, stableFileIdentity, error) {
	parts := splitRelativePath(relative)
	if len(parts) == 0 {
		return nil, stableFileIdentity{}, errors.New("artifact relative path is empty")
	}
	currentFD, err := unix.Dup(rootFD)
	if err != nil {
		return nil, stableFileIdentity{}, err
	}
	for _, part := range parts[:len(parts)-1] {
		nextFD, openErr := unix.Openat(currentFD, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		_ = unix.Close(currentFD)
		if openErr != nil {
			return nil, stableFileIdentity{}, fmt.Errorf("open artifact directory %s: %w", part, openErr)
		}
		currentFD = nextFD
	}
	fileFD, err := unix.Openat(currentFD, parts[len(parts)-1], unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	_ = unix.Close(currentFD)
	if err != nil {
		return nil, stableFileIdentity{}, fmt.Errorf("open artifact %s: %w", parts[len(parts)-1], err)
	}
	identity, err := stableIdentity(fileFD)
	if err != nil {
		_ = unix.Close(fileFD)
		return nil, stableFileIdentity{}, err
	}
	if !rootOrTaskOwned(identity.uid) {
		_ = unix.Close(fileFD)
		return nil, stableFileIdentity{}, errors.New("bounded input is not root/task owned")
	}
	file := os.NewFile(uintptr(fileFD), relative)
	if file == nil {
		_ = unix.Close(fileFD)
		return nil, stableFileIdentity{}, errors.New("open artifact: invalid descriptor")
	}
	return file, identity, nil
}

func readStableDescriptor(file *os.File, before stableFileIdentity, max int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(file, max+1))
	if err != nil {
		return nil, err
	}
	after, err := stableIdentity(int(file.Fd()))
	if err != nil {
		return nil, err
	}
	if !sameStableIdentity(before, after) {
		return nil, errors.New("bounded input changed while being read")
	}
	if int64(len(data)) > max || int64(len(data)) != after.size {
		return nil, errors.New("input exceeds bounded size or changed while being read")
	}
	return data, nil
}

func splitRelativePath(path string) []string {
	if path == "" || filepath.IsAbs(path) {
		return nil
	}
	for _, part := range strings.Split(path, string(filepath.Separator)) {
		if part == "." || part == ".." {
			return nil
		}
	}
	parts := strings.Split(filepath.Clean(path), string(filepath.Separator))
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return nil
		}
	}
	return parts
}

func splitAbsolutePath(path string) []string {
	trimmed := filepath.Clean(path)[1:]
	if trimmed == "" {
		return nil
	}
	parts := make([]string, 0, 4)
	for _, part := range strings.Split(trimmed, "/") {
		if part != "" {
			parts = append(parts, part)
		}
	}
	return parts
}
