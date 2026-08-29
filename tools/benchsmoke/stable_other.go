//go:build !linux

package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

// Local non-Linux runs do not launch the remote benchmark. Keep their file
// reads descriptor-based and size-stable; Linux remote execution uses the
// stronger dirfd/O_NOFOLLOW implementation in stable_linux.go.
func readStablePath(path string, max int64) ([]byte, error) {
	if max < 0 {
		return nil, errors.New("bounded input limit is invalid")
	}
	if err := noSymlinkComponents(path); err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	before, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() {
		return nil, errors.New("bounded input is not a regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, max+1))
	if err != nil {
		return nil, err
	}
	after, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return nil, errors.New("bounded input changed while being read")
	}
	if int64(len(data)) > max || int64(len(data)) != after.Size() {
		return nil, errors.New("input exceeds bounded size or changed while being read")
	}
	return data, nil
}

type artifactRoot struct{ path string }

func openArtifactRoot(path string) (*artifactRoot, error) {
	if err := noSymlinkComponents(path); err != nil {
		return nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, errors.New("artifact root is not a directory")
	}
	return &artifactRoot{path: filepath.Clean(path)}, nil
}

func (r *artifactRoot) Close() error { return nil }

func (r *artifactRoot) read(relative string, max int64) ([]byte, error) {
	return readStablePath(filepath.Join(r.path, relative), max)
}

func (r *artifactRoot) regular(relative string) error {
	path := filepath.Join(r.path, relative)
	if err := noSymlinkComponents(path); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("artifact is not a regular file")
	}
	return nil
}
