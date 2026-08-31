package benchrun

import (
	"errors"
	"fmt"
	"io"
	"os"
)

func openRegularArtifact(path string) (*os.File, int64, error) {
	lstat, err := os.Lstat(path)
	if err != nil {
		return nil, 0, err
	}
	if lstat.Mode()&os.ModeSymlink != 0 || !lstat.Mode().IsRegular() {
		return nil, 0, errors.New("artifact is not a regular file")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	opened, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, err
	}
	if !opened.Mode().IsRegular() || !os.SameFile(lstat, opened) {
		_ = f.Close()
		return nil, 0, errors.New("artifact changed while opening")
	}
	return f, opened.Size(), nil
}

func readBounded(path string, limit int64) ([]byte, error) {
	if limit <= 0 {
		return nil, errors.New("invalid artifact read limit")
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("artifact is not a regular file")
	}
	if info.Size() > limit {
		return nil, fmt.Errorf("artifact exceeds %d byte limit", limit)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, fmt.Errorf("artifact exceeds %d byte limit", limit)
	}
	return b, nil
}
