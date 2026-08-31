package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func readBounded(path string, max int64) ([]byte, error) {
	return readStablePath(path, max)
}

func readJSONRegular(path string, dst any) error {
	if err := noSymlinkComponents(path); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("artifact is not a regular file: %s", filepath.Base(path))
	}
	b, err := readBounded(path, 8<<20)
	if err != nil {
		return err
	}
	return unmarshalArtifact(filepath.Base(path), b, dst)
}

func readJSONArtifact(root *artifactRoot, relative string, dst any) error {
	b, err := root.read(relative, 8<<20)
	if err != nil {
		return err
	}
	return unmarshalArtifact(filepath.Base(relative), b, dst)
}

func unmarshalArtifact(name string, b []byte, dst any) error {
	if err := json.Unmarshal(b, dst); err != nil {
		var syntaxErr *json.SyntaxError
		if errors.As(err, &syntaxErr) || errors.Is(err, io.ErrUnexpectedEOF) {
			return transientProgressError{fmt.Errorf("malformed artifact %s: %w", name, err)}
		}
		return fmt.Errorf("artifact %s has invalid schema: %w", name, err)
	}
	return nil
}

func noSymlinkComponents(path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("artifact path must be absolute")
	}
	for _, part := range strings.Split(strings.TrimPrefix(path, string(filepath.Separator)), string(filepath.Separator)) {
		if part == "." || part == ".." {
			return errors.New("artifact path contains unsafe component")
		}
	}
	clean := filepath.Clean(path)
	if clean == string(filepath.Separator) {
		return errors.New("artifact path must not be filesystem root")
	}
	for current := string(filepath.Separator); ; {
		rel := strings.TrimPrefix(clean, string(filepath.Separator))
		if current != string(filepath.Separator) {
			rel = strings.TrimPrefix(clean, current+string(filepath.Separator))
		}
		part := rel
		if index := strings.IndexByte(rel, byte(filepath.Separator)); index >= 0 {
			part = rel[:index]
		}
		if part == "" || part == "." || part == ".." {
			return errors.New("artifact path contains unsafe component")
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err == nil && info.Mode()&os.ModeSymlink != 0 {
			return errors.New("artifact path crosses a symlink")
		}
		if current == clean {
			return nil
		}
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if err != nil {
			// Missing parents cannot conceal an existing symlink below them; the
			// final regular-file check still prevents a later link target.
			return nil
		}
	}
}

func requireRunSiblingsAt(root *artifactRoot, dir string) error {
	for _, name := range []string{"ledger.jsonl", "frames.jsonl", "process.jsonl", "runtime-metrics.jsonl", "db-before.json", "db-after.json", "stdout.log", "stderr.log"} {
		if err := root.regular(filepath.Join(dir, name)); err != nil {
			return fmt.Errorf("run sibling %s is not a regular file: %w", name, err)
		}
	}
	return nil
}

func validateJSONLines(path string) error {
	b, err := readBounded(path, 8<<20)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(bytes.NewReader(b))
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		var value json.RawMessage
		if err := json.Unmarshal(scanner.Bytes(), &value); err != nil {
			return fmt.Errorf("malformed JSONL sibling %s: %w", filepath.Base(path), err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("malformed JSONL sibling %s: %w", filepath.Base(path), err)
	}
	return nil
}

func validateJSONLinesAt(root *artifactRoot, relative string) error {
	b, err := root.read(relative, 8<<20)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(bytes.NewReader(b))
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		var value json.RawMessage
		if err := json.Unmarshal(scanner.Bytes(), &value); err != nil {
			return fmt.Errorf("malformed JSONL sibling %s: %w", filepath.Base(relative), err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("malformed JSONL sibling %s: %w", filepath.Base(relative), err)
	}
	return nil
}
