package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// candidateProvenance identifies the exact source tree used for the chaos
// binary. A successful run must never silently switch to HEAD or continue
// after another workstream changes the tree.
type candidateProvenance struct {
	Revision        string `json:"revision"`
	TreeFingerprint string `json:"tree_fingerprint"`
	BinarySHA256    string `json:"binary_sha256"`
	SnapshotSHA256  string `json:"snapshot_sha256"`
}

func captureCandidate(repoRoot string) (candidateProvenance, error) {
	revision, err := gitOutput(repoRoot, "rev-parse", "HEAD")
	if err != nil {
		return candidateProvenance{}, fmt.Errorf("read HEAD revision: %w", err)
	}
	status, err := gitOutput(repoRoot, "status", "--porcelain=v1", "--untracked-files=all")
	if err != nil {
		return candidateProvenance{}, fmt.Errorf("read working-tree status: %w", err)
	}
	diff, err := gitOutput(repoRoot, "diff", "--binary", "HEAD", "--")
	if err != nil {
		return candidateProvenance{}, fmt.Errorf("read working-tree diff: %w", err)
	}
	untracked, err := gitOutputBytes(repoRoot, "ls-files", "--others", "--exclude-standard", "-z")
	if err != nil {
		return candidateProvenance{}, fmt.Errorf("list untracked files: %w", err)
	}
	fingerprint, err := fingerprintTree(repoRoot, status, diff, untracked)
	if err != nil {
		return candidateProvenance{}, fmt.Errorf("fingerprint working tree: %w", err)
	}
	return candidateProvenance{
		Revision:        strings.TrimSpace(revision),
		TreeFingerprint: fingerprint,
	}, nil
}

func (c candidateProvenance) verify(repoRoot string) error {
	current, err := captureCandidate(repoRoot)
	if err != nil {
		return err
	}
	if c.Revision != current.Revision || c.TreeFingerprint != current.TreeFingerprint {
		return fmt.Errorf("candidate changed (expected %s/%s, got %s/%s)",
			c.Revision, c.TreeFingerprint, current.Revision, current.TreeFingerprint)
	}
	return nil
}

func gitOutput(repoRoot string, args ...string) (string, error) {
	b, err := gitOutputBytes(repoRoot, args...)
	return string(b), err
}

func gitOutputBytes(repoRoot string, args ...string) ([]byte, error) {
	cmdArgs := append([]string{"-C", repoRoot}, args...)
	cmd := exec.Command("git", cmdArgs...)
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	return out, nil
}

func fingerprintTree(repoRoot, status, diff string, untracked []byte) (string, error) {
	h := sha256.New()
	_, _ = io.WriteString(h, status)
	_, _ = io.WriteString(h, "\x00")
	_, _ = io.WriteString(h, diff)
	_, _ = io.WriteString(h, "\x00")
	for _, rawPath := range strings.Split(string(untracked), "\x00") {
		if rawPath == "" {
			continue
		}
		clean := filepath.Clean(rawPath)
		if filepath.IsAbs(rawPath) || clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
			return "", fmt.Errorf("git returned unsafe untracked path %q", rawPath)
		}
		full := filepath.Join(repoRoot, clean)
		info, err := os.Lstat(full)
		if err != nil {
			return "", fmt.Errorf("stat untracked file %q: %w", rawPath, err)
		}
		_, _ = io.WriteString(h, "untracked:")
		_, _ = io.WriteString(h, rawPath)
		_, _ = io.WriteString(h, fmt.Sprintf(":%o:", info.Mode()))
		if info.Mode()&os.ModeSymlink != 0 {
			link, err := os.Readlink(full)
			if err != nil {
				return "", fmt.Errorf("read untracked symlink %q: %w", rawPath, err)
			}
			_, _ = io.WriteString(h, link)
			continue
		}
		if !info.Mode().IsRegular() {
			return "", fmt.Errorf("untracked path %q is not a regular file or symlink", rawPath)
		}
		b, err := os.ReadFile(full)
		if err != nil {
			return "", fmt.Errorf("read untracked file %q: %w", rawPath, err)
		}
		_, _ = h.Write(b)
		_, _ = io.WriteString(h, "\x00")
	}
	if err := fingerprintReplayInputs(repoRoot, h); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Corpus replay consumes the complete corpus directory, including ignored
// scratch fixtures such as corpus/.tmp. Git status/diff deliberately omit
// ignored files, so hash those inputs explicitly or a replay could run against
// a candidate that the provenance record does not identify.
func fingerprintReplayInputs(repoRoot string, h io.Writer) error {
	root := filepath.Join(repoRoot, "corpus")
	info, err := os.Lstat(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("replay corpus path is not a directory")
	}
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		_, _ = io.WriteString(h, "replay-input:")
		_, _ = io.WriteString(h, rel)
		_, _ = io.WriteString(h, fmt.Sprintf(":%o:", entry.Type()))
		if entry.Type()&os.ModeSymlink != 0 {
			link, err := os.Readlink(path)
			if err != nil {
				return err
			}
			_, _ = io.WriteString(h, link)
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("replay input %q is not a regular file, directory, or symlink", rel)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		_, _ = h.Write(b)
		_, _ = io.WriteString(h, "\x00")
		return nil
	})
}

func sha256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func verifyBinaryHash(path, expected string) error {
	got, err := sha256File(path)
	if err != nil {
		return err
	}
	if expected == "" || got != expected {
		return fmt.Errorf("binary hash mismatch (expected %s, got %s)", expected, got)
	}
	return nil
}

// snapshotCandidate copies the complete candidate (including ignored replay
// fixtures) into a private immutable build root. The source fingerprint is
// checked again after copying, so a concurrent worktree mutation cannot be
// silently certified by a build of a mixed tree.
func snapshotCandidate(repoRoot string, expected candidateProvenance) (string, string, error) {
	snapshot, err := os.MkdirTemp("", "chaos-candidate-*")
	if err != nil {
		return "", "", fmt.Errorf("create candidate snapshot: %w", err)
	}
	removeSnapshot := true
	defer func() {
		if removeSnapshot {
			_ = os.RemoveAll(snapshot)
		}
	}()
	if err := copyCandidateTree(repoRoot, snapshot); err != nil {
		return "", "", fmt.Errorf("copy candidate snapshot: %w", err)
	}
	current, err := captureCandidate(repoRoot)
	if err != nil {
		return "", "", fmt.Errorf("recheck candidate after snapshot: %w", err)
	}
	if current.Revision != expected.Revision || current.TreeFingerprint != expected.TreeFingerprint {
		return "", "", fmt.Errorf("candidate changed while snapshotting (before %s/%s, after %s/%s)",
			expected.Revision, expected.TreeFingerprint, current.Revision, current.TreeFingerprint)
	}
	hash, err := sha256Directory(snapshot)
	if err != nil {
		return "", "", fmt.Errorf("hash candidate snapshot: %w", err)
	}
	removeSnapshot = false
	return snapshot, hash, nil
}

func copyCandidateTree(source, destination string) error {
	return filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if rel == ".git" || strings.HasPrefix(rel, ".git"+string(os.PathSeparator)) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		fileInfo, err := os.Lstat(path)
		if err != nil {
			return err
		}
		mode := fileInfo.Mode()
		dst := filepath.Join(destination, rel)
		switch {
		case mode&os.ModeSymlink != 0:
			link, err := os.Readlink(path)
			if err != nil {
				return err
			}
			return os.Symlink(link, dst)
		case mode.IsDir():
			if err := os.Mkdir(dst, mode.Perm()); err != nil {
				return err
			}
			return os.Chmod(dst, mode.Perm())
		case mode.IsRegular():
			in, err := os.Open(path)
			if err != nil {
				return err
			}
			out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode.Perm())
			if err != nil {
				_ = in.Close()
				return err
			}
			_, copyErr := io.Copy(out, in)
			closeInErr := in.Close()
			closeOutErr := out.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeInErr != nil {
				return closeInErr
			}
			if closeOutErr != nil {
				return closeOutErr
			}
			return os.Chmod(dst, mode.Perm())
		default:
			return fmt.Errorf("candidate path %q is not a regular file, directory, or symlink", rel)
		}
	})
}

func sha256Directory(root string) (string, error) {
	h := sha256.New()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		_, _ = io.WriteString(h, "snapshot:")
		_, _ = io.WriteString(h, rel)
		_, _ = io.WriteString(h, fmt.Sprintf(":%o:", entry.Type()))
		if entry.Type()&os.ModeSymlink != 0 {
			link, err := os.Readlink(path)
			if err != nil {
				return err
			}
			_, _ = io.WriteString(h, link)
			_, _ = io.WriteString(h, "\x00")
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("snapshot path %q is not a regular file, directory, or symlink", rel)
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		if _, err := io.Copy(h, f); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		_, _ = io.WriteString(h, "\x00")
		return nil
	})
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func verifySnapshotHash(path, expected string) error {
	got, err := sha256Directory(path)
	if err != nil {
		return err
	}
	if expected == "" || got != expected {
		return fmt.Errorf("candidate snapshot hash mismatch (expected %s, got %s)", expected, got)
	}
	return nil
}
