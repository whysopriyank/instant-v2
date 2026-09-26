package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const (
	chaosPGDataPrefix       = "chaospg-"
	chaosPGDataOwnerPrefix  = ".instant-v2-chaos-owner-"
	chaosPGDataOwnerVersion = 1
)

// These seams keep the destructive boundary testable without ever running a
// chaos process. Production uses the standard library implementations.
var (
	removePGDataTree   = removeMarkerOwnedPGDataTree
	chaosTempRoot      = os.TempDir
	beforePGDataMove   = func(string) {}
	beforePGDataUnlink = func(string) {}
)

// pgBin runs a Postgres binary with Homebrew's postgres@17 bin dir prepended
// to PATH so the harness works regardless of the caller's environment.
func pgBin(name string, args ...string) (string, error) {
	full := append([]string{name}, args...)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, full[0], full[1:]...)
	cmd.Env = append(os.Environ(),
		"PATH=/opt/homebrew/opt/postgresql@17/bin:"+os.Getenv("PATH"))
	out, err := cmd.CombinedOutput()
	if err == nil {
		return string(out), nil
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return string(out), fmt.Errorf("%v: %s", full, strings.TrimSpace(string(out)))
	}
	return "", fmt.Errorf("%s: %w", full[0], err)
}

func pgCtl(pgData string, expected os.FileInfo, args ...string) (string, error) {
	return pgBinInDir(pgData, expected, "pg_ctl", append([]string{"-D", "."}, args...)...)
}

func cleanupCluster(pgData string, expected os.FileInfo) error {
	fmt.Println("== [9] cleanup: stopping throwaway cluster + removing datadir ==")
	if err := verifyPGDataIdentity(pgData, expected); err != nil {
		fmt.Printf("   warning: refusing pg_ctl stop for changed datadir: %v\n", err)
		return err
	}
	var cleanupErr error
	if out, err := pgCtl(pgData, expected, "-m", "fast", "-w", "stop"); err != nil {
		fmt.Printf("   warning: pg_ctl stop: %v\n%s", err, out)
		cleanupErr = fmt.Errorf("pg_ctl stop: %w", err)
	}
	if err := verifyPGDataIdentity(pgData, expected); err != nil {
		fmt.Printf("   warning: refusing datadir removal after stop: %v\n", err)
		return errors.Join(cleanupErr, err)
	}
	if err := mustRm(pgData, expected); err != nil {
		fmt.Printf("   warning: refusing datadir removal: %v\n", err)
		cleanupErr = errors.Join(cleanupErr, err)
	}
	return cleanupErr
}

// safePGDataPath accepts a direct child of the resolved system temp directory
// or a chaos target beneath a mode-0700 per-run root. Requiring the
// chaos-specific basename prevents an operator typo from turning --pg-data
// into a recursive delete of a valuable directory.
func safePGDataPath(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", errors.New("--pg-data must not be empty")
	}
	if !filepath.IsAbs(raw) {
		return "", fmt.Errorf("--pg-data must be absolute: %q", raw)
	}
	tmpRoot, err := filepath.EvalSymlinks(chaosTempRoot())
	if err != nil {
		return "", fmt.Errorf("resolve temp directory: %w", err)
	}
	tmpRoot, err = filepath.Abs(filepath.Clean(tmpRoot))
	if err != nil {
		return "", fmt.Errorf("resolve temp directory: %w", err)
	}
	target := filepath.Clean(raw)
	parent, err := filepath.EvalSymlinks(filepath.Dir(target))
	if err != nil {
		return "", fmt.Errorf("resolve --pg-data parent: %w", err)
	}
	parent, err = filepath.Abs(filepath.Clean(parent))
	if err != nil {
		return "", fmt.Errorf("resolve --pg-data parent: %w", err)
	}
	privateParent := false
	if parent != tmpRoot {
		privateBase := filepath.Base(parent)
		privateInfo, statErr := os.Lstat(parent)
		privateParent = statErr == nil && privateInfo.IsDir() && privateInfo.Mode().Perm() == 0700 &&
			strings.HasPrefix(privateBase, ".instant-v2-chaos-root-") && filepath.Dir(parent) == tmpRoot
		if !privateParent {
			return "", fmt.Errorf("refusing --pg-data outside the system temp directory: %q", raw)
		}
	}
	rawParent := filepath.Clean(filepath.Dir(target))
	if !privateParent && rawParent != filepath.Clean(chaosTempRoot()) && rawParent != tmpRoot {
		if info, statErr := os.Lstat(rawParent); statErr == nil && info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("refusing symlinked --pg-data parent: %q", raw)
		}
	}
	base := filepath.Base(target)
	if base == "." || base == ".." || len(base) <= len(chaosPGDataPrefix) || !strings.HasPrefix(base, chaosPGDataPrefix) {
		return "", fmt.Errorf("refusing non-chaos --pg-data target: %q", raw)
	}
	// Keep the canonical parent in the returned path. This makes /tmp and a
	// symlinked /private/tmp equivalent while still rejecting a symlink target.
	if privateParent {
		return filepath.Join(parent, base), nil
	}
	return filepath.Join(tmpRoot, base), nil
}

type pgDataOwner struct {
	Version int    `json:"version"`
	Base    string `json:"base"`
	Dev     uint64 `json:"dev"`
	Ino     uint64 `json:"ino"`
	Token   string `json:"token"`
	// Sidecar is the identity of the sidecar itself, not serialized.
	Sidecar os.FileInfo `json:"-"`
}

func pgDataOwnerPath(dir string) string {
	return filepath.Join(filepath.Dir(dir), chaosPGDataOwnerPrefix+filepath.Base(dir))
}

func newPGDataOwner(dir string, identity os.FileInfo) (pgDataOwner, error) {
	dev, ino, ok := pgDataDevIno(identity)
	if !ok {
		return pgDataOwner{}, errors.New("cannot record --pg-data device/inode")
	}
	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		return pgDataOwner{}, fmt.Errorf("generate --pg-data ownership token: %w", err)
	}
	return pgDataOwner{
		Version: chaosPGDataOwnerVersion,
		Base:    filepath.Base(dir),
		Dev:     dev,
		Ino:     ino,
		Token:   hex.EncodeToString(token),
	}, nil
}

func validatePGDataOwner(owner pgDataOwner, base string, expected os.FileInfo) error {
	dev, ino, ok := pgDataDevIno(expected)
	if !ok || owner.Version != chaosPGDataOwnerVersion || owner.Base != base || owner.Dev != dev || owner.Ino != ino || owner.Token == "" {
		return errors.New("ownership sidecar mismatch")
	}
	return nil
}

func readPGDataOwner(dir string, expected os.FileInfo) (pgDataOwner, error) {
	target, err := safePGDataPath(dir)
	if err != nil {
		return pgDataOwner{}, err
	}
	sidecar := pgDataOwnerPath(target)
	info, err := os.Lstat(sidecar)
	if err != nil {
		return pgDataOwner{}, fmt.Errorf("ownership sidecar missing: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return pgDataOwner{}, errors.New("ownership sidecar is not a regular file")
	}
	b, err := os.ReadFile(sidecar)
	if err != nil {
		return pgDataOwner{}, fmt.Errorf("read ownership sidecar: %w", err)
	}
	var owner pgDataOwner
	if err := json.Unmarshal(b, &owner); err != nil {
		return pgDataOwner{}, fmt.Errorf("decode ownership sidecar: %w", err)
	}
	if err := validatePGDataOwner(owner, filepath.Base(target), expected); err != nil {
		return pgDataOwner{}, err
	}
	owner.Sidecar = info
	return owner, nil
}

// mustRm removes one marker-owned throwaway cluster. The platform-specific
// remover operates through directory descriptors and rechecks identity before
// unlinking, so a path replacement race cannot redirect recursive deletion.
func mustRm(dir string, expected os.FileInfo) error {
	target, err := safePGDataPath(dir)
	if err != nil {
		return err
	}
	if expected == nil {
		return errors.New("refusing marker cleanup without caller-owned identity")
	}
	info, err := os.Lstat(target)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return errors.New("--pg-data changed during validation")
		}
		return fmt.Errorf("inspect --pg-data: %w", err)
	}
	if !os.SameFile(expected, info) {
		return errors.New("--pg-data changed during validation")
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("refusing --pg-data that is not a directory")
	}
	owner, err := readPGDataOwner(target, expected)
	if err != nil {
		return fmt.Errorf("refusing unowned --pg-data: %w", err)
	}

	beforePGDataMove(target)
	if err := removePGDataTree(target, expected, owner); err != nil {
		return fmt.Errorf("remove marker-owned --pg-data: %w", err)
	}
	return nil
}

func verifyPGDataIdentity(dir string, expected os.FileInfo) error {
	target, err := safePGDataPath(dir)
	if err != nil {
		return err
	}
	got, err := os.Lstat(target)
	if err != nil {
		return fmt.Errorf("inspect --pg-data identity: %w", err)
	}
	if expected == nil || !os.SameFile(expected, got) {
		return errors.New("--pg-data changed during validation")
	}
	if got.Mode()&os.ModeSymlink != 0 || !got.IsDir() {
		return errors.New("--pg-data identity is not a directory")
	}
	return nil
}

type pgDataPreparation struct {
	Path     string
	Identity os.FileInfo
}

// preparePGData validates an existing target (removing it only if it carries
// the sidecar) and permits a new, safely-scoped target for initdb to create.
// Every successful preparation installs and verifies its ownership sidecar
// beside (not inside) PGDATA before initdb is invoked, making failed
// initialization safely reclaimable without confusing initdb with a prebuilt
// cluster.
func preparePGData(raw string) (pgDataPreparation, error) {
	requested, err := safePGDataPath(raw)
	if err != nil {
		return pgDataPreparation{}, err
	}
	target := requested
	var identity os.FileInfo
	if existing, statErr := os.Lstat(requested); statErr == nil {
		identity = existing
		if existing.Mode()&os.ModeSymlink != 0 || !existing.IsDir() {
			return pgDataPreparation{}, errors.New("refusing existing --pg-data that is not a directory")
		}
		sidecarPath := pgDataOwnerPath(target)
		sidecarInfo, sidecarErr := os.Lstat(sidecarPath)
		switch {
		case sidecarErr == nil:
			if sidecarInfo.Mode()&os.ModeSymlink != 0 || !sidecarInfo.Mode().IsRegular() {
				return pgDataPreparation{}, errors.New("refusing invalid --pg-data ownership sidecar")
			}
			if _, err := readPGDataOwner(target, existing); err != nil {
				return pgDataPreparation{}, fmt.Errorf("refusing unowned --pg-data: %w", err)
			}
			if err := mustRm(target, existing); err != nil {
				return pgDataPreparation{}, err
			}
		case errors.Is(sidecarErr, os.ErrNotExist):
			return pgDataPreparation{}, errors.New("refusing pre-existing unowned --pg-data")
		default:
			return pgDataPreparation{}, fmt.Errorf("inspect --pg-data ownership sidecar: %w", sidecarErr)
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return pgDataPreparation{}, fmt.Errorf("inspect --pg-data: %w", statErr)
	} else {
		// A predictable child of the shared system temp directory cannot be
		// safely bound between mkdir and open. Put a fresh target beneath a
		// mode-0700 per-run root, whose pathname is inaccessible to other users.
		privateRoot, err := os.MkdirTemp(filepath.Dir(requested), ".instant-v2-chaos-root-*")
		if err != nil {
			return pgDataPreparation{}, fmt.Errorf("create private --pg-data root: %w", err)
		}
		target = filepath.Join(privateRoot, filepath.Base(requested))
		if err := os.Mkdir(target, 0700); err != nil {
			return pgDataPreparation{}, fmt.Errorf("claim --pg-data directory: %w", err)
		}
	}
	if identity == nil {
		var statErr error
		identity, statErr = os.Lstat(target)
		if statErr != nil {
			return pgDataPreparation{}, fmt.Errorf("stat claimed --pg-data: %w", statErr)
		}
	}
	if err := verifyPGDataIdentity(target, identity); err != nil {
		return pgDataPreparation{}, fmt.Errorf("verify claimed --pg-data: %w", err)
	}
	owner, err := newPGDataOwner(target, identity)
	if err != nil {
		return pgDataPreparation{}, err
	}
	if err := writePGDataOwner(target, identity, owner); err != nil {
		return pgDataPreparation{}, fmt.Errorf("claim --pg-data ownership: %w", err)
	}
	return pgDataPreparation{Path: target, Identity: identity}, nil
}

func writePGDataOwner(dir string, expected os.FileInfo, owner pgDataOwner) error {
	target, err := safePGDataPath(dir)
	if err != nil {
		return err
	}
	return writePGDataOwnerAt(target, expected, owner)
}

type postmasterProcessIdentity struct {
	PID       int    `json:"pid"`
	StartTime int64  `json:"start_time"`
	DataDir   string `json:"data_dir"`
	Port      int    `json:"port"`
}

func readPostmasterIdentity(pgData string, expected os.FileInfo) (postmasterProcessIdentity, error) {
	target, err := safePGDataPath(pgData)
	if err != nil {
		return postmasterProcessIdentity{}, err
	}
	return readPostmasterIdentityDescriptor(target, expected)
}
