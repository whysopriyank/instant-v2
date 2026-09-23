//go:build darwin || linux

package corpus

import (
	cryptorand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

func openRootAndResolveComponents(evalPath string) (int, []string, error) {
	var rootFD int
	var err error
	var parts []string
	if filepath.IsAbs(evalPath) {
		rootFD, err = unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		if err != nil {
			return -1, nil, fmt.Errorf("open root directory: %w", err)
		}
		parts = strings.Split(evalPath, string(filepath.Separator))
	} else {
		rootFD, err = unix.Open(".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		if err != nil {
			return -1, nil, fmt.Errorf("open working directory: %w", err)
		}
		parts = strings.Split(evalPath, string(filepath.Separator))
	}
	var components []string
	for _, p := range parts {
		if p != "" && p != "." {
			components = append(components, p)
		}
	}
	return rootFD, components, nil
}

func reserveOutputDirPlatform(dir string) (*ReservedDir, error) {
	return reserveOutputDirWithHooks(dir, reserveHooks{})
}

func reserveOutputDirWithHooks(dir string, hooks reserveHooks) (*ReservedDir, error) {
	evalPath, err := validateFreshOutputDirPath(dir)
	if err != nil {
		return nil, err
	}

	rootFD, components, err := openRootAndResolveComponents(evalPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if rootFD >= 0 {
			_ = unix.Close(rootFD)
		}
	}()

	n := len(components)
	if n == 0 {
		return nil, fmt.Errorf("invalid output directory %q", dir)
	}

	currFD := rootFD
	rootFD = -1

	// Traverse ancestors descriptor-relatively with O_NOFOLLOW
	for i := 0; i < n-1; i++ {
		comp := components[i]
		var st unix.Stat_t
		err := unix.Fstatat(currFD, comp, &st, unix.AT_SYMLINK_NOFOLLOW)
		if err != nil {
			if errors.Is(err, unix.ENOENT) {
				if mkErr := unix.Mkdirat(currFD, comp, 0700); mkErr != nil && !errors.Is(mkErr, unix.EEXIST) {
					_ = unix.Close(currFD)
					return nil, fmt.Errorf("create ancestor directory %q: %w", comp, mkErr)
				}
				if err = unix.Fstatat(currFD, comp, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
					_ = unix.Close(currFD)
					return nil, fmt.Errorf("stat ancestor directory %q: %w", comp, err)
				}
			} else {
				_ = unix.Close(currFD)
				return nil, fmt.Errorf("stat ancestor %q: %w", comp, err)
			}
		}

		if (st.Mode & unix.S_IFMT) == unix.S_IFLNK {
			_ = unix.Close(currFD)
			return nil, fmt.Errorf("symlink component or redirection not allowed in output path: %q", comp)
		}
		if (st.Mode & unix.S_IFMT) != unix.S_IFDIR {
			_ = unix.Close(currFD)
			return nil, fmt.Errorf("path component %q exists and is not a directory", comp)
		}

		nextFD, err := unix.Openat(currFD, comp, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			_ = unix.Close(currFD)
			return nil, fmt.Errorf("open ancestor directory %q: %w", comp, err)
		}
		_ = unix.Close(currFD)
		currFD = nextFD
	}

	parentFD := currFD
	targetBase := components[n-1]

	var targetSt unix.Stat_t
	err = unix.Fstatat(parentFD, targetBase, &targetSt, unix.AT_SYMLINK_NOFOLLOW)
	if err == nil {
		_ = unix.Close(parentFD)
		if (targetSt.Mode & unix.S_IFMT) == unix.S_IFLNK {
			return nil, fmt.Errorf("output directory %q is a symlink (must be a fresh path)", dir)
		}
		if (targetSt.Mode & unix.S_IFMT) == unix.S_IFDIR {
			return nil, fmt.Errorf("output directory %q already exists (must be fresh)", dir)
		}
		return nil, fmt.Errorf("output path %q exists and is not a directory", dir)
	} else if !errors.Is(err, unix.ENOENT) {
		_ = unix.Close(parentFD)
		return nil, fmt.Errorf("stat output directory %q: %w", dir, err)
	}

	if hooks.afterStatBeforeMkdir != nil {
		if err := hooks.afterStatBeforeMkdir(); err != nil {
			_ = unix.Close(parentFD)
			return nil, fmt.Errorf("injected after-stat hook failure: %w", err)
		}
	}

	if err := unix.Mkdirat(parentFD, targetBase, 0700); err != nil {
		_ = unix.Close(parentFD)
		if errors.Is(err, unix.EEXIST) || errors.Is(err, syscall.EEXIST) || errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("output directory %q already exists (must be fresh, reservation race): %w", dir, err)
		}
		return nil, fmt.Errorf("reserve output directory %q: %w", dir, err)
	}

	dirFD, err := unix.Openat(parentFD, targetBase, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		_ = unix.Close(parentFD)
		// Fail-closed: no path-based cleanup
		return nil, fmt.Errorf("open reserved output directory: %w", err)
	}

	var parentSt unix.Stat_t
	if err := unix.Fstat(parentFD, &parentSt); err != nil {
		_ = unix.Close(dirFD)
		_ = unix.Close(parentFD)
		return nil, fmt.Errorf("stat reserved parent directory: %w", err)
	}

	var dirSt unix.Stat_t
	if err := unix.Fstat(dirFD, &dirSt); err != nil {
		_ = unix.Close(dirFD)
		_ = unix.Close(parentFD)
		return nil, fmt.Errorf("stat reserved output directory: %w", err)
	}
	if (dirSt.Mode & unix.S_IFMT) != unix.S_IFDIR {
		_ = unix.Close(dirFD)
		_ = unix.Close(parentFD)
		return nil, fmt.Errorf("reserved output path %q is not a directory", dir)
	}

	var verifySt unix.Stat_t
	if err := unix.Fstatat(parentFD, targetBase, &verifySt, unix.AT_SYMLINK_NOFOLLOW); err != nil ||
		uint64(verifySt.Dev) != uint64(dirSt.Dev) || uint64(verifySt.Ino) != uint64(dirSt.Ino) {
		_ = unix.Close(dirFD)
		_ = unix.Close(parentFD)
		return nil, errors.New("reserved directory identity mismatch during reservation")
	}

	parentFile := os.NewFile(uintptr(parentFD), filepath.Dir(evalPath))
	dirFile := os.NewFile(uintptr(dirFD), evalPath)

	return &ReservedDir{
		path:       evalPath,
		dirFile:    dirFile,
		parentFile: parentFile,
		dev:        uint64(dirSt.Dev),
		ino:        uint64(dirSt.Ino),
		parentDev:  uint64(parentSt.Dev),
		parentIno:  uint64(parentSt.Ino),
		baseName:   targetBase,
	}, nil
}

func checkFreshOutputDirPlatform(dir string) error {
	evalPath, err := validateFreshOutputDirPath(dir)
	if err != nil {
		return err
	}

	rootFD, components, err := openRootAndResolveComponents(evalPath)
	if err != nil {
		return err
	}
	defer func() {
		if rootFD >= 0 {
			_ = unix.Close(rootFD)
		}
	}()

	n := len(components)
	if n == 0 {
		return fmt.Errorf("invalid output directory %q", dir)
	}

	currFD := rootFD
	rootFD = -1

	for i := 0; i < n-1; i++ {
		comp := components[i]
		var st unix.Stat_t
		err := unix.Fstatat(currFD, comp, &st, unix.AT_SYMLINK_NOFOLLOW)
		if err != nil {
			if errors.Is(err, unix.ENOENT) {
				_ = unix.Close(currFD)
				return nil
			}
			_ = unix.Close(currFD)
			return fmt.Errorf("stat ancestor %q: %w", comp, err)
		}

		if (st.Mode & unix.S_IFMT) == unix.S_IFLNK {
			_ = unix.Close(currFD)
			return fmt.Errorf("symlink component or redirection not allowed in output path: %q", comp)
		}
		if (st.Mode & unix.S_IFMT) != unix.S_IFDIR {
			_ = unix.Close(currFD)
			return fmt.Errorf("path component %q exists and is not a directory", comp)
		}

		nextFD, err := unix.Openat(currFD, comp, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			_ = unix.Close(currFD)
			return fmt.Errorf("open ancestor directory %q: %w", comp, err)
		}
		_ = unix.Close(currFD)
		currFD = nextFD
	}

	parentFD := currFD
	defer unix.Close(parentFD)
	targetBase := components[n-1]

	var targetSt unix.Stat_t
	err = unix.Fstatat(parentFD, targetBase, &targetSt, unix.AT_SYMLINK_NOFOLLOW)
	if err == nil {
		if (targetSt.Mode & unix.S_IFMT) == unix.S_IFLNK {
			return fmt.Errorf("output directory %q is a symlink (must be a fresh path)", dir)
		}
		if (targetSt.Mode & unix.S_IFMT) == unix.S_IFDIR {
			return fmt.Errorf("output directory %q already exists (must be fresh)", dir)
		}
		return fmt.Errorf("output path %q exists and is not a directory", dir)
	} else if !errors.Is(err, unix.ENOENT) {
		return fmt.Errorf("stat output directory %q: %w", dir, err)
	}

	return nil
}

func writeEvidenceInDir(d *ReservedDir, target string, value any, hooks writeHooks) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return errors.New("reserved directory is closed")
	}

	cleanTarget := filepath.Clean(target)
	base := filepath.Base(cleanTarget)
	if cleanTarget != base {
		rel, err := filepath.Rel(d.path, cleanTarget)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || strings.Contains(rel, string(filepath.Separator)) {
			return fmt.Errorf("evidence file %q escapes reserved directory %q", target, d.path)
		}
	}
	if base == "." || base == string(filepath.Separator) || strings.ContainsAny(base, `/\`) {
		return fmt.Errorf("invalid evidence filename %q", target)
	}

	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	payload := append(b, '\n')

	dirFD := int(d.dirFile.Fd())

	// Verify pinned directory identity
	var dirSt unix.Stat_t
	if err := unix.Fstat(dirFD, &dirSt); err != nil || uint64(dirSt.Dev) != d.dev || uint64(dirSt.Ino) != d.ino {
		return fmt.Errorf("reserved directory %q was replaced or swapped", d.path)
	}

	// If parent is pinned, verify directory has not been unlinked or replaced under parent
	if d.parentFile != nil {
		var pSt unix.Stat_t
		if err := unix.Fstatat(int(d.parentFile.Fd()), d.baseName, &pSt, unix.AT_SYMLINK_NOFOLLOW); err != nil ||
			uint64(pSt.Dev) != d.dev || uint64(pSt.Ino) != d.ino {
			return fmt.Errorf("reserved directory %q was replaced or swapped", d.path)
		}
	}

	if hooks.beforeTempCreate != nil {
		if err := hooks.beforeTempCreate(); err != nil {
			return fmt.Errorf("injected temp create failure: %w", err)
		}
		// Post-hook check: re-verify directory identity has not been swapped under parent
		if d.parentFile != nil {
			var pSt unix.Stat_t
			if err := unix.Fstatat(int(d.parentFile.Fd()), d.baseName, &pSt, unix.AT_SYMLINK_NOFOLLOW); err != nil ||
				uint64(pSt.Dev) != d.dev || uint64(pSt.Ino) != d.ino {
				return fmt.Errorf("reserved directory %q was replaced or swapped", d.path)
			}
		}
	}

	// Generate random temp filename
	var rnd [16]byte
	if _, err := cryptorand.Read(rnd[:]); err != nil {
		return fmt.Errorf("generate temp filename: %w", err)
	}
	tmpName := fmt.Sprintf(".tmp-evidence-%x", rnd)

	// Create temp evidence with openat on pinned dir FD (O_CREAT|O_EXCL|O_NOFOLLOW, 0600)
	tmpFD, err := unix.Openat(dirFD, tmpName, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return fmt.Errorf("create temporary evidence file: %w", err)
	}

	tmpFile := os.NewFile(uintptr(tmpFD), tmpName)
	tmpClosed := false

	defer func() {
		if !tmpClosed {
			_ = tmpFile.Close()
		}
	}()

	if hooks.beforeWrite != nil {
		if err := hooks.beforeWrite(); err != nil {
			return fmt.Errorf("injected write failure: %w", err)
		}
	}
	if _, err := tmpFile.Write(payload); err != nil {
		return fmt.Errorf("write temporary evidence: %w", err)
	}

	if hooks.beforeSync != nil {
		if err := hooks.beforeSync(); err != nil {
			return fmt.Errorf("injected sync failure: %w", err)
		}
	}
	if err := tmpFile.Sync(); err != nil {
		return fmt.Errorf("sync temporary evidence: %w", err)
	}

	if hooks.beforeClose != nil {
		if err := hooks.beforeClose(); err != nil {
			return fmt.Errorf("injected close failure: %w", err)
		}
	}
	if err := tmpFile.Close(); err != nil {
		tmpClosed = true
		return fmt.Errorf("close temporary evidence: %w", err)
	}
	tmpClosed = true

	// Publish atomically with no-replace semantics
	if err := renameNoReplace(dirFD, tmpName, base); err != nil {
		if errors.Is(err, os.ErrExist) || errors.Is(err, unix.EEXIST) {
			return fmt.Errorf("evidence file %q already exists (write-once)", base)
		}
		return fmt.Errorf("publish evidence %q: %w", base, err)
	}
	if hooks.beforeDirSync != nil {
		if err := hooks.beforeDirSync(); err != nil {
			return fmt.Errorf("injected directory sync failure: %w; final artifact is untrusted and incomplete", err)
		}
	}
	if err := d.dirFile.Sync(); err != nil {
		return fmt.Errorf("sync evidence directory: %w; final artifact is untrusted and incomplete", err)
	}

	return nil
}

func openOrCreateDirPlatform(dir string) (*ReservedDir, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, errors.New("directory is required")
	}
	cleaned := filepath.Clean(dir)
	if cleaned == "." || cleaned == string(filepath.Separator) {
		return nil, fmt.Errorf("invalid directory %q", dir)
	}
	if !filepath.IsAbs(cleaned) && (cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator))) {
		return nil, fmt.Errorf("directory escapes current path: %q", dir)
	}
	parts := strings.Split(cleaned, string(filepath.Separator))
	for _, p := range parts {
		if p == ".." {
			return nil, fmt.Errorf("directory escapes current path: %q", dir)
		}
	}

	evalPath := cleaned
	if runtime.GOOS == "darwin" && filepath.IsAbs(cleaned) {
		for _, sysPrefix := range []string{"/var", "/tmp", "/etc"} {
			if evalPath == sysPrefix || strings.HasPrefix(evalPath, sysPrefix+"/") {
				evalPath = "/private" + evalPath
				break
			}
		}
	}

	rootFD, components, err := openRootAndResolveComponents(evalPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if rootFD >= 0 {
			_ = unix.Close(rootFD)
		}
	}()

	n := len(components)
	if n == 0 {
		return nil, fmt.Errorf("invalid directory %q", dir)
	}

	currFD := rootFD
	rootFD = -1

	for i := 0; i < n-1; i++ {
		comp := components[i]
		var st unix.Stat_t
		err := unix.Fstatat(currFD, comp, &st, unix.AT_SYMLINK_NOFOLLOW)
		if err != nil {
			if errors.Is(err, unix.ENOENT) {
				if mkErr := unix.Mkdirat(currFD, comp, 0700); mkErr != nil && !errors.Is(mkErr, unix.EEXIST) {
					_ = unix.Close(currFD)
					return nil, fmt.Errorf("create ancestor directory %q: %w", comp, mkErr)
				}
				if err = unix.Fstatat(currFD, comp, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
					_ = unix.Close(currFD)
					return nil, fmt.Errorf("stat ancestor directory %q: %w", comp, err)
				}
			} else {
				_ = unix.Close(currFD)
				return nil, fmt.Errorf("stat ancestor %q: %w", comp, err)
			}
		}

		if (st.Mode & unix.S_IFMT) == unix.S_IFLNK {
			_ = unix.Close(currFD)
			return nil, fmt.Errorf("symlink component or redirection not allowed in output path: %q", comp)
		}
		if (st.Mode & unix.S_IFMT) != unix.S_IFDIR {
			_ = unix.Close(currFD)
			return nil, fmt.Errorf("path component %q exists and is not a directory", comp)
		}

		nextFD, err := unix.Openat(currFD, comp, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			_ = unix.Close(currFD)
			return nil, fmt.Errorf("open ancestor directory %q: %w", comp, err)
		}
		_ = unix.Close(currFD)
		currFD = nextFD
	}

	parentFD := currFD
	targetBase := components[n-1]

	var targetSt unix.Stat_t
	err = unix.Fstatat(parentFD, targetBase, &targetSt, unix.AT_SYMLINK_NOFOLLOW)
	if err != nil {
		if errors.Is(err, unix.ENOENT) {
			if mkErr := unix.Mkdirat(parentFD, targetBase, 0700); mkErr != nil && !errors.Is(mkErr, unix.EEXIST) {
				_ = unix.Close(parentFD)
				return nil, fmt.Errorf("create evidence directory %q: %w", dir, mkErr)
			}
		} else {
			_ = unix.Close(parentFD)
			return nil, fmt.Errorf("stat evidence directory %q: %w", dir, err)
		}
	} else {
		if (targetSt.Mode & unix.S_IFMT) == unix.S_IFLNK {
			_ = unix.Close(parentFD)
			return nil, fmt.Errorf("evidence directory cannot be a symlink: %q", dir)
		}
		if (targetSt.Mode & unix.S_IFMT) != unix.S_IFDIR {
			_ = unix.Close(parentFD)
			return nil, fmt.Errorf("evidence path parent is not a directory: %q", dir)
		}
	}

	dirFD, err := unix.Openat(parentFD, targetBase, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		_ = unix.Close(parentFD)
		return nil, fmt.Errorf("open evidence directory: %w", err)
	}

	var parentSt unix.Stat_t
	if err := unix.Fstat(parentFD, &parentSt); err != nil {
		_ = unix.Close(dirFD)
		_ = unix.Close(parentFD)
		return nil, fmt.Errorf("stat evidence parent directory: %w", err)
	}
	var dirSt unix.Stat_t
	if err := unix.Fstat(dirFD, &dirSt); err != nil {
		_ = unix.Close(dirFD)
		_ = unix.Close(parentFD)
		return nil, fmt.Errorf("stat evidence directory: %w", err)
	}
	if (dirSt.Mode & unix.S_IFMT) != unix.S_IFDIR {
		_ = unix.Close(dirFD)
		_ = unix.Close(parentFD)
		return nil, fmt.Errorf("evidence path parent is not a directory: %q", dir)
	}

	parentFile := os.NewFile(uintptr(parentFD), filepath.Dir(evalPath))
	dirFile := os.NewFile(uintptr(dirFD), evalPath)

	return &ReservedDir{
		path:       evalPath,
		dirFile:    dirFile,
		parentFile: parentFile,
		dev:        uint64(dirSt.Dev),
		ino:        uint64(dirSt.Ino),
		parentDev:  uint64(parentSt.Dev),
		parentIno:  uint64(parentSt.Ino),
		baseName:   targetBase,
	}, nil
}

func writeEvidenceWithHooks(path string, value any, hooks writeHooks) error {
	cleaned := filepath.Clean(path)
	if cleaned != path || path == "." || path == string(filepath.Separator) {
		return fmt.Errorf("invalid evidence path %q", path)
	}
	if !filepath.IsAbs(cleaned) && (cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator))) {
		return fmt.Errorf("evidence path escapes output directory: %q", path)
	}
	dir := filepath.Dir(cleaned)
	base := filepath.Base(cleaned)

	reserved, err := openOrCreateDirPlatform(dir)
	if err != nil {
		return err
	}
	defer reserved.Close()

	return writeEvidenceInDir(reserved, base, value, hooks)
}

// WriteRawEvidence publishes one private, write-once raw-byte artifact into
// this reserved directory using the exact descriptor-relative discipline of
// WriteEvidence: pinned-FD identity checks, temporary openat with
// O_CREAT|O_EXCL|O_NOFOLLOW mode 0600, fsync, atomic no-replace publication,
// and directory fsync. Temp creation, writes, sync, and publication never
// touch the output pathname, so a swap of the visible directory between pin
// verifications cannot land payload in a replacement. Failed publication
// does not attempt ambiguous named-file cleanup.
func (d *ReservedDir) WriteRawEvidence(filename string, payload []byte) error {
	return writeRawEvidenceInDir(d, filename, payload, writeHooks{})
}

func writeRawEvidenceInDir(d *ReservedDir, target string, payload []byte, hooks writeHooks) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return errors.New("reserved directory is closed")
	}

	cleanTarget := filepath.Clean(target)
	base := filepath.Base(cleanTarget)
	if cleanTarget != base {
		rel, err := filepath.Rel(d.path, cleanTarget)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || strings.Contains(rel, string(filepath.Separator)) {
			return fmt.Errorf("evidence file %q escapes reserved directory %q", target, d.path)
		}
	}
	if base == "." || base == string(filepath.Separator) || strings.ContainsAny(base, `/\`) {
		return fmt.Errorf("invalid evidence filename %q", target)
	}

	dirFD := int(d.dirFile.Fd())

	// Verify pinned directory identity
	var dirSt unix.Stat_t
	if err := unix.Fstat(dirFD, &dirSt); err != nil || uint64(dirSt.Dev) != d.dev || uint64(dirSt.Ino) != d.ino {
		return fmt.Errorf("reserved directory %q was replaced or swapped", d.path)
	}

	// If parent is pinned, verify directory has not been unlinked or replaced under parent
	if d.parentFile != nil {
		var pSt unix.Stat_t
		if err := unix.Fstatat(int(d.parentFile.Fd()), d.baseName, &pSt, unix.AT_SYMLINK_NOFOLLOW); err != nil ||
			uint64(pSt.Dev) != d.dev || uint64(pSt.Ino) != d.ino {
			return fmt.Errorf("reserved directory %q was replaced or swapped", d.path)
		}
	}

	if hooks.beforeTempCreate != nil {
		if err := hooks.beforeTempCreate(); err != nil {
			return fmt.Errorf("injected temp create failure: %w", err)
		}
		// Post-hook check: re-verify directory identity has not been swapped under parent
		if d.parentFile != nil {
			var pSt unix.Stat_t
			if err := unix.Fstatat(int(d.parentFile.Fd()), d.baseName, &pSt, unix.AT_SYMLINK_NOFOLLOW); err != nil ||
				uint64(pSt.Dev) != d.dev || uint64(pSt.Ino) != d.ino {
				return fmt.Errorf("reserved directory %q was replaced or swapped", d.path)
			}
		}
	}

	// Generate random temp filename
	var rnd [16]byte
	if _, err := cryptorand.Read(rnd[:]); err != nil {
		return fmt.Errorf("generate temp filename: %w", err)
	}
	tmpName := fmt.Sprintf(".tmp-evidence-%x", rnd)

	// Create temp evidence with openat on pinned dir FD (O_CREAT|O_EXCL|O_NOFOLLOW, 0600)
	tmpFD, err := unix.Openat(dirFD, tmpName, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return fmt.Errorf("create temporary evidence file: %w", err)
	}

	tmpFile := os.NewFile(uintptr(tmpFD), tmpName)
	tmpClosed := false

	defer func() {
		if !tmpClosed {
			_ = tmpFile.Close()
		}
	}()

	if hooks.beforeWrite != nil {
		if err := hooks.beforeWrite(); err != nil {
			return fmt.Errorf("injected write failure: %w", err)
		}
	}
	if _, err := tmpFile.Write(payload); err != nil {
		return fmt.Errorf("write temporary evidence: %w", err)
	}

	if hooks.beforeSync != nil {
		if err := hooks.beforeSync(); err != nil {
			return fmt.Errorf("injected sync failure: %w", err)
		}
	}
	if err := tmpFile.Sync(); err != nil {
		return fmt.Errorf("sync temporary evidence: %w", err)
	}

	if hooks.beforeClose != nil {
		if err := hooks.beforeClose(); err != nil {
			return fmt.Errorf("injected close failure: %w", err)
		}
	}
	if err := tmpFile.Close(); err != nil {
		tmpClosed = true
		return fmt.Errorf("close temporary evidence: %w", err)
	}
	tmpClosed = true

	// Publish atomically with no-replace semantics
	if err := renameNoReplace(dirFD, tmpName, base); err != nil {
		if errors.Is(err, os.ErrExist) || errors.Is(err, unix.EEXIST) {
			return fmt.Errorf("evidence file %q already exists (write-once)", base)
		}
		return fmt.Errorf("publish evidence %q: %w", base, err)
	}
	if hooks.beforeDirSync != nil {
		if err := hooks.beforeDirSync(); err != nil {
			return fmt.Errorf("injected directory sync failure: %w; final artifact is untrusted and incomplete", err)
		}
	}
	if err := d.dirFile.Sync(); err != nil {
		return fmt.Errorf("sync evidence directory: %w; final artifact is untrusted and incomplete", err)
	}

	return nil
}
