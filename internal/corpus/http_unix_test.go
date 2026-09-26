//go:build darwin || linux

package corpus

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
)

func TestCheckFreshOutputDir(t *testing.T) {
	if err := CheckFreshOutputDir(""); err == nil {
		t.Fatal("empty output dir was accepted")
	}
	if err := CheckFreshOutputDir("."); err == nil {
		t.Fatal("'.' output dir was accepted")
	}
	if err := CheckFreshOutputDir(".."); err == nil {
		t.Fatal("'..' output dir was accepted")
	}
	if err := CheckFreshOutputDir("../escape"); err == nil {
		t.Fatal("escaping output dir was accepted")
	}

	temp := t.TempDir()
	freshPath := filepath.Join(temp, "fresh")
	if err := CheckFreshOutputDir(freshPath); err != nil {
		t.Fatalf("non-existent dir should be fresh: %v", err)
	}

	emptyDir := filepath.Join(temp, "empty")
	if err := os.Mkdir(emptyDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := CheckFreshOutputDir(emptyDir); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("existing empty dir was accepted: %v", err)
	}

	symlinkDir := filepath.Join(temp, "symlink_dir")
	if err := os.Symlink(emptyDir, symlinkDir); err != nil {
		t.Fatal(err)
	}
	if err := CheckFreshOutputDir(symlinkDir); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("symlink output dir was accepted: %v", err)
	}

	childUnderSymlink := filepath.Join(symlinkDir, "child")
	if err := CheckFreshOutputDir(childUnderSymlink); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("output dir under symlink parent was accepted: %v", err)
	}

	nonFreshDir := filepath.Join(temp, "nonfresh")
	if err := os.Mkdir(nonFreshDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nonFreshDir, "file.txt"), []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := CheckFreshOutputDir(nonFreshDir); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("non-fresh dir was accepted: %v", err)
	}

	filePath := filepath.Join(temp, "afile")
	if err := os.WriteFile(filePath, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := CheckFreshOutputDir(filePath); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("file path was accepted as dir: %v", err)
	}
}

func TestReserveOutputDirSymlinkAndParentRedirectionRejection(t *testing.T) {
	temp := t.TempDir()

	// 1. Direct symlink output path
	realDir := filepath.Join(temp, "real_target")
	if err := os.Mkdir(realDir, 0700); err != nil {
		t.Fatal(err)
	}
	symlinkPath := filepath.Join(temp, "symlink_out")
	if err := os.Symlink(realDir, symlinkPath); err != nil {
		t.Fatal(err)
	}
	if _, err := ReserveOutputDir(symlinkPath); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("direct symlink was accepted by ReserveOutputDir: %v", err)
	}

	// 2. Parent is a symlink
	childUnderSymlink := filepath.Join(symlinkPath, "child")
	if _, err := ReserveOutputDir(childUnderSymlink); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("child under symlink parent was accepted by ReserveOutputDir: %v", err)
	}

	// 3. Existing empty directory
	emptyDir := filepath.Join(temp, "existing_empty")
	if err := os.Mkdir(emptyDir, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := ReserveOutputDir(emptyDir); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("existing empty dir was accepted by ReserveOutputDir: %v", err)
	}
}

func TestReserveOutputDirDirectorySwapRejection(t *testing.T) {
	temp := t.TempDir()
	outDir := filepath.Join(temp, "reserved")

	reserved, err := ReserveOutputDir(outDir)
	if err != nil {
		t.Fatalf("failed to reserve output dir: %v", err)
	}
	defer func() { _ = reserved.Close() }()

	// Replace the directory on disk to give it a different inode identity
	if err := os.Remove(outDir); err != nil {
		t.Fatalf("failed to remove reserved dir: %v", err)
	}
	if err := os.Mkdir(outDir, 0700); err != nil {
		t.Fatalf("failed to recreate dir with new identity: %v", err)
	}

	// Writing evidence through the pinned reservation must fail closed because identity changed
	err = reserved.WriteEvidence("test.json", map[string]string{"foo": "bar"})
	if err == nil || (!strings.Contains(err.Error(), "replaced") && !strings.Contains(err.Error(), "swapped")) {
		t.Fatalf("directory swap was not rejected: %v", err)
	}

	// Replace with a symlink to another directory
	otherDir := filepath.Join(temp, "other")
	if err := os.Mkdir(otherDir, 0700); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(outDir)
	if err := os.Symlink(otherDir, outDir); err != nil {
		t.Fatal(err)
	}
	err = reserved.WriteEvidence("test2.json", map[string]string{"foo": "bar"})
	if err == nil || (!strings.Contains(err.Error(), "symlink") && !strings.Contains(err.Error(), "swapped") && !strings.Contains(err.Error(), "replaced")) {
		t.Fatalf("symlink swap was not rejected: %v", err)
	}
	// Verify no file was published into otherDir
	if _, err := os.Stat(filepath.Join(otherDir, "test2.json")); err == nil {
		t.Fatal("file was written through swapped symlink target!")
	}
}

func TestReserveOutputDirConcurrentAndDuplicate(t *testing.T) {
	temp := t.TempDir()
	targetPath := filepath.Join(temp, "concurrent-res")

	// 10 concurrent goroutines racing to reserve the same directory path
	const workers = 10
	var wg sync.WaitGroup
	var successCount int64
	var errCount int64

	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, err := ReserveOutputDir(targetPath)
			if err == nil {
				atomic.AddInt64(&successCount, 1)
				_ = res.Close()
			} else {
				atomic.AddInt64(&errCount, 1)
			}
		}()
	}

	close(start)
	wg.Wait()

	if successCount != 1 {
		t.Fatalf("expected exactly 1 successful reservation, got %d (errors: %d)", successCount, errCount)
	}
	if errCount != workers-1 {
		t.Fatalf("expected %d failed reservations, got %d", workers-1, errCount)
	}

	// Duplicate reservation after already reserved must fail
	if _, err := ReserveOutputDir(targetPath); err == nil {
		t.Fatal("duplicate reservation succeeded on existing directory")
	}
}

func TestReserveOutputDirMkdiratEEXISTFailClosed(t *testing.T) {
	temp := t.TempDir()
	targetPath := filepath.Join(temp, "race-dir")

	// Verify targetPath does not exist initially
	if _, err := os.Stat(targetPath); !os.IsNotExist(err) {
		t.Fatalf("target dir already exists before test: %v", err)
	}

	// Hook simulates a race where another process creates the directory
	// immediately after Fstatat verified ENOENT, but before Mkdirat is called.
	hookExecuted := false
	hooks := reserveHooks{
		afterStatBeforeMkdir: func() error {
			hookExecuted = true
			if err := os.Mkdir(targetPath, 0700); err != nil {
				return err
			}
			return nil
		},
	}

	res, err := reserveOutputDirWithHooks(targetPath, hooks)
	if !hookExecuted {
		t.Fatal("afterStatBeforeMkdir hook was not executed")
	}
	if res != nil {
		_ = res.Close()
		t.Fatal("reservation succeeded despite Mkdirat EEXIST race; violates fresh no-replace reservation")
	}
	if err == nil {
		t.Fatal("expected error on Mkdirat EEXIST race, got nil")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected already-exists error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "race") {
		t.Fatalf("expected race error message, got: %v", err)
	}
	if !errors.Is(err, os.ErrExist) && !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("expected errors.Is(err, os.ErrExist), got: %v", err)
	}

	// Verify no path-based cleanup deleted the directory created during the race
	fi, err := os.Stat(targetPath)
	if err != nil {
		t.Fatalf("directory created during race was deleted by reservation failure: %v", err)
	}
	if !fi.IsDir() {
		t.Fatalf("expected directory, got: %v", fi.Mode())
	}
}

func TestWriteEvidenceConcurrentWriteOnce(t *testing.T) {
	temp := t.TempDir()
	outDir := filepath.Join(temp, "writeonce-dir")
	reserved, err := ReserveOutputDir(outDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reserved.Close() }()

	const workers = 10
	var wg sync.WaitGroup
	var successCount int64
	var collisionCount int64

	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		idx := i
		go func() {
			defer wg.Done()
			<-start
			err := reserved.WriteEvidence("record.json", map[string]int{"worker": idx})
			if err == nil {
				atomic.AddInt64(&successCount, 1)
			} else if strings.Contains(err.Error(), "already exists") {
				atomic.AddInt64(&collisionCount, 1)
			}
		}()
	}

	close(start)
	wg.Wait()

	if successCount != 1 {
		t.Fatalf("expected exactly 1 winner in concurrent write-once, got %d", successCount)
	}
	if collisionCount != workers-1 {
		t.Fatalf("expected %d collisions, got %d", workers-1, collisionCount)
	}

	// Verify the published file has mode 0600
	finalPath := filepath.Join(outDir, "record.json")
	fi, err := os.Stat(finalPath)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0600 {
		t.Fatalf("expected mode 0600, got %#o", fi.Mode().Perm())
	}
}

func TestWriteEvidenceInjectedFailuresNoPartialArtifact(t *testing.T) {
	temp := t.TempDir()

	injectedErr := errors.New("simulated io error")

	cases := []struct {
		name  string
		hooks writeHooks
	}{
		{
			name: "write-failure",
			hooks: writeHooks{
				beforeWrite: func() error { return injectedErr },
			},
		},
		{
			name: "sync-failure",
			hooks: writeHooks{
				beforeSync: func() error { return injectedErr },
			},
		},
		{
			name: "close-failure",
			hooks: writeHooks{
				beforeClose: func() error { return injectedErr },
			},
		},
		{
			name: "dir-sync-failure",
			hooks: writeHooks{
				beforeDirSync: func() error { return injectedErr },
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(temp, tc.name)
			targetFile := filepath.Join(dir, "evidence.json")

			err := writeEvidenceWithHooks(targetFile, map[string]string{"key": "val"}, tc.hooks)
			if err == nil {
				t.Fatalf("expected failure for %s, got nil", tc.name)
			}
			if !errors.Is(err, injectedErr) && !strings.Contains(err.Error(), "simulated io error") {
				t.Fatalf("unexpected error for %s: %v", tc.name, err)
			}

			// Pre-publication failures cannot expose a final artifact. A directory-sync
			// failure may leave a private final artifact, but the operation is failed
			// and the artifact is explicitly untrusted/incomplete.
			if tc.name != "dir-sync-failure" {
				if _, err := os.Stat(targetFile); err == nil {
					t.Fatalf("final artifact exists despite %s failure", tc.name)
				}
			}
		})
	}
}

func TestWriteEvidenceIsPrivateAndWriteOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "evidence.json")
	if err := WriteEvidence(path, map[string]any{"raw": json.RawMessage(`{"ok":true}`)}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("evidence mode = %v err=%v", info, err)
	}
	if err := WriteEvidence(path, map[string]any{"overwrite": true}); err == nil {
		t.Fatal("evidence overwrite was accepted")
	}
	if err := WriteEvidence(filepath.Join("..", "escape.json"), map[string]any{}); err == nil {
		t.Fatal("relative traversal evidence path was accepted")
	}
}

func TestWriteEvidenceSwapBeforeTempCreateRejection(t *testing.T) {
	temp := t.TempDir()
	outDir := filepath.Join(temp, "reserved")

	reserved, err := ReserveOutputDir(outDir)
	if err != nil {
		t.Fatalf("failed to reserve output dir: %v", err)
	}
	defer func() { _ = reserved.Close() }()

	otherDir := filepath.Join(temp, "other_dir")
	if err := os.Mkdir(otherDir, 0700); err != nil {
		t.Fatal(err)
	}

	swapExecuted := false
	hooks := writeHooks{
		beforeTempCreate: func() error {
			if err := os.Remove(outDir); err != nil {
				return err
			}
			if err := os.Symlink(otherDir, outDir); err != nil {
				return err
			}
			swapExecuted = true
			return nil
		},
	}

	err = writeEvidenceInDir(reserved, "payload.json", map[string]string{"secret": "data"}, hooks)
	if !swapExecuted {
		t.Fatal("beforeTempCreate hook was not executed")
	}
	if err == nil || (!strings.Contains(err.Error(), "swapped") && !strings.Contains(err.Error(), "replaced")) {
		t.Fatalf("swap before temp create was not rejected: %v", err)
	}

	// Payload must NOT exist in otherDir
	if _, err := os.Stat(filepath.Join(otherDir, "payload.json")); err == nil {
		t.Fatal("payload was exposed in replacement directory!")
	}
	entries, _ := os.ReadDir(otherDir)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".tmp-evidence-") {
			t.Fatalf("temporary evidence file leaked in replacement directory: %s", e.Name())
		}
	}
}

func TestWriteEvidenceDirSyncFailureLeavesUntrustedArtifact(t *testing.T) {
	temp := t.TempDir()
	outDir := filepath.Join(temp, "reserved")

	reserved, err := ReserveOutputDir(outDir)
	if err != nil {
		t.Fatalf("failed to reserve output dir: %v", err)
	}
	defer func() { _ = reserved.Close() }()

	injectedErr := errors.New("simulated dir sync error")
	hooks := writeHooks{
		beforeDirSync: func() error {
			return injectedErr
		},
	}

	err = writeEvidenceInDir(reserved, "evidence.json", map[string]string{"status": "recorded"}, hooks)
	if err == nil || !strings.Contains(err.Error(), "simulated dir sync error") {
		t.Fatalf("expected injected dir sync failure, got: %v", err)
	}

	targetPath := filepath.Join(outDir, "evidence.json")
	if _, err := os.Stat(targetPath); err != nil {
		t.Fatalf("expected untrusted incomplete final artifact after failed sync: %v", err)
	}
}

func TestWriteEvidenceDirSyncFailureReplacementPreserved(t *testing.T) {
	temp := t.TempDir()
	outDir := filepath.Join(temp, "reserved")

	reserved, err := ReserveOutputDir(outDir)
	if err != nil {
		t.Fatalf("failed to reserve output dir: %v", err)
	}
	defer func() { _ = reserved.Close() }()

	targetPath := filepath.Join(outDir, "evidence.json")
	hooks := writeHooks{
		beforeDirSync: func() error {
			// A failed publication must not attempt a named-file cleanup that could
			// delete this replacement.
			_ = os.Remove(targetPath)
			_ = os.WriteFile(targetPath, []byte("attacker replacement"), 0600)
			return errors.New("simulated dir sync error")
		},
	}

	err = writeEvidenceInDir(reserved, "evidence.json", map[string]string{"status": "recorded"}, hooks)
	if err == nil {
		t.Fatal("expected dir sync error, got nil")
	}
	if !strings.Contains(err.Error(), "simulated dir sync error") {
		t.Fatalf("expected injected sync error, got: %v", err)
	}

	// Replacement file must remain because failed publication performs no
	// ambiguous named-file cleanup.
	data, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("replacement file was deleted: %v", err)
	}
	if string(data) != "attacker replacement" {
		t.Fatalf("replacement file content was modified: %s", string(data))
	}
}
