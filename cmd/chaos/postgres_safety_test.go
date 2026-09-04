package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func chaosPGFixture(t *testing.T, owned bool) string {
	t.Helper()
	root := t.TempDir()
	oldRoot := chaosTempRoot
	chaosTempRoot = func() string { return root }
	t.Cleanup(func() { chaosTempRoot = oldRoot })
	dir := filepath.Join(root, chaosPGDataPrefix+"test")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if owned {
		identity := pgIdentity(t, dir)
		owner, err := newPGDataOwner(dir, identity)
		if err != nil {
			t.Fatal(err)
		}
		if err := writePGDataOwner(dir, identity, owner); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func resetPGDataHooks(t *testing.T) {
	t.Helper()
	oldRemove, oldBefore, oldUnlink := removePGDataTree, beforePGDataMove, beforePGDataUnlink
	t.Cleanup(func() {
		removePGDataTree, beforePGDataMove, beforePGDataUnlink = oldRemove, oldBefore, oldUnlink
	})
}

func pgIdentity(t *testing.T, path string) os.FileInfo {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func TestMustRmRejectsUnownedTargetWithoutCallingRemoval(t *testing.T) {
	dir := chaosPGFixture(t, false)
	called := false
	resetPGDataHooks(t)
	removePGDataTree = func(string, os.FileInfo, pgDataOwner) error { called = true; return nil }

	err := mustRm(dir, pgIdentity(t, dir))
	if err == nil || !strings.Contains(err.Error(), "ownership sidecar") {
		t.Fatalf("unowned target accepted: %v", err)
	}
	if called {
		t.Fatal("recursive removal called for unowned target")
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("unowned target was changed: %v", err)
	}
}

func TestPrepareRejectsPreexistingUnownedTarget(t *testing.T) {
	dir := chaosPGFixture(t, false)
	if err := os.WriteFile(filepath.Join(dir, "unowned-sentinel"), []byte("must survive"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := preparePGData(dir); err == nil || !strings.Contains(err.Error(), "pre-existing unowned") {
		t.Fatalf("pre-existing unowned target accepted: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "unowned-sentinel")); err != nil {
		t.Fatalf("pre-existing unowned target changed: %v", err)
	}
}

func TestMustRmRejectsBroadAndRedirectedTargets(t *testing.T) {
	resetPGDataHooks(t)
	real := chaosPGFixture(t, true)
	root := filepath.Dir(real)
	for _, target := range []string{"", ".", root, filepath.Dir(root)} {
		t.Run(target, func(t *testing.T) {
			if err := mustRm(target, pgIdentity(t, real)); err == nil {
				t.Fatalf("unsafe target accepted: %q", target)
			}
		})
	}

	link := filepath.Join(root, chaosPGDataPrefix+"symlink-test")
	if err := os.Symlink(real, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if err := mustRm(link, pgIdentity(t, real)); err == nil || !strings.Contains(err.Error(), "changed during validation") {
		t.Fatalf("redirected target accepted: %v", err)
	}
}

func TestMustRmAcceptsMarkerOwnedTargetAndPreservesSibling(t *testing.T) {
	dir := chaosPGFixture(t, true)
	if err := os.WriteFile(filepath.Join(dir, "sentinel"), []byte("owned"), 0600); err != nil {
		t.Fatal(err)
	}
	sibling := filepath.Join(filepath.Dir(dir), chaosPGDataPrefix+"sibling")
	if err := os.Mkdir(sibling, 0700); err != nil {
		t.Fatal(err)
	}

	if err := mustRm(dir, pgIdentity(t, dir)); err != nil {
		t.Fatalf("marker-owned target rejected: %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("owned root was removed: %v", err)
	}
	if _, err := os.Stat(pgDataOwnerPath(dir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ownership sidecar was not removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "sentinel")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("owned contents remain or stat failed: %v", err)
	}
	if _, err := os.Stat(sibling); err != nil {
		t.Fatalf("sibling was removed: %v", err)
	}
}

func TestMustRmRejectsPathReplacementAfterValidation(t *testing.T) {
	dir := chaosPGFixture(t, true)
	parent := filepath.Dir(dir)
	original := filepath.Join(parent, ".original-"+filepath.Base(dir))
	resetPGDataHooks(t)
	beforePGDataMove = func(target string) {
		if err := os.Rename(target, original); err != nil {
			t.Fatalf("move original fixture: %v", err)
		}
		if err := os.Rename(pgDataOwnerPath(target), pgDataOwnerPath(original)); err != nil {
			t.Fatalf("move original ownership sidecar: %v", err)
		}
		if err := os.Mkdir(target, 0700); err != nil {
			t.Fatalf("create replacement fixture: %v", err)
		}
		replacementIdentity := pgIdentity(t, target)
		replacementOwner, err := newPGDataOwner(target, replacementIdentity)
		if err != nil {
			t.Fatalf("create replacement ownership: %v", err)
		}
		if err := writePGDataOwner(target, replacementIdentity, replacementOwner); err != nil {
			t.Fatalf("mark replacement fixture: %v", err)
		}
		if err := os.WriteFile(filepath.Join(target, "replacement-sentinel"), []byte("must survive"), 0600); err != nil {
			t.Fatalf("write replacement sentinel: %v", err)
		}
	}

	err := mustRm(dir, pgIdentity(t, dir))
	if err == nil || !strings.Contains(err.Error(), "changed during validation") {
		t.Fatalf("replacement was not rejected: %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("replacement path was not restored: %v", err)
	}
	if _, err := os.Stat(original); err != nil {
		t.Fatalf("original fixture was lost: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "replacement-sentinel")); err != nil {
		t.Fatalf("valid-marker replacement was cleaned: %v", err)
	}
}

func TestMustRmRejectsPathReplacementBeforeFinalUnlink(t *testing.T) {
	dir := chaosPGFixture(t, true)
	parent := filepath.Dir(dir)
	original := filepath.Join(parent, ".original-final-"+filepath.Base(dir))
	resetPGDataHooks(t)
	beforePGDataUnlink = func(_ string) {
		if err := os.Rename(dir, original); err != nil {
			t.Fatalf("move emptied original fixture: %v", err)
		}
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatalf("create final replacement fixture: %v", err)
		}
	}

	err := mustRm(dir, pgIdentity(t, dir))
	if err == nil || !strings.Contains(err.Error(), "before final identity check") {
		t.Fatalf("final replacement was not rejected: %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("replacement path was removed: %v", err)
	}
	if _, err := os.Stat(original); err != nil {
		t.Fatalf("owned original was lost: %v", err)
	}
}

func TestPreparePGDataAllowsOnlyNewSafeTarget(t *testing.T) {
	root := t.TempDir()
	oldRoot := chaosTempRoot
	chaosTempRoot = func() string { return root }
	t.Cleanup(func() { chaosTempRoot = oldRoot })
	path := filepath.Join(root, chaosPGDataPrefix+"new-test")
	got, err := preparePGData(path)
	if err != nil {
		t.Fatalf("new safe target rejected: %v", err)
	}
	if got.Path == "" || filepath.Base(got.Path) != filepath.Base(path) || got.Identity == nil {
		t.Fatalf("unexpected canonical target: %#v", got)
	}
	if filepath.Dir(got.Path) == root {
		t.Fatal("new target was left in the shared temp directory")
	}
	if _, err := readPGDataOwner(got.Path, got.Identity); err != nil {
		t.Fatalf("prepared ownership sidecar not verifiable: %v", err)
	}
	entries, err := os.ReadDir(got.Path)
	if err != nil {
		t.Fatalf("read prepared PGDATA: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("prepared PGDATA is not empty: %v", entries)
	}
	if _, err := os.Stat(pgDataOwnerPath(got.Path)); err != nil {
		t.Fatalf("prepared ownership sidecar missing: %v", err)
	}
}

func TestPreparePGDataSupportsInitdb(t *testing.T) {
	initdb, err := exec.LookPath("initdb")
	if err != nil {
		t.Skipf("initdb unavailable: %v", err)
	}
	root := t.TempDir()
	oldRoot := chaosTempRoot
	chaosTempRoot = func() string { return root }
	t.Cleanup(func() { chaosTempRoot = oldRoot })
	prepared, err := preparePGData(filepath.Join(root, chaosPGDataPrefix+"initdb-compat"))
	if err != nil {
		t.Fatal(err)
	}
	cleaned := false
	t.Cleanup(func() {
		if !cleaned {
			_ = mustRm(prepared.Path, prepared.Identity)
		}
	})
	entries, err := os.ReadDir(prepared.Path)
	if err != nil {
		t.Fatalf("read prepared PGDATA: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("prepared PGDATA is not empty before initdb: %v", entries)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, initdb, "-D", ".", "-U", "instant", "--auth=trust", "-E", "utf8", "-c", "shared_memory_type=mmap")
	cmd.Dir = prepared.Path
	output, err := cmd.CombinedOutput()
	if err != nil {
		if strings.Contains(string(output), "could not create shared memory segment") && strings.Contains(string(output), "Operation not permitted") {
			t.Skipf("initdb is present but sandbox denies PostgreSQL shared memory: %v\n%s", err, output)
		}
		t.Fatalf("initdb rejected sidecar-free PGDATA: %v\n%s", err, output)
	}
	if err := verifyPGDataIdentity(prepared.Path, prepared.Identity); err != nil {
		t.Fatalf("PGDATA identity changed after initdb: %v", err)
	}
	if _, err := os.Stat(filepath.Join(prepared.Path, "PG_VERSION")); err != nil {
		t.Fatalf("initdb did not initialize PGDATA: %v", err)
	}
	if err := mustRm(prepared.Path, prepared.Identity); err != nil {
		t.Fatalf("cleanup after initdb compatibility check failed: %v", err)
	}
	cleaned = true
}

func TestFailedInitCleanupRemovesOwnedContentsAndRetainsRoot(t *testing.T) {
	root := t.TempDir()
	oldRoot := chaosTempRoot
	chaosTempRoot = func() string { return root }
	t.Cleanup(func() { chaosTempRoot = oldRoot })
	prepared, err := preparePGData(filepath.Join(root, chaosPGDataPrefix+"failed-init"))
	if err != nil {
		t.Fatal(err)
	}
	owned := filepath.Join(prepared.Path, "partial-init-file")
	if err := os.WriteFile(owned, []byte("owned partial output"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := mustRm(prepared.Path, prepared.Identity); err != nil {
		t.Fatalf("owned partial cleanup failed: %v", err)
	}
	if _, err := os.Stat(prepared.Path); err != nil {
		t.Fatalf("owned root was removed: %v", err)
	}
	if _, err := os.Stat(pgDataOwnerPath(prepared.Path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ownership sidecar was not removed: %v", err)
	}
	if _, err := os.Stat(owned); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("owned partial contents remain: %v", err)
	}
}

func TestPreparedPGDataIdentityBindsInitdbPath(t *testing.T) {
	root := t.TempDir()
	oldRoot := chaosTempRoot
	chaosTempRoot = func() string { return root }
	t.Cleanup(func() { chaosTempRoot = oldRoot })
	path := filepath.Join(root, chaosPGDataPrefix+"initdb-race")
	prepared, err := preparePGData(path)
	if err != nil {
		t.Fatal(err)
	}
	replacement := filepath.Join(root, "replacement")
	if err := os.Rename(prepared.Path, replacement); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(prepared.Path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := verifyPGDataIdentity(prepared.Path, prepared.Identity); err == nil {
		t.Fatal("initdb path replacement accepted")
	}
	if err := writePGDataOwner(prepared.Path, prepared.Identity, pgDataOwner{}); err == nil {
		t.Fatal("ownership sidecar installed in replaced directory")
	}
	if _, err := os.Stat(pgDataOwnerPath(prepared.Path)); err != nil {
		t.Fatalf("prepared ownership sidecar unexpectedly disappeared: %v", err)
	}
	sentinel := filepath.Join(prepared.Path, "replacement-sentinel")
	if err := os.WriteFile(sentinel, []byte("must survive"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := mustRm(prepared.Path, prepared.Identity); err == nil {
		t.Fatal("initdb replacement accepted for cleanup")
	}
	if b, err := os.ReadFile(sentinel); err != nil || string(b) != "must survive" {
		t.Fatalf("replacement was recursively cleaned: %q err=%v", b, err)
	}
}

func TestPartialInitReplacementIsNeverRecursivelyCleaned(t *testing.T) {
	root := t.TempDir()
	oldRoot := chaosTempRoot
	chaosTempRoot = func() string { return root }
	t.Cleanup(func() { chaosTempRoot = oldRoot })
	path := filepath.Join(root, chaosPGDataPrefix+"partial-race")
	prepared, err := preparePGData(path)
	if err != nil {
		t.Fatal(err)
	}
	original := filepath.Join(root, "partial-original")
	if err := os.Rename(prepared.Path, original); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(prepared.Path, 0700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(prepared.Path, "unowned-sentinel")
	if err := os.WriteFile(sentinel, []byte("must survive"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := mustRm(prepared.Path, prepared.Identity); err == nil {
		t.Fatal("replacement accepted for partial cleanup")
	}
	if b, err := os.ReadFile(sentinel); err != nil || string(b) != "must survive" {
		t.Fatalf("replacement was recursively cleaned: %q err=%v", b, err)
	}
}
