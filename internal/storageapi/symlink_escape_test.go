package storageapi

// DA-001d/f: symlink escape and deterministic root refusal. Hermetic:
// real filesystem I/O under t.TempDir(), no database, no sockets.

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A planted root/<app-id> symlink must not let writes, reads, or deletes
// escape the configured root. Put (via confirmAppDir), Open, and Delete
// all reject the link.
func TestDiskBackendSymlinkAppDirRejected(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	outsideFile := filepath.Join(outside, "secret.txt")
	if err := os.WriteFile(outsideFile, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, redApp)
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	be := mustBackend(t, root, []byte("symlink-test"))
	key := redApp + "/" + redObj
	if err := be.Put(key, strings.NewReader("escape")); err == nil {
		t.Fatal("Put through symlinked app dir accepted")
	} else if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("Put symlink error = %v; want symlink refusal", err)
	}
	if _, err := be.Open(key); err == nil {
		t.Fatal("Open through symlinked app dir accepted")
	} else if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("Open symlink error = %v; want symlink refusal", err)
	}
	if err := be.Delete([]string{key}); err == nil {
		t.Fatal("Delete through symlinked app dir accepted")
	} else if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("Delete symlink error = %v; want symlink refusal", err)
	}
	if got, err := os.ReadFile(outsideFile); err != nil || string(got) != "outside" {
		t.Fatalf("outside file mutated: %q %v", got, err)
	}
	entries, err := os.ReadDir(outside)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != "secret.txt" {
			t.Fatalf("symlink escape wrote outside file %q", e.Name())
		}
	}
	// A victim file planted outside under the target object name must
	// survive the refused Delete.
	victim := filepath.Join(outside, redObj)
	if err := os.WriteFile(victim, []byte("victim"), 0o600); err != nil {
		t.Fatal(err)
	}
	_ = be.Delete([]string{key})
	if got, _ := os.ReadFile(victim); string(got) != "victim" {
		t.Fatalf("Delete through symlink removed outside file: %q", got)
	}
	_ = os.Remove(victim)
}

// A planted file-level symlink must not be dereferenced on read, and Delete
// of a symlink target path must not touch the outside file (os.Remove on a
// link removes the link itself).
func TestDiskBackendSymlinkFileNotDereferenced(t *testing.T) {
	root := t.TempDir()
	be := mustBackend(t, root, []byte("symlink-file-test"))
	key := redApp + "/" + redObj
	if err := be.Put(key, bytes.NewReader([]byte("inside"))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	_, _, p, err := be.file(key)
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, p); err != nil {
		t.Fatal(err)
	}
	if _, err := be.Open(key); err == nil {
		t.Fatal("Open dereferenced file symlink")
	} else if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("Open file-symlink error = %v; want symlink refusal", err)
	}
	if got, _ := os.ReadFile(outside); string(got) != "outside" {
		t.Fatalf("outside file changed: %q", got)
	}
}

// Deterministic refusal when the configured root is a regular file.
// Unlike permission-bit probes this fails identically for privileged and
// unprivileged users (MkdirAll returns ENOTDIR even as root).
func TestDiskBackendRefusesFileRoot(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "not-a-dir-*")
	if err != nil {
		t.Fatal(err)
	}
	name := f.Name()
	_ = f.Close()
	if _, err := NewDiskBackend(name, []byte("s")); err == nil {
		t.Fatal("file root accepted")
	} else if !strings.Contains(err.Error(), name) {
		t.Fatalf("refusal must name the root, got: %v", err)
	}
}

// Deterministic refusal when the root's parent is a regular file — covers
// the nonexistent/uncreatable-parent boundary without depending on uid,
// mount flags, or permission bits.
func TestDiskBackendRefusesUncreatableParent(t *testing.T) {
	parent, err := os.CreateTemp(t.TempDir(), "parent-file-*")
	if err != nil {
		t.Fatal(err)
	}
	parentName := parent.Name()
	_ = parent.Close()
	root := filepath.Join(parentName, "child")
	if _, err := NewDiskBackend(root, []byte("s")); err == nil {
		t.Fatal("uncreatable-parent root accepted")
	}
}
