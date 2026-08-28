//go:build linux

package benchrun

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestOpenProvisionExecutableBindsHashToExecutedObject(t *testing.T) {
	trueBytes, err := os.ReadFile("/usr/bin/true")
	if err != nil {
		t.Skip("/usr/bin/true is unavailable")
	}
	falseBytes, err := os.ReadFile("/usr/bin/false")
	if err != nil {
		t.Skip("/usr/bin/false is unavailable")
	}
	path := filepath.Join(t.TempDir(), "provision")
	if err := os.WriteFile(path, trueBytes, 0500); err != nil {
		t.Fatal(err)
	}
	f, _, err := openProvisionExecutable(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Chmod(0700); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("mutate"), 0); err == nil {
		t.Fatal("sealed executable accepted a write")
	}
	// Replace the authorized path after hashing. Descriptor execution must
	// still run the originally opened /usr/bin/true object.
	if err := os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, falseBytes, 0500); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("/proc/self/fd/3")
	cmd.ExtraFiles = []*os.File{f}
	if err := cmd.Run(); err != nil {
		t.Fatalf("descriptor execution followed replaced path: %v", err)
	}
}

func TestOpenProvisionExecutableRejectsSymlinkAndWritable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(path, []byte("not executable"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openProvisionExecutable(path); err == nil {
		t.Fatal("writable provision executable accepted")
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openProvisionExecutable(link); err == nil {
		t.Fatal("symlink provision executable accepted")
	}
}
