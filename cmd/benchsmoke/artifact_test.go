package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestSnapshotStableDoesNotMixSourceReplacement(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(root, "source.bin")
	dataA := bytes.Repeat([]byte{'A'}, 4<<20)
	dataB := bytes.Repeat([]byte{'B'}, len(dataA))
	for attempt := 0; attempt < 8; attempt++ {
		if err := os.WriteFile(source, dataA, 0o600); err != nil {
			t.Fatal(err)
		}
		destination := filepath.Join(root, "snapshot.bin")
		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := 0; i < 8; i++ {
				replacement := filepath.Join(root, "replacement.bin")
				if os.WriteFile(replacement, dataB, 0o600) == nil {
					_ = os.Rename(replacement, source)
				}
			}
		}()
		_, err := snapshotStable(source, destination, 0o500)
		<-done
		if err != nil {
			t.Fatal(err)
		}
		copied, err := os.ReadFile(destination)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(copied, dataA) && !bytes.Equal(copied, dataB) {
			t.Fatalf("snapshot mixed source replacements on attempt %d", attempt)
		}
		if err := os.Remove(destination); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStableArtifactReadsUseDescriptorValidation(t *testing.T) {
	mainSource, err := os.ReadFile("artifact.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mainSource), "readStablePath(path, max)") {
		t.Fatal("bounded artifact reads do not use the stable descriptor reader")
	}
	linuxSource, err := os.ReadFile("stable_linux.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"Openat", "O_NOFOLLOW", "Fstat", "sameStableIdentity"} {
		if !strings.Contains(string(linuxSource), required) {
			t.Fatalf("stable Linux artifact reader lacks %q", required)
		}
	}
}

func TestLinuxSnapshotPublishesThroughHeldDestinationDirFD(t *testing.T) {
	source, err := os.ReadFile("snapshot_linux.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"Openat", "Linkat", "Unlinkat", "Fstat"} {
		if !strings.Contains(string(source), required) {
			t.Fatalf("Linux snapshot does not use dirfd operation %q", required)
		}
	}
}

func TestLinuxOutputLockAcquisitionUsesDirFDNoFollow(t *testing.T) {
	source, err := os.ReadFile("lock_linux.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"Openat", "O_CREAT", "O_EXCL", "O_NOFOLLOW", "Flock"} {
		if !strings.Contains(string(source), required) {
			t.Fatalf("Linux output lock helper lacks %q", required)
		}
	}
}

func TestLinuxOutputLockRejectsSymlink(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires Linux dirfd/O_NOFOLLOW semantics")
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	decoy := filepath.Join(root, "decoy-lock")
	if err := os.WriteFile(decoy, []byte("decoy"), 0o600); err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(root, "output.lock")
	if err := os.Symlink(decoy, lock); err != nil {
		t.Fatal(err)
	}
	if file, err := openOutputLock(lock); err == nil {
		_ = file.Close()
		t.Fatal("output lock helper followed a symlink")
	}
}

func TestLinuxOutputLockNeverFollowsConcurrentSymlinkReplacement(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires Linux dirfd/O_NOFOLLOW semantics")
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	decoy := filepath.Join(root, "decoy-lock")
	if err := os.WriteFile(decoy, []byte("decoy"), 0o600); err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(root, "output.lock")
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = os.Remove(lock)
			_ = os.Symlink(decoy, lock)
			_ = os.Remove(lock)
		}
	}()
	defer func() {
		close(stop)
		<-done
		_ = os.Remove(lock)
	}()
	for attempt := 0; attempt < 256; attempt++ {
		file, openErr := openOutputLock(lock)
		if openErr != nil {
			continue
		}
		info, statErr := file.Stat()
		_ = file.Close()
		if statErr != nil {
			t.Fatal(statErr)
		}
		decoyInfo, statErr := os.Stat(decoy)
		if statErr != nil {
			t.Fatal(statErr)
		}
		if os.SameFile(info, decoyInfo) {
			t.Fatal("output lock helper acquired the decoy through a replacement race")
		}
	}
}

func TestLinuxSnapshotRejectsSymlinkedParentDuringSwap(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires Linux O_NOFOLLOW semantics")
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(root, "source-parent")
	parked := filepath.Join(root, "source-parent-parked")
	decoy := filepath.Join(root, "decoy")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(decoy, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(parent, "source"), bytes.Repeat([]byte{'A'}, 1<<20), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(decoy, "source"), bytes.Repeat([]byte{'B'}, 1<<20), 0o600); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if os.Rename(parent, parked) != nil {
				continue
			}
			_ = os.Symlink(decoy, parent)
			_ = os.Remove(parent)
			_ = os.Rename(parked, parent)
		}
	}()
	defer func() {
		close(stop)
		<-done
	}()
	for attempt := 0; attempt < 64; attempt++ {
		destination := filepath.Join(root, fmt.Sprintf("snapshot-%d.bin", attempt))
		_, snapshotErr := snapshotStable(filepath.Join(parent, "source"), destination, 0o500)
		if snapshotErr != nil {
			continue
		}
		copied, readErr := os.ReadFile(destination)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if len(copied) > 0 && copied[0] == 'B' {
			t.Fatal("snapshot followed a swapped symlink parent into the decoy")
		}
		if err := os.Remove(destination); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLinuxArtifactRootReadRemainsPinnedAcrossRootSwap(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires Linux dirfd semantics")
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	original := filepath.Join(root, "bundle")
	decoy := filepath.Join(root, "decoy")
	if err := os.Mkdir(original, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(decoy, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(original, "manifest.json"), []byte(`{"source":"original"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(decoy, "manifest.json"), []byte(`{"source":"decoy"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	artifacts, err := openArtifactRoot(original)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = artifacts.Close() }() // Test cleanup after descriptor identity assertions.
	parked := filepath.Join(root, "bundle-parked")
	if err := os.Rename(original, parked); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(decoy, original); err != nil {
		t.Fatal(err)
	}
	data, err := artifacts.read("manifest.json", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte("original")) {
		t.Fatalf("descriptor-pinned artifact reader followed swapped root: %s", data)
	}
}
