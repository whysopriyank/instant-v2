package storageapi

// DA-001a/DA-001d: durable-root refusal and file durability. Hermetic:
// real filesystem I/O under t.TempDir(), no database, no sockets, no sleeps.
// Backup smoke on an isolated scratch database remains owed to a runnable
// host (DA-001b/c); these tests cover the local file contract only.

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	redApp = "11111111-1111-1111-1111-111111111111"
	redObj = "22222222-2222-2222-2222-222222222222"
)

func mustBackend(t *testing.T, root string, secret []byte) *DiskBackend {
	t.Helper()
	b, err := NewDiskBackend(root, secret)
	if err != nil {
		t.Fatalf("NewDiskBackend(%q): %v", root, err)
	}
	return b
}

// Desired: an empty root never silently resolves to a temp default
// (DA-001d). FAILS while NewDiskBackend falls back to $TMPDIR.
func TestDiskBackendRefusesEmptyRoot(t *testing.T) {
	if _, err := NewDiskBackend("", []byte("s")); err == nil {
		t.Fatal("DA-001d RED: empty root accepted with temp fallback")
	}
}

// Desired: a relative root is refused — durability must never depend on
// the daemon's working directory (DA-001d).
func TestDiskBackendRefusesRelativeRoot(t *testing.T) {
	if _, err := NewDiskBackend("relative/files", []byte("s")); err == nil {
		t.Fatal("DA-001d RED: relative root accepted")
	}
}

// Desired: a malformed root is refused at construction, not at first
// upload (DA-001d).
func TestDiskBackendRefusesMalformedRoot(t *testing.T) {
	bad := string([]byte{'/', 'x', 0, 'y'})
	if _, err := NewDiskBackend(filepath.Join(t.TempDir(), bad), []byte("s")); err == nil {
		t.Fatal("DA-001d RED: malformed root accepted")
	}
}

// Desired: an unwritable root is refused at construction with the path in
// the error (DA-001d).
func TestDiskBackendRefusesUnwritableRoot(t *testing.T) {
	locked := filepath.Join(t.TempDir(), "locked")
	if err := os.MkdirAll(locked, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(locked, 0o555); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(locked, 0o755) }()
	if _, err := NewDiskBackend(locked, []byte("s")); err == nil {
		t.Fatal("DA-001d RED: unwritable root accepted")
	} else if !strings.Contains(err.Error(), locked) {
		t.Fatalf("refusal must name the root, got: %v", err)
	}
}

// Desired: a stored object survives backend re-instantiation on the same
// root — the restart half of open/sync/close + restart (DA-001a).
func TestDiskBackendPutSurvivesReopen(t *testing.T) {
	root := t.TempDir()
	key := redApp + "/" + redObj
	want := []byte("durable-payload-" + strings.Repeat("x", 1000))
	if err := mustBackend(t, root, []byte("red-test-secret")).Put(key, bytes.NewReader(want)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	obj, err := mustBackend(t, root, []byte("red-test-secret")).Open(key)
	if err != nil {
		t.Fatalf("Open after reopen: %v", err)
	}
	defer obj.Body.Close()
	got := new(bytes.Buffer)
	if _, err := got.ReadFrom(obj.Body); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatal("reopened backend returned different bytes")
	}
}

// Desired: traversal-shaped keys never escape the root (pin for the
// data-integrity review; splitKey enforces uuid/uuid).
func TestDiskBackendKeyTraversalRejected(t *testing.T) {
	b := mustBackend(t, t.TempDir(), []byte("red-test-secret"))
	for _, key := range []string{
		"../evil", redApp + "/../evil", redApp + "/..",
		"not-a-uuid/" + redObj, redApp + "/not-a-uuid", "no-slash",
	} {
		if err := b.Put(key, strings.NewReader("x")); err == nil {
			t.Fatalf("traversal key accepted: %q", key)
		}
	}
}
