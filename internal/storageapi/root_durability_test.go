package storageapi

// DA-001a/DA-001d: durable-root refusal and file durability. Hermetic:
// real filesystem I/O under t.TempDir(), no database, no sockets, no sleeps.
// Backup smoke on an isolated scratch database remains owed to a runnable
// host (DA-001b/c); these tests cover the local file contract only.

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
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
	defer func() { _ = obj.Body.Close() }()
	got := new(bytes.Buffer)
	if _, err := got.ReadFrom(obj.Body); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatal("reopened backend returned different bytes")
	}
}

// Desired: a first upload to a fresh root persists its namespace — the
// app directory entry is created and the object round-trips through a
// backend reopen (P1: the root fsync on creation is what makes the
// directory entry crash-durable; crash injection itself is out of scope
// for this deterministic suite, so this pins the path the fix touches).
func TestDiskBackendFirstUploadPersistsNamespace(t *testing.T) {
	root := t.TempDir()
	key := redApp + "/" + redObj
	want := []byte("first-upload-payload")
	be := mustBackend(t, root, []byte("red-test-secret"))
	if err := be.Put(key, bytes.NewReader(want)); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	if st, err := os.Stat(filepath.Join(root, redApp)); err != nil || !st.IsDir() {
		t.Fatalf("app dir entry missing after first Put: %v", err)
	}
	obj, err := mustBackend(t, root, []byte("red-test-secret")).Open(key)
	if err != nil {
		t.Fatalf("Open after reopen: %v", err)
	}
	defer func() { _ = obj.Body.Close() }()
	got := new(bytes.Buffer)
	if _, err := got.ReadFrom(obj.Body); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatal("reopened backend returned different bytes")
	}
}

// Desired: concurrent first uploads to one fresh app directory all
// succeed with readable results — no contender may trust (EEXIST) or
// bypass another's unconfirmed initialization, and none may fail on a
// creation race.
func TestDiskBackendConcurrentFirstUploads(t *testing.T) {
	root := t.TempDir()
	be := mustBackend(t, root, []byte("red-test-secret"))
	const n = 32
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := redApp + "/" + fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
			errs[i] = be.Put(key, bytes.NewReader([]byte(fmt.Sprintf("payload-%d", i))))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent Put %d: %v", i, err)
		}
	}
	reopened := mustBackend(t, root, []byte("red-test-secret"))
	for i := 0; i < n; i++ {
		obj, err := reopened.Open(redApp + "/" + fmt.Sprintf("00000000-0000-4000-8000-%012d", i))
		if err != nil {
			t.Fatalf("Open %d after reopen: %v", i, err)
		}
		got := new(bytes.Buffer)
		_, rerr := got.ReadFrom(obj.Body)
		_ = obj.Body.Close()
		if rerr != nil || got.String() != fmt.Sprintf("payload-%d", i) {
			t.Fatalf("object %d mismatch: %q, err %v", i, got.String(), rerr)
		}
	}
}

// Desired: a failed root confirmation leaves no entry behind — the next
// upload re-initializes (and re-confirms) instead of trusting it.
func TestDiskBackendFailedRootSyncRollsBack(t *testing.T) {
	root := t.TempDir()
	be := mustBackend(t, root, []byte("red-test-secret"))
	fail := errors.New("injected root sync failure")
	be.syncRoot = func(string) error { return fail }
	key := redApp + "/" + redObj
	if err := be.Put(key, bytes.NewReader([]byte("x"))); err == nil {
		t.Fatal("Put with failing root sync must fail")
	}
	if _, err := os.Stat(filepath.Join(root, redApp)); !os.IsNotExist(err) {
		t.Fatalf("unconfirmed app dir entry left behind: %v", err)
	}
	// Recovery: with a working sync the same upload initializes cleanly.
	be.syncRoot = syncDir
	if err := be.Put(key, bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("Put after rollback: %v", err)
	}
}

// A failed cleanup must not turn an unconfirmed directory into a trusted
// existing directory on retry. The injected sync failure leaves a child to
// make directory removal fail, as can happen with concurrent external I/O.
func TestDiskBackendFailedCleanupDoesNotBypassRootSync(t *testing.T) {
	root := t.TempDir()
	be := mustBackend(t, root, []byte("test-secret"))
	failure := errors.New("root sync failed")
	calls := 0
	be.syncRoot = func(string) error {
		calls++
		if err := os.WriteFile(filepath.Join(root, redApp, "occupied"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		return failure
	}
	key := redApp + "/" + redObj
	if err := be.Put(key, strings.NewReader("first")); !errors.Is(err, failure) {
		t.Fatalf("first Put = %v; want sync failure", err)
	}
	if err := be.Put(key, strings.NewReader("retry")); !errors.Is(err, failure) {
		t.Fatalf("retry bypassed failed root confirmation: %v", err)
	}
	if calls != 2 {
		t.Fatalf("root confirmation attempts = %d; want 2", calls)
	}
	be.syncRoot = syncDir
	if err := be.Put(key, strings.NewReader("recovered")); err != nil {
		t.Fatalf("confirmed retry: %v", err)
	}
}

// Desired: a second concurrent first upload blocks until the first root
// confirmation completes — it cannot acknowledge before confirmation.
// Barrier: syncRoot signals entry then blocks; the second Put must not
// complete within the bound; release lets both succeed. No sleeps; explicit
// entry signal + bounded wait.
func TestDiskBackendSecondUploadWaitsForRootConfirmation(t *testing.T) {
	root := t.TempDir()
	be := mustBackend(t, root, []byte("red-test-secret"))
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	be.syncRoot = func(s string) error {
		once.Do(func() { close(entered) })
		<-release
		return syncDir(s)
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- be.Put(redApp+"/"+redObj, strings.NewReader("first")) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first Put did not enter root confirmation")
	}
	secondDone := make(chan error, 1)
	go func() {
		key := redApp + "/00000000-0000-4000-8000-000000000001"
		secondDone <- be.Put(key, strings.NewReader("second"))
	}()
	select {
	case err := <-secondDone:
		t.Fatalf("second upload acknowledged before root confirmation: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first Put after release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first Put did not complete after release")
	}
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second Put after release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second Put did not complete after release")
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
