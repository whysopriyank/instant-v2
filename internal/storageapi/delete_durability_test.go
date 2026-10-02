package storageapi

import (
	"errors"
	"strings"
	"testing"
)

func TestDiskBackendDeleteRequiresDirectorySync(t *testing.T) {
	backend := mustBackend(t, t.TempDir(), []byte("delete-sync-test"))
	key := redApp + "/" + redObj
	if err := backend.PutIfAbsent(key, strings.NewReader("owned")); err != nil {
		t.Fatal(err)
	}
	syncErr := errors.New("delete directory sync failed")
	backend.syncObject = func(string) error { return syncErr }
	if err := backend.Delete([]string{key}); !errors.Is(err, syncErr) {
		t.Fatalf("Delete error = %v, want durability failure", err)
	}
	if _, err := backend.Open(key); err == nil {
		t.Fatal("Delete did not unlink the owned object")
	}
	if err := backend.Delete([]string{key}); !errors.Is(err, syncErr) {
		t.Fatalf("Delete retry error = %v, want durability failure even after unlink", err)
	}
}

func TestDiskBackendFailedCreateSyncsRemoval(t *testing.T) {
	backend := mustBackend(t, t.TempDir(), []byte("rollback-sync-test"))
	key := redApp + "/" + redObj
	calls := 0
	syncErr := errors.New("object directory sync failed")
	backend.syncObject = func(string) error { calls++; return syncErr }
	if err := backend.PutIfAbsent(key, strings.NewReader("owned")); !errors.Is(err, ErrUploadCleanup) {
		t.Fatalf("PutIfAbsent error = %v, want cleanup durability failure", err)
	}
	if calls != 2 {
		t.Fatalf("sync calls = %d, want create and rollback sync", calls)
	}
}
