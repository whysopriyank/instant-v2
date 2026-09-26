package storageapi

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type cleanupProbeStore struct {
	deleteErr error
	deleted   [][]string
}

type uploadProbeStore struct {
	data      []byte
	deleteErr error
}

func (s *uploadProbeStore) PresignUpload(string, string, time.Duration) (string, error) {
	return "", nil
}

func (s *uploadProbeStore) PresignDownload(string, time.Duration) (string, error) {
	return "", nil
}

func (s *uploadProbeStore) Put(string, io.Reader) error { return nil }

func (s *uploadProbeStore) PutIfAbsent(_ string, r io.Reader) error {
	var err error
	s.data, err = io.ReadAll(r)
	return err
}

func (s *uploadProbeStore) Open(string) (*Object, error) {
	return &Object{Body: io.NopCloser(bytes.NewReader(s.data)), Size: int64(len(s.data))}, nil
}

func (s *uploadProbeStore) Delete([]string) error { return s.deleteErr }

func (s *cleanupProbeStore) PresignUpload(string, string, time.Duration) (string, error) {
	return "", nil
}

func (s *cleanupProbeStore) PresignDownload(string, time.Duration) (string, error) {
	return "", nil
}

func (s *cleanupProbeStore) Put(string, io.Reader) error { return nil }

func (s *cleanupProbeStore) PutIfAbsent(string, io.Reader) error { return nil }

func (s *cleanupProbeStore) Open(string) (*Object, error) { return nil, errors.New("not implemented") }

func (s *cleanupProbeStore) Delete(keys []string) error {
	s.deleted = append(s.deleted, append([]string(nil), keys...))
	return s.deleteErr
}

func TestCleanupAfterMetadataFailure(t *testing.T) {
	linkErr := ErrFilenameTaken

	t.Run("deletes only newly created object", func(t *testing.T) {
		store := &cleanupProbeStore{}
		if err := cleanupAfterMetadataFailure(store, "app/new", true, linkErr); !errors.Is(err, linkErr) {
			t.Fatalf("error = %v, want link error", err)
		}
		if len(store.deleted) != 1 || len(store.deleted[0]) != 1 || store.deleted[0][0] != "app/new" {
			t.Fatalf("deleted = %#v, want only app/new", store.deleted)
		}
	})

	t.Run("surfaces cleanup failure", func(t *testing.T) {
		cleanupErr := errors.New("delete unavailable")
		store := &cleanupProbeStore{deleteErr: cleanupErr}
		err := cleanupAfterMetadataFailure(store, "app/new", true, linkErr)
		if !errors.Is(err, linkErr) || !errors.Is(err, errUploadCleanup) || !errors.Is(err, cleanupErr) {
			t.Fatalf("error = %v, want link and cleanup errors", err)
		}
		if len(store.deleted) != 1 || store.deleted[0][0] != "app/new" {
			t.Fatalf("deleted = %#v, want attempted cleanup of app/new", store.deleted)
		}
	})

	t.Run("does not delete pre-existing object", func(t *testing.T) {
		store := &cleanupProbeStore{deleteErr: errors.New("must not be called")}
		err := cleanupAfterMetadataFailure(store, "app/existing", false, linkErr)
		if !errors.Is(err, linkErr) {
			t.Fatalf("error = %v, want link error", err)
		}
		if len(store.deleted) != 0 {
			t.Fatalf("deleted = %#v, want no cleanup for pre-existing object", store.deleted)
		}
	})
}

func TestDiskBackendPutIfAbsentPreservesExistingObject(t *testing.T) {
	backend := mustBackend(t, t.TempDir(), []byte("atomicity-test"))
	key := redApp + "/" + redObj
	if err := backend.Put(key, strings.NewReader("original")); err != nil {
		t.Fatalf("initial Put: %v", err)
	}
	if err := backend.PutIfAbsent(key, strings.NewReader("replacement")); !errors.Is(err, ErrObjectExists) {
		t.Fatalf("PutIfAbsent error = %v, want ErrObjectExists", err)
	}
	obj, err := backend.Open(key)
	if err != nil {
		t.Fatalf("Open existing object: %v", err)
	}
	defer func() { _ = obj.Body.Close() }()
	got, err := io.ReadAll(obj.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("original")) {
		t.Fatalf("existing object changed to %q", got)
	}
}

func TestDiskBackendPutIfAbsentRequiresDirectorySync(t *testing.T) {
	backend := mustBackend(t, t.TempDir(), []byte("sync-test"))
	key := redApp + "/" + redObj
	if err := backend.Put(key, strings.NewReader("original")); err != nil {
		t.Fatalf("initial Put: %v", err)
	}
	syncErr := errors.New("directory sync failed")
	backend.syncObject = func(string) error { return syncErr }
	if err := backend.PutIfAbsent(key, strings.NewReader("replacement")); !errors.Is(err, syncErr) {
		t.Fatalf("existing-object retry error = %v, want sync failure", err)
	}
	newKey := redApp + "/33333333-3333-4333-8333-333333333333"
	if err := backend.PutIfAbsent(newKey, strings.NewReader("new")); !errors.Is(err, syncErr) {
		t.Fatalf("new-object sync error = %v, want sync failure", err)
	}
	if _, err := backend.Open(newKey); err == nil {
		t.Fatal("failed exclusive write left an object after directory-sync failure")
	}
}

func TestUploadKeyLockSerializesSameKey(t *testing.T) {
	h := &Handler{}
	unlockFirst := h.lockUploadKey("app/object")
	acquired := make(chan struct{})
	done := make(chan struct{})
	go func() {
		unlockSecond := h.lockUploadKey("app/object")
		close(acquired)
		unlockSecond()
		close(done)
	}()
	select {
	case <-acquired:
		t.Fatal("same-key lock acquired concurrently")
	case <-time.After(10 * time.Millisecond):
	}
	unlockFirst()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("same-key lock did not release")
	}
}

func TestPresignedFilenameCannotBeRetargeted(t *testing.T) {
	h, srv, appID := newHandlerSrvApp(t)
	uploadURL, id, _ := presignUpload(t, srv, appID, "signed.txt")
	u, err := url.Parse(uploadURL)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("filename", "retargeted.txt")
	u.RawQuery = q.Encode()
	if status, _ := putBody(t, u.String(), []byte("should fail")); status != http.StatusForbidden {
		t.Fatalf("retargeted filename status = %d, want 403", status)
	}
	if status, _ := putBody(t, uploadURL, []byte("signed bytes")); status != http.StatusOK {
		t.Fatalf("original signed upload status = %d, want 200", status)
	}
	obj, err := h.Store.Open(appID + "/" + id)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = obj.Body.Close() }()
	got, err := io.ReadAll(obj.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("signed bytes")) {
		t.Fatalf("stored bytes = %q, want signed bytes", got)
	}
}

func TestOversizedUploadCleanupFailureReturnsServerError(t *testing.T) {
	secret := []byte("oversized-upload-secret")
	store := &uploadProbeStore{deleteErr: errors.New("delete unavailable")}
	h := &Handler{Store: store, Secret: secret, MaxUploadBytes: 3}
	exp := time.Now().Add(time.Minute).Unix()
	q := url.Values{}
	q.Set("app-id", redApp)
	q.Set("filename", "too-large.txt")
	q.Set("expires", fmt.Sprintf("%d", exp))
	q.Set("signature", signPayload(secret, "upload", redApp, redObj, "too-large.txt", exp))
	req := httptest.NewRequest(http.MethodPut, "/storage/upload/"+redObj+"?"+q.Encode(), strings.NewReader("123456"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("oversized cleanup failure status = %d, want 500", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "delete unavailable") {
		t.Fatalf("response = %q, want cleanup failure", rec.Body.String())
	}
}
