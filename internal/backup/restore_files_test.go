package backup_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/instant-v2/instant-v2/internal/backup"
	"github.com/instant-v2/instant-v2/internal/storageapi"
)

func fileArchive(t *testing.T, ids, locations []string, bodies []string) []byte {
	t.Helper()
	entities := ""
	entries := []string{"config.json", `{"title":"v1 files","schema":{"entities":{"$files":{"attrs":{"path":{"valueType":"string","config":{"unique":true,"indexed":true}}}}}}}`}
	for i, id := range ids {
		entities += fmt.Sprintf(`{"entity":{"id":%q,"path":%q,"location-id":%q,"size":%d,"content-type":"text/plain","content-disposition":"inline","key-version":1},"createdAt":1700000000000}`+"\n", id, "file-"+id, locations[i], len(bodies[i]))
	}
	entries = append(entries, "entities/$files.jsonl", entities)
	for i, location := range locations {
		entries = append(entries, "files/"+location, bodies[i])
	}
	return restoreZip(t, entries...)
}

func TestV1RestoreFiles(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()
	id, location := uuidStr(newUUID()), uuidStr(newUUID())
	archive := fileArchive(t, []string{id}, []string{location}, []string{"exact blob bytes"})
	source := bytes.Clone(archive)
	root := t.TempDir()
	store, err := storageapi.NewDiskBackend(root, []byte("test-secret"))
	if err != nil {
		t.Fatal(err)
	}
	h := &backup.Handler{Pool: pool, Files: store, AdminTokenCheck: fakeAuth{valid: "restore-token"}.check}
	req := httptest.NewRequest(http.MethodPost, "/backup/"+uuidStr(appID)+"/restore-v1zip", bytes.NewReader(archive))
	req.Header.Set("Authorization", "Bearer restore-token")
	response := httptest.NewRecorder()
	h.ServeHTTP(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("HTTP ZIP restore=%d %s", response.Code, response.Body.String())
	}
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT jsonb_object_agg(a.label,t.value) FROM triples t JOIN attrs a ON a.id=t.attr_id WHERE t.app_id=$1 AND t.entity_id=$2`, appID, id).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var metadata map[string]any
	if err := json.Unmarshal(raw, &metadata); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"id": id, "path": "file-" + id, "location-id": location, "size": float64(len("exact blob bytes")), "content-type": "text/plain", "content-disposition": "inline", "key-version": float64(1)}
	gotJSON, _ := json.Marshal(metadata)
	wantJSON, _ := json.Marshal(want)
	if !bytes.Equal(gotJSON, wantJSON) {
		t.Fatalf("metadata %s, want %s", gotJSON, wantJSON)
	}
	reopened, err := storageapi.NewDiskBackend(root, []byte("test-secret"))
	if err != nil {
		t.Fatal(err)
	}
	object, err := reopened.Open(uuidStr(appID) + "/" + id)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(object.Body)
	if err != nil {
		t.Fatal(err)
	}
	if err := object.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if string(data) != "exact blob bytes" || object.Size != int64(len(data)) {
		t.Fatalf("reopened object = %q size %d", data, object.Size)
	}
	if _, err := reopened.Open(uuidStr(appID) + "/" + location); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stored at location instead of entity-id: %v", err)
	}
	if !bytes.Equal(source, archive) {
		t.Error("restore mutated source archive")
	}
}

type restoreFaultStore struct {
	storageapi.ObjectStore
	puts       int
	failAt     int
	cleanupErr error
	putErr     error
	deletes    int
	cancel     context.CancelFunc
}

func (s *restoreFaultStore) PutIfAbsent(key string, r io.Reader) error {
	s.puts++
	if s.puts == s.failAt {
		return errors.New("injected file write failure")
	}
	err := s.ObjectStore.PutIfAbsent(key, r)
	if err == nil && s.putErr != nil {
		return s.putErr
	}
	if err == nil && s.cancel != nil {
		s.cancel()
	}
	return err
}

func (s *restoreFaultStore) Delete(keys []string) error {
	s.deletes++
	if s.cleanupErr != nil {
		return s.cleanupErr
	}
	return s.ObjectStore.Delete(keys)
}

func TestV1RestoreUploadCleanupUncertainty(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()
	before, _ := exportApp(t, ctx, pool, appID, backup.ExportOptions{})
	id, location := uuidStr(newUUID()), uuidStr(newUUID())
	archive := fileArchive(t, []string{id}, []string{location}, []string{"uncertain object"})
	source := bytes.Clone(archive)
	root := t.TempDir()
	disk, err := storageapi.NewDiskBackend(root, []byte("test-secret"))
	if err != nil {
		t.Fatal(err)
	}
	store := &restoreFaultStore{ObjectStore: disk, putErr: storageapi.ErrUploadCleanup}
	h := &backup.Handler{Pool: pool, Files: store, AdminTokenCheck: fakeAuth{valid: "restore-token"}.check}
	req := httptest.NewRequest(http.MethodPost, "/backup/"+uuidStr(appID)+"/restore-v1zip", bytes.NewReader(archive))
	req.Header.Set("Authorization", "Bearer restore-token")
	response := httptest.NewRecorder()
	h.ServeHTTP(response, req)
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), backup.ErrRestoreCleanup.Error()) {
		t.Errorf("uncertain object cleanup response=%d %s", response.Code, response.Body.String())
	}
	if store.deletes != 0 {
		t.Errorf("restore deleted an ownership-uncertain object: %d calls", store.deletes)
	}
	after, _ := exportApp(t, ctx, pool, appID, backup.ExportOptions{})
	if !bytes.Equal(before, after) || !bytes.Equal(source, archive) {
		t.Error("uncertain object cleanup mutated SQL target or source")
	}
	reopened, err := storageapi.NewDiskBackend(root, []byte("test-secret"))
	if err != nil {
		t.Fatal(err)
	}
	object, err := reopened.Open(uuidStr(appID) + "/" + id)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(object.Body)
	_ = object.Body.Close()
	if err != nil || string(data) != "uncertain object" {
		t.Fatalf("ownership-uncertain object was changed: %q %v", data, err)
	}
}

func TestV1RestoreFileFailurePreservesTarget(t *testing.T) {
	for _, outcome := range []string{"second-put", "existing-object", "cleanup-failure", "commit-unknown"} {
		t.Run(outcome, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			pool, appID, cleanup := env(t)
			defer cleanup()
			before, _ := exportApp(t, context.Background(), pool, appID, backup.ExportOptions{})
			id1, id2 := uuidStr(newUUID()), uuidStr(newUUID())
			loc1, loc2 := uuidStr(newUUID()), uuidStr(newUUID())
			archive := fileArchive(t, []string{id1, id2}, []string{loc1, loc2}, []string{"first", "second"})
			source := bytes.Clone(archive)
			root := t.TempDir()
			disk, err := storageapi.NewDiskBackend(root, []byte("test-secret"))
			if err != nil {
				t.Fatal(err)
			}
			store := &restoreFaultStore{ObjectStore: disk, failAt: 2}
			if outcome == "existing-object" {
				for _, id := range []string{id1, id2} {
					if err := disk.PutIfAbsent(uuidStr(appID)+"/"+id, bytes.NewBufferString("old object")); err != nil {
						t.Fatal(err)
					}
				}
				store.failAt = 0
			}
			if outcome == "cleanup-failure" {
				store.cleanupErr = errors.New("injected cleanup failure")
			}
			if outcome == "commit-unknown" {
				archive = fileArchive(t, []string{id1}, []string{loc1}, []string{"first"})
				source = bytes.Clone(archive)
				store.failAt = 0
				store.cancel = cancel
			}
			_, err = backup.RestoreV1Zip(ctx, pool, bytes.NewReader(archive), int64(len(archive)), appID, store)
			if err == nil {
				t.Fatal("injected restore failure succeeded")
			}
			if outcome == "cleanup-failure" && !errors.Is(err, backup.ErrRestoreCleanup) {
				t.Errorf("cleanup error not surfaced: %v", err)
			}
			if outcome == "commit-unknown" && !errors.Is(err, backup.ErrCommitOutcomeUnknown) {
				t.Errorf("commit uncertainty not surfaced: %v", err)
			}
			after, _ := exportApp(t, context.Background(), pool, appID, backup.ExportOptions{})
			if !bytes.Equal(before, after) {
				t.Error("failed restore mutated SQL target")
			}
			if !bytes.Equal(source, archive) {
				t.Error("failed restore mutated source archive")
			}
			reopened, err := storageapi.NewDiskBackend(root, []byte("test-secret"))
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range []string{id1, id2} {
				object, err := reopened.Open(uuidStr(appID) + "/" + id)
				if outcome == "existing-object" {
					if err != nil {
						t.Fatal(err)
					}
					data, err := io.ReadAll(object.Body)
					_ = object.Body.Close()
					if err != nil || string(data) != "old object" {
						t.Errorf("existing object changed: %q %v", data, err)
					}
				} else if (outcome == "cleanup-failure" || outcome == "commit-unknown") && err == nil {
					_ = object.Body.Close() // Retention is mandatory on uncertainty; cleanup failure is explicit.
				} else if !errors.Is(err, os.ErrNotExist) {
					t.Errorf("unexpected retained object: %s %v", id, err)
				}
			}
			if outcome == "commit-unknown" {
				object, err := reopened.Open(uuidStr(appID) + "/" + id1)
				if err != nil {
					t.Fatalf("unknown commit deleted object: %v", err)
				}
				_ = object.Body.Close()
			}
		})
	}
}

func TestV1RestoreRejectsInvalidFileArchive(t *testing.T) {
	for _, problem := range []string{"missing-blob", "orphan-blob", "duplicate-blob", "invalid-location", "wrong-size"} {
		t.Run(problem, func(t *testing.T) {
			ctx := context.Background()
			pool, appID, cleanup := env(t)
			defer cleanup()
			before, _ := exportApp(t, ctx, pool, appID, backup.ExportOptions{})
			id, location := uuidStr(newUUID()), uuidStr(newUUID())
			archive := fileArchive(t, []string{id}, []string{location}, []string{"first"})
			zr, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
			if err != nil {
				t.Fatal(err)
			}
			entries := []string{}
			for _, entry := range zr.File {
				if problem == "missing-blob" && strings.HasPrefix(entry.Name, "files/") {
					continue
				}
				r, err := entry.Open()
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(r)
				_ = r.Close()
				if err != nil {
					t.Fatal(err)
				}
				if entry.Name == "entities/$files.jsonl" {
					if problem == "invalid-location" {
						data = bytes.ReplaceAll(data, []byte(location), []byte("../outside"))
					}
					if problem == "wrong-size" {
						data = bytes.ReplaceAll(data, []byte(`"size":5`), []byte(`"size":6`))
					}
				}
				entries = append(entries, entry.Name, string(data))
			}
			if problem == "orphan-blob" {
				entries = append(entries, "files/"+uuidStr(newUUID()), "orphan")
			}
			if problem == "duplicate-blob" {
				entries = append(entries, "files/"+location, "first")
			}
			archive = restoreZip(t, entries...)
			source := bytes.Clone(archive)
			root := t.TempDir()
			disk, err := storageapi.NewDiskBackend(root, []byte("test-secret"))
			if err != nil {
				t.Fatal(err)
			}
			store := &restoreFaultStore{ObjectStore: disk}
			_, err = backup.RestoreV1Zip(ctx, pool, bytes.NewReader(archive), int64(len(archive)), appID, store)
			if err == nil {
				t.Fatal("invalid archive accepted")
			}
			if store.puts != 0 {
				t.Errorf("invalid archive wrote %d objects before full validation", store.puts)
			}
			after, _ := exportApp(t, ctx, pool, appID, backup.ExportOptions{})
			if !bytes.Equal(before, after) {
				t.Error("invalid archive changed target SQL state")
			}
			if !bytes.Equal(source, archive) {
				t.Error("invalid archive changed source")
			}
			if files, err := os.ReadDir(root); err != nil || len(files) != 0 {
				t.Errorf("invalid archive changed object root: %v %v", files, err)
			}
		})
	}
}

func TestV1RestoreCommitRejectionCleansFiles(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()
	before, _ := exportApp(t, ctx, pool, appID, backup.ExportOptions{})
	if _, err := pool.Exec(ctx, `CREATE FUNCTION reject_restore_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected commit rejection'; END $$;
	CREATE CONSTRAINT TRIGGER reject_restore_commit AFTER INSERT ON triples DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_restore_commit()`); err != nil {
		t.Fatal(err)
	}
	id, location := uuidStr(newUUID()), uuidStr(newUUID())
	archive := fileArchive(t, []string{id}, []string{location}, []string{"first"})
	root := t.TempDir()
	disk, err := storageapi.NewDiskBackend(root, []byte("test-secret"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = backup.RestoreV1Zip(ctx, pool, bytes.NewReader(archive), int64(len(archive)), appID, disk)
	if err == nil || !strings.Contains(err.Error(), "injected commit rejection") || errors.Is(err, backup.ErrCommitOutcomeUnknown) {
		t.Fatalf("known commit rejection incorrectly classified: %v", err)
	}
	after, _ := exportApp(t, ctx, pool, appID, backup.ExportOptions{})
	if !bytes.Equal(before, after) {
		t.Error("rejected commit changed target state")
	}
	reopened, err := storageapi.NewDiskBackend(root, []byte("test-secret"))
	if err != nil {
		t.Fatal(err)
	}
	if object, err := reopened.Open(uuidStr(appID) + "/" + id); !errors.Is(err, os.ErrNotExist) {
		if object != nil {
			_ = object.Body.Close()
		}
		t.Errorf("rejected commit left an object: %v", err)
	}
}
