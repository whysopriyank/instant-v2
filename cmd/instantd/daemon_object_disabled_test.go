package main

// DA-001c: object backup remains disabled with one stable explicit response.
// Against a real daemon process: every object route 503s, no backend is
// constructed, no staging/final artifact appears, no S3 credentials are
// needed, and ordinary storage upload/download still works.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/testkit"
)

func TestDA001ObjectBackupDisabled(t *testing.T) {
	fixture := testkit.NewPostgres(t, testkit.PostgresOptions{})
	bin := da001BuildInstantd(t)
	root, err := os.MkdirTemp("", "da001-backup-disabled-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })

	addr := fmt.Sprintf("127.0.0.1:%d", da001FreePort(t))
	logPath := filepath.Join(t.TempDir(), "backup-disabled.log")
	d := da001Start(t, bin, fixture.DSN, root, addr, logPath)
	da001WaitHealth(t, addr, true)
	defer da001Stop(t, d)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	appID := da001NewUUID(t)
	creatorID := da001NewUUID(t)
	adminID := da001NewUUID(t)
	appStr := platform.UUIDToStr(appID)
	adminStr := platform.UUIDToStr(adminID)
	st := storage.New(fixture.Pool)
	if err := st.WithTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO instant_users (id,email) VALUES ($1,$2)`, creatorID, "da001-backup@example.test"); err != nil {
			return err
		}
		if err := platform.CreateApp(ctx, tx, creatorID, appID, "da001-backup-disabled"); err != nil {
			return err
		}
		return platform.SetAdminToken(ctx, tx, appID, adminID)
	}); err != nil {
		t.Fatalf("seed app: %v", err)
	}
	base := "http://" + addr
	auth := map[string]string{"Authorization": "Bearer " + adminStr}

	before, err := da001ListFiles(root)
	if err != nil {
		t.Fatal(err)
	}

	// Every selected object route must 503 with the documented stable body.
	for _, tc := range []struct{ method, path string }{
		{http.MethodPut, "/backup/" + appStr + "/object?key=dumps/a.ndjson"},
		{http.MethodGet, "/backup/" + appStr + "/object?key=dumps/a.ndjson"},
		{http.MethodPost, "/backup/" + appStr + "/restore-object?key=dumps/a.ndjson"},
	} {
		req, err := http.NewRequest(tc.method, base+tc.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range auth {
			req.Header.Set(k, v)
		}
		resp, err := da001HTTP.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("%s %s = %d %q; want 503", tc.method, tc.path, resp.StatusCode, body)
		}
		if !strings.Contains(string(body), "no object store wired") {
			t.Fatalf("%s %s body = %q; want stable 503", tc.method, tc.path, body)
		}
	}

	// No local staging or final backup object may appear under the storage
	// root as a result of the disabled calls.
	afterDisabled, err := da001ListFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterDisabled) != len(before) {
		t.Fatalf("disabled object calls created files: before=%q after=%q", before, afterDisabled)
	}

	// No S3 backend is constructed: the daemon log assembles file storage
	// only and never mentions an object store endpoint/bucket. The process
	// started with no S3 credential env at all.
	if b, _ := os.ReadFile(logPath); strings.Contains(strings.ToLower(string(b)), "s3") && strings.Contains(strings.ToLower(string(b)), "bucket") {
		t.Fatalf("disabled daemon wired an object store:\n%s", b)
	}

	// Ordinary local storage upload/download remains functional without any
	// provider contact.
	payload := []byte("da001-backup-disabled-storage-still-works")
	id, _ := da001Upload(t, base, appStr, adminStr, "docs/still-here.txt", payload, "text/plain; charset=utf-8")
	got, _, _ := da001Download(t, base, appStr, adminStr, id)
	if !bytes.Equal(got, payload) {
		t.Fatal("storage round trip failed while object backup is disabled")
	}

	// Local (non-object) backup export still streams; object disablement
	// does not break the selected local backup surface.
	req, _ := http.NewRequest(http.MethodGet, base+"/backup/"+appStr, nil)
	for k, v := range auth {
		req.Header.Set(k, v)
	}
	resp, err := da001HTTP.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	dump, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.HasPrefix(string(dump), `{"kind":"header"`) {
		t.Fatalf("local backup export = %d %q; want NDJSON stream", resp.StatusCode, dump)
	}
}

func da001ListFiles(root string) ([]string, error) {
	var out []string
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			rel, _ := filepath.Rel(root, p)
			out = append(out, rel)
		}
		return nil
	})
	return out, err
}
