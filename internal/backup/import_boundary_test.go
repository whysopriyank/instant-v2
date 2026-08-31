package backup_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/instant-v2/instant-v2/internal/backup"
)

type failedDumpReader struct{}

func (failedDumpReader) Read([]byte) (int, error) { return 0, errors.New("injected dump read failure") }

func TestImportReadErrorAfterChecksumRollsBack(t *testing.T) {
	ctx := context.Background()
	source, appID, cleanup := env(t)
	defer cleanup()
	seedTodoApp(t, ctx, source, appID, 1)
	dump, _ := exportApp(t, ctx, source, appID, backup.ExportOptions{})
	destination := newDatabase(t)
	_, err := backup.Import(ctx, destination, io.MultiReader(bytes.NewReader(dump), failedDumpReader{}), appID)
	if err == nil || !strings.Contains(err.Error(), "injected dump read failure") {
		t.Errorf("import error = %v, want underlying read failure after checksum", err)
	}
	var exists bool
	if err := destination.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM apps WHERE id=$1)`, appID).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("failed import committed the app instead of rolling back")
	}
}

func TestRestoreObjectWithoutStore(t *testing.T) {
	h := &backup.Handler{AdminTokenCheck: func(context.Context, string, string) (bool, error) { return true, nil }}
	req := httptest.NewRequest(http.MethodPost, "/backup/00000000-0000-4000-8000-000000000001/restore-object?key=dump", nil)
	rec := httptest.NewRecorder()
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Errorf("restore-object panicked instead of returning 503: %v", recovered)
		}
	}()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || rec.Body.String() != `{"message":"no object store wired"}` {
		t.Fatalf("status/body = %d %s, want 503 no object store wired", rec.Code, rec.Body.String())
	}
}
