package backup_test

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/instant-v2/instant-v2/internal/backup"
	"github.com/instant-v2/instant-v2/internal/platform"
)

func emptyRestoreTarget(t *testing.T, appID [16]byte) *pgxpool.Pool {
	t.Helper()
	pool, shellID, cleanup := env(t)
	t.Cleanup(cleanup)
	if _, err := pool.Exec(context.Background(), `UPDATE apps SET id=$1 WHERE id=$2`, appID, shellID); err != nil {
		t.Fatal(err)
	}
	return pool
}

func restoreZip(t *testing.T, entries ...string) []byte {
	t.Helper()
	var buf bytes.Buffer
	z := zip.NewWriter(&buf)
	for i := 0; i < len(entries); i += 2 {
		w, err := z.Create(entries[i])
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(entries[i+1])); err != nil {
			t.Fatal(err)
		}
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestRestoreRejectsNonemptyTarget(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()
	seedTodoApp(t, ctx, pool, appID, 1)
	before, _ := exportApp(t, ctx, pool, appID, backup.ExportOptions{})
	source := bytes.Clone(before)
	_, err := backup.Import(ctx, pool, bytes.NewReader(source), appID)
	if err == nil || !strings.Contains(err.Error(), "non-empty") {
		t.Errorf("restore error = %v; want explicit non-empty rejection", err)
	}
	after, _ := exportApp(t, ctx, pool, appID, backup.ExportOptions{})
	if !bytes.Equal(before, after) {
		t.Error("rejected restore changed target state")
	}
	if !bytes.Equal(before, source) {
		t.Error("restore changed source dump")
	}
}

func TestV1RestoreRejectsNonemptyTarget(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()
	seedTodoApp(t, ctx, pool, appID, 1)
	before, _ := exportApp(t, ctx, pool, appID, backup.ExportOptions{})
	archive := restoreZip(t, "config.json", `{"title":"overwrite","schema":{"blobs":{"new":{"text":{"valueType":"string"}}}}}`)
	source := bytes.Clone(archive)
	_, err := backup.RestoreV1Zip(ctx, pool, bytes.NewReader(archive), int64(len(archive)), appID)
	if err == nil || !strings.Contains(err.Error(), "non-empty") {
		t.Errorf("restore error = %v; want explicit non-empty rejection", err)
	}
	after, _ := exportApp(t, ctx, pool, appID, backup.ExportOptions{})
	if !bytes.Equal(before, after) {
		t.Error("rejected v1 restore changed target state")
	}
	if !bytes.Equal(source, archive) {
		t.Error("restore changed source ZIP")
	}
}

func TestV1RestoreRejectsUnwiredBlobs(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()
	before, _ := exportApp(t, ctx, pool, appID, backup.ExportOptions{})
	archive := restoreZip(t, "config.json", `{"title":"overwrite","schema":{"blobs":{"new":{"text":{"valueType":"string"}}}}}`, "files/00000000-0000-4000-8000-000000000001", "blob bytes")
	_, err := backup.RestoreV1Zip(ctx, pool, bytes.NewReader(archive), int64(len(archive)), appID)
	if err == nil {
		t.Error("restore without file store accepted and skipped blob")
	}
	after, _ := exportApp(t, ctx, pool, appID, backup.ExportOptions{})
	if !bytes.Equal(before, after) {
		t.Error("failed unwired blob restore changed target")
	}
}

func TestRestoreRejectsNonemptyComponents(t *testing.T) {
	for _, format := range []string{"ndjson", "v1zip"} {
		for _, component := range []string{"attrs", "rules", "transactions"} {
			t.Run(format+"/"+component, func(t *testing.T) {
				ctx := context.Background()
				pool, appID, cleanup := env(t)
				defer cleanup()
				dump, _ := exportApp(t, ctx, pool, appID, backup.ExportOptions{})
				switch component {
				case "attrs":
					tx, err := pool.Begin(ctx)
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = tx.Rollback(ctx) }()
					if _, err := platform.GetOrCreateAttr(ctx, tx, appID, "only", "value", "blob", "one", false, false); err != nil {
						t.Fatal(err)
					}
					if err := tx.Commit(ctx); err != nil {
						t.Fatal(err)
					}
				case "rules":
					if _, err := pool.Exec(ctx, `INSERT INTO rules (app_id, code) VALUES ($1,'{}')`, appID); err != nil {
						t.Fatal(err)
					}
				case "transactions":
					if _, err := pool.Exec(ctx, `INSERT INTO transactions (app_id) VALUES ($1)`, appID); err != nil {
						t.Fatal(err)
					}
				}
				before, _ := exportApp(t, ctx, pool, appID, backup.ExportOptions{})
				var err error
				if format == "ndjson" {
					_, err = backup.Import(ctx, pool, bytes.NewReader(dump), appID)
				} else {
					archive := restoreZip(t, "config.json", `{"title":"overwrite","schema":{"blobs":{"new":{"text":{"valueType":"string"}}}}}`)
					_, err = backup.RestoreV1Zip(ctx, pool, bytes.NewReader(archive), int64(len(archive)), appID)
				}
				if err == nil || !strings.Contains(err.Error(), "non-empty") {
					t.Errorf("restore error=%v, want non-empty rejection", err)
				}
				after, _ := exportApp(t, ctx, pool, appID, backup.ExportOptions{})
				if !bytes.Equal(before, after) {
					t.Error("nonempty component restore changed target state")
				}
			})
		}
	}
}

type pausedRestoreReader struct {
	header  []byte
	tail    []byte
	entered chan struct{}
	release chan struct{}
}

func (r *pausedRestoreReader) Read(p []byte) (int, error) {
	if len(r.header) > 0 {
		n := copy(p, r.header)
		r.header = r.header[n:]
		return n, nil
	}
	if r.entered != nil {
		close(r.entered)
		r.entered = nil
		<-r.release
	}
	if len(r.tail) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.tail)
	r.tail = r.tail[n:]
	return n, nil
}

func TestRestoreAdmissionBlocksConcurrentWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, appID, cleanup := env(t)
	defer cleanup()
	dump, _ := exportApp(t, ctx, pool, appID, backup.ExportOptions{})
	i := bytes.IndexByte(dump, '\n') + 1
	entered, release := make(chan struct{}), make(chan struct{})
	r := &pausedRestoreReader{header: dump[:i], tail: dump[i:], entered: entered, release: release}
	result := make(chan error, 1)
	go func() { _, err := backup.Import(ctx, pool, r, appID); result <- err }()
	defer func() {
		close(release)
		select {
		case err := <-result:
			if err != nil {
				t.Errorf("restore result: %v", err)
			}
		case <-ctx.Done():
			t.Error("restore did not stop")
		}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("restore did not reach locked read boundary")
	}
	writer, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Rollback(context.Background()) }()
	if _, err := writer.Exec(ctx, `SET LOCAL lock_timeout='50ms'`); err != nil {
		t.Fatal(err)
	}
	_, err = writer.Exec(ctx, `INSERT INTO rules(app_id,code) VALUES($1,'{}')`, appID)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Fatalf("concurrent writer=%v; want lock exclusion", err)
	}
}

func TestRestoreAllowsEmptyShellWithAdminToken(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()
	token := newUUID()
	if _, err := pool.Exec(ctx, `INSERT INTO app_admin_tokens(token,app_id) VALUES($1,$2)`, token, appID); err != nil {
		t.Fatal(err)
	}
	dump, _ := exportApp(t, ctx, pool, appID, backup.ExportOptions{})
	if _, err := backup.Import(ctx, pool, bytes.NewReader(dump), appID); err != nil {
		t.Fatal(err)
	}
	var preserved bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app_admin_tokens WHERE token=$1 AND app_id=$2)`, token, appID).Scan(&preserved); err != nil {
		t.Fatal(err)
	}
	if !preserved {
		t.Error("restore removed shell admin token")
	}
}

func TestImportCommitFailureRollsBackSequence(t *testing.T) {
	ctx := context.Background()
	source, appID, cleanup := env(t)
	defer cleanup()
	seedTodoApp(t, ctx, source, appID, 1)
	dump, _ := exportApp(t, ctx, source, appID, backup.ExportOptions{})
	target := newDatabase(t)
	if _, err := target.Exec(ctx, `CREATE FUNCTION reject_restore_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected commit rejection'; END $$;
	CREATE CONSTRAINT TRIGGER reject_restore_commit AFTER INSERT ON triples DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_restore_commit()`); err != nil {
		t.Fatal(err)
	}
	_, err := backup.Import(ctx, target, bytes.NewReader(dump), appID)
	if err == nil || !strings.Contains(err.Error(), "injected commit rejection") || errors.Is(err, backup.ErrCommitOutcomeUnknown) {
		t.Fatalf("known commit rejection=%v", err)
	}
	var sequence int64
	var called, appExists bool
	if err := target.QueryRow(ctx, `SELECT last_value,is_called FROM transactions_id_seq`).Scan(&sequence, &called); err != nil {
		t.Fatal(err)
	}
	if sequence != 1 || called {
		t.Errorf("failed commit changed sequence=%d/%v", sequence, called)
	}
	if err := target.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM apps WHERE id=$1)`, appID).Scan(&appExists); err != nil {
		t.Fatal(err)
	}
	if appExists {
		t.Error("failed commit persisted restored app")
	}
}

func TestImportNeverMovesSequenceBackward(t *testing.T) {
	ctx := context.Background()
	source, appID, cleanup := env(t)
	defer cleanup()
	seedTodoApp(t, ctx, source, appID, 1)
	dump, _ := exportApp(t, ctx, source, appID, backup.ExportOptions{})
	target := newDatabase(t)
	if _, err := target.Exec(ctx, `SELECT setval('transactions_id_seq',500,true)`); err != nil {
		t.Fatal(err)
	}
	if _, err := backup.Import(ctx, target, bytes.NewReader(dump), appID); err != nil {
		t.Fatal(err)
	}
	var next int64
	if err := target.QueryRow(ctx, `SELECT nextval('transactions_id_seq')`).Scan(&next); err != nil {
		t.Fatal(err)
	}
	if next != 501 {
		t.Errorf("next transaction id=%d, want retained sequence floor501", next)
	}
}
