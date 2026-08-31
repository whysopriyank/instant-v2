package backup_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/instant-v2/instant-v2/internal/backup"
	"github.com/instant-v2/instant-v2/internal/platform"
)

// TestExportResumeOffset verifies ?from-offset=N semantics: the first N
// logical records are skipped, the emitted dump still self-verifies on
// import, and counts reflect only what was emitted.
func TestExportResumeOffset(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()

	seedTodoApp(t, ctx, pool, appID, 2)
	full, fullCounts := exportApp(t, ctx, pool, appID, backup.ExportOptions{})
	if fullCounts.Transactions != 1 {
		t.Fatalf("expected 1 journal record in full dump: %+v", fullCounts)
	}
	if got := strings.Count(string(full), `"kind":"triple"`); got != 10 {
		t.Fatalf("full dump triple records: %d", got)
	}

	// Skip the first 2 logical records (both attrs).
	partial, partCounts := exportApp(t, ctx, pool, appID, backup.ExportOptions{FromOffset: 2})
	if partCounts.Attrs != fullCounts.Attrs-2 || partCounts.Triples != fullCounts.Triples {
		t.Fatalf("offset counts wrong: partial=%+v full=%+v", partCounts, fullCounts)
	}
	if !bytes.Contains(partial, []byte(`"kind":"header"`)) || !bytes.Contains(partial, []byte(`"kind":"checksum"`)) {
		t.Fatal("partial dump must keep header and checksum lines")
	}
	// First two attr lines must be gone (attrs are ordered by id).
	nAttrs := strings.Count(string(partial), `{"kind":"attr"`)
	if nAttrs != 2 {
		t.Fatalf("expected 2 remaining attr lines, got %d", nAttrs)
	}

	pool = newDatabase(t)
	// Import of the partial dump fails cleanly (triples reference missing
	// attrs) but its checksum still validates ordering-wise; assert the
	// failure is about unknown attrs, not checksum.
	_, err := backup.Import(ctx, pool, bytes.NewReader(partial), appID)
	if err == nil || !strings.Contains(err.Error(), "unknown attrs") {
		t.Fatalf("expected unknown-attr error for partial dump, got: %v", err)
	}

	pool2, appID2, cleanup2 := env(t)
	defer cleanup2()
	seedTodoApp(t, ctx, pool2, appID2, 1)
	// Export skipping all triples+attrs+rule → only transactions remain.
	all, _ := exportApp(t, ctx, pool2, appID2, backup.ExportOptions{})
	totalRecords := countLogical(all)
	tail, tailCounts := exportApp(t, ctx, pool2, appID2, backup.ExportOptions{FromOffset: totalRecords - tailCount(pool2, appID2, t)})
	if tailCounts.Transactions <= 0 || tailCounts.Triples != 0 || tailCounts.Attrs != 0 {
		t.Fatalf("tail counts unexpected: %+v", tailCounts)
	}
	pool2 = newDatabase(t)
	if _, err := backup.Import(ctx, pool2, bytes.NewReader(tail), appID2); err != nil {
		t.Fatalf("tail-only import failed: %v", err)
	}
}

// countLogical counts logical record lines (header/checksum excluded).
func countLogical(dump []byte) int {
	n := 0
	for _, kind := range []string{"attr", "triple", "rule", "transaction"} {
		n += bytes.Count(dump, []byte(`{"kind":"`+kind+`"`))
	}
	return n
}

func tailCount(pool *pgxpool.Pool, appID [16]byte, t *testing.T) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM transactions WHERE app_id=$1`, appID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestExportMemoryCap proves streaming: exporting 100k triples keeps heap
// growth under a fixed cap (rows stream out of PG one at a time; nothing
// accumulates server- or client-side).
func TestExportMemoryCap(t *testing.T) {
	if testing.Short() {
		t.Skip("memory cap test in short mode")
	}
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()

	const n = 100_000
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	a, err := platform.GetOrCreateAttr(ctx, tx, appID, "big", "payload", "blob", "one", false, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// Bulk seed: one INSERT per 10k chunk with PG-computed md5 (cardinality-
	// one blob attr → ea=true, all other flags false). Much faster than the
	// storage helper for a pure fixture.
	const chunk = 10_000
	vals := make([]string, 0, chunk)
	idStrs := make([]string, 0, chunk)
	for base := 0; base < n; base += chunk {
		vals = vals[:0]
		idStrs = idStrs[:0]
		for i := base; i < base+chunk && i < n; i++ {
			idStrs = append(idStrs, uuidStr(newUUID()))
			vals = append(vals, fmt.Sprintf("payload-%d", i))
		}
		if _, err := pool.Exec(ctx, `
			WITH pairs AS (
				SELECT e.e AS entity_id, u.v AS value_text, u.ord
				FROM unnest($3::text[]) WITH ORDINALITY AS u(v, ord)
				JOIN unnest($4::text[]) WITH ORDINALITY AS e(e, ord) ON e.ord = u.ord
			)
			INSERT INTO triples (app_id, entity_id, attr_id, value, value_md5,
			                     ea, eav, av, ave, vae)
			SELECT $1, p.entity_id::uuid, $2, to_json(p.value_text),
			       md5(to_json(p.value_text)::text), true, false, false, false, false
			FROM pairs p`,
			appID, a.ID, vals, idStrs); err != nil {
			t.Fatal(err)
		}
	}

	runtime.GC()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	type countingWriter struct {
		n    int64
		last strings.Builder
	}
	cw := &countingWriter{}
	w := ioWriterFunc(func(p []byte) (int, error) {
		cw.n += int64(len(p))
		if cw.n > cwKeptBytes {
			// keep only the trailing window
			s := cw.last.String()
			cw.last.Reset()
			s += string(p)
			if len(s) > cwKeptBytes {
				s = s[len(s)-cwKeptBytes:]
			}
			cw.last.WriteString(s)
		} else {
			cw.last.Write(p)
		}
		return len(p), nil
	})

	counts, err := backup.Export(ctx, w, pool, appID, backup.ExportOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if counts.Triples != n {
		t.Fatalf("exported %d triples, want %d", counts.Triples, n)
	}
	tail := cw.last.String()
	if !strings.Contains(tail, `"kind":"checksum"`) || !strings.Contains(tail, `"records":`) {
		t.Fatalf("missing checksum trailer in tail: %q", tail)
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	growth := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	if growth < 0 {
		growth = 0
	}
	const capBytes = 32 << 20
	if growth > capBytes {
		t.Fatalf("export accumulated %d bytes of live heap (> %d) — not streaming?", growth, capBytes)
	}
}

const cwKeptBytes = 512

func ioWriterFunc(fn func([]byte) (int, error)) io.Writer { return fnWriter{fn} }

type fnWriter struct{ fn func([]byte) (int, error) }

func (f fnWriter) Write(p []byte) (int, error) { return f.fn(p) }

// ---- HTTP surface ----
