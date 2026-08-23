package waltail_test

import (
	"database/sql"
	"os"
	"testing"

	"github.com/instant-v2/instant-v2/internal/waltail"
)

// TestCheckpointRestartProvesMonotonicity covers the crash-replay contract:
// a confirmed LSN survives process restart (fresh Checkpoint handle), and
// stale confirms (older LSN) can never move the watermark backwards.
func TestCheckpointRestartProvesMonotonicity(t *testing.T) {
	d := os.Getenv("DATABASE_URL")
	if d == "" {
		t.Skip("DATABASE_URL not set")
	}
	db, err := sql.Open("pgx", d)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Fresh namespace for this test.
	if _, err := db.Exec(`DROP TABLE IF EXISTS tail_state`); err != nil {
		t.Fatal(err)
	}
	cp1, err := waltail.OpenCheckpoint(db)
	if err != nil {
		t.Fatal(err)
	}
	if got := cp1.Last(); got != 0 {
		t.Fatalf("fresh checkpoint = %d, want 0", got)
	}

	lsnA := uint64(0x2EBA860)
	lsnB := lsnA + 4096

	cp1.Confirm(lsnA)
	if got := cp1.Last(); got != lsnA {
		t.Fatalf("after confirm A: %d, want %d", got, lsnA)
	}
	// Stale confirm (below current watermark) must be ignored.
	cp1.Confirm(lsnA - 1)
	if got := cp1.Last(); got != lsnA {
		t.Fatalf("stale confirm moved watermark: %d, want %d", got, lsnA)
	}
	cp1.Confirm(lsnB)

	// "Crash": brand-new process state over the same table.
	cp2, err := waltail.OpenCheckpoint(db)
	if err != nil {
		t.Fatal(err)
	}
	if got := cp2.Last(); got != lsnB {
		t.Fatalf("restart lost checkpoint: %d, want %d", got, lsnB)
	}
}
