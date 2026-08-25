package platform

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// gatedQ blocks every Query until the gate closes, then returns zero rows.
type gatedQ struct {
	gate  chan struct{}
	first atomic.Bool
}

func (g *gatedQ) QueryRow(ctx context.Context, _ string, _ ...any) pgx.Row {
	select {
	case <-g.gate:
	case <-ctx.Done():
	}
	return &gatedRows{}
}

func (g *gatedQ) Query(ctx context.Context, _ string, _ ...any) (pgx.Rows, error) {
	if g.first.CompareAndSwap(false, true) {
		select {
		case <-g.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &gatedRows{}, nil
}

// gatedRows is an exhausted pgx.Rows: zero iterations, no error.
type gatedRows struct{}

func (r *gatedRows) Close()                        {}
func (r *gatedRows) Next() bool                    { return false }
func (r *gatedRows) Err() error                    { return nil }
func (r *gatedRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }
func (r *gatedRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}
func (r *gatedRows) Scan(dest ...any) error { return pgx.ErrNoRows }
func (r *gatedRows) Values() ([]any, error) { return nil, nil }
func (r *gatedRows) RawValues() [][]byte    { return nil }
func (r *gatedRows) Conn() *pgx.Conn        { return nil }

// TestForDoesNotBlockAcrossApps pins the lock-across-I/O fix: c.mu guards
// the cache maps only. A slow/hung catalog load for app A must NOT stall a
// concurrent For for app B — previously one global mutex was held across
// LoadAttrCatalog, so any cold load serialized every transact and refresh
// in the process behind it.
func TestForDoesNotBlockAcrossApps(t *testing.T) {
	gate := make(chan struct{})
	q := &gatedQ{gate: gate}
	c := NewCatalogCache(q, q)
	appA := "6f8e99dd-4f28-450b-85df-001f945c425e"
	appB := "77436b4c-1d7f-4760-b9f9-b97081c10431"

	errA := make(chan error, 1)
	go func() {
		_, err := c.For(context.Background(), appA) // parks inside the stubbed DB query
		errA <- err
	}()
	time.Sleep(50 * time.Millisecond) // let A enter its query

	done := make(chan struct{})
	go func() {
		if _, err := c.For(context.Background(), appB); err != nil {
			t.Errorf("For(appB): %v", err)
		}
		close(done)
	}()
	select {
	case <-done:
		// B completed while A's load was still parked — no lock across I/O.
	case <-time.After(1 * time.Second):
		t.Fatal("For(appB) blocked behind appA's in-flight load: mutex held across DB query")
	}

	close(gate)
	select {
	case err := <-errA:
		if err != nil {
			t.Fatalf("For(appA): %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("appA load never finished after gate release")
	}

	// RuleDocFor must follow the same discipline.
	gate2 := make(chan struct{})
	q2 := &gatedQ{gate: gate2}
	c2 := NewCatalogCache(q2, q2)
	errC := make(chan error, 1)
	go func() {
		_, err := c2.RuleDocFor(context.Background(), appA)
		errC <- err
	}()
	time.Sleep(50 * time.Millisecond)

	done2 := make(chan struct{})
	go func() {
		if _, err := c2.RuleDocFor(context.Background(), appB); err != nil {
			t.Errorf("RuleDocFor(appB): %v", err)
		}
		close(done2)
	}()
	select {
	case <-done2:
	case <-time.After(1 * time.Second):
		t.Fatal("RuleDocFor(appB) blocked behind appA's load")
	}
	close(gate2)
	if err := <-errC; err != nil {
		t.Fatalf("RuleDocFor(appA): %v", err)
	}
}
