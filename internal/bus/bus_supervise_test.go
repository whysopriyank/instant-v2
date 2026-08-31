package bus

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// dyingConn fails its first WaitForNotification with a transient error.
type dyingConn struct {
	failLeft atomic.Int64
}

func (f *dyingConn) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func (f *dyingConn) WaitForNotification(ctx context.Context) (*pgconn.Notification, error) {
	if f.failLeft.Add(-1) >= 0 {
		return nil, errors.New("pg: server closed the connection unexpectedly")
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestRunSupervisedReconnects pins that one transient LISTEN/NOTIFY failure
// does not end cross-node invalidation for the process lifetime: the
// supervisor re-acquires a fresh connection instead of returning like
// RunWithLogger does. Backoff is floored by reconnectInitial (1s), so two
// acquisitions fit comfortably inside the 5s bound.
func TestRunSupervisedReconnects(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var acquires atomic.Int64
	reconnected := make(chan struct{})
	acquire := func(ctx context.Context) (Conn, func(), error) {
		if acquires.Add(1) == 2 {
			close(reconnected)
		}
		c := &dyingConn{}
		c.failLeft.Store(1) // every conn dies once → forces repeat retries
		return c, func() {}, nil
	}

	done := make(chan struct{})
	go func() {
		RunSupervised(ctx, acquire, func(Invalidation) {}, slog.New(slog.DiscardHandler))
		close(done)
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("supervisor did not stop after cancellation")
		}
	})
	select {
	case <-reconnected:
	case <-ctx.Done():
		t.Fatalf("supervisor did not re-acquire after connection loss (acquires=%d)", acquires.Load())
	}
}
