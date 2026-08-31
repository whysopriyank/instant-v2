package platform

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/perms"
)

// snapshotQuery captures the first read before the invalidation barrier. Later
// reads see the replacement, as a fresh PostgreSQL statement would after commit.
type snapshotQuery struct {
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
	rows    func(old bool) pgx.Rows
}

func (q *snapshotQuery) Query(ctx context.Context, _ string, _ ...any) (pgx.Rows, error) {
	first := q.calls.Add(1) == 1
	rows := q.rows(first)
	if first {
		close(q.entered)
		select {
		case <-q.release:
		case <-ctx.Done():
			rows.Close()
			return nil, ctx.Err()
		}
	}
	return rows, nil
}

func (*snapshotQuery) QueryRow(context.Context, string, ...any) pgx.Row {
	panic("cache loads should use Query")
}

type ruleSnapshotRows struct {
	gatedRows
	raw  []byte
	done bool
}

func (r *ruleSnapshotRows) Next() bool { return !r.done }
func (r *ruleSnapshotRows) Scan(dest ...any) error {
	*dest[0].(*[]byte) = r.raw
	r.done = true
	return nil
}

func TestCatalogCacheInvalidationDuringLoad(t *testing.T) {
	appID, attrID := [16]byte{1}, [16]byte{2}
	app := UUIDToStr(appID)
	cases := []struct {
		name string
		rows func(old bool) pgx.Rows
		load func(context.Context, *CatalogCache) (string, error)
		want string
	}{
		{
			name: "For",
			rows: func(old bool) pgx.Rows {
				etype, label := "todos", "fresh"
				if old {
					label = "stale"
				}
				return &scriptedRows{rows: []Attr{{ID: attrID, AppID: appID, Etype: &etype, Label: &label, ValueType: "blob", Cardinality: "one"}}}
			},
			load: func(ctx context.Context, cache *CatalogCache) (string, error) {
				cat, err := cache.For(ctx, app)
				if err != nil {
					return "", err
				}
				attr, ok := cat.ByID(attrID)
				if !ok || cat.AppID != appID || attr.AppID != appID || attr.Label == nil {
					return "", fmt.Errorf("unexpected catalog: %+v attr=%+v", cat, attr)
				}
				return *attr.Label, nil
			},
			want: "fresh",
		},
		{
			name: "RuleDocFor",
			rows: func(old bool) pgx.Rows {
				allow := "false"
				if old {
					allow = "true"
				}
				return &ruleSnapshotRows{raw: []byte(`{"todos":{"allow":{"view":"` + allow + `"}}}`)}
			},
			load: func(ctx context.Context, cache *CatalogCache) (string, error) {
				doc, err := cache.RuleDocFor(ctx, app)
				if err != nil {
					return "", err
				}
				allow, err := perms.Check("todos", "view", doc, perms.Bindings{})
				return strconv.FormatBool(allow), err
			},
			want: "false",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			q := &snapshotQuery{entered: make(chan struct{}), release: make(chan struct{}), rows: tc.rows}
			cache := NewCatalogCache(q, q)
			var release sync.Once
			completed := make(chan struct{})
			var got string
			var loadErr error
			go func() {
				got, loadErr = tc.load(ctx, cache)
				close(completed)
			}()
			t.Cleanup(func() {
				cancel()
				release.Do(func() { close(q.release) })
				select {
				case <-completed:
				case <-time.After(time.Second):
					t.Error("cache loader did not exit")
				}
			})
			select {
			case <-q.entered:
			case <-ctx.Done():
				t.Fatal("old snapshot was not captured")
			}
			cache.Invalidate(app)
			release.Do(func() { close(q.release) })
			select {
			case <-completed:
			case <-ctx.Done():
				t.Fatal("cache load did not finish")
			}
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			if got != tc.want {
				t.Errorf("in-flight read returned stale data: got %q want %q", got, tc.want)
			}
			cached, err := tc.load(ctx, cache)
			if err != nil {
				t.Fatal(err)
			}
			if cached != tc.want {
				t.Errorf("stale data was republished: got %q want %q", cached, tc.want)
			}
			if calls := q.calls.Load(); calls != 2 {
				t.Errorf("query count = %d, want one old read and one retry", calls)
			}
		})
	}
}
