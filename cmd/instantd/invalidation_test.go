package main

import (
	"context"
	"log/slog"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/bus"
	"github.com/instant-v2/instant-v2/internal/config"
	"github.com/instant-v2/instant-v2/internal/reactive"
)

type publishedInvalidation struct {
	ctx context.Context
	inv bus.Invalidation
}

type capturePublisher struct{ calls chan publishedInvalidation }

func (p capturePublisher) PublishInvalidation(ctx context.Context, inv bus.Invalidation) error {
	p.calls <- publishedInvalidation{ctx, inv}
	return nil
}

func TestInvalidationPublishContract(t *testing.T) {
	for _, changes := range []bool{false, true} {
		t.Run(map[bool]string{false: "topics", true: "entities"}[changes], func(t *testing.T) {
			calls := make(chan publishedInvalidation, 1)
			a := &appRuntime{
				notifier:  &reactive.Notifier{Store: reactive.NewStore()},
				publisher: capturePublisher{calls}, logger: slog.Default(),
			}
			type requestKey struct{}
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), requestKey{}, "request"))
			cancel()
			want := bus.Invalidation{AppID: "app", AttrIDs: []string{"a", "b"}, TxID: 42, AttrsChanged: true}
			if changes {
				input := []reactive.Change{{Etype: "todos", EntityID: "entity", AttrIDs: []string{"b", "a", "b"}}}
				a.invalidateChanges(ctx, "app", input, 42, true)
				input[0].AttrIDs[0] = "changed-after-call"
				want.Changes = []bus.EntityChange{{Etype: "todos", EntityID: "entity", AttrIDs: []string{"b", "a", "b"}}}
			} else {
				a.invalidate(ctx, "app", []string{"a", "b"}, 42, true)
			}
			select {
			case got := <-calls:
				sort.Strings(got.inv.AttrIDs) // top-level attr-set order is intentionally unspecified
				if !reflect.DeepEqual(got.inv, want) {
					t.Fatalf("published %#v; want %#v", got.inv, want)
				}
				if got.ctx.Err() != nil || got.ctx.Value(requestKey{}) != "request" {
					t.Fatal("publication must detach cancellation but retain request context values")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("invalidation was not published")
			}
		})
	}
}

func TestDisabledInvalidationBusNeedsNoPool(t *testing.T) {
	a := &appRuntime{}
	conn, err := a.startInvalidationBus(context.Background(), config.Config{InvalidationBus: "none"})
	if conn != nil || err != nil || a.publisher != nil {
		t.Fatalf("disabled bus: conn=%v err=%v publisher=%v", conn, err, a.publisher)
	}
}
