package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/instant-v2/instant-v2/internal/bus"
	"github.com/instant-v2/instant-v2/internal/config"
	"github.com/instant-v2/instant-v2/internal/reactive"
)

// pgPublisher adapts a dedicated connection to bus.Publisher. The listener
// gets its own conn: pgconn is not safe for concurrent Exec +
// WaitForNotification. The mutex serializes publishes because pgx.Conn is
// single-flight — concurrent goroutines publishing invalidations would trip
// "conn busy".
type pgPublisher struct {
	mu sync.Mutex
	pg bus.Conn
}

func (p *pgPublisher) PublishInvalidation(ctx context.Context, inv bus.Invalidation) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return bus.Publish(ctx, p.pg, inv)
}

// dedicatedConn pins one pool connection for the process lifetime. LISTEN
// state dies with the connection, so the bus must never share pooled conns.
func dedicatedConn(ctx context.Context, pool *pgxpool.Pool) (*pgxpool.Conn, error) {
	return pool.Acquire(ctx)
}

func (a *appRuntime) startInvalidationBus(ctx context.Context, cfg config.Config) (*pgxpool.Conn, error) {
	if cfg.InvalidationBus != "postgres" {
		return nil, nil
	}
	pubConn, perr := dedicatedConn(ctx, a.pool)
	if perr != nil {
		return nil, fmt.Errorf("bus publisher conn: %w", perr)
	}
	a.publisher = &pgPublisher{pg: pubConn.Conn()}

	// Supervised listener: a Postgres restart must not end
	// cross-node invalidation for the process lifetime. The
	// supervisor re-acquires a dedicated conn and re-LISTENs with
	// bounded backoff; each attempt owns its conn's release.
	acquireListener := func(cctx context.Context) (bus.Conn, func(), error) {
		c, cerr := dedicatedConn(cctx, a.pool)
		if cerr != nil {
			return nil, nil, cerr
		}
		return c.Conn(), c.Release, nil
	}
	go bus.RunSupervised(ctx, acquireListener, func(inv bus.Invalidation) {
		// APPLY ONLY — never republish. Routing received
		// events through the invalidate closures would echo
		// every event back onto the bus from every node: an
		// infinite amplify loop that starves the listeners
		// (observed live as pgx "conn busy" storms).
		// Entity-annotated events let peers splice too
		// (docs/09 §T2.5); degraded payloads carry attr ids
		// only and take the topic-wide path.
		//
		// Schema propagation: a peer's cached catalog must not
		// outlive another node's attrs.create — drop it so the
		// next query reloads (cross-node half of the staleness
		// fix; the writer's own node invalidates inline).
		if inv.AttrsChanged {
			a.cats.Invalidate(inv.AppID)
		}
		if len(inv.Changes) > 0 {
			changes := make([]reactive.Change, 0, len(inv.Changes))
			for _, c := range inv.Changes {
				changes = append(changes, reactive.Change{
					Etype: c.Etype, EntityID: c.EntityID, AttrIDs: c.AttrIDs,
				})
			}
			a.notifier.NotifyChanges(ctx, inv.AppID, changes, inv.TxID)
			return
		}
		a.notifier.Notify(ctx, inv.AppID, inv.AttrIDs, inv.TxID)
	}, a.logger)
	a.logger.Info("invalidation bus enabled", "channel", bus.Channel, "node", cfg.NodeName())
	return pubConn, nil
}

func (a *appRuntime) invalidate(ctx context.Context, appID string, attrIDs []string, txID int64, attrsChanged bool) {
	a.notifier.Notify(ctx, appID, attrIDs, txID)
	if a.publisher != nil {
		go func() {
			if err := a.publisher.PublishInvalidation(context.WithoutCancel(ctx),
				bus.Invalidation{AppID: appID, AttrIDs: attrIDs, TxID: txID, AttrsChanged: attrsChanged}); err != nil {
				a.logger.Warn("bus publish failed", "err", err)
			}
		}()
	}
}

func (a *appRuntime) invalidateChanges(ctx context.Context, appID string, changes []reactive.Change, txID int64, attrsChanged bool) {
	a.notifier.NotifyChanges(ctx, appID, changes, txID)
	if a.publisher != nil {
		attrSet := map[string]bool{}
		entity := make([]bus.EntityChange, 0, len(changes))
		for _, c := range changes {
			for _, attrID := range c.AttrIDs {
				attrSet[attrID] = true
			}
			attrs := append([]string(nil), c.AttrIDs...)
			entity = append(entity, bus.EntityChange{
				Etype: c.Etype, EntityID: c.EntityID, AttrIDs: attrs,
			})
		}
		attrs := make([]string, 0, len(attrSet))
		for attrID := range attrSet {
			attrs = append(attrs, attrID)
		}
		go func() {
			if err := a.publisher.PublishInvalidation(context.WithoutCancel(ctx),
				bus.Invalidation{AppID: appID, AttrIDs: attrs, TxID: txID, Changes: entity, AttrsChanged: attrsChanged}); err != nil {
				a.logger.Warn("bus publish failed", "err", err)
			}
		}()
	}
}
