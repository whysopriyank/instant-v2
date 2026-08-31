package benchharness

import (
	"context"
	"fmt"
	"sync"
)

// runReceivers owns only the measured run's event readers and receipt stream.
// Session setup and warm-up finish before readers start; stop joins every
// reader before closing receipts so Execute can drain a complete observation.
type runReceivers struct {
	driver  *TargetDriver
	clients []*TargetSession
	writers []*TargetSession
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	once    sync.Once

	receipts chan Receipt
	prefixes *commitPrefixes
	mu       sync.Mutex
	errors   []string
}

func newRunReceivers(ctx context.Context, driver *TargetDriver, plan RunPlan, clients, writers []*TargetSession) *runReceivers {
	ctx, cancel := context.WithCancel(ctx)
	r := &runReceivers{driver: driver, clients: clients, writers: writers, ctx: ctx, cancel: cancel, receipts: make(chan Receipt, max(64, plan.Workload.Subscribers))}
	r.prefixes = newCommitPrefixesFor(r.emit, plan.Workload.Family, plan.WriterID)
	for i, client := range clients {
		queryID := plan.Workload.Fixture.Queries[i].ID
		r.startReader(client, queryID, "client-"+queryID)
	}
	// Transports publish transact-ok even after the normalized Ack is consumed.
	// Drain each T writer independently so its event buffer cannot stall writes.
	for _, writer := range writers {
		r.startWriter(writer)
	}
	return r
}

func (r *runReceivers) emit(item Receipt) {
	select {
	case r.receipts <- item:
	case <-r.ctx.Done():
	}
}

func (r *runReceivers) recordProtocol(clientID string, ev SessionEvent, decodeErr error) {
	message := "protocol error"
	if decodeErr != nil {
		message = decodeErr.Error()
	} else if ev.Error != "" {
		message = ev.Error
	}
	r.mu.Lock()
	r.errors = append(r.errors, fmt.Sprintf("client=%s op=%s: %s", clientID, ev.Op, message))
	r.mu.Unlock()
}

func (r *runReceivers) protocolErrors() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.errors...)
}

func (r *runReceivers) startReader(client *TargetSession, queryID, recipientID string) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for {
			if err := client.waitRead(r.ctx); err != nil {
				return
			}
			select {
			case <-r.ctx.Done():
				return
			case ev, ok := <-client.Events():
				if !ok {
					return
				}
				if ev.Op == "error" || ev.Op == "protocol-error" {
					client.recordEventFor(ev, ev.Error, queryID, recipientID)
					r.recordProtocol(client.clientID, ev, nil)
					continue
				}
				if ev.Op != "refresh-ok" && ev.Op != "refresh-ok-delta" {
					client.recordEventFor(ev, "", queryID, recipientID)
					continue
				}
				items, err := r.driver.decodeReceipts(client, ev, queryID, recipientID, r.prefixes)
				if err != nil {
					client.recordEventFor(ev, err.Error(), queryID, recipientID)
					r.recordProtocol(client.clientID, ev, err)
					continue
				}
				client.recordEventFor(ev, "", queryID, recipientID)
				for _, item := range items {
					r.emit(item)
				}
			}
		}
	}()
}

func (r *runReceivers) startWriter(writer *TargetSession) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for {
			select {
			case <-r.ctx.Done():
				return
			case ev, ok := <-writer.Events():
				if !ok {
					return
				}
				protocolErr := ""
				if ev.Op == "error" || ev.Op == "protocol-error" {
					protocolErr = ev.Error
					r.recordProtocol(writer.clientID, ev, nil)
				}
				writer.recordEvent(ev, protocolErr)
			}
		}
	}()
}

func (r *runReceivers) stop() {
	r.once.Do(func() {
		r.cancel()
		closeTargetSessions(r.clients)
		closeTargetSessions(r.writers)
		r.wg.Wait()
		close(r.receipts)
	})
}

func (r *runReceivers) hooks(plan RunPlan, evidence *evidenceCollector) RunHooks {
	return RunHooks{
		Submit: func(ctx context.Context, mutation Mutation) (Ack, error) {
			return r.clients[0].Transact(ctx, mutation)
		},
		SubmitWriter: func(ctx context.Context, writer int, mutation Mutation) (Ack, error) {
			if writer < 0 || writer >= len(r.writers) {
				return Ack{}, fmt.Errorf("writer index %d is not configured", writer)
			}
			return r.writers[writer].Transact(ctx, mutation)
		},
		Receipts:     r.receipts,
		StopReceipts: r.stop,
		OnCommitted:  func(mutation Mutation, ack Ack, prefix int) { r.prefixes.Record(mutation, ack, prefix) },
		OnClientBehavior: func(ctx context.Context, clientID, _ int, behavior ClientBehavior) error {
			if clientID < 0 || clientID >= len(r.clients) {
				return fmt.Errorf("behavior client index %d is outside subscriber set", clientID)
			}
			if behavior.PauseReads {
				r.clients[clientID].PauseReads(behavior.PauseFor)
			}
			if behavior.Reconnect {
				if err := waitPhase(ctx, behavior.Backoff); err != nil {
					return err
				}
				if err := r.clients[clientID].Reconnect(ctx); err != nil {
					return err
				}
				query := plan.Workload.Fixture.Queries[clientID]
				r.startReader(r.clients[clientID], query.ID, "client-"+query.ID)
			}
			return nil
		},
		FinalSnapshot: r.driver.finalSnapshotter(plan.Workload.Fixture, evidence),
	}
}
