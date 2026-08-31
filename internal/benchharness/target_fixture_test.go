package benchharness

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type scriptedTargetSession struct {
	mu        sync.Mutex
	events    chan SessionEvent
	closed    chan struct{}
	broadcast func(SessionEvent)
}

func newScriptedTargetSession() *scriptedTargetSession {
	return &scriptedTargetSession{events: make(chan SessionEvent, 16), closed: make(chan struct{})}
}

func (s *scriptedTargetSession) Send(_ context.Context, msg SessionMessage) (Ack, error) {
	switch msg.Op {
	case "init":
		s.events <- SessionEvent{Op: "init-ok", ClientEventID: msg.ClientEventID}
	case "add-query":
		s.events <- SessionEvent{Op: "add-query-ok", ClientEventID: msg.ClientEventID}
		s.events <- scriptedRefresh(0, "initial")
	case "transact":
		s.events <- SessionEvent{Op: "transact-ok", ClientEventID: msg.ClientEventID, TxID: "1", ServerTransactionID: "1", At: time.Now()}
		if s.broadcast != nil {
			s.broadcast(scriptedRefresh(1, "changed"))
		} else {
			s.events <- scriptedRefresh(1, "changed")
		}
		return Ack{Accepted: true, ServerTransactionID: "1"}, nil
	}
	return Ack{Accepted: true}, nil
}

func (s *scriptedTargetSession) Events() <-chan SessionEvent { return s.events }

func (s *scriptedTargetSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.closed:
	default:
		close(s.closed)
		close(s.events)
	}
	return nil
}

type scriptedDialer struct {
	mu       sync.Mutex
	sessions []*scriptedTargetSession
}

type countingRunDialer struct {
	mu             sync.Mutex
	sessions       []*countingRunSession
	nextTx         int64
	value          int64
	ids            []string
	transactionIDs []string
	probePresent   atomic.Bool
	provisions     atomic.Int64
	staleCleanOnce atomic.Bool
}

type countingRunSession struct {
	dialer    *countingRunDialer
	events    chan SessionEvent
	clientID  string
	mu        sync.Mutex
	closed    bool
	snapshots []bool
}

func (d *countingRunDialer) Dial(_ context.Context, opts SessionOptions) (Session, error) {
	s := &countingRunSession{dialer: d, events: make(chan SessionEvent, 64), clientID: opts.ClientID}
	d.mu.Lock()
	d.sessions = append(d.sessions, s)
	d.ids = append(d.ids, opts.ClientID)
	d.mu.Unlock()
	return s, nil
}

func (s *countingRunSession) push(ev SessionEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.events <- ev
}

func (s *countingRunSession) Send(_ context.Context, msg SessionMessage) (Ack, error) {
	switch msg.Op {
	case "init":
		s.push(SessionEvent{Op: "init-ok", ClientEventID: msg.ClientEventID})
	case "add-query":
		s.push(SessionEvent{Op: "add-query-ok", ClientEventID: msg.ClientEventID})
		staleClean := strings.Contains(s.clientID, "clean-check") && s.dialer.provisions.Load() >= 2 && s.dialer.staleCleanOnce.CompareAndSwap(true, false)
		if staleClean {
			s.dialer.probePresent.Store(true)
		}
		s.mu.Lock()
		s.snapshots = append(s.snapshots, s.dialer.probePresent.Load())
		s.mu.Unlock()
		s.push(s.dialer.refresh())
		if staleClean {
			s.dialer.probePresent.Store(false)
		}
	case "transact":
		if msg.ClientEventID == "qualification-probe" {
			s.dialer.probePresent.Store(true)
		}
		s.dialer.mu.Lock()
		s.dialer.transactionIDs = append(s.dialer.transactionIDs, msg.ClientEventID)
		s.dialer.mu.Unlock()
		tx := atomic.AddInt64(&s.dialer.nextTx, 1)
		atomic.StoreInt64(&s.dialer.value, tx)
		s.push(SessionEvent{Op: "transact-ok", ClientEventID: msg.ClientEventID, TxID: fmt.Sprint(tx), ProcessedTxID: fmt.Sprint(tx), ServerTransactionID: fmt.Sprint(tx), ProcessedTransactionID: fmt.Sprint(tx)})
		s.dialer.broadcast(s.dialer.refresh())
		return Ack{Accepted: true, ServerTransactionID: fmt.Sprint(tx), ProcessedTransactionID: fmt.Sprint(tx)}, nil
	}
	return Ack{Accepted: true}, nil
}

func (s *countingRunSession) Events() <-chan SessionEvent { return s.events }

func (s *countingRunSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.events)
	}
	return nil
}

func (d *countingRunDialer) refresh() SessionEvent {
	value := atomic.LoadInt64(&d.value)
	todos := []any{map[string]any{"id": "todo-1", "value": value}}
	if d.probePresent.Load() {
		todos = append(todos, map[string]any{"id": "00000000-0000-4000-8000-000000000099", "value": "probe-only"})
	}
	b, _ := json.Marshal(map[string]any{"processed-tx-id": value, "computations": []any{map[string]any{"instaql-query": map[string]any{"todos": map[string]any{}}, "instaql-result": map[string]any{"data": map[string]any{"todos": todos}}}}})
	return SessionEvent{Op: "refresh-ok", Payload: b, ProcessedTransactionID: fmt.Sprint(value), At: time.Now()}
}

func (d *countingRunDialer) broadcast(ev SessionEvent) {
	d.mu.Lock()
	sessions := append([]*countingRunSession(nil), d.sessions...)
	d.mu.Unlock()
	for _, session := range sessions {
		session.push(ev)
	}
}

func (d *scriptedDialer) Dial(context.Context, SessionOptions) (Session, error) {
	s := newScriptedTargetSession()
	s.broadcast = func(event SessionEvent) {
		d.mu.Lock()
		defer d.mu.Unlock()
		for _, target := range d.sessions {
			select {
			case target.events <- event:
			case <-target.closed:
			}
		}
	}
	d.mu.Lock()
	d.sessions = append(d.sessions, s)
	d.mu.Unlock()
	return s, nil
}

func scriptedRefresh(version int, value string) SessionEvent {
	b, _ := json.Marshal(map[string]any{"processed-tx-id": version, "computations": []any{map[string]any{"instaql-query": map[string]any{"todos": map[string]any{}}, "instaql-result": map[string]any{"data": map[string]any{"todos": []any{map[string]any{"id": "todo-1", "value": value}}}}}}})
	return SessionEvent{Op: "refresh-ok", Payload: b, ProcessedTransactionID: "1", At: time.Now()}
}
