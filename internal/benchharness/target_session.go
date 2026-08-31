package benchharness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

type TargetSession struct {
	driver      *TargetDriver
	sessionMu   sync.RWMutex
	session     Session
	clientID    string
	mu          sync.Mutex
	pending     []SessionEvent
	states      map[string]Materialized
	queries     map[string]Query
	wireQueries map[string]json.RawMessage
	evidence    *evidenceCollector
	pauseMu     sync.Mutex
	pausedTill  time.Time
}

func (d *TargetDriver) OpenSession(ctx context.Context, clientID string) (*TargetSession, error) {
	return d.openSession(ctx, clientID, newEvidenceCollector(EvidenceBudget{}))
}

func (d *TargetDriver) openSession(ctx context.Context, clientID string, evidence *evidenceCollector) (*TargetSession, error) {
	if clientID == "" {
		return nil, fmt.Errorf("client id is required")
	}
	if evidence == nil {
		evidence = newEvidenceCollector(EvidenceBudget{})
	}
	sess, err := d.dialSession(ctx, clientID)
	if err != nil {
		return nil, err
	}
	ts := &TargetSession{driver: d, session: sess, clientID: clientID, states: map[string]Materialized{}, queries: map[string]Query{}, wireQueries: map[string]json.RawMessage{}, evidence: evidence}
	if err := ts.Init(ctx); err != nil {
		_ = sess.Close()
		return nil, err
	}
	return ts, nil
}

func (d *TargetDriver) dialSession(ctx context.Context, clientID string) (Session, error) {
	opts := SessionOptions{URL: d.cfg.SessionURL, AppID: d.cfg.AppID, ClientID: clientID, Headers: d.cfg.Headers.Clone()}
	if d.cfg.Dialer != nil {
		return d.cfg.Dialer.Dial(ctx, opts)
	}
	if d.cfg.Transport == TransportWebSocket {
		return DialWebSocket(ctx, opts)
	}
	return DialSSE(ctx, opts)
}

func (s *TargetSession) Init(ctx context.Context) error {
	eventID := "bench-init-" + s.clientID
	payload := map[string]any{"app-id": s.driver.cfg.AppID, "versions": s.driver.cfg.Versions}
	if s.driver.cfg.RefreshToken != "" {
		payload["refresh-token"] = s.driver.cfg.RefreshToken
	}
	if s.driver.cfg.AdminToken != "" {
		payload["__admin-token"] = s.driver.cfg.AdminToken
	}
	if _, err := s.currentSession().Send(ctx, SessionMessage{Op: "init", ClientEventID: eventID, Payload: payload}); err != nil {
		return err
	}
	_, err := s.await(ctx, func(ev SessionEvent) bool { return ev.Op == "init-ok" && ev.ClientEventID == eventID })
	return err
}

func (s *TargetSession) Subscribe(ctx context.Context, query Query) (Refresh, error) {
	if s.driver.cfg.QueryBuilder == nil {
		return Refresh{}, &UnsupportedTargetError{Check: "query", Reason: "query builder is not configured"}
	}
	q, err := s.driver.cfg.QueryBuilder(query)
	if err != nil {
		return Refresh{}, err
	}
	wireQuery, err := json.Marshal(q)
	if err != nil {
		return Refresh{}, fmt.Errorf("encode query %s: %w", query.ID, err)
	}
	s.mu.Lock()
	s.wireQueries[query.ID] = append(json.RawMessage(nil), wireQuery...)
	s.mu.Unlock()
	eventID := "bench-query-" + s.clientID + "-" + query.ID
	if _, err := s.currentSession().Send(ctx, SessionMessage{Op: "add-query", ClientEventID: eventID, Payload: map[string]any{"q": q}}); err != nil {
		return Refresh{}, err
	}
	queryAck, err := s.await(ctx, func(ev SessionEvent) bool {
		return (ev.Op == "add-query-ok" || ev.Op == "add-query-exists") && ev.ClientEventID == eventID
	})
	if err != nil {
		return Refresh{}, err
	}
	refresh, available, err := decodeAddQuerySnapshot(queryAck, query.ID, s.driver.cfg.AttributeAliases, wireQuery)
	if err != nil {
		return Refresh{}, err
	}
	if !available {
		for {
			ev, err := s.await(ctx, func(ev SessionEvent) bool { return ev.Op == "refresh-ok" || ev.Op == "refresh-ok-delta" })
			if err != nil {
				return Refresh{}, err
			}
			refresh, err = s.decodeRefresh(ev, query.ID)
			if err != nil {
				return Refresh{}, err
			}
			if refresh.Kind != RefreshNoop {
				break
			}
		}
	}
	if refresh.Full == nil {
		return Refresh{}, &UnsupportedTargetError{Check: "initial_snapshot", Reason: "initial add-query refresh did not provide a full materialized baseline"}
	}
	s.mu.Lock()
	s.states[query.ID] = *refresh.Full
	s.queries[query.ID] = query
	s.mu.Unlock()
	return refresh, nil
}

func (s *TargetSession) Transact(ctx context.Context, mutation Mutation) (Ack, error) {
	if s.driver.cfg.TransactionBuilder == nil {
		return Ack{}, &UnsupportedTargetError{Check: "transact", Reason: "transaction builder is not configured"}
	}
	steps, err := s.driver.cfg.TransactionBuilder(mutation)
	if err != nil {
		return Ack{}, err
	}
	return s.currentSession().Send(ctx, SessionMessage{Op: "transact", ClientEventID: mutation.EventID, Payload: map[string]any{"tx-steps": steps}})
}

func (s *TargetSession) FinalSnapshot(ctx context.Context, query Query) (Materialized, error) {
	refresh, err := s.Subscribe(ctx, query)
	if err != nil {
		return Materialized{}, err
	}
	if refresh.Full == nil {
		return Materialized{}, &UnsupportedTargetError{Check: "final_snapshot", Reason: "snapshot decoder did not return full state"}
	}
	return *refresh.Full, nil
}

func (s *TargetSession) Events() <-chan SessionEvent { return s.currentSession().Events() }

func (s *TargetSession) Close() error {
	s.sessionMu.RLock()
	defer s.sessionMu.RUnlock()
	return s.session.Close()
}

func (s *TargetSession) currentSession() Session {
	s.sessionMu.RLock()
	defer s.sessionMu.RUnlock()
	return s.session
}

func closeTargetSessions(sessions []*TargetSession) {
	for _, session := range sessions {
		_ = session.Close()
	}
}

// Reconnect replaces the transport, replays init and resubscribes every query
// previously installed on this logical client. The old transport is closed
// first so its reader terminates; the caller starts a new reader afterwards.
func (s *TargetSession) Reconnect(ctx context.Context) error {
	if s == nil || s.driver == nil {
		return errors.New("nil target session")
	}
	s.sessionMu.Lock()
	old := s.session
	s.sessionMu.Unlock()
	// A normal shutdown often reports the transport's close status; it must
	// not prevent the replacement session from being established.
	_ = old.Close()
	next, err := s.driver.dialSession(ctx, s.clientID)
	if err != nil {
		return err
	}
	s.sessionMu.Lock()
	s.session = next
	s.pending = nil
	s.sessionMu.Unlock()
	if err := s.Init(ctx); err != nil {
		_ = next.Close()
		return err
	}
	s.mu.Lock()
	queries := make([]Query, 0, len(s.queries))
	for _, query := range s.queries {
		queries = append(queries, query)
	}
	s.mu.Unlock()
	for _, query := range queries {
		if _, err := s.Subscribe(ctx, query); err != nil {
			_ = next.Close()
			return err
		}
	}
	return nil
}

// recordEvent stores only bounded digest/metadata evidence; wire payload bytes
// are released after decoding and are never retained by a target session.
func (s *TargetSession) recordEvent(ev SessionEvent, protocolErr string) {
	if s == nil || s.evidence == nil {
		return
	}
	s.evidence.record(ev, s.clientID, "", "", protocolErr)
}

func (s *TargetSession) recordEventFor(ev SessionEvent, protocolErr, queryID, recipientID string) {
	if s == nil || s.evidence == nil {
		return
	}
	s.evidence.record(ev, s.clientID, queryID, recipientID, protocolErr)
}

func (s *TargetSession) RawFrames() []RawFrameEvidence {
	frames, _ := s.evidence.snapshot()
	return frames
}

// PauseReads applies an application-side pause. The transport remains open;
// the run reader intentionally does not consume events until the deadline,
// reproducing a slow consumer without pretending that the server stopped.
func (s *TargetSession) PauseReads(forDuration time.Duration) {
	if forDuration <= 0 {
		return
	}
	s.pauseMu.Lock()
	until := time.Now().Add(forDuration)
	if until.After(s.pausedTill) {
		s.pausedTill = until
	}
	s.pauseMu.Unlock()
}

func (s *TargetSession) waitRead(ctx context.Context) error {
	s.pauseMu.Lock()
	until := s.pausedTill
	s.pauseMu.Unlock()
	if until.IsZero() || !time.Now().Before(until) {
		return nil
	}
	timer := time.NewTimer(time.Until(until))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *TargetSession) await(ctx context.Context, match func(SessionEvent) bool) (SessionEvent, error) {
	for {
		s.mu.Lock()
		for i, ev := range s.pending {
			if match(ev) {
				s.pending = append(s.pending[:i], s.pending[i+1:]...)
				s.mu.Unlock()
				return ev, nil
			}
		}
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return SessionEvent{}, ctx.Err()
		case ev, ok := <-s.currentSession().Events():
			if !ok {
				return SessionEvent{}, errors.New("target session closed while awaiting protocol event")
			}
			protocolErr := ""
			if ev.Op == "error" || ev.Op == "protocol-error" {
				protocolErr = ev.Error
			}
			s.recordEvent(ev, protocolErr)
			if ev.Op == "error" || ev.Op == "protocol-error" {
				return SessionEvent{}, fmt.Errorf("target protocol error: %s", ev.Error)
			}
			if match(ev) {
				return ev, nil
			}
			// A refresh can race its add-query acknowledgement. Retain it so
			// the subsequent await observes the actual initial snapshot.
			if ev.Op == "refresh-ok" || ev.Op == "refresh-ok-delta" {
				s.mu.Lock()
				s.pending = append(s.pending, ev)
				s.mu.Unlock()
			}
		}
	}
}

func (s *TargetSession) decodeRefresh(ev SessionEvent, queryID string) (Refresh, error) {
	if s.driver.cfg.DecodeRefresh != nil {
		return s.driver.cfg.DecodeRefresh(ev, queryID)
	}
	s.mu.Lock()
	wireQuery := append(json.RawMessage(nil), s.wireQueries[queryID]...)
	s.mu.Unlock()
	if s.driver.cfg.Kind == TargetV1 {
		return decodeV1WireRefresh(ev, queryID, s.driver.cfg.AttributeAliases, wireQuery)
	}
	return decodeWireRefresh(ev, queryID, s.driver.cfg.AttributeAliases, wireQuery)
}
