package benchharness

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// SessionMessage is a target-neutral request. Payload contains protocol fields
// after op/client-event-id and is never interpreted by the scheduler.
type SessionMessage struct {
	Op            string
	ClientEventID string
	Payload       map[string]any
}

type SessionEvent struct {
	Op                     string
	ClientEventID          string
	ServerTransactionID    string
	ProcessedTransactionID string
	QueryID                string
	Payload                json.RawMessage
	At                     time.Time
	Error                  string
	// ServerTransactionID and ProcessedTransactionID are canonical names.
	// TxID/ProcessedTxID preserve the exact wire names for adapters/reporters.
	TxID          string
	ProcessedTxID string
}

// Session is the minimal transport contract used by the benchmark driver.
// Events is closed after the target connection closes; Send serializes writes.
type Session interface {
	Send(context.Context, SessionMessage) (Ack, error)
	Events() <-chan SessionEvent
	Close() error
}

// SessionDialer allows WS, SSE, and synthetic test transports to share the
// same writer/oracle/ledger implementation.
type SessionDialer interface {
	Dial(context.Context, SessionOptions) (Session, error)
}

type SessionOptions struct {
	URL         string
	AppID       string
	ClientID    string
	Headers     http.Header
	EventBuffer int
	MachineID   string
	SessionID   string
	SSEToken    string
}

type JSONSession struct {
	conn       *websocket.Conn
	mu         sync.Mutex
	events     chan SessionEvent
	done       chan struct{}
	closeOnce  sync.Once
	readErr    error
	ackMu      sync.Mutex
	ackWaiters map[string]chan Ack
}

// defaultWSReadLimit matches internal/sync's bounded default. Keeping the
// client-side limit finite prevents a malformed target frame from turning a
// benchmark reader into an unbounded allocator while allowing contract-sized
// materialized refreshes.
const defaultWSReadLimit int64 = 4 << 20

// DialWebSocket opens the production session endpoint without coupling the
// harness to V1 or V2 message details.
func DialWebSocket(ctx context.Context, opts SessionOptions) (Session, error) {
	if opts.URL == "" {
		return nil, fmt.Errorf("session URL is empty")
	}
	conn, _, err := websocket.Dial(ctx, opts.URL, &websocket.DialOptions{HTTPHeader: opts.Headers})
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(defaultWSReadLimit)
	n := opts.EventBuffer
	if n <= 0 {
		n = 256
	}
	s := &JSONSession{conn: conn, events: make(chan SessionEvent, n), done: make(chan struct{}), ackWaiters: make(map[string]chan Ack)}
	go s.readLoop(ctx)
	return s, nil
}

func (s *JSONSession) Send(ctx context.Context, msg SessionMessage) (Ack, error) {
	if msg.Op == "" {
		return Ack{}, fmt.Errorf("session operation is empty")
	}
	p := make(map[string]any, len(msg.Payload)+2)
	for k, v := range msg.Payload {
		p[k] = v
	}
	p["op"] = msg.Op
	if msg.ClientEventID != "" {
		p["client-event-id"] = msg.ClientEventID
	}
	b, err := json.Marshal(p)
	if err != nil {
		return Ack{}, err
	}
	var waiter chan Ack
	if msg.Op == "transact" {
		if msg.ClientEventID == "" {
			return Ack{}, fmt.Errorf("transact requires client-event-id")
		}
		waiter = make(chan Ack, 1)
		s.ackMu.Lock()
		s.ackWaiters[msg.ClientEventID] = waiter
		s.ackMu.Unlock()
		defer s.removeAckWaiter(msg.ClientEventID, waiter)
	}
	s.mu.Lock()
	err = s.conn.Write(ctx, websocket.MessageText, b)
	s.mu.Unlock()
	if err != nil {
		return Ack{}, err
	}
	if waiter == nil {
		return Ack{Accepted: true}, nil
	}
	select {
	case ack := <-waiter:
		if !ack.Accepted {
			return ack, fmt.Errorf("transact was not acknowledged: %s", ack.Error)
		}
		return ack, nil
	case <-ctx.Done():
		return Ack{}, ctx.Err()
	case <-s.done:
		return Ack{}, fmt.Errorf("session closed before transact acknowledgement")
	}
}

func (s *JSONSession) Events() <-chan SessionEvent { return s.events }
func (s *JSONSession) Close() error {
	var err error
	s.closeOnce.Do(func() { close(s.done); err = s.conn.Close(websocket.StatusNormalClosure, "") })
	return err
}

func (s *JSONSession) readLoop(ctx context.Context) {
	defer close(s.events)
	defer s.failAckWaiters()
	for {
		_, b, err := s.conn.Read(ctx)
		if err != nil {
			s.readErr = err
			return
		}
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal(b, &envelope); err != nil {
			if !s.emit(SessionEvent{Op: "protocol-error", Error: err.Error(), Payload: append([]byte(nil), b...), At: time.Now()}) {
				return
			}
			continue
		}
		ev := SessionEvent{At: time.Now()}
		_ = json.Unmarshal(envelope["op"], &ev.Op)
		_ = json.Unmarshal(envelope["client-event-id"], &ev.ClientEventID)
		_ = json.Unmarshal(envelope["server-transaction-id"], &ev.ServerTransactionID)
		_ = json.Unmarshal(envelope["processed-transaction-id"], &ev.ProcessedTransactionID)
		_ = json.Unmarshal(envelope["query-id"], &ev.QueryID)
		_ = json.Unmarshal(envelope["message"], &ev.Error)
		ev.TxID = firstString(envelope, "tx-id", "server-transaction-id")
		ev.ServerTransactionID = ev.TxID
		ev.ProcessedTxID = firstString(envelope, "processed-tx-id", "processed-transaction-id")
		ev.ProcessedTransactionID = ev.ProcessedTxID
		if ev.Op == "" {
			ev.Op, ev.Error = "protocol-error", "frame missing op"
		}
		s.deliverAck(ev)
		ev.Payload = append(ev.Payload[:0], b...)
		if !s.emit(ev) {
			return
		}
	}
}

func firstString(fields map[string]json.RawMessage, keys ...string) string {
	for _, key := range keys {
		if raw, ok := fields[key]; ok {
			if value := scalarString(raw); value != "" {
				return value
			}
		}
	}
	return ""
}

// scalarString preserves the wire representation of identifiers. The V1
// runtime emits numeric tx-id/processed-tx-id values while some compatible
// adapters emit strings; both are valid evidence and neither should be
// silently discarded by a string-only decoder.
func scalarString(raw json.RawMessage) string {
	var value string
	if json.Unmarshal(raw, &value) == nil {
		return value
	}
	var number json.Number
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if decoder.Decode(&number) == nil {
		return number.String()
	}
	return ""
}
func (s *JSONSession) deliverAck(ev SessionEvent) {
	if ev.ClientEventID == "" || (ev.Op != "transact-ok" && ev.Op != "error") {
		return
	}
	s.ackMu.Lock()
	ch := s.ackWaiters[ev.ClientEventID]
	if ch != nil {
		ch <- Ack{ServerTransactionID: ev.TxID, ProcessedTransactionID: ev.ProcessedTxID, Accepted: ev.Op == "transact-ok", Error: ev.Error}
		delete(s.ackWaiters, ev.ClientEventID)
	}
	s.ackMu.Unlock()
}
func (s *JSONSession) removeAckWaiter(id string, ch chan Ack) {
	s.ackMu.Lock()
	if current := s.ackWaiters[id]; current == ch {
		delete(s.ackWaiters, id)
	}
	s.ackMu.Unlock()
}
func (s *JSONSession) failAckWaiters() {
	s.ackMu.Lock()
	defer s.ackMu.Unlock()
	for id, ch := range s.ackWaiters {
		_ = id
		close(ch)
	}
	s.ackWaiters = make(map[string]chan Ack)
}

func (s *JSONSession) emit(ev SessionEvent) bool {
	select {
	case s.events <- ev:
		return true
	case <-s.done:
		return false
	}
}

// SSESession implements the GET stream + POST message pair used by the SSE
// fallback. It only parses data lines; event ids and framing remain in Payload.
type SSESession struct {
	client        *http.Client
	opts          SessionOptions
	postMu        sync.Mutex
	events        chan SessionEvent
	cancel        context.CancelFunc
	done          chan struct{}
	closeOnce     sync.Once
	ackMu         sync.Mutex
	ackWaiters    map[string]chan Ack
	handshake     chan struct{}
	handshakeOnce sync.Once
	machineID     string
	sessionID     string
	sseToken      string
}

func DialSSE(ctx context.Context, opts SessionOptions) (Session, error) {
	if opts.URL == "" {
		return nil, fmt.Errorf("SSE URL is empty")
	}
	n := opts.EventBuffer
	if n <= 0 {
		n = 256
	}
	cctx, cancel := context.WithCancel(ctx)
	s := &SSESession{client: http.DefaultClient, opts: opts, events: make(chan SessionEvent, n), cancel: cancel, done: make(chan struct{}), ackWaiters: make(map[string]chan Ack), handshake: make(chan struct{})}
	getURL := opts.URL
	if opts.AppID != "" {
		if u, parseErr := url.Parse(opts.URL); parseErr == nil && u.Query().Get("app_id") == "" {
			q := u.Query()
			q.Set("app_id", opts.AppID)
			u.RawQuery = q.Encode()
			getURL = u.String()
		}
	}
	// V1 validates app_id on POST as well as GET. Keep the resolved URL in
	// the session options so both halves of the SSE transport address the same
	// app; callers may still provide an explicit query string.
	s.opts.URL = getURL
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, getURL, nil)
	if err != nil {
		cancel()
		return nil, err
	}
	req.Header = opts.Headers.Clone()
	resp, err := s.client.Do(req)
	if err != nil {
		cancel()
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		resp.Body.Close()
		cancel()
		return nil, fmt.Errorf("SSE GET status %s", resp.Status)
	}
	go s.read(resp.Body)
	select {
	case <-s.handshake:
	case <-ctx.Done():
		s.Close()
		return nil, ctx.Err()
	}
	return s, nil
}
func (s *SSESession) read(body io.ReadCloser) {
	defer close(s.events)
	defer s.failAckWaiters()
	defer body.Close()
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 4096), 16<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		b := []byte(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		var e map[string]json.RawMessage
		if err := json.Unmarshal(b, &e); err != nil {
			select {
			case s.events <- SessionEvent{Op: "protocol-error", Error: err.Error(), Payload: append([]byte(nil), b...), At: time.Now()}:
			case <-s.done:
				return
			}
			continue
		}
		ev := SessionEvent{At: time.Now(), Payload: append([]byte(nil), b...)}
		_ = json.Unmarshal(e["op"], &ev.Op)
		_ = json.Unmarshal(e["client-event-id"], &ev.ClientEventID)
		ev.TxID = firstString(e, "tx-id", "server-transaction-id")
		ev.ServerTransactionID = ev.TxID
		ev.ProcessedTxID = firstString(e, "processed-tx-id", "processed-transaction-id")
		ev.ProcessedTransactionID = ev.ProcessedTxID
		_ = json.Unmarshal(e["query-id"], &ev.QueryID)
		_ = json.Unmarshal(e["message"], &ev.Error)
		if ev.Op == "" {
			ev.Op, ev.Error = "protocol-error", "frame missing op"
		}
		if ev.Op == "init-ok" || ev.Op == "sse-init" {
			s.captureHandshake(e)
		}
		s.deliverAck(ev)
		select {
		case s.events <- ev:
		case <-s.done:
			return
		}
	}
}

func (s *SSESession) captureHandshake(fields map[string]json.RawMessage) {
	var app string
	_ = json.Unmarshal(fields["app-id"], &app)
	s.machineID = firstString(fields, "machine-id")
	s.sessionID = firstString(fields, "session-id")
	s.sseToken = firstString(fields, "sse-token")
	if app != "" {
		s.opts.AppID = app
	}
	if s.machineID != "" && s.sessionID != "" && s.sseToken != "" {
		s.handshakeOnce.Do(func() { close(s.handshake) })
	}
}

func (s *SSESession) deliverAck(ev SessionEvent) {
	if ev.ClientEventID == "" || (ev.Op != "transact-ok" && ev.Op != "error") {
		return
	}
	s.ackMu.Lock()
	if ch := s.ackWaiters[ev.ClientEventID]; ch != nil {
		ch <- Ack{ServerTransactionID: ev.TxID, ProcessedTransactionID: ev.ProcessedTxID, Accepted: ev.Op == "transact-ok", Error: ev.Error}
		delete(s.ackWaiters, ev.ClientEventID)
	}
	s.ackMu.Unlock()
}

func (s *SSESession) removeAckWaiter(id string, ch chan Ack) {
	s.ackMu.Lock()
	if current := s.ackWaiters[id]; current == ch {
		delete(s.ackWaiters, id)
	}
	s.ackMu.Unlock()
}
func (s *SSESession) failAckWaiters() {
	s.ackMu.Lock()
	defer s.ackMu.Unlock()
	for _, ch := range s.ackWaiters {
		close(ch)
	}
	s.ackWaiters = make(map[string]chan Ack)
}

func (s *SSESession) Send(ctx context.Context, msg SessionMessage) (Ack, error) {
	if msg.Op == "" {
		return Ack{}, fmt.Errorf("session operation is empty")
	}
	var waiter chan Ack
	if msg.Op == "transact" {
		if msg.ClientEventID == "" {
			return Ack{}, fmt.Errorf("transact requires client-event-id")
		}
		waiter = make(chan Ack, 1)
		s.ackMu.Lock()
		s.ackWaiters[msg.ClientEventID] = waiter
		s.ackMu.Unlock()
		defer s.removeAckWaiter(msg.ClientEventID, waiter)
	}
	p := make(map[string]any, len(msg.Payload)+2)
	for k, v := range msg.Payload {
		p[k] = v
	}
	p["op"] = msg.Op
	if msg.ClientEventID != "" {
		p["client-event-id"] = msg.ClientEventID
	}
	payload, err := json.Marshal(p)
	if err != nil {
		return Ack{}, err
	}
	request := map[string]any{"messages": []json.RawMessage{payload}}
	if s.machineID != "" {
		request["machine_id"] = s.machineID
	}
	if s.sessionID != "" {
		request["session_id"] = s.sessionID
	}
	if s.sseToken != "" {
		request["sse_token"] = s.sseToken
	}
	if s.opts.AppID != "" {
		request["app_id"] = s.opts.AppID
	}
	body, err := json.Marshal(request)
	if err != nil {
		return Ack{}, err
	}
	s.postMu.Lock()
	defer s.postMu.Unlock()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.opts.URL, strings.NewReader(string(body)))
	if err != nil {
		return Ack{}, err
	}
	req.Header = s.opts.Headers.Clone()
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return Ack{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return Ack{}, fmt.Errorf("SSE POST status %s", resp.Status)
	}
	if waiter == nil {
		return Ack{Accepted: true}, nil
	}
	select {
	case ack := <-waiter:
		if !ack.Accepted {
			return ack, fmt.Errorf("transact was not acknowledged: %s", ack.Error)
		}
		return ack, nil
	case <-ctx.Done():
		return Ack{}, ctx.Err()
	case <-s.done:
		return Ack{}, fmt.Errorf("session closed before transact acknowledgement")
	}
}
func (s *SSESession) Events() <-chan SessionEvent { return s.events }
func (s *SSESession) Close() error {
	s.closeOnce.Do(func() { close(s.done); s.cancel() })
	return nil
}

// StallDetector is per-client state. It reports a stall only after a write has
// been observed and no receipt has arrived for the configured interval.
type StallDetector struct {
	mu          sync.Mutex
	timeout     time.Duration
	lastWrite   time.Time
	lastReceipt time.Time
}

func NewStallDetector(timeout time.Duration) *StallDetector {
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	return &StallDetector{timeout: timeout}
}
func (s *StallDetector) ObserveWrite(at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastWrite = at
}
func (s *StallDetector) ObserveReceipt(at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastReceipt = at
}
func (s *StallDetector) Stalled(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastWrite.IsZero() {
		return false
	}
	if s.lastReceipt.After(s.lastWrite) {
		return false
	}
	return now.Sub(s.lastWrite) > s.timeout
}
