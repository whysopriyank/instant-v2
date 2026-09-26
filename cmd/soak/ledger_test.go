package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// fakeClock implements Clock for deterministic testing without wall-clock races.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start}
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// EV-001a: Every committed transaction creates exactly one ledger entry.
func TestLedger_EV001a_ExactlyOneEntryPerCommittedTx(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC))
	ledger := NewTransactionLedger(clock)

	entry, err := ledger.RecordSubmit("t-0-1", 0)
	if err != nil {
		t.Fatalf("unexpected error on submit: %v", err)
	}
	if entry == nil || entry.ClientEventID != "t-0-1" {
		t.Fatalf("expected entry with ClientEventID 't-0-1', got %#v", entry)
	}
	if entry.State != StateSubmitted {
		t.Fatalf("expected StateSubmitted, got %s", entry.State)
	}

	// Attempting duplicate submit must fail immediately.
	_, dupErr := ledger.RecordSubmit("t-0-1", 0)
	if dupErr == nil {
		t.Fatal("expected error on duplicate submit, got nil")
	}
	if !strings.Contains(dupErr.Error(), "duplicate transaction") {
		t.Fatalf("expected duplicate error, got %v", dupErr)
	}

	// Ledger must contain exactly one entry.
	entries := ledger.Entries()
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 entry, got %d", len(entries))
	}
}

func TestLedger_ReconnectSequenceRemainsUnique(t *testing.T) {
	ledger := NewTransactionLedger(newFakeClock(time.Now()))
	if first, second := ledger.NextSequence(7), ledger.NextSequence(7); first != 1 || second != 2 {
		t.Fatalf("session sequence was not reconnect-safe: first=%d second=%d", first, second)
	}
}

func TestLedger_MissingAckIdentityIsRaceSafe(t *testing.T) {
	ledger := NewTransactionLedger(newFakeClock(time.Now()))
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = ledger.RecordAck("", "1")
		}()
	}
	wg.Wait()
	if ledger.TerminalError() == nil {
		t.Fatal("missing ack identity did not become terminal")
	}
}

// EV-001b: Refresh/ack frames correlate to known entries.
func TestLedger_EV001b_CorrelateAcksAndRefreshes(t *testing.T) {
	start := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	ledger := NewTransactionLedger(clock)

	_, err := ledger.RecordSubmit("t-0-1", 0)
	if err != nil {
		t.Fatalf("submit error: %v", err)
	}

	clock.Advance(10 * time.Millisecond)
	ackEntry, err := ledger.RecordAck("t-0-1", "101")
	if err != nil {
		t.Fatalf("ack error: %v", err)
	}
	if ackEntry.State != StateAcknowledged || ackEntry.ServerTxID != "101" {
		t.Fatalf("expected StateAcknowledged with ServerTxID 101, got %#v", ackEntry)
	}
	if !ackEntry.AckAt.Equal(start.Add(10 * time.Millisecond)) {
		t.Fatalf("unexpected AckAt: %v", ackEntry.AckAt)
	}

	clock.Advance(15 * time.Millisecond)
	refEntry, err := ledger.RecordRefresh("101", 0)
	if err != nil {
		t.Fatalf("refresh error: %v", err)
	}
	if refEntry.State != StateResolved {
		t.Fatalf("expected StateResolved, got %s", refEntry.State)
	}
	if !refEntry.RefreshAt.Equal(start.Add(25 * time.Millisecond)) {
		t.Fatalf("unexpected RefreshAt: %v", refEntry.RefreshAt)
	}
	expectedLag := 25 * time.Millisecond
	if refEntry.Lag != expectedLag {
		t.Fatalf("expected lag %v, got %v", expectedLag, refEntry.Lag)
	}
}

// EV-001b: Refresh frame arriving before Ack frame must be buffered and correlated when Ack arrives.
func TestLedger_EV001b_RefreshBeforeAckBuffered(t *testing.T) {
	start := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	ledger := NewTransactionLedger(clock)

	_, err := ledger.RecordSubmit("t-0-1", 0)
	if err != nil {
		t.Fatalf("submit error: %v", err)
	}

	// Refresh arrives first.
	clock.Advance(10 * time.Millisecond)
	refEntry, err := ledger.RecordRefresh("201", 0)
	if err != nil {
		t.Fatalf("refresh before ack returned unexpected error: %v", err)
	}
	if refEntry != nil {
		t.Fatalf("expected nil entry for buffered refresh, got %#v", refEntry)
	}

	// Ack arrives later.
	clock.Advance(5 * time.Millisecond)
	ackEntry, err := ledger.RecordAck("t-0-1", "201")
	if err != nil {
		t.Fatalf("ack error: %v", err)
	}
	if ackEntry.State != StateResolved {
		t.Fatalf("expected StateResolved after buffered refresh correlation, got %s", ackEntry.State)
	}
	if !ackEntry.RefreshBeforeAck {
		t.Fatal("expected RefreshBeforeAck to be true")
	}
	if !ackEntry.RefreshAt.Equal(start.Add(10 * time.Millisecond)) {
		t.Fatalf("expected RefreshAt at 10ms, got %v", ackEntry.RefreshAt)
	}
	if !ackEntry.AckAt.Equal(start.Add(15 * time.Millisecond)) {
		t.Fatalf("expected AckAt at 15ms, got %v", ackEntry.AckAt)
	}
	if ackEntry.Lag != 10*time.Millisecond {
		t.Fatalf("expected lag 10ms, got %v", ackEntry.Lag)
	}
}

// EV-001b: Unknown ack must be rejected.
func TestLedger_EV001b_RejectUnknownAck(t *testing.T) {
	clock := newFakeClock(time.Now())
	ledger := NewTransactionLedger(clock)

	_, err := ledger.RecordAck("unknown-eid", "101")
	if err == nil {
		t.Fatal("expected error on unknown ack, got nil")
	}
	if !strings.Contains(err.Error(), "unknown ack") {
		t.Fatalf("expected unknown ack error, got %v", err)
	}
}

// EV-001b: Duplicate ack must be rejected.
func TestLedger_EV001b_RejectDuplicateAck(t *testing.T) {
	clock := newFakeClock(time.Now())
	ledger := NewTransactionLedger(clock)

	_, _ = ledger.RecordSubmit("t-0-1", 0)
	_, err := ledger.RecordAck("t-0-1", "101")
	if err != nil {
		t.Fatalf("first ack failed: %v", err)
	}

	_, dupErr := ledger.RecordAck("t-0-1", "101")
	if dupErr == nil {
		t.Fatal("expected error on duplicate ack, got nil")
	}
	if !strings.Contains(dupErr.Error(), "duplicate ack") {
		t.Fatalf("expected duplicate ack error, got %v", dupErr)
	}
}

// EV-001b: Invalid-order ack (non-monotonic server tx-id) must be rejected.
func TestLedger_EV001b_RejectInvalidOrderAck(t *testing.T) {
	clock := newFakeClock(time.Now())
	ledger := NewTransactionLedger(clock)

	_, _ = ledger.RecordSubmit("t-0-1", 0)
	_, _ = ledger.RecordSubmit("t-0-2", 0)

	_, err := ledger.RecordAck("t-0-1", "200")
	if err != nil {
		t.Fatalf("first ack failed: %v", err)
	}

	// Tx 2 arrives with smaller tx-id than Tx 1.
	_, orderErr := ledger.RecordAck("t-0-2", "199")
	if orderErr == nil {
		t.Fatal("expected error on non-monotonic ack, got nil")
	}
	if !strings.Contains(orderErr.Error(), "invalid-order ack") {
		t.Fatalf("expected invalid-order ack error, got %v", orderErr)
	}
}

// EV-001b: Unknown refresh must be rejected.
func TestLedger_EV001b_RejectUnknownRefresh(t *testing.T) {
	clock := newFakeClock(time.Now())
	ledger := NewTransactionLedger(clock)

	// No pending transactions in flight; refresh arrives with non-existent tx-id.
	_, err := ledger.RecordRefresh("unknown-watermark", 0)
	if err == nil {
		t.Fatal("expected error on unknown refresh, got nil")
	}
	if !strings.Contains(err.Error(), "unknown refresh") {
		t.Fatalf("expected unknown refresh error, got %v", err)
	}
}

// EV-001b: Equal refresh watermarks are idempotent.
func TestLedger_EV001b_EqualRefreshWatermarkIsIdempotent(t *testing.T) {
	clock := newFakeClock(time.Now())
	ledger := NewTransactionLedger(clock)

	_, _ = ledger.RecordSubmit("t-0-1", 0)
	_, _ = ledger.RecordAck("t-0-1", "101")
	_, err := ledger.RecordRefresh("101", 0)
	if err != nil {
		t.Fatalf("first refresh failed: %v", err)
	}

	if _, err := ledger.RecordRefresh("101", 0); err != nil {
		t.Fatalf("equal refresh watermark should be idempotent: %v", err)
	}
}

func TestLedger_EqualWatermarkPreservesRefreshBeforeAckTime(t *testing.T) {
	start := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	ledger := NewTransactionLedger(clock)
	_, _ = ledger.RecordSubmit("t-0-1", 0)
	clock.Advance(10 * time.Millisecond)
	_, _ = ledger.RecordRefresh("101", 0)
	clock.Advance(5 * time.Millisecond)
	_, _ = ledger.RecordRefresh("101", 0)
	clock.Advance(5 * time.Millisecond)
	entry, err := ledger.RecordAck("t-0-1", "101")
	if err != nil {
		t.Fatal(err)
	}
	if !entry.RefreshBeforeAck || !entry.RefreshAt.Equal(start.Add(10*time.Millisecond)) {
		t.Fatalf("equal watermark changed refresh-before-ack timing: %+v", entry)
	}
}

func TestLedger_WatermarkDoesNotResolveAnotherSession(t *testing.T) {
	ledger := NewTransactionLedger(newFakeClock(time.Now()))
	_, _ = ledger.RecordSubmit("t-0-1", 0)
	_, _ = ledger.RecordAck("t-0-1", "101")
	if _, err := ledger.RecordRefresh("101", 1); err != nil {
		t.Fatal(err)
	}
	if got := ledger.UnresolvedCountForSession(0); got != 1 {
		t.Fatalf("session 1 watermark resolved session 0 transaction: %d", got)
	}
}

func TestLedger_EV001b_CoalescedRefreshWatermarkResolvesAllCoveredAcks(t *testing.T) {
	clock := newFakeClock(time.Now())
	ledger := NewTransactionLedger(clock)
	for i, tx := range []string{"101", "102", "103"} {
		id := fmt.Sprintf("t-0-%d", i+1)
		if _, err := ledger.RecordSubmit(id, 0); err != nil {
			t.Fatal(err)
		}
		if _, err := ledger.RecordAck(id, tx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := ledger.RecordRefresh("103", 0); err != nil {
		t.Fatal(err)
	}
	if got := ledger.UnresolvedCount(); got != 0 {
		t.Fatalf("coalesced watermark left %d unresolved entries", got)
	}
}

// EV-001b: Invalid-order refresh (decreasing processed-tx-id) must be rejected.
func TestLedger_EV001b_RejectInvalidOrderRefresh(t *testing.T) {
	clock := newFakeClock(time.Now())
	ledger := NewTransactionLedger(clock)

	_, _ = ledger.RecordSubmit("t-0-1", 0)
	_, _ = ledger.RecordSubmit("t-0-2", 0)
	_, _ = ledger.RecordAck("t-0-1", "101")
	_, _ = ledger.RecordAck("t-0-2", "102")

	_, err := ledger.RecordRefresh("102", 0)
	if err != nil {
		t.Fatalf("refresh 102 failed: %v", err)
	}

	_, orderErr := ledger.RecordRefresh("101", 0)
	if orderErr == nil {
		t.Fatal("expected error on invalid order refresh, got nil")
	}
	if !strings.Contains(orderErr.Error(), "invalid-order refresh") {
		t.Fatalf("expected invalid-order refresh error, got %v", orderErr)
	}
}

// EV-001c: Protocol error frames become classified terminal failures.
func TestLedger_EV001c_ProtocolErrorTerminal(t *testing.T) {
	clock := newFakeClock(time.Now())
	ledger := NewTransactionLedger(clock)

	_, _ = ledger.RecordSubmit("t-0-1", 0)
	err := ledger.RecordError("t-0-1", "rate-limited: server busy")
	if err == nil {
		t.Fatal("expected non-nil terminal error, got nil")
	}
	if !strings.Contains(err.Error(), "protocol error") {
		t.Fatalf("expected protocol error message, got %v", err)
	}

	entry, ok := ledger.Entry("t-0-1")
	if !ok || entry.State != StateTerminal || entry.TerminalReason != "rate-limited: server busy" {
		t.Fatalf("expected StateTerminal with terminal reason, got %#v", entry)
	}

	// Subsequent operations must fail due to terminal error.
	_, submitErr := ledger.RecordSubmit("t-0-2", 0)
	if submitErr == nil {
		t.Fatal("expected submit to fail after terminal error")
	}
}

// EV-001d: Completion requires zero unresolved entries after bounded quiescence.
func TestLedger_EV001d_QuiescenceSuccess(t *testing.T) {
	clock := newFakeClock(time.Now())
	ledger := NewTransactionLedger(clock)

	_, _ = ledger.RecordSubmit("t-0-1", 0)
	_, _ = ledger.RecordAck("t-0-1", "101")
	_, _ = ledger.RecordRefresh("101", 0)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if err := ledger.QuiesceSession(ctx, 0, 1*time.Second); err != nil {
		t.Fatalf("expected quiescence success, got: %v", err)
	}
}

// EV-001d: Completion fails when transactions remain unresolved at quiescence timeout.
func TestLedger_EV001d_QuiescenceTimeoutUnresolved(t *testing.T) {
	clock := newFakeClock(time.Now())
	ledger := NewTransactionLedger(clock)

	_, _ = ledger.RecordSubmit("t-0-1", 0)
	// Transaction is never acknowledged or refreshed.

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Advance clock past timeout while quiescing.
	go func() {
		time.Sleep(15 * time.Millisecond)
		clock.Advance(2 * time.Second)
	}()
	err := ledger.QuiesceSession(ctx, 0, 500*time.Millisecond)
	if err == nil {
		t.Fatal("expected quiescence timeout error, got nil")
	}
	if !strings.Contains(err.Error(), "quiescence timeout") {
		t.Fatalf("expected quiescence timeout error, got %v", err)
	}

	entry, _ := ledger.Entry("t-0-1")
	if entry.State != StateTerminal || !strings.Contains(entry.TerminalReason, "quiescence_timeout") {
		t.Fatalf("expected terminal quiescence reason on unresolved entry, got %#v", entry)
	}
}

// EV-001e: Evidence records send/ack/refresh times and terminal reason per transaction.
func TestLedger_EV001e_TimingAndEvidenceRecords(t *testing.T) {
	t0 := time.Date(2026, 9, 8, 14, 0, 0, 0, time.UTC)
	clock := newFakeClock(t0)
	ledger := NewTransactionLedger(clock)

	_, _ = ledger.RecordSubmit("t-0-1", 0)
	clock.Advance(50 * time.Millisecond)
	_, _ = ledger.RecordAck("t-0-1", "101")
	clock.Advance(75 * time.Millisecond)
	_, _ = ledger.RecordRefresh("101", 0)

	entries := ledger.Entries()
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if !e.SubmittedAt.Equal(t0) {
		t.Fatalf("expected SubmittedAt %v, got %v", t0, e.SubmittedAt)
	}
	if !e.AckAt.Equal(t0.Add(50 * time.Millisecond)) {
		t.Fatalf("expected AckAt %v, got %v", t0.Add(50*time.Millisecond), e.AckAt)
	}
	if !e.RefreshAt.Equal(t0.Add(125 * time.Millisecond)) {
		t.Fatalf("expected RefreshAt %v, got %v", t0.Add(125*time.Millisecond), e.RefreshAt)
	}
	if e.Lag != 125*time.Millisecond {
		t.Fatalf("expected lag 125ms, got %v", e.Lag)
	}
}

// Pipelined writes test: proves replacement of the broken single lastTxAt accounting.
// In the old implementation, pipelined writes overwrote lastTxAt and cleared it on the first refresh.
// In EV-001, each transaction is tracked independently.
func TestLedger_PipelinedWritesNotOverwritten(t *testing.T) {
	t0 := time.Date(2026, 9, 8, 15, 0, 0, 0, time.UTC)
	clock := newFakeClock(t0)
	ledger := NewTransactionLedger(clock)

	// Submit Tx 1 at t0
	_, _ = ledger.RecordSubmit("t-0-1", 0)
	clock.Advance(10 * time.Millisecond)

	// Submit Tx 2 at t0+10ms (in the old code, this overwrote lastTxAt!)
	_, _ = ledger.RecordSubmit("t-0-2", 0)
	clock.Advance(10 * time.Millisecond)

	// Submit Tx 3 at t0+20ms
	_, _ = ledger.RecordSubmit("t-0-3", 0)
	clock.Advance(10 * time.Millisecond)

	// Acks arrive
	_, _ = ledger.RecordAck("t-0-1", "101")
	clock.Advance(5 * time.Millisecond)
	_, _ = ledger.RecordAck("t-0-2", "102")
	clock.Advance(5 * time.Millisecond)
	_, _ = ledger.RecordAck("t-0-3", "103")
	clock.Advance(10 * time.Millisecond)

	// Refreshes arrive
	_, _ = ledger.RecordRefresh("101", 0)
	clock.Advance(10 * time.Millisecond)
	_, _ = ledger.RecordRefresh("102", 0)
	clock.Advance(10 * time.Millisecond)
	_, _ = ledger.RecordRefresh("103", 0)

	e1, _ := ledger.Entry("t-0-1")
	e2, _ := ledger.Entry("t-0-2")
	e3, _ := ledger.Entry("t-0-3")

	if e1.State != StateResolved || e2.State != StateResolved || e3.State != StateResolved {
		t.Fatalf("all entries must be StateResolved, got %s, %s, %s", e1.State, e2.State, e3.State)
	}

	// Tx 1: submitted at 0ms, refreshed at 50ms -> lag 50ms
	if e1.Lag != 50*time.Millisecond {
		t.Fatalf("expected e1 lag 50ms, got %v", e1.Lag)
	}
	// Tx 2: submitted at 10ms, refreshed at 60ms -> lag 50ms
	if e2.Lag != 50*time.Millisecond {
		t.Fatalf("expected e2 lag 50ms, got %v", e2.Lag)
	}
	// Tx 3: submitted at 20ms, refreshed at 70ms -> lag 50ms
	if e3.Lag != 50*time.Millisecond {
		t.Fatalf("expected e3 lag 50ms, got %v", e3.Lag)
	}

	samples := ledger.LagSamples()
	if len(samples) != 3 {
		t.Fatalf("expected exactly 3 lag samples, got %d", len(samples))
	}
}

// fakeSessionConn allows injecting frames into runSession deterministically without network sockets.
type fakeSessionConn struct {
	inbound  chan []byte
	outbound [][]byte
	onWrite  func([]byte)
	mu       sync.Mutex
	closed   bool
}

func newFakeSessionConn() *fakeSessionConn {
	return &fakeSessionConn{
		inbound:  make(chan []byte, 100),
		outbound: make([][]byte, 0),
	}
}

func (c *fakeSessionConn) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	select {
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	case data, ok := <-c.inbound:
		if !ok {
			return 0, nil, fmt.Errorf("connection closed")
		}
		return websocket.MessageText, data, nil
	}
}

func (c *fakeSessionConn) Write(ctx context.Context, typ websocket.MessageType, p []byte) error {
	c.mu.Lock()
	b := make([]byte, len(p))
	copy(b, p)
	c.outbound = append(c.outbound, b)
	cb := c.onWrite
	c.mu.Unlock()
	if cb != nil {
		cb(b)
	}
	return nil
}

func (c *fakeSessionConn) Close(status websocket.StatusCode, reason string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		close(c.inbound)
	}
	return nil
}

func (c *fakeSessionConn) SetReadLimit(limit int64) {}

func (c *fakeSessionConn) PushFrame(v any) {
	b, _ := json.Marshal(v)
	c.inbound <- b
}

// TestSession_DeterministicLifecycle runs runSession with an injected connection, clock,
// and synthetic frames, proving complete deterministic verification with zero live network or soak.
func TestSession_DeterministicLifecycle(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 9, 8, 16, 0, 0, 0, time.UTC))
	ledger := NewTransactionLedger(clock)
	conn := newFakeSessionConn()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		refreshes atomic.Int64
		transacts atomic.Int64
		connects  atomic.Int64
		writeGate = make(chan struct{}, 1)
	)

	attr := "attr-todo-title"
	txInterval := 10 * time.Millisecond
	quiescence := 500 * time.Millisecond

	// Feed initial protocol frames:
	// 1. init-ok
	conn.PushFrame(map[string]any{"op": "init-ok", "app-id": "test-app"})
	// 2. add-query-ok with snapshot
	conn.PushFrame(map[string]any{
		"op":              "add-query-ok",
		"client-event-id": "q-0",
		"result":          []any{map[string]any{"id": "todo-1"}},
		"processed-tx-id": 100,
	})

	// Provide 1 write token
	writeGate <- struct{}{}

	errCh := make(chan error, 1)
	go func() {
		errCh <- driveSessionWithConn(ctx, conn, ledger, clock, 0, "test-app", &attr, &txInterval,
			&refreshes, &transacts, &connects, writeGate, quiescence)
	}()

	// Wait for the write to be sent by checking transacts counter
	deadline := time.Now().Add(2 * time.Second)
	for transacts.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if transacts.Load() != 1 {
		t.Fatalf("expected 1 transact sent, got %d", transacts.Load())
	}

	// Feed ack: transact-ok
	clock.Advance(10 * time.Millisecond)
	conn.PushFrame(map[string]any{
		"op":              "transact-ok",
		"client-event-id": "t-0-1",
		"tx-id":           101,
	})

	// Feed refresh: refresh-ok
	clock.Advance(15 * time.Millisecond)
	conn.PushFrame(map[string]any{
		"op":              "refresh-ok",
		"processed-tx-id": 101,
		"computations":    []any{},
	})

	// Wait for refresh to be processed
	for refreshes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	// Cancel context to trigger quiescence and clean exit
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("driveSessionWithConn returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("driveSessionWithConn hung during shutdown")
	}

	// Verify ledger state
	entry, ok := ledger.Entry("t-0-1")
	if !ok {
		t.Fatal("entry t-0-1 not found in ledger")
	}
	if entry.State != StateResolved {
		t.Fatalf("expected entry StateResolved, got %s", entry.State)
	}
	if entry.Lag != 25*time.Millisecond {
		t.Fatalf("expected lag 25ms, got %v", entry.Lag)
	}
	if ledger.UnresolvedCount() != 0 {
		t.Fatalf("expected 0 unresolved entries, got %d", ledger.UnresolvedCount())
	}
}

func TestInitFrameCarriesSDKVersionOnlyWhenSet(t *testing.T) {
	for _, tc := range []struct {
		version string
		want    bool
	}{{"", false}, {"0.23.0", true}} {
		initSDKVersion = tc.version
		conn := newFakeSessionConn()
		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() {
			errCh <- driveSessionWithConn(ctx, conn, NewTransactionLedger(nil), newFakeClock(time.Now()), 0, "app",
				nil, nil, nil, nil, nil, nil, time.Second)
		}()
		deadline := time.Now().Add(2 * time.Second)
		var first []byte
		for time.Now().Before(deadline) {
			conn.mu.Lock()
			if len(conn.outbound) > 0 {
				first = conn.outbound[0]
			}
			conn.mu.Unlock()
			if first != nil {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
		<-errCh
		var f map[string]any
		if err := json.Unmarshal(first, &f); err != nil || f["op"] != "init" {
			t.Fatalf("version %q: first frame is not init: %s", tc.version, first)
		}
		versions, has := f["versions"].(map[string]any)
		if has != tc.want || (tc.want && versions["@instantdb/core"] != tc.version) {
			t.Fatalf("version %q: init versions = %v", tc.version, f["versions"])
		}
	}
	initSDKVersion = ""
}

// A session idle through ramp/settle has a snapshot older than the stall
// window; its first write must get the full window to see a refresh rather
// than being declared stalled on the next tick. A genuine 20 s gap after it
// started writing must still trip the guard.
func TestStallGuardMeasuresFromFirstWriteNotIdleSnapshot(t *testing.T) {
	clock := newFakeClock(time.Now())
	conn := newFakeSessionConn()
	ledger := NewTransactionLedger(clock)
	var refreshes, transacts, connects atomic.Int64
	writeGate := make(chan struct{}, 1)
	attr := "attr"
	txInterval := 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn.PushFrame(map[string]any{"op": "init-ok", "app-id": "app"})
	conn.PushFrame(map[string]any{"op": "add-query-ok", "client-event-id": "q-0", "result": []any{}, "processed-tx-id": 1})
	errCh := make(chan error, 1)
	go func() {
		errCh <- driveSessionWithConn(ctx, conn, ledger, clock, 0, "app", &attr, &txInterval,
			&refreshes, &transacts, &connects, writeGate, time.Second)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for refreshes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	clock.Advance(2 * time.Minute) // idle ramp + settle: no writes, no refreshes
	// Keep the global write gate flowing as the real scheduler does, so the
	// session keeps ticking (the stall check runs before each gate wait).
	go func() {
		for {
			select {
			case writeGate <- struct{}{}:
			case <-ctx.Done():
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	for transacts.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	clock.Advance(3 * time.Second) // well inside the window since the first write
	select {
	case err := <-errCh:
		t.Fatalf("session declared stalled %v after its first write: %v", 3*time.Second, err)
	case <-time.After(100 * time.Millisecond):
	}
	clock.Advance(20 * time.Second) // now a real gap: no refresh for 23 s of writing
	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "refresh stream stalled") {
			t.Fatalf("genuine stall reported as %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("genuine 23 s refresh gap was not detected")
	}
}
