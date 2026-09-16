package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Clock provides time abstraction for deterministic testing.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// EntryState represents the lifecycle phase of a soak transaction.
type EntryState string

const (
	StateSubmitted    EntryState = "submitted"
	StateAcknowledged EntryState = "acknowledged"
	StateRefreshed    EntryState = "refreshed"
	StateResolved     EntryState = "resolved"
	StateTerminal     EntryState = "terminal"
)

// LedgerEntry records the full lifecycle and timing evidence for one committed transaction.
type LedgerEntry struct {
	ClientEventID    string        `json:"client_event_id"`
	ServerTxID       string        `json:"server_tx_id,omitempty"`
	SessionID        int           `json:"session_id"`
	SubmittedAt      time.Time     `json:"submitted_at"`
	AckAt            time.Time     `json:"ack_at,omitempty"`
	RefreshAt        time.Time     `json:"refresh_at,omitempty"`
	Lag              time.Duration `json:"lag,omitempty"`
	RefreshBeforeAck bool          `json:"refresh_before_ack,omitempty"`
	State            EntryState    `json:"state"`
	TerminalReason   string        `json:"terminal_reason,omitempty"`
}

type bufferedRefresh struct {
	at        time.Time
	sessionID int
}

// TransactionLedger maintains the deterministic per-transaction accounting
// required by EV-001, replacing implicit single lastTxAt accounting.
type TransactionLedger struct {
	mu    sync.Mutex
	clock Clock

	// byClientEvent indexes entries by client-event-id.
	byClientEvent map[string]*LedgerEntry

	// byServerTx indexes entries by server tx-id once acknowledged.
	byServerTx map[string]*LedgerEntry

	// bufferedRefreshes retains non-numeric refresh identities until an ack supplies
	// the matching server transaction identity. Numeric runtime identities are
	// watermarks and are tracked in lastRefreshTxID instead.
	bufferedRefreshes map[string]bufferedRefresh

	// lastAckTxID tracks monotonic server-tx-id ordering per session.
	lastAckTxID map[int]int64

	// lastRefreshTxID tracks monotonic processed-tx-id ordering per session.
	lastRefreshTxID map[int]int64
	lastRefreshAt   map[int]time.Time

	// lagSamples records write→refresh latency for resolved transactions.
	lagSamples []time.Duration

	// terminalErr captures the first terminal failure encountered.
	terminalErr error

	// nextSequence keeps client event/entity identities unique across reconnects.
	nextSequence map[int]int64
}

// NewTransactionLedger constructs a new ledger using the specified clock.
func NewTransactionLedger(clock Clock) *TransactionLedger {
	if clock == nil {
		clock = realClock{}
	}
	return &TransactionLedger{
		clock:             clock,
		byClientEvent:     make(map[string]*LedgerEntry),
		byServerTx:        make(map[string]*LedgerEntry),
		bufferedRefreshes: make(map[string]bufferedRefresh),
		lastAckTxID:       make(map[int]int64),
		lastRefreshTxID:   make(map[int]int64),
		lastRefreshAt:     make(map[int]time.Time),
		lagSamples:        make([]time.Duration, 0),
		nextSequence:      make(map[int]int64),
	}
}

// NextSequence allocates a session-local sequence that remains unique across
// connection generations and therefore across reconnects.
func (l *TransactionLedger) NextSequence(sessionID int) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.nextSequence[sessionID]++
	return l.nextSequence[sessionID]
}

// RecordSubmit registers a newly submitted transaction in the ledger.
// EV-001a: Every committed transaction creates exactly one ledger entry.
func (l *TransactionLedger) RecordSubmit(clientEventID string, sessionID int) (*LedgerEntry, error) {
	if clientEventID == "" {
		return nil, fmt.Errorf("ledger: empty client-event-id")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.terminalErr != nil {
		return nil, l.terminalErr
	}

	if _, exists := l.byClientEvent[clientEventID]; exists {
		err := fmt.Errorf("ledger: duplicate transaction client-event-id %q", clientEventID)
		l.terminalErr = err
		return nil, err
	}

	entry := &LedgerEntry{
		ClientEventID: clientEventID,
		SessionID:     sessionID,
		SubmittedAt:   l.clock.Now(),
		State:         StateSubmitted,
	}
	l.byClientEvent[clientEventID] = entry
	return entry, nil
}

// RecordAck correlates a transact-ok frame to a known submitted transaction.
// EV-001b: Ack frames correlate to known entries; reject unknown, duplicate, or invalid-order outcomes.
func (l *TransactionLedger) RecordAck(clientEventID, serverTxID string) (*LedgerEntry, error) {
	if clientEventID == "" {
		err := fmt.Errorf("ledger: ack frame missing client-event-id")
		l.setTerminalError(err)
		return nil, err
	}
	if serverTxID == "" {
		err := fmt.Errorf("ledger: ack frame missing tx-id for client-event-id %q", clientEventID)
		l.setTerminalError(err)
		return nil, err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.terminalErr != nil {
		return nil, l.terminalErr
	}

	entry, ok := l.byClientEvent[clientEventID]
	if !ok {
		err := fmt.Errorf("ledger: unknown ack client-event-id %q", clientEventID)
		l.setTerminalErrorLocked(err)
		return nil, err
	}

	if !entry.AckAt.IsZero() {
		err := fmt.Errorf("ledger: duplicate ack for client-event-id %q", clientEventID)
		entry.State = StateTerminal
		entry.TerminalReason = "duplicate_ack"
		l.setTerminalErrorLocked(err)
		return nil, err
	}

	if existing, exists := l.byServerTx[serverTxID]; exists && existing.ClientEventID != clientEventID {
		err := fmt.Errorf("ledger: server tx-id %q already claimed by client-event-id %q", serverTxID, existing.ClientEventID)
		entry.State = StateTerminal
		entry.TerminalReason = "duplicate_server_tx_id"
		l.setTerminalErrorLocked(err)
		return nil, err
	}

	// Check monotonic ordering when server-tx-id is numeric.
	if num, err := strconv.ParseInt(serverTxID, 10, 64); err == nil {
		last := l.lastAckTxID[entry.SessionID]
		if last > 0 && num == last {
			dupErr := fmt.Errorf("ledger: duplicate ack for client-event-id %q (tx-id %q)", clientEventID, serverTxID)
			entry.State = StateTerminal
			entry.TerminalReason = "duplicate_ack"
			l.setTerminalErrorLocked(dupErr)
			return nil, dupErr
		}
		if last > 0 && num < last {
			orderErr := fmt.Errorf("ledger: invalid-order ack: session %d tx-id %d < previous %d", entry.SessionID, num, last)
			entry.State = StateTerminal
			entry.TerminalReason = "invalid_ack_order"
			l.setTerminalErrorLocked(orderErr)
			return nil, orderErr
		}
		l.lastAckTxID[entry.SessionID] = num
	}

	now := l.clock.Now()
	entry.ServerTxID = serverTxID
	entry.AckAt = now
	l.byServerTx[serverTxID] = entry

	// Correlate with any buffered refresh frame that arrived before this ack.
	if buf, exists := l.bufferedRefreshes[serverTxID]; exists {
		delete(l.bufferedRefreshes, serverTxID)
		entry.RefreshAt = buf.at
		entry.RefreshBeforeAck = true
		entry.Lag = entry.RefreshAt.Sub(entry.SubmittedAt)
		entry.State = StateResolved
		l.lagSamples = append(l.lagSamples, entry.Lag)
	} else {
		entry.State = StateAcknowledged
		l.resolveEntryAgainstWatermarksLocked(entry)
	}

	return entry, nil
}

// RecordRefresh correlates a refresh-ok or refresh-ok-delta frame via processed-tx-id.
// EV-001b: Refresh frames correlate to known entries; reject unknown, duplicate, or invalid-order outcomes.
func (l *TransactionLedger) RecordRefresh(processedTxID string, observingSessionID int) (*LedgerEntry, error) {
	if processedTxID == "" {
		err := fmt.Errorf("ledger: refresh frame missing processed-tx-id")
		l.setTerminalError(err)
		return nil, err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.terminalErr != nil {
		return nil, l.terminalErr
	}

	// processed-tx-id is a session watermark, not an exact transaction receipt.
	// Equal watermarks are idempotent because refreshes may be coalesced.
	if num, err := strconv.ParseInt(processedTxID, 10, 64); err == nil {
		last := l.lastRefreshTxID[observingSessionID]
		if last > 0 && num < last {
			orderErr := fmt.Errorf("ledger: invalid-order refresh: session %d processed-tx-id %d < previous %d", observingSessionID, num, last)
			l.setTerminalErrorLocked(orderErr)
			return nil, orderErr
		}
		if num > last || last == 0 {
			l.lastRefreshTxID[observingSessionID] = num
			l.lastRefreshAt[observingSessionID] = l.clock.Now()
		}
		return l.resolveWatermarkLocked(num, observingSessionID), nil
	}

	// Non-numeric identities are only supported as exact receipts for a pending
	// transaction. Numeric runtime identities use the watermark path above.
	if entry, ok := l.byServerTx[processedTxID]; ok {
		if entry.RefreshAt.IsZero() {
			l.resolveEntryLocked(entry, l.clock.Now(), false)
		}
		return entry, nil
	}

	for _, e := range l.byClientEvent {
		if e.AckAt.IsZero() && e.State == StateSubmitted {
			if _, exists := l.bufferedRefreshes[processedTxID]; !exists {
				l.bufferedRefreshes[processedTxID] = bufferedRefresh{at: l.clock.Now(), sessionID: observingSessionID}
			}
			return nil, nil
		}
	}

	err := fmt.Errorf("ledger: unknown refresh processed-tx-id %q", processedTxID)
	l.setTerminalErrorLocked(err)
	return nil, err
}

// RecordError records a protocol error frame as a classified terminal failure.
// EV-001c: Protocol error frames become classified terminal failures.
func (l *TransactionLedger) RecordError(clientEventID, reason string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if clientEventID != "" {
		if entry, ok := l.byClientEvent[clientEventID]; ok {
			entry.State = StateTerminal
			entry.TerminalReason = reason
		}
	}

	err := fmt.Errorf("ledger protocol error: %s", reason)
	l.setTerminalErrorLocked(err)
	return err
}

// setTerminalErrorLocked records the terminal error.
func (l *TransactionLedger) setTerminalErrorLocked(err error) {
	if l.terminalErr == nil {
		l.terminalErr = err
	}
}

func (l *TransactionLedger) setTerminalError(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.setTerminalErrorLocked(err)
}

func (l *TransactionLedger) resolveEntryLocked(entry *LedgerEntry, at time.Time, beforeAck bool) {
	if entry == nil || !entry.RefreshAt.IsZero() {
		return
	}
	entry.RefreshAt = at
	entry.RefreshBeforeAck = beforeAck
	entry.Lag = at.Sub(entry.SubmittedAt)
	entry.State = StateResolved
	l.lagSamples = append(l.lagSamples, entry.Lag)
}

func (l *TransactionLedger) resolveEntryAgainstWatermarksLocked(entry *LedgerEntry) {
	num, err := strconv.ParseInt(entry.ServerTxID, 10, 64)
	if err != nil {
		return
	}
	sessionID := entry.SessionID
	if watermark := l.lastRefreshTxID[sessionID]; watermark >= num {
		at := l.lastRefreshAt[sessionID]
		if at.IsZero() {
			at = l.clock.Now()
		}
		l.resolveEntryLocked(entry, at, true)
	}
}

func (l *TransactionLedger) resolveWatermarkLocked(watermark int64, sessionID int) *LedgerEntry {
	var newest *LedgerEntry
	for _, entry := range l.byClientEvent {
		if entry.SessionID != sessionID || entry.AckAt.IsZero() || !entry.RefreshAt.IsZero() {
			continue
		}
		num, err := strconv.ParseInt(entry.ServerTxID, 10, 64)
		if err == nil && num <= watermark {
			l.resolveEntryLocked(entry, l.clock.Now(), false)
			newest = entry
		}
	}
	return newest
}

// TerminalError returns any recorded terminal error.
func (l *TransactionLedger) TerminalError() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.terminalErr
}

// UnresolvedCount returns the total number of transactions not yet in StateResolved.
func (l *TransactionLedger) UnresolvedCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	count := 0
	for _, e := range l.byClientEvent {
		if e.State != StateResolved {
			count++
		}
	}
	return count
}

// UnresolvedCountForSession returns unresolved transactions submitted by a specific session.
func (l *TransactionLedger) UnresolvedCountForSession(sessionID int) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	count := 0
	for _, e := range l.byClientEvent {
		if e.SessionID == sessionID && e.State != StateResolved {
			count++
		}
	}
	return count
}

// QuiesceSession waits for all transactions submitted by sessionID to resolve
// before timeout. Returns non-nil if any remain unresolved or a terminal error was set.
// EV-001d: Completion requires zero unresolved entries after bounded quiescence.
func (l *TransactionLedger) QuiesceSession(ctx context.Context, sessionID int, timeout time.Duration) error {
	deadline := l.clock.Now().Add(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		l.mu.Lock()
		if l.terminalErr != nil {
			err := l.terminalErr
			l.mu.Unlock()
			return err
		}

		unresolved := 0
		for _, e := range l.byClientEvent {
			if e.SessionID == sessionID && e.State != StateResolved {
				unresolved++
			}
		}

		if unresolved == 0 {
			l.mu.Unlock()
			return nil
		}

		if l.clock.Now().After(deadline) {
			// Mark remaining unresolved entries with terminal reason.
			for _, e := range l.byClientEvent {
				if e.SessionID == sessionID && e.State != StateResolved {
					e.State = StateTerminal
					if e.TerminalReason == "" {
						if e.AckAt.IsZero() {
							e.TerminalReason = "quiescence_timeout_missing_ack"
						} else {
							e.TerminalReason = "quiescence_timeout_missing_refresh"
						}
					}
				}
			}
			err := fmt.Errorf("ledger: session %d quiescence timeout with %d unresolved transactions", sessionID, unresolved)
			l.setTerminalErrorLocked(err)
			l.mu.Unlock()
			return err
		}
		l.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// QuiesceAll waits for all transactions across all sessions to resolve before timeout.
// EV-001d: Completion requires zero unresolved entries after bounded quiescence.
func (l *TransactionLedger) QuiesceAll(ctx context.Context, timeout time.Duration) error {
	deadline := l.clock.Now().Add(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		l.mu.Lock()
		if l.terminalErr != nil {
			err := l.terminalErr
			l.mu.Unlock()
			return err
		}

		unresolved := 0
		for _, e := range l.byClientEvent {
			if e.State != StateResolved {
				unresolved++
			}
		}

		if unresolved == 0 && len(l.bufferedRefreshes) == 0 {
			l.mu.Unlock()
			return nil
		}

		if l.clock.Now().After(deadline) {
			for _, e := range l.byClientEvent {
				if e.State != StateResolved {
					e.State = StateTerminal
					if e.TerminalReason == "" {
						if e.AckAt.IsZero() {
							e.TerminalReason = "quiescence_timeout_missing_ack"
						} else {
							e.TerminalReason = "quiescence_timeout_missing_refresh"
						}
					}
				}
			}
			unclaimed := len(l.bufferedRefreshes)
			err := fmt.Errorf("ledger: quiescence timeout with %d unresolved transactions, %d unclaimed buffered refreshes", unresolved, unclaimed)
			l.setTerminalErrorLocked(err)
			l.mu.Unlock()
			return err
		}
		l.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// LagSamples returns a copy of write→refresh lag samples recorded by the ledger.
func (l *TransactionLedger) LagSamples() []time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]time.Duration, len(l.lagSamples))
	copy(out, l.lagSamples)
	return out
}

// Entries returns a copy of all ledger entries sorted by client-event-id.
// EV-001e: Evidence records send/ack/refresh times and terminal reason per transaction.
func (l *TransactionLedger) Entries() []*LedgerEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]*LedgerEntry, 0, len(l.byClientEvent))
	for _, e := range l.byClientEvent {
		entryCopy := *e
		out = append(out, &entryCopy)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ClientEventID < out[j].ClientEventID
	})
	return out
}

// Entry returns a copy of the entry for the given client-event-id.
func (l *TransactionLedger) Entry(clientEventID string) (*LedgerEntry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.byClientEvent[clientEventID]
	if !ok {
		return nil, false
	}
	entryCopy := *e
	return &entryCopy, true
}

// rawToString normalizes a raw JSON message into a clean string representation.
// Handles both JSON strings ("101") and JSON numbers (101).
func rawToString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var num json.Number
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(&num); err == nil {
		return num.String()
	}
	return strings.Trim(string(raw), "\"")
}
