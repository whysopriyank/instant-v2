package benchharness

import (
	"fmt"
	"sync"
)

func (d *TargetDriver) decodeReceipts(client *TargetSession, ev SessionEvent, queryID, recipientID string, prefixes *commitPrefixes) ([]Receipt, error) {
	if d.cfg.DecodeReceipt != nil {
		items, err := d.cfg.DecodeReceipt(ev, queryID)
		if err != nil {
			return nil, err
		}
		ready := make([]Receipt, 0, len(items))
		for i := range items {
			items[i].QueryID = queryID
			items[i].RecipientID = recipientID
			if items[i].At.IsZero() {
				items[i].At = ev.At
			}
			if items[i].ProcessedTransactionID == "" {
				items[i].ProcessedTransactionID = ev.ProcessedTransactionID
			}
			if items[i].ObservationDigest == "" {
				items[i].ObservationDigest = mustDigest(items[i].Observed)
			}
			resolved, ok := prefixes.ResolveAll(items[i])
			if !ok {
				continue
			}
			ready = append(ready, resolved...)
		}
		return ready, nil
	}
	refresh, err := client.decodeRefresh(ev, queryID)
	if err != nil {
		return nil, err
	}
	if refresh.Kind == RefreshNoop {
		return nil, nil
	}
	var materialized Materialized
	if refresh.Full != nil {
		materialized = *refresh.Full
	} else if refresh.Delta != nil {
		client.mu.Lock()
		previous := client.states[queryID]
		client.mu.Unlock()
		materialized, err = ApplyDelta(previous, *refresh.Delta)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("refresh has neither full nor delta state")
	}
	client.mu.Lock()
	client.states[queryID] = materialized
	client.mu.Unlock()
	item := Receipt{QueryID: queryID, RecipientID: recipientID, Observed: materialized, ObservationDigest: mustDigest(materialized), ProcessedTransactionID: refresh.ProcessedTransactionID, At: refresh.At, EvidenceRef: "wire-computations"}
	if item.At.IsZero() {
		item.At = ev.At
	}
	resolved, ok := prefixes.ResolveAll(item)
	if !ok {
		return nil, nil
	}
	return resolved, nil
}

// commitPrefixes captures the actual acknowledged server-tx → oracle-prefix
// order. Readers may observe refreshes before their acknowledgement, so the
// unresolved receipt is buffered until Record supplies the valid prefix.
type commitPrefixes struct {
	mu          sync.Mutex
	byTx        map[string]commitPrefix
	byEvent     map[string]commitPrefix
	commits     []commitPrefix
	pending     []Receipt
	lastEmitted map[string]int
	lastDigest  map[string]string
	emit        func(Receipt)
	family      Family
	writerID    string
}

func newCommitPrefixes(emit func(Receipt)) *commitPrefixes {
	return newCommitPrefixesFor(emit, "", "writer-0")
}

func newCommitPrefixesFor(emit func(Receipt), family Family, writerID string) *commitPrefixes {
	if writerID == "" {
		writerID = "writer-0"
	}
	return &commitPrefixes{byTx: map[string]commitPrefix{}, byEvent: map[string]commitPrefix{}, lastEmitted: map[string]int{}, lastDigest: map[string]string{}, emit: emit, family: family, writerID: writerID}
}

type commitPrefix struct {
	prefix    int
	eventID   string
	writerID  string
	serverID  string
	processed string
}

func (m *commitPrefixes) Record(mutation Mutation, ack Ack, prefix int) {
	if prefix <= 0 {
		return
	}
	writerID := m.writerID
	if m.family == FamilyT {
		writerID = fmt.Sprintf("writer-%d", int((mutation.Sequence-1)%8))
	}
	commit := commitPrefix{prefix: prefix, eventID: mutation.EventID, writerID: writerID, serverID: ack.ServerTransactionID, processed: ack.ProcessedTransactionID}
	m.mu.Lock()
	m.commits = append(m.commits, commit)
	m.byEvent[commit.eventID] = commit
	if ack.ServerTransactionID != "" {
		m.byTx[ack.ServerTransactionID] = commit
	}
	if ack.ProcessedTransactionID != "" {
		m.byTx[ack.ProcessedTransactionID] = commit
	}
	ready := make([]Receipt, 0)
	remaining := make([]Receipt, 0, len(m.pending))
	for _, item := range m.pending {
		if resolved, ok := m.resolveKnownLocked(item); ok {
			ready = append(ready, resolved...)
		} else {
			remaining = append(remaining, item)
		}
	}
	m.pending = remaining
	m.mu.Unlock()
	for _, item := range ready {
		if m.emit != nil {
			m.emit(item)
		}
	}
}

func (m *commitPrefixes) Lookup(id string) (int, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	commit, ok := m.byTx[id]
	if !ok {
		return 0, false
	}
	return commit.prefix, true
}

// ResolveAll maps one semantic refresh to every committed event through its
// processed watermark. A coalesced snapshot at prefix N is evidence for each
// applicable event 1..N; emitting only the final event would falsely report
// all preceding ledger rows as drops.
func (m *commitPrefixes) ResolveAll(r Receipt) ([]Receipt, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if resolved, ok := m.resolveKnownLocked(r); ok {
		return resolved, true
	}
	m.pending = append(m.pending, r)
	return nil, false
}

func (m *commitPrefixes) resolveKnownLocked(r Receipt) ([]Receipt, bool) {
	digest := r.ObservationDigest
	if digest == "" {
		digest = mustDigest(r.Observed)
	}
	if r.ClientEventID != "" {
		commit, ok := m.byEvent[r.ClientEventID]
		if !ok {
			return nil, false
		}
		if r.Prefix <= 0 {
			r.Prefix = commit.prefix
		}
		if r.WriterID == "" {
			r.WriterID = commit.writerID
		}
		r.ProvesIntermediate = r.Prefix == commit.prefix && processedIDMatches(commit, r.ProcessedTransactionID)
		cohort := receiptCohortKey(r)
		lastDigest, seen := m.lastDigest[cohort]
		if r.Prefix <= m.lastEmitted[cohort] && (!seen || digest == lastDigest) {
			return nil, true
		}
		m.lastEmitted[cohort] = r.Prefix
		m.lastDigest[cohort] = digest
		return []Receipt{r}, true
	}
	if r.ProcessedTransactionID == "" {
		return nil, false
	}
	watermark, ok := m.byTx[r.ProcessedTransactionID]
	if !ok {
		return nil, false
	}
	cohort := receiptCohortKey(r)
	last := m.lastEmitted[cohort]
	lastDigest, seen := m.lastDigest[cohort]
	if watermark.prefix <= last && (!seen || digest == lastDigest) {
		return nil, true
	}
	if watermark.prefix <= last {
		// A changed observation may be a previously invalid proof. Re-emit the
		// committed prefix so a later valid proof is not suppressed.
		last = 0
	}
	capacity := watermark.prefix - last
	if capacity < 0 {
		capacity = 0
	}
	resolved := make([]Receipt, 0, capacity)
	for _, commit := range m.commits {
		if commit.prefix <= last {
			continue
		}
		if commit.prefix > watermark.prefix {
			break
		}
		item := r
		item.ClientEventID = commit.eventID
		item.Prefix = watermark.prefix
		item.WriterID = commit.writerID
		item.ProvesIntermediate = false
		resolved = append(resolved, item)
	}
	if len(resolved) > 0 {
		m.lastEmitted[cohort] = watermark.prefix
		m.lastDigest[cohort] = digest
		if len(resolved) == 1 {
			resolved[0].ProvesIntermediate = watermark.prefix == resolved[0].Prefix && processedIDMatches(watermark, r.ProcessedTransactionID)
		}
	}
	return resolved, true
}

func processedIDMatches(commit commitPrefix, processed string) bool {
	return processed != "" && (processed == commit.serverID || processed == commit.processed)
}

func receiptCohortKey(r Receipt) string {
	return r.QueryID + "\x00" + r.RecipientID
}

func (m *commitPrefixes) Resolve(r Receipt) (Receipt, bool) {
	resolved, ok := m.ResolveAll(r)
	if !ok || len(resolved) == 0 {
		return Receipt{}, false
	}
	return resolved[0], true
}
