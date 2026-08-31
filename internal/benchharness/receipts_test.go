package benchharness

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestDecodeReceiptErrorIsReturnedToRunAdapter(t *testing.T) {
	driver := &TargetDriver{cfg: TargetConfig{DecodeRefresh: func(SessionEvent, string) (Refresh, error) { return Refresh{}, errors.New("malformed computations") }}}
	client := &TargetSession{driver: driver, states: map[string]Materialized{}}
	_, err := driver.decodeReceipts(client, SessionEvent{Op: "refresh-ok", At: time.Now()}, "q", "client-q", newCommitPrefixes(nil))
	if err == nil || !strings.Contains(err.Error(), "malformed computations") {
		t.Fatalf("decoder error was swallowed: %v", err)
	}
}

func TestCommitPrefixesMapsNumericWatermarkWithoutWireEventID(t *testing.T) {
	w, _ := NewWorkload(FamilyH, 300, 47)
	q := w.Fixture.Queries[0]
	m1, m2 := w.Mutation(1), w.Mutation(2)
	o := NewPrefixOracle(w.Fixture)
	o.Append(m1)
	d1, _ := o.ExpectedDigest(q.ID, 1)
	o.Append(m2)
	d2, _ := o.ExpectedDigest(q.ID, 2)
	cp := newCommitPrefixes(nil)
	cp.Record(m1, Ack{Accepted: true, ServerTransactionID: "101", ProcessedTransactionID: "101"}, 1)
	cp.Record(m2, Ack{Accepted: true, ServerTransactionID: "102", ProcessedTransactionID: "102"}, 2)

	exact, ok := cp.ResolveAll(Receipt{QueryID: q.ID, RecipientID: "client-" + q.ID, ProcessedTransactionID: "101", Observed: mustMaterialized(o, q.ID, 1)})
	if !ok || len(exact) != 1 || exact[0].ClientEventID != m1.EventID || exact[0].Prefix != 1 {
		t.Fatalf("numeric exact watermark was not correlated: %#v", exact)
	}
	if !exact[0].ProvesIntermediate {
		t.Fatalf("single exact receipt did not prove its intermediate prefix: %#v", exact[0])
	}
	coalesced, ok := cp.ResolveAll(Receipt{QueryID: q.ID, RecipientID: "client-" + q.ID, ProcessedTransactionID: "102", Observed: mustMaterialized(o, q.ID, 2)})
	if !ok || len(coalesced) != 1 || coalesced[0].ClientEventID != m2.EventID {
		t.Fatalf("numeric coalesced watermark did not emit only newly covered event: %#v", coalesced)
	}
	if coalesced[0].Prefix != 2 {
		t.Fatalf("coalesced receipts used wrong watermark: %#v", coalesced)
	}
	if !coalesced[0].ProvesIntermediate {
		t.Fatalf("single newly emitted receipt at its exact watermark was not exact: %#v", coalesced[0])
	}
	if repeated, ok := cp.ResolveAll(Receipt{QueryID: q.ID, RecipientID: "client-" + q.ID, ProcessedTransactionID: "102", Observed: mustMaterialized(o, q.ID, 2)}); !ok || len(repeated) != 0 {
		t.Fatalf("repeated coalesced watermark was not bounded: %#v/%t", repeated, ok)
	}
	// A first observation at watermark 102 must still expand the entire
	// committed prefix; the incremental gate above has already emitted 101.
	cpAll := newCommitPrefixes(nil)
	cpAll.Record(m1, Ack{Accepted: true, ServerTransactionID: "101", ProcessedTransactionID: "101"}, 1)
	cpAll.Record(m2, Ack{Accepted: true, ServerTransactionID: "102", ProcessedTransactionID: "102"}, 2)
	all, ok := cpAll.ResolveAll(Receipt{QueryID: q.ID, RecipientID: "client-" + q.ID, ProcessedTransactionID: "102", Observed: mustMaterialized(o, q.ID, 2)})
	if !ok || len(all) != 2 {
		t.Fatalf("initial coalesced watermark did not expand committed prefix: %#v", all)
	}
	if all[0].ProvesIntermediate || all[1].ProvesIntermediate {
		t.Fatalf("multi-event expansion was mislabeled exact: %#v", all)
	}

	l := NewLedger()
	for i, mutation := range []Mutation{m1, m2} {
		key := LedgerKey{PairID: "p", RunID: "r", WriterID: "writer-0", RecipientID: "client-" + q.ID, ClientEventID: mutation.EventID}
		if err := l.AddExpected(key, mutation); err != nil {
			t.Fatal(err)
		}
		digest := d1
		if i == 1 {
			digest = d2
		}
		l.SetExpectation(key, []string{q.ID}, []string{key.RecipientID}, digest, i+1)
		l.Acknowledged(key, Ack{Accepted: true}, time.Now(), nil)
		observation := all[i]
		observationDigest := mustDigest(observation.Observed)
		latest := ""
		if i == 0 {
			latest = d2
		}
		if err := l.Observe(key, Observation{ObservedDigest: observationDigest, ExpectedDigest: digest, LatestExpectedDigest: latest, Prefix: observation.Prefix, ExpectedPrefix: i + 1, Applicable: true, At: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	rows := l.Rows()
	if rows[0].Coverage == CoverageMissing || rows[1].Coverage == CoverageMissing {
		t.Fatalf("exact/coalesced ledger coverage was not recorded: %#v", rows)
	}
}

func TestCommitPrefixesDoesNotSuppressChangedProofAtSameWatermark(t *testing.T) {
	w, _ := NewWorkload(FamilyH, 300, 49)
	q, mutation := w.Fixture.Queries[0], w.Mutation(1)
	o := NewPrefixOracle(w.Fixture)
	o.Append(mutation)
	good := mustMaterialized(o, q.ID, 1)
	bad := Materialized{QueryID: q.ID, Entities: map[string]Entity{}}
	cp := newCommitPrefixes(nil)
	cp.Record(mutation, Ack{Accepted: true, ProcessedTransactionID: "101"}, 1)
	if first, ok := cp.ResolveAll(Receipt{QueryID: q.ID, RecipientID: "client-" + q.ID, ProcessedTransactionID: "101", Observed: bad}); !ok || len(first) != 1 {
		t.Fatalf("initial proof was not emitted: %#v/%t", first, ok)
	}
	if retry, ok := cp.ResolveAll(Receipt{QueryID: q.ID, RecipientID: "client-" + q.ID, ProcessedTransactionID: "101", Observed: good}); !ok || len(retry) != 1 || retry[0].ClientEventID != mutation.EventID {
		t.Fatalf("changed proof at same watermark was suppressed: %#v/%t", retry, ok)
	}
}

func mustMaterialized(o *PrefixOracle, queryID string, prefix int) Materialized {
	m, err := o.Materialize(queryID, prefix)
	if err != nil {
		panic(err)
	}
	return m
}
