package benchharness

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEvidenceCollectorCountsMillionsWithoutRetainingPayloads(t *testing.T) {
	collector := newEvidenceCollector(EvidenceBudget{MaxFrames: 2_000_000, MaxBytes: 100_000_000, MaxRetainedFrames: 8})
	payload := json.RawMessage(`{"op":"refresh-ok","computations":[]}`)
	event := SessionEvent{Op: "refresh-ok", Payload: payload, At: time.Now()}
	for i := 0; i < 1_000_000; i++ {
		collector.record(event, "client-0", "q-0", "recipient-0", "")
	}
	frames, stats := collector.snapshot()
	if len(frames) != 8 || stats.ObservedFrames != 1_000_000 || stats.ObservedBytes != int64(len(payload))*1_000_000 {
		t.Fatalf("bounded evidence accounting mismatch: retained=%d stats=%#v", len(frames), stats)
	}
	if !stats.Truncated || stats.Overflow || stats.StreamDigest == "" {
		t.Fatalf("unexpected bounded evidence state: %#v", stats)
	}
	if got := stats.ByClass[FrameApplicationRefresh]; got.Frames != 1_000_000 || got.Bytes != int64(len(payload))*1_000_000 {
		t.Fatalf("application refresh classification mismatch: %#v", stats.ByClass)
	}
	if len(frames[0].PayloadDigest) != 64 || frames[0].PayloadBytes != int64(len(payload)) {
		t.Fatalf("payload was not represented by bounded digest/size: %#v", frames[0])
	}

	overflow := newEvidenceCollector(EvidenceBudget{MaxFrames: 3, MaxBytes: 6, MaxRetainedFrames: 2})
	for i := 0; i < 10; i++ {
		overflow.record(SessionEvent{Op: "refresh-ok", Payload: []byte("123")}, "c", "q", "r", "")
	}
	_, overflowStats := overflow.snapshot()
	if !overflowStats.Overflow || overflowStats.ObservedFrames != 10 || overflowStats.ObservedBytes != 30 {
		t.Fatalf("budget overflow was not explicit: %#v", overflowStats)
	}
}

func TestEvidenceCollectorClassifiesControlAndProtocolFrames(t *testing.T) {
	collector := newEvidenceCollector(EvidenceBudget{MaxRetainedFrames: 16})
	frames := []SessionEvent{
		{Op: "init-ok", Payload: []byte("init")},
		{Op: "add-query-ok", Payload: []byte("query")},
		{Op: "transact-ok", Payload: []byte("ack")},
		{Op: "refresh-ok", Payload: []byte("refresh")},
		{Op: "protocol-error", Payload: []byte("bad")},
		{Op: "refresh-ok", Payload: []byte("decode-failed")},
	}
	for i, ev := range frames {
		protocolErr := ""
		if i == len(frames)-1 {
			protocolErr = "malformed refresh"
		}
		collector.record(ev, "client", "query", "recipient", protocolErr)
	}
	retained, stats := collector.snapshot()
	if got := stats.ByClass[FrameSessionInit]; got.Frames != 1 || got.Bytes != 4 {
		t.Fatalf("session-init classification mismatch: %#v", stats.ByClass)
	}
	if got := stats.ByClass[FrameQueryLifecycle]; got.Frames != 1 || got.Bytes != 5 {
		t.Fatalf("query classification mismatch: %#v", stats.ByClass)
	}
	if got := stats.ByClass[FrameTransactionAck]; got.Frames != 1 || got.Bytes != 3 {
		t.Fatalf("ack classification mismatch: %#v", stats.ByClass)
	}
	if got := stats.ByClass[FrameApplicationRefresh]; got.Frames != 1 || got.Bytes != 7 {
		t.Fatalf("application refresh classification mismatch: %#v", stats.ByClass)
	}
	if got := stats.ByClass[FrameProtocolError]; got.Frames != 2 || got.Bytes != 3+13 {
		t.Fatalf("protocol classification mismatch: %#v", stats.ByClass)
	}
	if len(retained) != len(frames) || retained[0].Class != FrameSessionInit || retained[3].Class != FrameApplicationRefresh || retained[5].Class != FrameProtocolError {
		t.Fatalf("raw frame class metadata mismatch: %#v", retained)
	}
}

func TestEvidenceCollectorMeasuredWindowExcludesSetupAndRetainsTotals(t *testing.T) {
	collector := newEvidenceCollector(EvidenceBudget{MaxRetainedFrames: 1})
	started := time.Unix(100, 0)
	setupPayload := []byte("setup-refresh")
	collector.record(SessionEvent{Op: "refresh-ok", Payload: setupPayload, At: started.Add(-time.Second)}, "client", "q", "recipient", "")
	collector.beginMeasured(started)
	measuredBytes := int64(0)
	for i, payload := range [][]byte{[]byte("measured-a"), []byte("measured-bb"), []byte("measured-ccc")} {
		measuredBytes += int64(len(payload))
		collector.record(SessionEvent{Op: "refresh-ok", Payload: payload, At: started.Add(time.Duration(i+1) * time.Second / 4)}, "client", "q", "recipient", "")
	}
	finished := time.Unix(101, 0)
	collector.endMeasured(finished)
	retained, stats := collector.snapshot()
	all := stats.ByClass[FrameApplicationRefresh]
	measured := stats.MeasuredByClass[FrameApplicationRefresh]
	if all.Frames != 4 || all.Bytes != int64(len(setupPayload))+measuredBytes {
		t.Fatalf("cumulative application evidence mismatch: %#v", stats)
	}
	if measured.Frames != 3 || measured.Bytes != measuredBytes {
		t.Fatalf("measured application evidence includes setup frames: %#v", stats.MeasuredByClass)
	}
	if !stats.MeasuredStartedAt.Equal(started) || !stats.MeasuredFinishedAt.Equal(finished) {
		t.Fatalf("measured boundary timestamps were not retained: %#v", stats)
	}
	if len(retained) != 1 || retained[0].PayloadBytes != int64(len(setupPayload)) {
		t.Fatalf("bounded sample does not prove pre-window exclusion: %#v", retained)
	}
}

func TestEvidenceCollectorMeasuredWindowUsesEventTimeNotProcessingTime(t *testing.T) {
	collector := newEvidenceCollector(EvidenceBudget{MaxRetainedFrames: 16})
	started := time.Unix(200, 0)
	finished := started.Add(time.Second)
	collector.beginMeasured(started)
	// This frame is consumed after the start callback but was emitted before
	// it; event time must keep it out of measured totals.
	collector.record(SessionEvent{Op: "refresh-ok", Payload: []byte("queued-before"), At: started.Add(-time.Nanosecond)}, "client", "q", "recipient", "")
	collector.endMeasured(finished)
	// This frame is consumed during convergence after the end callback, but
	// its server timestamp lies inside the measured interval and must count.
	collector.record(SessionEvent{Op: "refresh-ok", Payload: []byte("queued-during"), At: started.Add(500 * time.Millisecond)}, "client", "q", "recipient", "")
	// Missing event time is never treated as a local processing timestamp.
	collector.record(SessionEvent{Op: "refresh-ok", Payload: []byte("missing-time")}, "client", "q", "recipient", "")
	_, stats := collector.snapshot()
	measured := stats.MeasuredByClass[FrameApplicationRefresh]
	if measured.Frames != 1 || measured.Bytes != int64(len("queued-during")) {
		t.Fatalf("measured classification used processing state/time: %#v", stats.MeasuredByClass)
	}
}
