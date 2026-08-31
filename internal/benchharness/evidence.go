package benchharness

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"strconv"
	"sync"
	"time"
)

// FrameClass identifies the protocol role of a received frame. It is kept
// separate from Op because B must be able to count application refresh bytes
// without including session handshakes, query/transaction acknowledgements,
// or protocol failures.
type FrameClass string

const (
	FrameApplicationRefresh FrameClass = "application_refresh"
	FrameSessionInit        FrameClass = "session_init"
	FrameQueryLifecycle     FrameClass = "query_lifecycle"
	FrameTransactionAck     FrameClass = "transaction_ack"
	FrameProtocolError      FrameClass = "protocol_error"
	FrameOther              FrameClass = "other"
)

// FrameClassStats reports all observed payloads in a class, including frames
// omitted from the bounded RawFrames sample.
type FrameClassStats struct {
	Frames int64
	Bytes  int64
}

// EvidenceStats reports cumulative frame accounting independently of the
// bounded metadata sample. Overflow is a harness/resource failure; Truncated
// only means that the optional sample reached its retention cap.
type EvidenceStats struct {
	ObservedFrames    int64
	ObservedBytes     int64
	RetainedFrames    int
	MaxFrames         int64
	MaxBytes          int64
	MaxRetainedFrames int
	Truncated         bool
	Overflow          bool
	StreamDigest      string
	ByClass           map[FrameClass]FrameClassStats
	// MeasuredByClass is reset at the measured-start boundary and excludes all
	// qualification, cleanup, ramp, settle, and warm-up frames. It is
	// cumulative even when RawFrames retention is truncated.
	MeasuredByClass    map[FrameClass]FrameClassStats
	MeasuredStartedAt  time.Time
	MeasuredFinishedAt time.Time
}

// RawFrameEvidence is normalized, bounded wire evidence. PayloadBytes and
// PayloadDigest preserve exact size and a digest of the received payload
// without retaining user data. ProtocolError is bounded text.
type RawFrameEvidence struct {
	ClientID               string
	QueryID                string
	RecipientID            string
	Op                     string
	Class                  FrameClass
	ClientEventID          string
	ServerTransactionID    string
	ProcessedTransactionID string
	At                     time.Time
	PayloadBytes           int64
	PayloadDigest          string
	ProtocolError          string
}

// FrameEvidence is the concise integration name for RawFrameEvidence. Keep
// both names source-compatible while callers migrate to RawFrames.
type FrameEvidence = RawFrameEvidence

type evidenceCollector struct {
	mu                 sync.Mutex
	budget             EvidenceBudget
	stream             hash.Hash
	frames             int64
	bytes              int64
	byClass            map[FrameClass]FrameClassStats
	measuredByClass    map[FrameClass]FrameClassStats
	measuredStartedAt  time.Time
	measuredFinishedAt time.Time
	samples            []RawFrameEvidence
	truncated          bool
	overflow           bool
}

func newEvidenceCollector(budget EvidenceBudget) *evidenceCollector {
	return &evidenceCollector{budget: normalizeEvidenceBudget(budget), stream: sha256.New(), byClass: make(map[FrameClass]FrameClassStats), measuredByClass: make(map[FrameClass]FrameClassStats)}
}

func (c *evidenceCollector) beginMeasured(at time.Time) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.measuredStartedAt = at
	c.measuredFinishedAt = time.Time{}
	c.measuredByClass = make(map[FrameClass]FrameClassStats)
}

func (c *evidenceCollector) endMeasured(at time.Time) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.measuredFinishedAt = at
}

// record accounts for one received frame. It intentionally hashes payload
// bytes in place and retains only a bounded digest/metadata sample.
func (c *evidenceCollector) record(ev SessionEvent, clientID, queryID, recipientID, protocolErr string) {
	if c == nil {
		return
	}
	payloadBytes := int64(len(ev.Payload))
	payloadDigest := sha256.Sum256(ev.Payload)
	class := classifyFrame(ev, protocolErr)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.frames < int64(^uint64(0)>>1) {
		c.frames++
	} else {
		c.overflow = true
	}
	if payloadBytes > (int64(^uint64(0)>>1) - c.bytes) {
		c.bytes = int64(^uint64(0) >> 1)
		c.overflow = true
	} else {
		c.bytes += payloadBytes
	}
	classStats := c.byClass[class]
	classStats.Frames++
	classStats.Bytes += payloadBytes
	c.byClass[class] = classStats
	// Classification is based on the target-observed event time rather than
	// reader processing time. This excludes a queued pre-boundary frame that
	// is consumed later and includes an in-window frame consumed during grace.
	// A missing event timestamp is never assigned the local processing time.
	if !ev.At.IsZero() && !c.measuredStartedAt.IsZero() &&
		!ev.At.Before(c.measuredStartedAt) &&
		(c.measuredFinishedAt.IsZero() || !ev.At.After(c.measuredFinishedAt)) {
		measuredStats := c.measuredByClass[class]
		measuredStats.Frames++
		measuredStats.Bytes += payloadBytes
		c.measuredByClass[class] = measuredStats
	}
	writeEvidenceToken(c.stream, clientID)
	writeEvidenceToken(c.stream, queryID)
	writeEvidenceToken(c.stream, recipientID)
	writeEvidenceToken(c.stream, ev.Op)
	writeEvidenceToken(c.stream, ev.ClientEventID)
	writeEvidenceToken(c.stream, ev.ServerTransactionID)
	writeEvidenceToken(c.stream, ev.ProcessedTransactionID)
	writeEvidenceToken(c.stream, hex.EncodeToString(payloadDigest[:]))
	writeEvidenceToken(c.stream, protocolErr)

	if c.frames > c.budget.MaxFrames || c.bytes > c.budget.MaxBytes {
		c.overflow = true
		return
	}
	if len(c.samples) >= c.budget.MaxRetainedFrames {
		c.truncated = true
		return
	}
	c.samples = append(c.samples, RawFrameEvidence{
		ClientID:               boundEvidenceText(clientID, 128),
		QueryID:                boundEvidenceText(queryID, 128),
		RecipientID:            boundEvidenceText(recipientID, 128),
		Op:                     boundEvidenceText(ev.Op, 64),
		Class:                  class,
		ClientEventID:          boundEvidenceText(ev.ClientEventID, 128),
		ServerTransactionID:    boundEvidenceText(ev.ServerTransactionID, 128),
		ProcessedTransactionID: boundEvidenceText(ev.ProcessedTransactionID, 128),
		At:                     ev.At,
		PayloadBytes:           payloadBytes,
		PayloadDigest:          hex.EncodeToString(payloadDigest[:]),
		ProtocolError:          boundEvidenceText(protocolErr, 256),
	})
}

func writeEvidenceToken(h hash.Hash, value string) {
	_, _ = io.WriteString(h, strconv.Itoa(len(value)))
	_, _ = io.WriteString(h, ":")
	_, _ = io.WriteString(h, value)
}

func boundEvidenceText(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[:max]
}

func (c *evidenceCollector) snapshot() ([]RawFrameEvidence, EvidenceStats) {
	if c == nil {
		return nil, EvidenceStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	samples := append([]RawFrameEvidence(nil), c.samples...)
	byClass := make(map[FrameClass]FrameClassStats, len(c.byClass))
	for class, stats := range c.byClass {
		byClass[class] = stats
	}
	measuredByClass := make(map[FrameClass]FrameClassStats, len(c.measuredByClass))
	for class, stats := range c.measuredByClass {
		measuredByClass[class] = stats
	}
	digest := hex.EncodeToString(c.stream.Sum(nil))
	return samples, EvidenceStats{ObservedFrames: c.frames, ObservedBytes: c.bytes, RetainedFrames: len(samples), MaxFrames: c.budget.MaxFrames, MaxBytes: c.budget.MaxBytes, MaxRetainedFrames: c.budget.MaxRetainedFrames, Truncated: c.truncated, Overflow: c.overflow, StreamDigest: digest, ByClass: byClass, MeasuredByClass: measuredByClass, MeasuredStartedAt: c.measuredStartedAt, MeasuredFinishedAt: c.measuredFinishedAt}
}

func classifyFrame(ev SessionEvent, protocolErr string) FrameClass {
	if protocolErr != "" || ev.Op == "error" || ev.Op == "protocol-error" {
		return FrameProtocolError
	}
	switch ev.Op {
	case "refresh-ok", "refresh-ok-delta":
		return FrameApplicationRefresh
	case "init-ok":
		return FrameSessionInit
	case "add-query-ok", "add-query-exists", "remove-query-ok":
		return FrameQueryLifecycle
	case "transact-ok":
		return FrameTransactionAck
	default:
		return FrameOther
	}
}
