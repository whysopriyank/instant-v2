package benchharness

// This file contains the narrow target-facing adapter used by the Wave 6
// runner. It owns transport/session semantics, but deliberately does not own
// pair ordering, artifact serialization, or statistics aggregation.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type TargetKind string

const (
	TargetV1 TargetKind = "v1"
	TargetV2 TargetKind = "v2"
)

type TargetTransport string

const (
	TransportWebSocket TargetTransport = "websocket"
	TransportSSE       TargetTransport = "sse"
)

// TargetMetadata is independently collected target evidence. A benchmark
// claim must not rely on a value copied from the command line: MetadataProbe
// should query the target/control plane or the connected benchmark database.
type TargetMetadata struct {
	Revision          string
	DirtyTreeHash     string
	DatabaseName      string
	PostgresVersion   string
	InvalidationMode  string
	OutputPlugin      string
	IdentityAttribute IdentityAttributeMetadata
}

// IdentityAttributeMetadata is read-only catalog evidence for the canonical
// fixture identity attribute. It is populated by the live DB probe and is
// compared against the signed target contract before qualification.
type IdentityAttributeMetadata struct {
	ID          string
	EntityType  string
	Label       string
	ValueType   string
	Cardinality string
	Unique      bool
	Indexed     bool
	Required    bool
	Primary     bool
	Identity    bool
}

type QualificationCheck struct {
	Passed  bool
	Details string
}

// TargetQualification retains failed evidence instead of collapsing it to a
// zero-delivery result. Unsupported marks a missing, unobservable prerequisite
// (for example, no metadata or semantic decoder), which is distinct from a
// target actively failing a protocol or health check.
type TargetQualification struct {
	TargetID         string
	Kind             TargetKind
	Transport        TargetTransport
	Revision         string
	DatabaseName     string
	PostgresVersion  string
	InvalidationMode string
	OutputPlugin     string
	Checks           map[string]QualificationCheck
	Passed           bool
	Unsupported      bool
	Failure          string
	StartedAt        time.Time
	FinishedAt       time.Time
}

// LiveRefreshProbe is a four-subscriber semantic preflight. Query and
// transaction payloads are built by the target adapter, while the wire
// session itself always uses the existing init/add-query/transact protocol.
type LiveRefreshProbe struct {
	Query    Query
	Mutation Mutation
	Timeout  time.Duration
	Validate func(initial, refreshed Refresh) error
}

// NewFourClientSemanticProbe freezes the qualification topology used by the
// contract. The caller supplies the frozen fixture query/mutation and a
// semantic validator; the driver always executes it with four independent
// sessions before a target is claim-eligible.
func NewFourClientSemanticProbe(query Query, mutation Mutation, validate func(initial, refreshed Refresh) error) LiveRefreshProbe {
	return LiveRefreshProbe{Query: query, Mutation: mutation, Timeout: 20 * time.Second, Validate: validate}
}

// EvidenceBudget bounds run-level frame accounting. Payload bytes are counted
// and hashed as they arrive, but payloads are never retained. Zero values use
// the contract defaults; MaxRetainedFrames bounds the in-memory metadata
// sample and does not stop accounting.
type EvidenceBudget struct {
	MaxFrames         int64
	MaxBytes          int64
	MaxRetainedFrames int
}

const (
	defaultEvidenceMaxFrames         int64 = 10_000_000
	defaultEvidenceMaxBytes          int64 = 8 << 30
	defaultEvidenceMaxRetainedFrames       = 8192
	// SSE permits a data line up to 16 MiB; using that shared transport bound
	// keeps the derived cumulative budget valid for either live transport.
	maxEvidenceFrameBytes int64 = 16 << 20
	// With the canonical 20-second semantic-readiness timeout, the exponential
	// backoff permits at most 84 checks even if each check itself is immediate.
	maxEvidenceReadinessAttempts = 84
	// SSE emits a transport handshake before the protocol init acknowledgement;
	// WebSocket has no larger setup shape. The shared bound therefore charges
	// two setup frames for every session, regardless of transport.
	evidenceSessionSetupFrames int64 = 2
	// A subscription costs one add-query acknowledgement and one full refresh.
	evidenceQuerySnapshotFrames int64 = 2
	// Qualification opens four clients, subscribes each, and observes one
	// post-transaction refresh per client plus one transaction acknowledgement.
	// 4*(2 setup + 2 subscription) + 4 refresh + 1 ack = 21.
	evidenceQualificationFrames int64 = 21
	evidenceWriterSessions            = 8
	// Keep a small fixed allowance for handshake/control variants while the
	// lifecycle-specific terms account for every known bounded operation.
	evidenceLifecycleControlFrames int64 = 128
)

// MaxAbsoluteEvidenceBytes is the hard ceiling for a derived cumulative wire
// evidence budget. The counter/digest path does not retain payloads, but the
// ceiling keeps malformed workload inputs from turning a run into an
// effectively unbounded accounting operation.
const MaxAbsoluteEvidenceBytes int64 = 64 << 40

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

// TargetConfig is the complete explicit target contract. Required fields are
// intentionally redundant with environment/provisioner state so the adapter
// can reject an unproven run before it emits measurements.
//
// QueryBuilder must return an InstaQL object for add-query's q field.
// TransactionBuilder must return the existing tx-steps array (not a made-up
// HTTP transaction format). DecodeRefresh may be nil only when the target-kind
// qualified wire decoder is sufficient: V1 uses its legacy refresh envelope,
// while V2 uses the standard computations[].instaql-query/result decoder.
// Provisioner and MetadataProbe are required for Prepare/Qualify; they are
// callbacks because process/database orchestration is outside this package's
// ownership.
type TargetConfig struct {
	ID        string
	Kind      TargetKind
	Transport TargetTransport

	SessionURL   string // ws(s)://.../runtime/session or http(s)://.../runtime/sse
	HealthURL    string // http(s)://.../health
	AdminBaseURL string // optional; if present, route existence is checked
	AppID        string
	AdminToken   string
	RefreshToken string
	Versions     map[string]string
	Headers      http.Header
	// AttributeAliases maps semantic oracle names (id, value, bucket, rank) to
	// their wire attribute labels/UUIDs. Label-shaped results work with the
	// empty map; UUID-shaped results require the explicit aliases supplied by
	// the live-config adapter.
	AttributeAliases map[string]string

	Revision          string
	DirtyTreeHash     string
	DatabaseName      string
	PostgresVersion   string
	InvalidationMode  string
	OutputPlugin      string
	IdentityAttribute IdentityAttributeMetadata

	MetadataProbe      func(context.Context) (TargetMetadata, error)
	Provisioner        func(context.Context) error
	QueryBuilder       func(Query) (any, error)
	TransactionBuilder func(Mutation) ([]any, error)
	DecodeRefresh      func(SessionEvent, string) (Refresh, error)
	DecodeReceipt      func(SessionEvent, string) ([]Receipt, error)
	Probe              LiveRefreshProbe

	// Dialer is a test seam and a place for a caller to inject a transport
	// instrumenter. Production callers should leave it nil.
	Dialer     SessionDialer
	HTTPClient *http.Client
	// EvidenceBudget applies cumulatively to all sessions opened for one Run.
	// OpenSession uses the defaults because it has no run-level owner.
	EvidenceBudget EvidenceBudget
}

// UnsupportedTargetError is returned when the adapter cannot prove a
// required qualification or semantic observation. Callers should retain the
// qualification record and classify the run as unsupported/setup_invalid.
type UnsupportedTargetError struct {
	Check  string
	Reason string
}

func (e *UnsupportedTargetError) Error() string {
	if e == nil {
		return "target capability is unsupported"
	}
	return fmt.Sprintf("target capability unsupported (%s): %s", e.Check, e.Reason)
}

// TargetRunArtifacts are normalized harness outputs. The benchrun adapter can
// map Result.Ledger rows and these stable fields into its versioned artifact
// schema without importing target or product packages.
type TargetRunArtifacts struct {
	TargetID           string
	RunID              string
	PairID             string
	Family             Family
	Subscribers        int
	InitialSnapshots   int
	Qualification      TargetQualification
	Result             RunResult
	RawFrames          []RawFrameEvidence
	ProtocolErrors     []string
	BehaviorErrors     []string
	Evidence           EvidenceStats
	ExpectedMutations  int
	ExpectedRows       int
	MeasuredStartedAt  time.Time
	MeasuredFinishedAt time.Time
	RampDuration       time.Duration
	SettleDuration     time.Duration
	WarmupDuration     time.Duration
	WarmupMutations    int
	StartedAt          time.Time
	FinishedAt         time.Time
}

func (a TargetRunArtifacts) CoverageCounts() map[Coverage]int {
	out := map[Coverage]int{}
	if a.Result.Ledger == nil {
		return out
	}
	for _, row := range a.Result.Ledger.Rows() {
		out[row.Coverage]++
	}
	return out
}

type TargetDriver struct {
	cfg    TargetConfig
	client *http.Client
}

func NewTargetDriver(cfg TargetConfig) (*TargetDriver, error) {
	if err := validateTargetConfig(cfg); err != nil {
		return nil, err
	}
	client := cfg.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	return &TargetDriver{cfg: cfg, client: client}, nil
}

func validateTargetConfig(cfg TargetConfig) error {
	if err := validateEvidenceBudget(cfg.EvidenceBudget); err != nil {
		return err
	}
	if err := validateReadinessTimeout(cfg.Probe.Timeout); err != nil {
		return err
	}
	if cfg.ID == "" {
		return fmt.Errorf("target id is required")
	}
	if cfg.Kind != TargetV1 && cfg.Kind != TargetV2 {
		return fmt.Errorf("unsupported target kind %q", cfg.Kind)
	}
	if cfg.Transport != TransportWebSocket && cfg.Transport != TransportSSE {
		return fmt.Errorf("unsupported target transport %q", cfg.Transport)
	}
	if err := validateProtocolEndpoint(cfg.SessionURL, cfg.Transport); err != nil {
		return err
	}
	if err := validateHTTPURL(cfg.HealthURL); err != nil {
		return fmt.Errorf("health endpoint: %w", err)
	}
	if err := ValidateLoopbackURL(cfg.SessionURL); err != nil {
		return fmt.Errorf("session endpoint: %w", err)
	}
	if err := ValidateLoopbackURL(cfg.HealthURL); err != nil {
		return fmt.Errorf("health endpoint: %w", err)
	}
	if !looksLikeUUID(cfg.AppID) {
		return fmt.Errorf("app id must be a UUID")
	}
	if cfg.Revision == "" {
		return fmt.Errorf("target revision is required")
	}
	if cfg.DatabaseName == "" || !strings.HasPrefix(cfg.DatabaseName, "instant_bench_") {
		return fmt.Errorf("database name must begin with instant_bench_")
	}
	if cfg.PostgresVersion == "" {
		return fmt.Errorf("PostgreSQL version is required")
	}
	if cfg.InvalidationMode == "" {
		return fmt.Errorf("invalidation mode is required")
	}
	if cfg.Kind == TargetV1 && !strings.EqualFold(cfg.OutputPlugin, "wal2json") {
		return fmt.Errorf("V1 requires output plugin wal2json")
	}
	return nil
}

// validateReadinessTimeout keeps the signed/live target configuration within
// the canonical readiness contract. A non-positive value retains the legacy
// default fallback in waitForTargetReadiness; only an explicit timeout above
// the bounded contract is rejected.
func validateReadinessTimeout(timeout time.Duration) error {
	if timeout > defaultSemanticReadinessTimeout {
		return fmt.Errorf("readiness timeout %s exceeds canonical maximum %s", timeout, defaultSemanticReadinessTimeout)
	}
	return nil
}

func validateEvidenceBudget(b EvidenceBudget) error {
	if b.MaxFrames < 0 || b.MaxFrames > defaultEvidenceMaxFrames {
		return fmt.Errorf("evidence frame budget exceeds hard ceiling")
	}
	if b.MaxBytes < 0 || b.MaxBytes > MaxAbsoluteEvidenceBytes {
		return fmt.Errorf("evidence byte budget exceeds hard ceiling")
	}
	if b.MaxRetainedFrames < 0 || b.MaxRetainedFrames > defaultEvidenceMaxRetainedFrames {
		return fmt.Errorf("retained evidence frame budget exceeds hard ceiling")
	}
	return nil
}

func validateProtocolEndpoint(raw string, transport TargetTransport) error {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return fmt.Errorf("invalid session endpoint %q", raw)
	}
	if transport == TransportWebSocket && u.Scheme != "ws" && u.Scheme != "wss" {
		return fmt.Errorf("websocket target requires ws or wss URL")
	}
	if transport == TransportSSE && u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("SSE target requires http or https URL")
	}
	return nil
}

func validateHTTPURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("expected http(s) URL")
	}
	return nil
}

func looksLikeUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range strings.ToLower(s) {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
			continue
		}
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func (d *TargetDriver) Provision(ctx context.Context) error {
	if d == nil {
		return fmt.Errorf("nil target driver")
	}
	if d.cfg.Provisioner == nil {
		return &UnsupportedTargetError{Check: "provision", Reason: "no target provisioner/seed callback configured"}
	}
	return d.cfg.Provisioner(ctx)
}

const defaultSemanticReadinessTimeout = 20 * time.Second

// waitForSemanticReadiness retries only after an observed semantic failure.
// The bounded backoff avoids hammering a target that is still completing its
// provision/reset work, while the caller's context and timeout keep the
// readiness barrier finite. A successful check is the only completion signal;
// this is not a fixed sleep masquerading as readiness.
func waitForSemanticReadiness(ctx context.Context, timeout time.Duration, check func(context.Context) error) error {
	if check == nil {
		return errors.New("semantic readiness check is not configured")
	}
	if timeout <= 0 {
		timeout = defaultSemanticReadinessTimeout
	}
	checkCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var lastErr error
	for attempt := 0; ; attempt++ {
		lastErr = check(checkCtx)
		if lastErr == nil {
			return nil
		}
		if err := checkCtx.Err(); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("semantic readiness deadline after %s: %w", timeout, lastErr)
		}
		delay := 10 * time.Millisecond
		for i := 0; i < attempt && delay < 250*time.Millisecond; i++ {
			delay *= 2
		}
		if delay > 250*time.Millisecond {
			delay = 250 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-checkCtx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("semantic readiness deadline after %s: %w", timeout, lastErr)
		case <-timer.C:
		}
	}
}

// waitForTargetReadiness proves both independently observed metadata and the
// semantic fixture state before a run advances to qualification or ramp.
// verifyCleanFixture specifically rejects a lingering qualification probe and
// compares every frozen query against the prefix-zero oracle.
func (d *TargetDriver) waitForTargetReadiness(ctx context.Context, fixture Fixture, runID string, evidence *evidenceCollector) error {
	timeout := d.cfg.Probe.Timeout
	if timeout <= 0 {
		timeout = defaultSemanticReadinessTimeout
	}
	var attempt int
	return waitForSemanticReadiness(ctx, timeout, func(checkCtx context.Context) error {
		attempt++
		if err := d.VerifyMetadata(checkCtx); err != nil {
			return fmt.Errorf("metadata not ready: %w", err)
		}
		if err := d.verifyCleanFixture(checkCtx, fixture, fmt.Sprintf("%s-%d", runID, attempt), evidence); err != nil {
			return err
		}
		return nil
	})
}

// ValidateBehaviorWindow prevents a direct RunPlan from silently extending
// the bounded S/R workload beyond the canonical duration used by the signed
// contract. Shorter windows remain valid and are reflected in their derived
// reconnect allowance; non-behavior families ignore the field.
func ValidateBehaviorWindow(w Workload, behaviorSeconds int) error {
	if behaviorSeconds <= 0 || (w.Family != FamilyS && w.Family != FamilyR) {
		return nil
	}
	if w.DurationSeconds <= 0 {
		return fmt.Errorf("behavior window %ds has no canonical workload duration", behaviorSeconds)
	}
	if behaviorSeconds > w.DurationSeconds {
		return fmt.Errorf("behavior window %ds exceeds canonical workload duration %ds", behaviorSeconds, w.DurationSeconds)
	}
	return nil
}

// Prepare provisions/seeds the target through the caller-owned callback and
// then runs the independent qualification gate. It does not reset anything;
// destructive database setup remains in soaksetup's guarded owner.
func (d *TargetDriver) Prepare(ctx context.Context) (TargetQualification, error) {
	if err := d.Provision(ctx); err != nil {
		return TargetQualification{}, err
	}
	return d.Qualify(ctx)
}

func (d *TargetDriver) Qualify(ctx context.Context) (q TargetQualification, retErr error) {
	return d.qualify(ctx, nil)
}

// VerifyMetadata performs only the non-mutating metadata/identity check used
// after a qualification probe has been cleaned up. It deliberately does not
// open sessions or execute the live refresh probe.
func (d *TargetDriver) VerifyMetadata(ctx context.Context) error {
	if d == nil {
		return fmt.Errorf("nil target driver")
	}
	if d.cfg.MetadataProbe == nil {
		return &UnsupportedTargetError{Check: "metadata", Reason: "no independent metadata probe configured"}
	}
	metadata, err := d.cfg.MetadataProbe(ctx)
	if err != nil {
		return err
	}
	return compareMetadata(d.cfg, metadata, func(string, bool, string) {})
}

func (d *TargetDriver) qualify(ctx context.Context, evidence *evidenceCollector) (q TargetQualification, retErr error) {
	q = TargetQualification{TargetID: d.cfg.ID, Kind: d.cfg.Kind, Transport: d.cfg.Transport, Revision: d.cfg.Revision, DatabaseName: d.cfg.DatabaseName, PostgresVersion: d.cfg.PostgresVersion, InvalidationMode: d.cfg.InvalidationMode, OutputPlugin: d.cfg.OutputPlugin, Checks: map[string]QualificationCheck{}, StartedAt: time.Now()}
	defer func() { q.FinishedAt = time.Now() }()
	mark := func(name string, passed bool, detail string) {
		q.Checks[name] = QualificationCheck{Passed: passed, Details: detail}
		if !passed && q.Failure == "" {
			q.Failure = name + ": " + detail
		}
	}
	if err := validateTargetConfig(d.cfg); err != nil {
		mark("config", false, err.Error())
		return q, err
	}
	if err := d.checkHealth(ctx); err != nil {
		mark("health", false, err.Error())
		return q, err
	}
	mark("health", true, "target health endpoint returned ready")
	if d.cfg.MetadataProbe == nil {
		detail := "no independent metadata probe configured"
		mark("metadata", false, detail)
		q.Unsupported = true
		err := &UnsupportedTargetError{Check: "metadata", Reason: detail}
		return q, err
	}
	metadata, err := d.cfg.MetadataProbe(ctx)
	if err != nil {
		mark("metadata", false, err.Error())
		return q, err
	}
	if err := compareMetadata(d.cfg, metadata, mark); err != nil {
		return q, err
	}
	if d.cfg.AdminBaseURL != "" {
		if err := d.checkAdminRoutes(ctx, mark); err != nil {
			return q, err
		}
	} else {
		mark("admin_routes", true, "not applicable: no admin control base configured")
	}
	if d.cfg.Probe.Validate == nil || d.cfg.QueryBuilder == nil || d.cfg.TransactionBuilder == nil {
		detail := "four-subscriber live-refresh probe requires query, transaction, and semantic validation callbacks"
		mark("live_refresh", false, detail)
		q.Unsupported = true
		err := &UnsupportedTargetError{Check: "live_refresh", Reason: detail}
		return q, err
	}
	if err := d.liveRefreshProbe(ctx, d.cfg.Probe, evidence); err != nil {
		mark("live_refresh", false, err.Error())
		return q, err
	}
	mark("live_refresh", true, "four subscribers observed a validated post-transaction refresh")
	q.Passed = true
	return q, nil
}

func compareMetadata(cfg TargetConfig, got TargetMetadata, mark func(string, bool, string)) error {
	checks := []struct {
		name, want, actual string
	}{
		{"revision", cfg.Revision, got.Revision},
		{"database", cfg.DatabaseName, got.DatabaseName},
		{"postgres_version", cfg.PostgresVersion, got.PostgresVersion},
		{"invalidation_mode", cfg.InvalidationMode, got.InvalidationMode},
	}
	if cfg.Kind == TargetV1 {
		checks = append(checks, struct{ name, want, actual string }{"output_plugin", "wal2json", got.OutputPlugin})
	}
	for _, check := range checks {
		if check.actual == "" {
			mark(check.name, false, "metadata probe returned no value")
			return &UnsupportedTargetError{Check: check.name, Reason: "independent target evidence is unavailable"}
		}
		if check.actual != check.want {
			mark(check.name, false, fmt.Sprintf("configured %q, observed %q", check.want, check.actual))
			return fmt.Errorf("target %s mismatch: configured %q, observed %q", check.name, check.want, check.actual)
		}
		mark(check.name, true, check.actual)
	}
	if cfg.IdentityAttribute.ID != "" {
		if err := compareIdentityAttribute(cfg.IdentityAttribute, got.IdentityAttribute, mark); err != nil {
			return err
		}
	}
	if cfg.DirtyTreeHash != "" || got.DirtyTreeHash != "" {
		mark("clean_revision", false, "dirty product tree is not claim-eligible")
		return fmt.Errorf("target revision is dirty")
	}
	mark("clean_revision", true, "clean revision evidence")
	return nil
}

func compareIdentityAttribute(want, got IdentityAttributeMetadata, mark func(string, bool, string)) error {
	if got.ID == "" {
		mark("identity_attribute", false, "metadata probe returned no identity attribute")
		return &UnsupportedTargetError{Check: "identity_attribute", Reason: "independent identity catalog evidence is unavailable"}
	}
	stringsChecks := []struct {
		name, want, actual string
	}{
		{"identity_attribute_id", want.ID, got.ID},
		{"identity_attribute_entity_type", want.EntityType, got.EntityType},
		{"identity_attribute_label", want.Label, got.Label},
		{"identity_attribute_value_type", want.ValueType, got.ValueType},
		{"identity_attribute_cardinality", want.Cardinality, got.Cardinality},
	}
	for _, check := range stringsChecks {
		if check.actual != check.want {
			mark("identity_attribute", false, fmt.Sprintf("%s configured %q, observed %q", check.name, check.want, check.actual))
			return fmt.Errorf("identity attribute %s mismatch: configured %q, observed %q", check.name, check.want, check.actual)
		}
	}
	boolChecks := []struct {
		name         string
		want, actual bool
	}{
		{"unique", want.Unique, got.Unique},
		{"indexed", want.Indexed, got.Indexed},
		{"required", want.Required, got.Required},
		{"primary", want.Primary, got.Primary},
		{"identity", want.Identity, got.Identity},
	}
	for _, check := range boolChecks {
		if check.actual != check.want {
			mark("identity_attribute", false, fmt.Sprintf("identity attribute %s configured %t, observed %t", check.name, check.want, check.actual))
			return fmt.Errorf("identity attribute %s mismatch: configured %t, observed %t", check.name, check.want, check.actual)
		}
	}
	mark("identity_attribute", true, "catalog identity metadata matches signed fixture contract")
	return nil
}

func (d *TargetDriver) checkHealth(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.cfg.HealthURL, nil)
	if err != nil {
		return err
	}
	req.Header = d.cfg.Headers.Clone()
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("health status %s", resp.Status)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return err
	}
	var body struct {
		OK *bool `json:"ok"`
	}
	if len(bytes.TrimSpace(b)) > 0 && json.Unmarshal(b, &body) == nil && body.OK != nil && !*body.OK {
		return fmt.Errorf("health endpoint reported not ready")
	}
	return nil
}

func (d *TargetDriver) checkAdminRoutes(ctx context.Context, mark func(string, bool, string)) error {
	for _, route := range []string{"/query", "/transact", "/subscribe-query"} {
		routeURL, err := adminRouteURL(d.cfg.AdminBaseURL, route)
		if err != nil {
			mark("admin_routes", false, err.Error())
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, routeURL, strings.NewReader(`{}`))
		if err != nil {
			mark("admin_routes", false, err.Error())
			return err
		}
		req.Header = d.cfg.Headers.Clone()
		if req.Header == nil {
			req.Header = make(http.Header)
		}
		req.Header.Set("Content-Type", "application/json")
		if d.cfg.AdminToken != "" {
			req.Header.Set("Authorization", "Bearer "+d.cfg.AdminToken)
		}
		resp, err := d.client.Do(req)
		if err != nil {
			mark("admin_routes", false, err.Error())
			return err
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			mark("admin_routes", false, route+" returned 404")
			return fmt.Errorf("admin route %s is unavailable", route)
		}
	}
	mark("admin_routes", true, "query/transact/subscribe-query routes responded")
	return nil
}

func adminRouteURL(base, route string) (string, error) {
	u, err := url.Parse(base)
	if err != nil || u.Hostname() == "" {
		return "", fmt.Errorf("invalid admin base URL %q", base)
	}
	path := strings.TrimRight(u.Path, "/")
	if path == "" {
		path = "/admin"
	} else if path != "/admin" && !strings.HasSuffix(path, "/admin") {
		path += "/admin"
	}
	u.Path = path + route
	u.RawPath = ""
	return u.String(), nil
}

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
	opts := SessionOptions{URL: d.cfg.SessionURL, AppID: d.cfg.AppID, ClientID: clientID, Headers: d.cfg.Headers.Clone(), MachineID: "", SessionID: "", SSEToken: ""}
	var sess Session
	var err error
	if d.cfg.Dialer != nil {
		sess, err = d.cfg.Dialer.Dial(ctx, opts)
	} else if d.cfg.Transport == TransportWebSocket {
		sess, err = DialWebSocket(ctx, opts)
	} else {
		sess, err = DialSSE(ctx, opts)
	}
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
	opts := SessionOptions{URL: s.driver.cfg.SessionURL, AppID: s.driver.cfg.AppID, ClientID: s.clientID, Headers: s.driver.cfg.Headers.Clone()}
	var next Session
	var err error
	if s.driver.cfg.Dialer != nil {
		next, err = s.driver.cfg.Dialer.Dial(ctx, opts)
	} else if s.driver.cfg.Transport == TransportWebSocket {
		next, err = DialWebSocket(ctx, opts)
	} else {
		next, err = DialSSE(ctx, opts)
	}
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

func (d *TargetDriver) liveRefreshProbe(ctx context.Context, probe LiveRefreshProbe, evidence *evidenceCollector) error {
	timeout := probe.Timeout
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	pctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	clients := make([]*TargetSession, 0, 4)
	var initial Refresh
	for i := 0; i < 4; i++ {
		client, err := d.openSession(pctx, fmt.Sprintf("qualification-%d", i), evidence)
		if err != nil {
			for _, opened := range clients {
				_ = opened.Close()
			}
			return err
		}
		refresh, err := client.Subscribe(pctx, probe.Query)
		if err != nil {
			_ = client.Close()
			for _, opened := range clients {
				_ = opened.Close()
			}
			return err
		}
		if i == 0 {
			initial = refresh
		}
		clients = append(clients, client)
	}
	defer func() {
		for _, client := range clients {
			_ = client.Close()
		}
	}()
	ack, err := clients[0].Transact(pctx, probe.Mutation)
	if err != nil {
		return err
	}
	if _, ok := saturationOrder(ack); !ok {
		return &UnsupportedTargetError{Check: "transaction_order", Reason: "target did not expose a numeric server tx-id or processed-tx-id"}
	}
	for _, client := range clients {
		for {
			ev, err := client.await(pctx, func(ev SessionEvent) bool { return ev.Op == "refresh-ok" || ev.Op == "refresh-ok-delta" })
			if err != nil {
				return err
			}
			ref, err := client.decodeRefresh(ev, probe.Query.ID)
			if err != nil {
				return err
			}
			if ref.Kind == RefreshNoop {
				continue
			}
			if err := probe.Validate(initial, ref); err != nil {
				return err
			}
			break
		}
	}
	return nil
}

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

func normalizeEvidenceBudget(b EvidenceBudget) EvidenceBudget {
	if b.MaxFrames <= 0 {
		b.MaxFrames = defaultEvidenceMaxFrames
	}
	if b.MaxBytes <= 0 {
		b.MaxBytes = defaultEvidenceMaxBytes
	}
	if b.MaxRetainedFrames <= 0 {
		b.MaxRetainedFrames = defaultEvidenceMaxRetainedFrames
	}
	return b
}

// EvidenceBudgetForWorkload derives the cumulative live evidence budget for a
// frozen workload. Payload bytes are accounted per received frame, so the
// cumulative bound is the conservative transport frame bound multiplied by a
// workload-derived recipient cardinality plus a bounded setup allowance.
//
// This function is intentionally independent of observed rows or bytes. A
// short/partial run must remain a failed attempt and must never enlarge its
// own budget. Callers that provide any non-zero operator EvidenceBudget keep
// that cap; this derived path is used only for an entirely zero budget.
func EvidenceBudgetForWorkload(w Workload, mutations int) (EvidenceBudget, error) {
	return evidenceBudgetForMutationCount(w, mutations, true)
}

// EvidenceBudgetForWorkloadWithWarmup includes warm-up mutations in the
// cumulative bound. Warm-up traffic is outside the ledger denominator but is
// still accounted by the run-level collector.
func EvidenceBudgetForWorkloadWithWarmup(w Workload, mutations, warmupMutations int) (EvidenceBudget, error) {
	if warmupMutations < 0 {
		return EvidenceBudget{}, errors.New("warm-up mutation count cannot be negative")
	}
	measuredMutations, err := maxEvidenceMutations(w, mutations)
	if err != nil {
		return EvidenceBudget{}, err
	}
	total, err := safeEvidenceAdd(int64(measuredMutations), int64(warmupMutations))
	if err != nil || total > int64(^uint(0)>>1) {
		return EvidenceBudget{}, errors.New("derived evidence mutation count overflow")
	}
	return evidenceBudgetForMutationCount(w, int(total), false)
}

func maxEvidenceMutations(w Workload, mutations int) (int, error) {
	if mutations <= 0 {
		mutations = w.Measured
	}
	if w.Family == FamilyT {
		if mutations <= 0 {
			return 4096, nil
		}
		if mutations > 4096 {
			return 4096, nil
		}
	}
	if mutations <= 0 && w.DurationSeconds > 0 && w.TxRate > 0 {
		rate := int64(w.TxRate)
		if float64(rate) != w.TxRate {
			return 0, errors.New("evidence budget requires an integral workload rate")
		}
		total, err := safeEvidenceMultiply(int64(w.DurationSeconds), rate)
		if err != nil || total > int64(^uint(0)>>1) {
			return 0, errors.New("derived evidence mutation count overflow")
		}
		return int(total), nil
	}
	if mutations <= 0 {
		return 1, nil
	}
	return mutations, nil
}

func evidenceBudgetForMutationCount(w Workload, mutations int, clampSaturation bool) (EvidenceBudget, error) {
	queryCount := len(w.Fixture.Queries)
	if w.Subscribers <= 0 || queryCount == 0 || w.Subscribers != queryCount {
		return EvidenceBudget{}, errors.New("evidence budget requires one query per subscriber")
	}
	if mutations <= 0 {
		mutations = w.Measured
	}
	if mutations <= 0 && w.DurationSeconds > 0 && w.TxRate > 0 {
		rate := int64(w.TxRate)
		if float64(rate) != w.TxRate {
			return EvidenceBudget{}, errors.New("evidence budget requires an integral workload rate")
		}
		derived, err := safeEvidenceMultiply(int64(w.DurationSeconds), rate)
		if err != nil || derived > int64(^uint(0)>>1) {
			return EvidenceBudget{}, errors.New("derived evidence mutation count overflow")
		}
		mutations = int(derived)
	}
	if w.Family == FamilyT {
		if mutations <= 0 {
			mutations = 4096
		}
		if clampSaturation && mutations > 4096 {
			mutations = 4096
		}
	}
	if mutations <= 0 {
		mutations = 1
	}

	perMutation, err := evidenceRecipientUpperBound(w, queryCount)
	if err != nil {
		return EvidenceBudget{}, err
	}
	refreshFrames, err := safeEvidenceMultiply(int64(mutations), perMutation)
	if err != nil {
		return EvidenceBudget{}, err
	}
	lifecycleFrames, err := evidenceLifecycleFrameAllowance(w, queryCount, mutations)
	if err != nil {
		return EvidenceBudget{}, err
	}
	frameBudget, err := safeEvidenceAdd(refreshFrames, lifecycleFrames)
	if err != nil {
		return EvidenceBudget{}, err
	}
	bytes, err := safeEvidenceMultiply(frameBudget, maxEvidenceFrameBytes)
	if err != nil || bytes > MaxAbsoluteEvidenceBytes {
		return EvidenceBudget{}, errors.New("derived evidence budget exceeds hard ceiling")
	}
	if bytes < defaultEvidenceMaxBytes {
		bytes = defaultEvidenceMaxBytes
	}
	return EvidenceBudget{MaxFrames: defaultEvidenceMaxFrames, MaxBytes: bytes, MaxRetainedFrames: defaultEvidenceMaxRetainedFrames}, nil
}

func evidenceRecipientUpperBound(w Workload, queryCount int) (int64, error) {
	if queryCount <= 0 {
		return 0, errors.New("evidence budget requires at least one query")
	}
	// X/C/T assign each mutation to one deterministic bucket. Only canonical
	// fixtures prove that the queries are evenly distributed; a custom fixture
	// must use the all-query upper bound rather than inheriting that assumption.
	if canonicalBucketOnlyFixture(w, queryCount) {
		perBucket := queryCount / w.Cohorts
		if queryCount%w.Cohorts != 0 {
			perBucket++
		}
		if perBucket > 0 {
			return int64(perBucket), nil
		}
	}
	return int64(queryCount), nil
}

func canonicalBucketOnlyFixture(w Workload, queryCount int) bool {
	if (w.Family != FamilyX && w.Family != FamilyC && w.Family != FamilyT) || w.Cohorts <= 0 || len(w.Fixture.Queries) != queryCount {
		return false
	}
	for i, query := range w.Fixture.Queries {
		if query.ID != fmt.Sprintf("q-%06d", i) || query.MatchAll || query.TopN != 0 || query.Bucket != i%w.Cohorts {
			return false
		}
	}
	return true
}

func evidenceLifecycleFrameAllowance(w Workload, queryCount, mutations int) (int64, error) {
	queries := int64(queryCount)
	// Each readiness attempt opens one session. Its uniform worst-case setup is
	// the SSE handshake plus protocol init, followed by one add-query
	// acknowledgement and one full snapshot per query. There are two readiness
	// passes, each with the bounded retry count above.
	perReadinessAttempt, err := safeEvidenceMultiply(queries, evidenceQuerySnapshotFrames)
	if err != nil {
		return 0, err
	}
	perReadinessAttempt, err = safeEvidenceAdd(perReadinessAttempt, evidenceSessionSetupFrames)
	if err != nil {
		return 0, err
	}
	readinessAttempts, err := safeEvidenceMultiply(2, int64(maxEvidenceReadinessAttempts))
	if err != nil {
		return 0, err
	}
	readiness, err := safeEvidenceMultiply(perReadinessAttempt, readinessAttempts)
	if err != nil {
		return 0, err
	}
	// Measured subscribers and final-snapshot caches each open at most one
	// session per canonical wire query: the same two-frame setup plus the
	// add-query acknowledgement and initial/full refresh.
	perSessionQueryFrames, err := safeEvidenceAdd(evidenceSessionSetupFrames, evidenceQuerySnapshotFrames)
	if err != nil {
		return 0, err
	}
	measuredSubscriptions, err := safeEvidenceMultiply(queries, perSessionQueryFrames)
	if err != nil {
		return 0, err
	}
	finalSnapshots, err := safeEvidenceMultiply(queries, perSessionQueryFrames)
	if err != nil {
		return 0, err
	}
	lifecycle, err := safeEvidenceAdd(readiness, measuredSubscriptions)
	if err != nil {
		return 0, err
	}
	lifecycle, err = safeEvidenceAdd(lifecycle, finalSnapshots)
	if err != nil {
		return 0, err
	}
	lifecycle, err = safeEvidenceAdd(lifecycle, evidenceQualificationFrames)
	if err != nil {
		return 0, err
	}
	// Every measured/warm-up mutation has one transaction acknowledgement. T
	// adds eight writer-session setups, while the refresh fan-out is already
	// represented by the recipient upper bound.
	transactionFrames, err := safeEvidenceMultiply(int64(mutations), 1)
	if err != nil {
		return 0, err
	}
	lifecycle, err = safeEvidenceAdd(lifecycle, transactionFrames)
	if err != nil {
		return 0, err
	}
	if w.Family == FamilyT {
		writerFrames, writerErr := safeEvidenceMultiply(int64(evidenceWriterSessions), evidenceSessionSetupFrames)
		if writerErr != nil {
			return 0, writerErr
		}
		lifecycle, err = safeEvidenceAdd(lifecycle, writerFrames)
		if err != nil {
			return 0, err
		}
	}
	// R reconnects affect exactly one in ten clients at each 30-second epoch.
	// Each replacement session costs the same four-frame worst case: two setup
	// frames (SSE handshake + init) and two query lifecycle frames (add-query
	// acknowledgement + refresh). The factor remains four for both transports.
	if w.Family == FamilyR && w.DurationSeconds > 0 {
		affected, err := safeEvidenceAdd(queries, 9)
		if err != nil {
			return 0, err
		}
		affected /= 10
		epochs := int64(w.DurationSeconds) / 30
		reconnects, err := safeEvidenceMultiply(affected, epochs)
		if err != nil {
			return 0, err
		}
		reconnectFrames, err := safeEvidenceMultiply(reconnects, 4)
		if err != nil {
			return 0, err
		}
		lifecycle, err = safeEvidenceAdd(lifecycle, reconnectFrames)
		if err != nil {
			return 0, err
		}
	}
	return safeEvidenceAdd(lifecycle, evidenceLifecycleControlFrames)
}

func safeEvidenceMultiply(a, b int64) (int64, error) {
	if a < 0 || b < 0 || (b != 0 && a > (int64(^uint64(0)>>1))/b) {
		return 0, errors.New("derived evidence budget overflow")
	}
	return a * b, nil
}

func safeEvidenceAdd(a, b int64) (int64, error) {
	if a < 0 || b < 0 || a > (int64(^uint64(0)>>1))-b {
		return 0, errors.New("derived evidence budget overflow")
	}
	return a + b, nil
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

// verifyCleanFixture proves that the second provisioning pass restored the
// frozen benchmark state. Qualification deliberately performs a mutation, so
// metadata identity alone is insufficient: each measured query gets a fresh
// full snapshot and is compared with the prefix-zero semantic oracle before a
// measured subscriber is opened.
func (d *TargetDriver) verifyCleanFixture(ctx context.Context, fixture Fixture, runID string, evidence *evidenceCollector) error {
	oracle := NewPrefixOracle(fixture)
	probeID := d.cfg.Probe.Mutation.EntityID
	client, err := d.openSession(ctx, runID+"-clean-check", evidence)
	if err != nil {
		return fmt.Errorf("clean fixture open session: %w", err)
	}
	defer client.Close()
	snapshots := make(map[string]Materialized)
	for _, query := range fixture.Queries {
		expected, err := oracle.Materialize(query.ID, 0)
		if err != nil {
			return fmt.Errorf("clean fixture query %q oracle: %w", query.ID, err)
		}
		if probeID != "" {
			if _, present := expected.Entities[probeID]; present {
				return fmt.Errorf("clean fixture query %q includes reserved qualification probe entity %q", query.ID, probeID)
			}
		}
		wireQuery, err := d.cfg.QueryBuilder(query)
		if err != nil {
			return fmt.Errorf("clean fixture query %q build: %w", query.ID, err)
		}
		wireKey, err := json.Marshal(wireQuery)
		if err != nil {
			return fmt.Errorf("clean fixture query %q encode: %w", query.ID, err)
		}
		actual, ok := snapshots[string(wireKey)]
		if !ok {
			actualRefresh, snapshotErr := client.Subscribe(ctx, query)
			if snapshotErr != nil {
				return fmt.Errorf("clean fixture query %q snapshot: %w", query.ID, snapshotErr)
			}
			if actualRefresh.Full == nil {
				return fmt.Errorf("clean fixture query %q snapshot was not full", query.ID)
			}
			actual = *actualRefresh.Full
			snapshots[string(wireKey)] = actual
		}
		actual.QueryID = query.ID
		if probeID != "" {
			if _, present := actual.Entities[probeID]; present {
				return fmt.Errorf("clean fixture query %q still contains qualification probe entity %q", query.ID, probeID)
			}
		}
		expectedDigest, err := expected.Digest()
		if err != nil {
			return fmt.Errorf("clean fixture query %q expected digest: %w", query.ID, err)
		}
		actualDigest, err := actual.Digest()
		if err != nil {
			return fmt.Errorf("clean fixture query %q actual digest: %w", query.ID, err)
		}
		if expectedDigest != actualDigest {
			return fmt.Errorf("clean fixture query %q diverges after qualification cleanup: expected %s, observed %s", query.ID, expectedDigest, actualDigest)
		}
	}
	return nil
}

// Run executes a single target-side RunPlan. Qualification must be supplied
// by the caller (typically Prepare/Qualify); this method intentionally does
// not silently downgrade a failed target into synthetic measurements.
func (d *TargetDriver) Run(ctx context.Context, plan RunPlan) (TargetRunArtifacts, error) {
	if d == nil {
		return TargetRunArtifacts{}, fmt.Errorf("nil target driver")
	}
	base := TargetRunArtifacts{TargetID: d.cfg.ID, RunID: plan.RunID, PairID: plan.PairID, Family: plan.Workload.Family, Subscribers: plan.Workload.Subscribers, RampDuration: plan.RampDuration, SettleDuration: plan.SettleDuration, WarmupDuration: plan.WarmupDuration, WarmupMutations: plan.WarmupMutations}
	if err := ValidateBehaviorWindow(plan.Workload, plan.BehaviorSeconds); err != nil {
		return base, err
	}
	if d.cfg.QueryBuilder == nil || d.cfg.TransactionBuilder == nil {
		return base, &UnsupportedTargetError{Check: "run", Reason: "query and transaction builders are required"}
	}
	if len(plan.Workload.Fixture.Queries) == 0 || plan.Workload.Subscribers != len(plan.Workload.Fixture.Queries) {
		return base, &UnsupportedTargetError{Check: "run", Reason: "workload must contain one frozen query per subscriber"}
	}
	mutations := plan.Mutations
	if mutations <= 0 {
		mutations = plan.Workload.Measured
	}
	if plan.Workload.Family == FamilyT {
		if mutations <= 0 {
			mutations = 4096
		}
		if mutations > 4096 {
			mutations = 4096
		}
	}
	if mutations <= 0 {
		mutations = 1
	}
	plan.Mutations = mutations
	budget := d.cfg.EvidenceBudget
	if budget == (EvidenceBudget{}) {
		budgetWorkload := plan.Workload
		if (plan.Workload.Family == FamilyR || plan.Workload.Family == FamilyS) && plan.BehaviorSeconds > 0 {
			// R reconnect epochs are driven by the run plan's behavior window,
			// which may be shorter than the canonical workload duration.
			budgetWorkload.DurationSeconds = plan.BehaviorSeconds
		}
		var budgetErr error
		budget, budgetErr = EvidenceBudgetForWorkloadWithWarmup(budgetWorkload, mutations, plan.WarmupMutations)
		if budgetErr != nil {
			return base, fmt.Errorf("derive live evidence budget: %w", budgetErr)
		}
	}
	evidence := newEvidenceCollector(budget)
	// Provision is deliberately per attempt. A caller may use it to create a
	// fresh database/process; the semantic barrier proves that exact attempt
	// has reached the clean fixture before qualification starts.
	if err := d.Provision(ctx); err != nil {
		return base, err
	}
	if err := d.waitForTargetReadiness(ctx, plan.Workload.Fixture, plan.RunID+"-pre-qualification-clean-check", evidence); err != nil {
		base.RawFrames, base.Evidence = evidence.snapshot()
		return base, fmt.Errorf("pre-qualification readiness: %w", err)
	}
	qualification, err := d.qualify(ctx, evidence)
	base.Qualification = qualification
	if err != nil {
		return base, err
	}
	// The four-client qualification probe intentionally mutates a probe row.
	// Restore the exact clean fixture before any measured subscriber opens, then
	// verify identity/metadata without issuing another mutation or probe.
	if err := d.Provision(ctx); err != nil {
		return base, fmt.Errorf("restore clean fixture after qualification: %w", err)
	}
	if err := d.waitForTargetReadiness(ctx, plan.Workload.Fixture, plan.RunID+"-post-qualification-clean-check", evidence); err != nil {
		base.RawFrames, base.Evidence = evidence.snapshot()
		return base, fmt.Errorf("post-qualification readiness: %w", err)
	}

	clients := make([]*TargetSession, 0, len(plan.Workload.Fixture.Queries))
	rampStarted := time.Now()
	closeOpened := func() {
		for _, opened := range clients {
			_ = opened.Close()
		}
	}
	for i, query := range plan.Workload.Fixture.Queries {
		if err := waitRamp(ctx, rampStarted, plan.RampDuration, i, len(plan.Workload.Fixture.Queries)); err != nil {
			closeOpened()
			return base, err
		}
		client, openErr := d.openSession(ctx, fmt.Sprintf("%s-client-%d", plan.RunID, i), evidence)
		if openErr != nil {
			closeOpened()
			return base, openErr
		}
		if _, openErr = client.Subscribe(ctx, query); openErr != nil {
			_ = client.Close()
			closeOpened()
			return base, openErr
		}
		clients = append(clients, client)
	}
	if err := waitPhase(ctx, plan.SettleDuration); err != nil {
		closeOpened()
		return base, err
	}

	writerSessions := make([]*TargetSession, 0, 8)
	if plan.Workload.Family == FamilyT {
		for i := 0; i < 8; i++ {
			writer, writerErr := d.openSession(ctx, fmt.Sprintf("%s-writer-%d", plan.RunID, i), evidence)
			if writerErr != nil {
				for _, opened := range writerSessions {
					_ = opened.Close()
				}
				closeOpened()
				return base, writerErr
			}
			writerSessions = append(writerSessions, writer)
		}
	}
	if err := d.runWarmup(ctx, plan, clients, writerSessions); err != nil {
		for _, writer := range writerSessions {
			_ = writer.Close()
		}
		closeOpened()
		return base, err
	}
	// Execute invokes these callbacks exactly when it records the measured
	// boundaries. Preserve caller callbacks (for example, process samplers)
	// while making the evidence window use the same timestamps.
	onMeasuredStart := plan.OnMeasuredStart
	onMeasuredEnd := plan.OnMeasuredEnd
	plan.OnMeasuredStart = func(at time.Time) error {
		evidence.beginMeasured(at)
		if onMeasuredStart != nil {
			return onMeasuredStart(at)
		}
		return nil
	}
	plan.OnMeasuredEnd = func(at time.Time) error {
		evidence.endMeasured(at)
		if onMeasuredEnd != nil {
			return onMeasuredEnd(at)
		}
		return nil
	}

	started := time.Now()
	producerCtx, stopProducers := context.WithCancel(ctx)
	receipts := make(chan Receipt, max(64, plan.Workload.Subscribers))
	var producerWG sync.WaitGroup
	var protocolMu sync.Mutex
	var protocolErrors []string
	recordProtocol := func(clientID string, ev SessionEvent, decodeErr error) {
		message := "protocol error"
		if decodeErr != nil {
			message = decodeErr.Error()
		} else if ev.Error != "" {
			message = ev.Error
		}
		protocolMu.Lock()
		protocolErrors = append(protocolErrors, fmt.Sprintf("client=%s op=%s: %s", clientID, ev.Op, message))
		protocolMu.Unlock()
	}
	emitReceipt := func(item Receipt) {
		select {
		case receipts <- item:
		case <-producerCtx.Done():
		}
	}
	commitPrefixes := newCommitPrefixesFor(emitReceipt, plan.Workload.Family, plan.WriterID)
	var startReader func(*TargetSession, string, string)
	startReader = func(client *TargetSession, queryID, recipientID string) {
		producerWG.Add(1)
		go func() {
			defer producerWG.Done()
			for {
				if err := client.waitRead(producerCtx); err != nil {
					return
				}
				select {
				case <-producerCtx.Done():
					return
				case ev, ok := <-client.Events():
					if !ok {
						return
					}
					if ev.Op == "error" || ev.Op == "protocol-error" {
						client.recordEventFor(ev, ev.Error, queryID, recipientID)
						recordProtocol(client.clientID, ev, nil)
						continue
					}
					if ev.Op != "refresh-ok" && ev.Op != "refresh-ok-delta" {
						client.recordEventFor(ev, "", queryID, recipientID)
						continue
					}
					items, decodeErr := d.decodeReceipts(client, ev, queryID, recipientID, commitPrefixes)
					if decodeErr != nil {
						client.recordEventFor(ev, decodeErr.Error(), queryID, recipientID)
						recordProtocol(client.clientID, ev, decodeErr)
						continue
					}
					client.recordEventFor(ev, "", queryID, recipientID)
					for _, item := range items {
						emitReceipt(item)
					}
				}
			}
		}()
	}
	for i, client := range clients {
		startReader(client, plan.Workload.Fixture.Queries[i].ID, "client-"+plan.Workload.Fixture.Queries[i].ID)
	}
	// JSONSession/SSESession publish transact-ok frames to their event stream
	// even when the acknowledgement waiter has consumed the normalized Ack.
	// Drain each T writer independently or a long saturation run can fill an
	// event buffer and turn a later write into an artificial stall.
	for _, writer := range writerSessions {
		producerWG.Add(1)
		go func(writer *TargetSession) {
			defer producerWG.Done()
			for {
				select {
				case <-producerCtx.Done():
					return
				case ev, ok := <-writer.Events():
					if !ok {
						return
					}
					protocolErr := ""
					if ev.Op == "error" || ev.Op == "protocol-error" {
						protocolErr = ev.Error
						recordProtocol(writer.clientID, ev, nil)
					}
					writer.recordEvent(ev, protocolErr)
				}
			}
		}(writer)
	}
	stopOnce := sync.Once{}
	stop := func() {
		stopOnce.Do(func() {
			stopProducers()
			for _, client := range clients {
				_ = client.Close()
			}
			for _, writer := range writerSessions {
				_ = writer.Close()
			}
			producerWG.Wait()
			close(receipts)
		})
	}
	result, runErr := Execute(ctx, plan, RunHooks{
		Submit: func(ctx context.Context, mutation Mutation) (Ack, error) {
			return clients[0].Transact(ctx, mutation)
		},
		SubmitWriter: func(ctx context.Context, writer int, mutation Mutation) (Ack, error) {
			if writer < 0 || writer >= len(writerSessions) {
				return Ack{}, fmt.Errorf("writer index %d is not configured", writer)
			}
			return writerSessions[writer].Transact(ctx, mutation)
		},
		Receipts:     receipts,
		StopReceipts: stop,
		OnCommitted:  func(mutation Mutation, ack Ack, prefix int) { commitPrefixes.Record(mutation, ack, prefix) },
		OnClientBehavior: func(ctx context.Context, clientID, _ int, behavior ClientBehavior) error {
			if clientID < 0 || clientID >= len(clients) {
				return fmt.Errorf("behavior client index %d is outside subscriber set", clientID)
			}
			if behavior.PauseReads {
				clients[clientID].PauseReads(behavior.PauseFor)
			}
			if behavior.Reconnect {
				if err := waitPhase(ctx, behavior.Backoff); err != nil {
					return err
				}
				if err := clients[clientID].Reconnect(ctx); err != nil {
					return err
				}
				query := plan.Workload.Fixture.Queries[clientID]
				startReader(clients[clientID], query.ID, "client-"+query.ID)
			}
			return nil
		},
		FinalSnapshot: d.finalSnapshotter(plan.Workload.Fixture, evidence),
	})
	stop()
	rawFrames, evidenceStats := evidence.snapshot()
	protocolMu.Lock()
	protocolCopy := append([]string(nil), protocolErrors...)
	protocolMu.Unlock()
	if runErr == nil && len(protocolCopy) > 0 {
		runErr = fmt.Errorf("target protocol errors: %s", strings.Join(protocolCopy, "; "))
	}
	base.Result = result
	base.RawFrames = rawFrames
	base.Evidence = evidenceStats
	base.ExpectedMutations = result.ExpectedMutations
	base.ExpectedRows = result.ExpectedRows
	base.MeasuredStartedAt = result.MeasuredStartedAt
	base.MeasuredFinishedAt = result.MeasuredFinishedAt
	base.ProtocolErrors = protocolCopy
	base.BehaviorErrors = append([]string(nil), result.BehaviorErrors...)
	base.InitialSnapshots = len(clients)
	base.StartedAt = started
	base.FinishedAt = time.Now()
	if evidenceStats.Overflow && runErr == nil {
		runErr = fmt.Errorf("target evidence budget exceeded: observed %d frames/%d bytes (limits %d/%d)", evidenceStats.ObservedFrames, evidenceStats.ObservedBytes, evidenceStats.MaxFrames, evidenceStats.MaxBytes)
	}
	return base, runErr
}

func (d *TargetDriver) finalSnapshotter(fixture Fixture, evidence *evidenceCollector) func(context.Context, string, string) (Materialized, error) {
	type cachedSnapshot struct {
		materialized Materialized
		err          error
	}
	queries := make(map[string]Query, len(fixture.Queries))
	for _, query := range fixture.Queries {
		queries[query.ID] = query
	}
	cache := make(map[string]cachedSnapshot)
	return func(ctx context.Context, recipient, queryID string) (Materialized, error) {
		query, ok := queries[queryID]
		if !ok {
			return Materialized{}, fmt.Errorf("unknown final-snapshot query %q", queryID)
		}
		wireQuery, err := d.cfg.QueryBuilder(query)
		if err != nil {
			return Materialized{}, fmt.Errorf("final snapshot query %q build: %w", queryID, err)
		}
		wireKey, err := json.Marshal(wireQuery)
		if err != nil {
			return Materialized{}, fmt.Errorf("final snapshot query %q encode: %w", queryID, err)
		}
		cached, ok := cache[string(wireKey)]
		if !ok {
			fresh, openErr := d.openSession(ctx, recipient+"-final", evidence)
			if openErr != nil {
				cached.err = openErr
			} else {
				cached.materialized, cached.err = fresh.FinalSnapshot(ctx, query)
				_ = fresh.Close()
			}
			cache[string(wireKey)] = cached
		}
		cached.materialized.QueryID = queryID
		return cached.materialized, cached.err
	}
}

func waitPhase(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func waitRamp(ctx context.Context, started time.Time, duration time.Duration, index, total int) error {
	if duration <= 0 || total <= 1 || index <= 0 {
		return nil
	}
	// The final subscriber is admitted at the end of the declared ramp. Use
	// integer nanoseconds to avoid accumulating floating-point schedule drift.
	offset := time.Duration(int64(duration) * int64(index) / int64(total-1))
	wait := time.Until(started.Add(offset))
	if wait <= 0 {
		return nil
	}
	return waitPhase(ctx, wait)
}

func (d *TargetDriver) runWarmup(ctx context.Context, plan RunPlan, clients, writers []*TargetSession) error {
	count := plan.WarmupMutations
	if count <= 0 {
		return waitPhase(ctx, plan.WarmupDuration)
	}
	if len(clients) == 0 {
		return &UnsupportedTargetError{Check: "warmup", Reason: "no subscriber session is available for warm-up"}
	}
	start := plan.WarmupStart
	if start <= 0 {
		start = 1_000_000
	}
	rate := plan.WarmupRate
	if rate <= 0 {
		rate = plan.Rate
	}
	if rate <= 0 {
		rate = plan.Workload.TxRate
	}
	if rate <= 0 {
		rate = 8
	}
	scheduler, err := NewBlockingScheduler(rate, time.Now())
	if err != nil {
		return err
	}
	items := make([]Mutation, count)
	for i := range items {
		items[i] = plan.Workload.Mutation(start + int64(i))
	}
	writer := clients[0]
	if plan.Workload.Family == FamilyT && len(writers) > 0 {
		writer = writers[0]
	}
	warmCtx := ctx
	var cancel context.CancelFunc
	if plan.WarmupDuration > 0 {
		warmCtx, cancel = context.WithTimeout(ctx, plan.WarmupDuration)
		defer cancel()
	}
	started := time.Now()
	if err := scheduler.Run(warmCtx, items, func(callCtx context.Context, mutation Mutation) error {
		ack, err := writer.Transact(callCtx, mutation)
		if err != nil {
			return err
		}
		if !ack.Accepted {
			return fmt.Errorf("warm-up mutation %s was not accepted", mutation.EventID)
		}
		return nil
	}); err != nil {
		return err
	}
	if plan.WarmupDuration > 0 {
		if remaining := plan.WarmupDuration - time.Since(started); remaining > 0 {
			return waitPhase(ctx, remaining)
		}
	}
	return nil
}

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

// wireComputation is the protocol-level computation object used by both V1
// and V2 refresh frames. Keeping this shape here lets the decoder inspect all
// computations before deciding whether a frame is relevant to a subscription.
type wireComputation struct {
	Query  json.RawMessage `json:"instaql-query"`
	Result json.RawMessage `json:"instaql-result"`
	Delta  json.RawMessage `json:"delta"`
}

// DecodeWireRefresh parses the actual V1/V2 envelope using label-shaped
// attributes. A TargetSession supplies the explicit UUID alias map and the
// subscribed wire query through decodeWireRefresh below.
func DecodeWireRefresh(ev SessionEvent, queryID string) (Refresh, error) {
	return decodeWireRefresh(ev, queryID, nil, nil)
}

// DecodeWireRefreshWithAliases parses a refresh where the target may encode
// semantic attributes as UUIDs. aliases maps semantic names (id, value,
// bucket, rank) to their wire labels/UUIDs. Unknown attributes remain lossless.
func DecodeWireRefreshWithAliases(ev SessionEvent, queryID string, aliases map[string]string) (Refresh, error) {
	return decodeWireRefresh(ev, queryID, aliases, nil)
}

// DecodeWireRefreshForQuery additionally filters a multi-computation frame to
// the computation(s) matching wireQuery. This is the form used by live
// sessions, where unrelated subscriptions can share a refresh envelope.
func DecodeWireRefreshForQuery(ev SessionEvent, queryID string, wireQuery json.RawMessage, aliases map[string]string) (Refresh, error) {
	return decodeWireRefresh(ev, queryID, aliases, wireQuery)
}

// decodeV1WireRefresh accepts the legacy V1 refresh envelope. V1 legitimately
// emits refresh-ok with an empty computations array when only attrs changed;
// that frame is a metadata-only no-op, not a malformed snapshot. V2 keeps the
// stricter generic decoder below, which requires at least one computation.
func decodeV1WireRefresh(ev SessionEvent, queryID string, aliases map[string]string, expectedQuery json.RawMessage) (Refresh, error) {
	if ev.Op != "refresh-ok" {
		return Refresh{}, fmt.Errorf("V1 legacy decoder rejects %s", ev.Op)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(ev.Payload, &fields); err != nil {
		return Refresh{}, err
	}
	for name := range fields {
		switch name {
		case "op", "processed-tx-id", "processed-isn", "computations", "attrs", "trace-id":
		default:
			return Refresh{}, fmt.Errorf("V1 refresh contains unsupported field %q", name)
		}
	}
	if err := validateV1RefreshField(fields, "op", func(raw json.RawMessage) error {
		var op string
		if err := json.Unmarshal(raw, &op); err != nil || op != "refresh-ok" {
			return fmt.Errorf("must be %q", "refresh-ok")
		}
		if op != ev.Op {
			return fmt.Errorf("does not match event op %q", ev.Op)
		}
		return nil
	}); err != nil {
		return Refresh{}, fmt.Errorf("V1 refresh op %w", err)
	}
	if err := validateV1RefreshField(fields, "processed-tx-id", validateV1Integer); err != nil {
		return Refresh{}, fmt.Errorf("V1 refresh processed-tx-id %w", err)
	}
	if err := validateV1RefreshField(fields, "processed-isn", validateV1ISN); err != nil {
		return Refresh{}, fmt.Errorf("V1 refresh processed-isn %w", err)
	}
	if attrs, ok := fields["attrs"]; ok {
		if err := validateV1Attrs(attrs); err != nil {
			return Refresh{}, fmt.Errorf("V1 refresh attrs %w", err)
		}
	}
	computationsRaw, ok := fields["computations"]
	if !ok || rawIsNull(computationsRaw) {
		return Refresh{}, fmt.Errorf("V1 refresh missing computations")
	}
	var computations []json.RawMessage
	if err := json.Unmarshal(computationsRaw, &computations); err != nil {
		return Refresh{}, fmt.Errorf("V1 refresh computations must be an array: %w", err)
	}
	for i, raw := range computations {
		var computation map[string]json.RawMessage
		if err := json.Unmarshal(raw, &computation); err != nil {
			return Refresh{}, fmt.Errorf("V1 computation %d must be an object: %w", i, err)
		}
		if _, ok := computation["delta"]; ok {
			return Refresh{}, fmt.Errorf("V1 computation %d contains unsupported delta", i)
		}
		if query, ok := computation["instaql-query"]; !ok || rawIsNull(query) {
			return Refresh{}, fmt.Errorf("V1 computation %d missing instaql-query", i)
		}
		if result, ok := computation["instaql-result"]; !ok || rawIsNull(result) {
			return Refresh{}, fmt.Errorf("V1 computation %d missing instaql-result", i)
		}
		var queryObject map[string]json.RawMessage
		if err := json.Unmarshal(computation["instaql-query"], &queryObject); err != nil || queryObject == nil {
			return Refresh{}, fmt.Errorf("V1 computation %d instaql-query must be an object", i)
		}
	}
	if len(computations) == 0 {
		attrs, ok := fields["attrs"]
		if !ok || rawIsNull(attrs) {
			return Refresh{}, fmt.Errorf("V1 metadata-only refresh missing attrs")
		}
		processed := scalarString(fields["processed-tx-id"])
		// The transaction id is retained as wire evidence only. An attrs-only
		// refresh carries no semantic query state transition, so it must never
		// advance the materialized state's version.
		return Refresh{QueryID: queryID, Kind: RefreshNoop, ProcessedTransactionID: processed, At: ev.At}, nil
	}
	return decodeWireRefresh(ev, queryID, aliases, expectedQuery)
}

func validateV1RefreshField(fields map[string]json.RawMessage, name string, validate func(json.RawMessage) error) error {
	raw, ok := fields[name]
	if !ok || rawIsNull(raw) {
		return fmt.Errorf("is missing")
	}
	if err := validate(raw); err != nil {
		return err
	}
	return nil
}

func validateV1Integer(raw json.RawMessage) error {
	if trimmed := bytes.TrimSpace(raw); len(trimmed) == 0 || trimmed[0] == '"' {
		return fmt.Errorf("must be an integer")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var number json.Number
	if err := decoder.Decode(&number); err != nil {
		return fmt.Errorf("must be an integer")
	}
	value, err := strconv.ParseInt(number.String(), 10, 64)
	if err != nil || value < 0 {
		return fmt.Errorf("must be a non-negative integer")
	}
	return nil
}

func validateV1Attrs(raw json.RawMessage) error {
	if rawIsNull(raw) {
		return fmt.Errorf("must be an array")
	}
	var attrs []json.RawMessage
	if err := json.Unmarshal(raw, &attrs); err != nil {
		return fmt.Errorf("must be an array: %w", err)
	}
	for i, attrRaw := range attrs {
		var attr map[string]json.RawMessage
		if err := json.Unmarshal(attrRaw, &attr); err != nil || attr == nil {
			return fmt.Errorf("item %d must be an object", i)
		}
	}
	return nil
}

func validateV1ISN(raw json.RawMessage) error {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil || !validV1ISN(value) {
		return fmt.Errorf("must be an ISN string in slot/LSN form")
	}
	return nil
}

func validV1ISN(value string) bool {
	parts := strings.Split(value, "/")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return false
	}
	for _, part := range parts {
		for _, r := range part {
			if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
				return false
			}
		}
	}
	return true
}

// V1 and V2 return the initial materialized result in add-query-ok. Some test
// adapters emit a separate refresh frame instead, so absence of result is an
// explicit fallback rather than a fabricated empty snapshot.
func decodeAddQuerySnapshot(ev SessionEvent, queryID string, aliases map[string]string, expectedQuery json.RawMessage) (Refresh, bool, error) {
	if ev.Op != "add-query-ok" {
		return Refresh{}, false, nil
	}
	if len(bytes.TrimSpace(ev.Payload)) == 0 {
		return Refresh{}, false, nil
	}
	var envelope struct {
		Query     json.RawMessage `json:"q"`
		Result    json.RawMessage `json:"result"`
		Processed json.RawMessage `json:"processed-tx-id"`
	}
	if err := json.Unmarshal(ev.Payload, &envelope); err != nil {
		return Refresh{}, true, fmt.Errorf("decode add-query snapshot: %w", err)
	}
	if len(envelope.Result) == 0 || rawIsNull(envelope.Result) {
		return Refresh{}, false, nil
	}
	query := envelope.Query
	if len(query) == 0 || rawIsNull(query) {
		query = expectedQuery
	}
	if len(query) == 0 || rawIsNull(query) {
		return Refresh{}, true, errors.New("add-query snapshot missing query")
	}
	payload, err := json.Marshal(struct {
		Computations []wireComputation `json:"computations"`
		Processed    json.RawMessage   `json:"processed-tx-id,omitempty"`
	}{Computations: []wireComputation{{Query: query, Result: envelope.Result}}, Processed: envelope.Processed})
	if err != nil {
		return Refresh{}, true, err
	}
	ev.Op = "refresh-ok"
	ev.Payload = payload
	refresh, err := decodeWireRefresh(ev, queryID, aliases, expectedQuery)
	return refresh, true, err
}

func decodeWireRefresh(ev SessionEvent, queryID string, aliases map[string]string, expectedQuery json.RawMessage) (Refresh, error) {
	if ev.Op != "refresh-ok" && ev.Op != "refresh-ok-delta" {
		return Refresh{}, fmt.Errorf("not a refresh event: %s", ev.Op)
	}
	var envelope struct {
		Computations []wireComputation `json:"computations"`
		Processed    json.RawMessage   `json:"processed-tx-id"`
	}
	if err := json.Unmarshal(ev.Payload, &envelope); err != nil {
		return Refresh{}, err
	}
	if len(envelope.Computations) == 0 {
		return Refresh{}, fmt.Errorf("refresh missing computations")
	}
	computations, err := matchingComputations(envelope.Computations, expectedQuery)
	if err != nil {
		return Refresh{}, err
	}
	if len(expectedQuery) == 0 || rawIsNull(expectedQuery) {
		for i, computation := range computations {
			if len(computation.Query) == 0 || rawIsNull(computation.Query) {
				return Refresh{}, fmt.Errorf("computation %d missing instaql-query", i)
			}
		}
	}
	refresh := Refresh{QueryID: queryID, Kind: RefreshFull, ProcessedTransactionID: scalarString(envelope.Processed), At: ev.At}
	if n, err := strconv.ParseInt(refresh.ProcessedTransactionID, 10, 64); err == nil && n > 0 {
		refresh.StateVersion = n
	}

	hasDelta := ev.Op == "refresh-ok-delta"
	for _, computation := range computations {
		if len(computation.Delta) > 0 && !rawIsNull(computation.Delta) {
			hasDelta = true
		}
	}
	if hasDelta {
		refresh.Kind = RefreshDelta
		delta := &Delta{QueryID: queryID, StateVersion: refresh.StateVersion}
		for _, computation := range computations {
			if len(computation.Delta) == 0 || rawIsNull(computation.Delta) {
				return Refresh{}, fmt.Errorf("delta refresh computation missing delta")
			}
			if len(computation.Result) > 0 && !rawIsNull(computation.Result) {
				return Refresh{}, fmt.Errorf("delta refresh computation contains both result and delta")
			}
			var patch struct {
				Ops []struct {
					Op     string          `json:"op"`
					ID     string          `json:"id"`
					Entity json.RawMessage `json:"entity"`
				} `json:"ops"`
			}
			if err := json.Unmarshal(computation.Delta, &patch); err != nil {
				return Refresh{}, fmt.Errorf("decode refresh delta: %w", err)
			}
			for _, op := range patch.Ops {
				switch op.Op {
				case "remove":
					if op.ID == "" {
						return Refresh{}, fmt.Errorf("refresh delta remove has empty entity id")
					}
					delta.Removes = append(delta.Removes, op.ID)
				case "add", "update":
					entity, err := wireEntity(op.ID, op.Entity, aliases)
					if err != nil {
						return Refresh{}, err
					}
					if op.Op == "add" {
						delta.Adds = append(delta.Adds, entity)
					} else {
						delta.Updates = append(delta.Updates, entity)
					}
				default:
					return Refresh{}, fmt.Errorf("unknown refresh delta op %q", op.Op)
				}
			}
		}
		refresh.Delta = delta
		return refresh, nil
	}

	result := Materialized{QueryID: queryID, Entities: map[string]Entity{}}
	for _, computation := range computations {
		if len(computation.Query) == 0 || rawIsNull(computation.Query) {
			return Refresh{}, fmt.Errorf("computation missing instaql-query")
		}
		part, err := decodeWireResult(computation.Result, queryID, aliases)
		if err != nil {
			return Refresh{}, err
		}
		for id, entity := range part.Entities {
			result.Entities[id] = entity
		}
	}
	refresh.Full = &result
	return refresh, nil
}

func matchingComputations(all []wireComputation, expectedQuery json.RawMessage) ([]wireComputation, error) {
	if len(expectedQuery) == 0 || rawIsNull(expectedQuery) {
		return all, nil
	}
	var matches []wireComputation
	for i, computation := range all {
		if len(computation.Query) == 0 || rawIsNull(computation.Query) {
			return nil, fmt.Errorf("computation %d missing instaql-query", i)
		}
		equal, err := sameWireJSON(expectedQuery, computation.Query)
		if err != nil {
			return nil, fmt.Errorf("compare computation %d query: %w", i, err)
		}
		if equal {
			matches = append(matches, computation)
		}
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("refresh has no computation matching subscribed query")
	}
	return matches, nil
}

func rawIsNull(raw []byte) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func sameWireJSON(left, right []byte) (bool, error) {
	var l, r any
	if err := json.Unmarshal(left, &l); err != nil {
		return false, err
	}
	if err := json.Unmarshal(right, &r); err != nil {
		return false, err
	}
	// encoding/json deterministically orders object keys, so this comparison
	// ignores harmless whitespace/key-order differences in wire frames.
	lb, err := json.Marshal(l)
	if err != nil {
		return false, err
	}
	rb, err := json.Marshal(r)
	if err != nil {
		return false, err
	}
	return bytes.Equal(lb, rb), nil
}

func decodeWireResult(raw json.RawMessage, queryID string, aliases map[string]string) (Materialized, error) {
	if len(raw) == 0 || rawIsNull(raw) {
		return Materialized{}, fmt.Errorf("computation missing instaql-result")
	}
	var nodes []struct {
		Data struct {
			DatalogResult struct {
				JoinRows [][][]json.RawMessage `json:"join-rows"`
			} `json:"datalog-result"`
		} `json:"data"`
	}
	if json.Unmarshal(raw, &nodes) == nil && len(nodes) > 0 {
		out := Materialized{QueryID: queryID, Entities: map[string]Entity{}}
		identityRequired := identityAliasConfigured(aliases)
		identitySeen := make(map[string]bool)
		for _, node := range nodes {
			for _, row := range node.Data.DatalogResult.JoinRows {
				for _, triple := range row {
					if len(triple) < 3 {
						continue
					}
					id := scalarString(triple[0])
					attr := scalarString(triple[1])
					if id == "" || attr == "" {
						continue
					}
					if err := rejectNoncanonicalWireUUID(id); err != nil {
						return Materialized{}, err
					}
					if identityRequired {
						if err := validateWireEntityID(id); err != nil {
							return Materialized{}, err
						}
					}
					value, err := rawValue(triple[2])
					if err != nil {
						return Materialized{}, err
					}
					entity := out.Entities[id]
					entity.ID = id
					if entity.Attributes == nil {
						entity.Attributes = map[string]any{}
					}
					semantic := normalizeWireAttribute(attr, aliases)
					if semantic == "id" {
						if identityRequired {
							if identitySeen[id] {
								return Materialized{}, fmt.Errorf("duplicate identity triple for entity %s", id)
							}
							if err := validateWireIdentityValue(id, value); err != nil {
								return Materialized{}, err
							}
							identitySeen[id] = true
							out.Entities[id] = entity
						}
						continue
					}
					switch semantic {
					case "bucket":
						if n, ok := numericInt(value); ok {
							entity.Bucket = n
							out.Entities[id] = entity
							continue
						}
					case "rank":
						if n, ok := numericInt(value); ok {
							entity.Rank = n
							out.Entities[id] = entity
							continue
						}
					}
					entity.Attributes[semantic] = value
					out.Entities[id] = entity
				}
			}
		}
		if identityRequired {
			for id := range out.Entities {
				if !identitySeen[id] {
					return Materialized{}, fmt.Errorf("missing identity triple for entity %s", id)
				}
			}
		}
		return out, nil
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(raw, &root); err != nil {
		return Materialized{}, err
	}
	if data, ok := root["data"]; ok {
		if err := json.Unmarshal(data, &root); err != nil {
			return Materialized{}, err
		}
	}
	out := Materialized{QueryID: queryID, Entities: map[string]Entity{}}
	for _, collection := range root {
		var entities []json.RawMessage
		if json.Unmarshal(collection, &entities) != nil {
			continue
		}
		for _, rawEntity := range entities {
			id, err := wireObjectEntityID(rawEntity, aliases)
			if err != nil {
				return Materialized{}, err
			}
			if id == "" {
				continue
			}
			entity, err := wireEntity(id, rawEntity, aliases)
			if err != nil {
				return Materialized{}, err
			}
			out.Entities[entity.ID] = entity
		}
	}
	return out, nil
}

var wireUUIDRE = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func identityAliasConfigured(aliases map[string]string) bool {
	return aliases != nil && aliases["id"] != ""
}

func validateWireEntityID(id string) error {
	if !wireUUIDRE.MatchString(id) {
		return fmt.Errorf("identity entity id %q is not a UUID", id)
	}
	if id != strings.ToLower(id) {
		return fmt.Errorf("identity entity id %q must be a lowercase canonical UUID", id)
	}
	return nil
}

func rejectNoncanonicalWireUUID(value string) error {
	if wireUUIDRE.MatchString(value) && value != strings.ToLower(value) {
		return fmt.Errorf("entity UUID %q must be a lowercase canonical UUID", value)
	}
	return nil
}

func validateWireIdentityValue(entityID string, value any) error {
	identity, ok := value.(string)
	if !ok || !wireUUIDRE.MatchString(identity) || identity != strings.ToLower(identity) {
		return fmt.Errorf("identity value for entity %s must be a UUID string", entityID)
	}
	if identity != entityID {
		return fmt.Errorf("identity value for entity %s does not equal entity UUID", entityID)
	}
	return nil
}

func wireObjectEntityID(raw json.RawMessage, aliases map[string]string) (string, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return "", err
	}
	idRaw, ok := fields["id"]
	if !ok {
		if identityAliasConfigured(aliases) {
			return "", errors.New("object result entity is missing identity id")
		}
		return "", nil
	}
	value, err := rawValue(idRaw)
	if err != nil {
		return "", err
	}
	id, ok := value.(string)
	if !ok || id == "" {
		return "", errors.New("object result entity id must be a non-empty string")
	}
	if err := rejectNoncanonicalWireUUID(id); err != nil {
		return "", err
	}
	if identityAliasConfigured(aliases) {
		if err := validateWireEntityID(id); err != nil {
			return "", err
		}
	}
	return id, nil
}

func wireEntity(id string, raw json.RawMessage, aliases map[string]string) (Entity, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return Entity{}, err
	}
	entity := Entity{ID: id, Attributes: map[string]any{}}
	for key, value := range fields {
		decoded, err := rawValue(value)
		if err != nil {
			return Entity{}, err
		}
		semantic := normalizeWireAttribute(key, aliases)
		if semantic == "id" {
			// The object envelope's literal id and any explicit UUID alias
			// represent the row identity. Validate and omit both.
			if identityAliasConfigured(aliases) {
				if err := validateWireIdentityValue(id, decoded); err != nil {
					return Entity{}, err
				}
			}
			continue
		}
		switch semantic {
		case "bucket":
			if n, ok := numericInt(decoded); ok {
				entity.Bucket = n
				continue
			}
		case "rank":
			if n, ok := numericInt(decoded); ok {
				entity.Rank = n
				continue
			}
		}
		entity.Attributes[semantic] = decoded
	}
	return entity, nil
}

func normalizeWireAttribute(key string, aliases map[string]string) string {
	key = canonicalWireUUID(key)
	for _, semantic := range []string{"id", "value", "bucket", "rank"} {
		alias := ""
		if aliases != nil {
			alias = canonicalWireUUID(aliases[semantic])
		}
		if key == semantic || (alias != "" && alias == key) {
			return semantic
		}
	}
	return key
}

func canonicalWireUUID(value string) string {
	if wireUUIDRE.MatchString(value) {
		return strings.ToLower(value)
	}
	return value
}

func numericInt(v any) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), n == float64(int(n))
	case float32:
		return int(n), n == float32(int(n))
	case int:
		return n, true
	case int64:
		return int(n), int64(int(n)) == n
	case json.Number:
		i, err := strconv.ParseInt(string(n), 10, 64)
		return int(i), err == nil && int64(int(i)) == i
	default:
		return 0, false
	}
}

func rawValue(raw json.RawMessage) (any, error) {
	var out any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&out); err != nil {
		return nil, err
	}
	return normalizeJSONNumber(out), nil
}

func normalizeJSONNumber(v any) any {
	switch x := v.(type) {
	case json.Number:
		if i, err := strconv.ParseInt(string(x), 10, 64); err == nil {
			return float64(i)
		}
		if f, err := strconv.ParseFloat(string(x), 64); err == nil {
			return f
		}
		return string(x)
	case []any:
		for i := range x {
			x[i] = normalizeJSONNumber(x[i])
		}
	case map[string]any:
		for k := range x {
			x[k] = normalizeJSONNumber(x[k])
		}
	}
	return v
}
