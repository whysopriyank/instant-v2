package benchharness

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
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
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
