package benchrun

import "time"

// SchemaVersion is bumped whenever a raw artifact field changes semantics.
const SchemaVersion = "bench-v1"

type ValueStatus string

const (
	StatusMissing       ValueStatus = "missing"
	StatusZero          ValueStatus = "zero"
	StatusValue         ValueStatus = "value"
	StatusUnsupported   ValueStatus = "unsupported"
	StatusNotApplicable ValueStatus = "not_applicable"
	StatusFailed        ValueStatus = "failed"
)

// Measurement keeps an absent observation distinct from a measured zero.
type Measurement struct {
	Status ValueStatus `json:"status"`
	Value  float64     `json:"value,omitempty"`
	Unit   string      `json:"unit,omitempty"`
	Error  string      `json:"error,omitempty"`
}

func Missing(unit string) Measurement { return Measurement{Status: StatusMissing, Unit: unit} }
func Zero(unit string) Measurement    { return Measurement{Status: StatusZero, Unit: unit} }
func Unsupported(unit, reason string) Measurement {
	return Measurement{Status: StatusUnsupported, Unit: unit, Error: reason}
}

type Manifest struct {
	SchemaVersion   string            `json:"schema_version"`
	BundleID        string            `json:"bundle_id"`
	PairID          string            `json:"pair_id"`
	Family          string            `json:"family"`
	SubscriberScale int               `json:"subscriber_scale"`
	V1SHA           string            `json:"v1_sha,omitempty"`
	V2SHA           string            `json:"v2_sha,omitempty"`
	DirtyTreeHash   string            `json:"dirty_tree_hash,omitempty"`
	SourceTree      string            `json:"source_tree,omitempty"`
	Executables     map[string]string `json:"executables,omitempty"`
	CommandLine     []string          `json:"command_line,omitempty"`
	Seed            int64             `json:"seed"`
	RunOrder        []string          `json:"run_order"`
	// TargetOrder records the target permutation executed for each block of a
	// three-target run. It is intentionally separate from RunOrder, whose
	// AB/BA values are retained for the two-target format.
	TargetOrder           []ScheduleBlock   `json:"target_order,omitempty"`
	TargetRevisions       map[string]string `json:"target_revisions,omitempty"`
	Comparisons           []ComparisonSpec  `json:"comparisons,omitempty"`
	HostID                string            `json:"host_id,omitempty"`
	DatabaseIDs           map[string]string `json:"database_ids,omitempty"`
	TargetProvenance      map[string]string `json:"target_provenance,omitempty"`
	PostgresVersion       string            `json:"postgres_version,omitempty"`
	ToolchainVersion      string            `json:"toolchain_version,omitempty"`
	SchemaHash            string            `json:"schema_hash,omitempty"`
	FixtureHash           string            `json:"fixture_hash,omitempty"`
	ConfigHash            string            `json:"config_hash,omitempty"`
	EvidenceMaxFrames     int64             `json:"evidence_max_frames,omitempty"`
	EvidenceMaxBytes      int64             `json:"evidence_max_bytes,omitempty"`
	EvidenceMaxRetained   int               `json:"evidence_max_retained_frames,omitempty"`
	ArtifactMaxTotalBytes int64             `json:"artifact_max_total_bytes"`
	ArtifactMaxFileBytes  int64             `json:"artifact_max_file_bytes"`
	ContentRoot           string            `json:"content_root,omitempty"`
	ApprovalPublicKey     string            `json:"approval_public_key,omitempty"`
	ApprovalSignature     string            `json:"approval_signature,omitempty"`
	StartedAt             time.Time         `json:"started_at"`
	EndedAt               time.Time         `json:"ended_at,omitempty"`
	Artifacts             map[string]string `json:"artifacts,omitempty"`
}

type Plan struct {
	SchemaVersion   string   `json:"schema_version"`
	Seed            int64    `json:"seed"`
	Families        []string `json:"families"`
	Scales          []int    `json:"scales"`
	Pairs           int      `json:"pairs"`
	RampSeconds     int      `json:"ramp_seconds"`
	SettleSeconds   int      `json:"settle_seconds"`
	WarmupSeconds   int      `json:"warmup_seconds"`
	WarmupMutations int      `json:"warmup_mutations,omitempty"`
	MeasureSeconds  int      `json:"measure_seconds"`
	GraceSeconds    int      `json:"grace_seconds"`
	ConfigHash      string   `json:"config_hash,omitempty"`
}

type Environment struct {
	SchemaVersion string            `json:"schema_version"`
	OS            string            `json:"os,omitempty"`
	Kernel        string            `json:"kernel,omitempty"`
	CPUModel      string            `json:"cpu_model,omitempty"`
	CPUCount      int               `json:"cpu_count,omitempty"`
	MemoryBytes   Measurement       `json:"memory_bytes"`
	PowerMode     string            `json:"power_mode,omitempty"`
	Swap          string            `json:"swap,omitempty"`
	RuntimeLimits map[string]string `json:"runtime_limits,omitempty"`
	Env           map[string]string `json:"env,omitempty"`
}

type RunOrder struct {
	SchemaVersion string          `json:"schema_version"`
	PairID        string          `json:"pair_id"`
	Seed          int64           `json:"seed"`
	Order         []string        `json:"order"`
	Blocks        []ScheduleBlock `json:"blocks,omitempty"`
}

// ScheduleBlock describes the execution order of targets in one balanced
// three-target repetition. Order is a permutation of the manifest target
// identities, not a comparison direction; every target is still measured
// once per block and pairwise aggregation happens later by target identity.
type ScheduleBlock struct {
	Index int      `json:"index"`
	Order []string `json:"order"`
}

// ComparisonSpec is an explicit pairwise claim surface. Keeping the target
// identities and revisions with the comparison prevents V1/current-V2 and
// reference-V2 observations from being mixed in one aggregate.
type ComparisonSpec struct {
	ID                string `json:"id"`
	BaselineID        string `json:"baseline_id"`
	CandidateID       string `json:"candidate_id"`
	BaselineRevision  string `json:"baseline_revision,omitempty"`
	CandidateRevision string `json:"candidate_revision,omitempty"`
}

type Target struct {
	SchemaVersion    string        `json:"schema_version"`
	ID               string        `json:"id"`
	Role             string        `json:"role"`
	Kind             string        `json:"kind"`
	Revision         string        `json:"revision,omitempty"`
	DirtyHash        string        `json:"dirty_hash,omitempty"`
	Endpoint         string        `json:"endpoint,omitempty"`
	Protocol         string        `json:"protocol,omitempty"`
	InvalidationMode string        `json:"invalidation_mode,omitempty"`
	OutputPlugin     string        `json:"output_plugin,omitempty"`
	DatabaseName     string        `json:"database_name,omitempty"`
	PostgresVersion  string        `json:"postgres_version,omitempty"`
	MetadataHash     string        `json:"metadata_hash,omitempty"`
	ProcessPIDEnv    string        `json:"process_pid_env,omitempty"`
	ProcessPIDFile   string        `json:"process_pid_file,omitempty"`
	ExecutablePath   string        `json:"executable_path,omitempty"`
	ExecutableHash   string        `json:"executable_hash,omitempty"`
	FixturePath      string        `json:"fixture_path,omitempty"`
	FixtureHash      string        `json:"fixture_hash,omitempty"`
	Qualification    Qualification `json:"qualification"`
}

type Qualification struct {
	Passed  bool            `json:"passed"`
	Checks  map[string]bool `json:"checks"`
	Failure string          `json:"failure,omitempty"`
}

type Run struct {
	SchemaVersion              string                 `json:"schema_version"`
	ID                         string                 `json:"id"`
	PairID                     string                 `json:"pair_id"`
	TargetID                   string                 `json:"target_id"`
	TargetRevision             string                 `json:"target_revision,omitempty"`
	ScheduleBlock              int                    `json:"schedule_block,omitempty"`
	Family                     string                 `json:"family"`
	Scale                      int                    `json:"scale"`
	Seed                       int64                  `json:"seed"`
	StartedAt                  time.Time              `json:"started_at"`
	EndedAt                    time.Time              `json:"ended_at,omitempty"`
	MeasuredStartedAt          time.Time              `json:"measured_started_at,omitempty"`
	MeasuredFinishedAt         time.Time              `json:"measured_finished_at,omitempty"`
	PrimaryClass               FailureClass           `json:"primary_class"`
	Failure                    string                 `json:"failure,omitempty"`
	Measurements               map[string]Measurement `json:"measurements,omitempty"`
	PhaseDurations             map[string]Measurement `json:"phase_durations,omitempty"`
	WarmupMutations            int                    `json:"warmup_mutations,omitempty"`
	ExpectedLedgerRows         int                    `json:"expected_ledger_rows,omitempty"`
	ExpectedMutationRecipients int                    `json:"expected_mutation_recipients,omitempty"`
	EvidenceMaxFrames          int64                  `json:"evidence_max_frames,omitempty"`
	EvidenceMaxBytes           int64                  `json:"evidence_max_bytes,omitempty"`
	EvidenceMaxRetained        int                    `json:"evidence_max_retained_frames,omitempty"`
	ProtocolErrors             []string               `json:"protocol_errors,omitempty"`
	BehaviorErrors             []string               `json:"behavior_errors,omitempty"`
	CollectorProvenance        map[string]string      `json:"collector_provenance,omitempty"`
}

type LedgerRow struct {
	SchemaVersion                     string    `json:"schema_version"`
	PairID                            string    `json:"pair_id"`
	RunID                             string    `json:"run_id"`
	WriterID                          string    `json:"writer_id"`
	RecipientID                       string    `json:"recipient_id"`
	ClientEventID                     string    `json:"client_event_id"`
	ServerTransactionID               string    `json:"server_transaction_id,omitempty"`
	ExpectedQuerySet                  []string  `json:"expected_query_set"`
	ExpectedRecipientSet              []string  `json:"expected_recipient_set"`
	SubmittedAt                       time.Time `json:"submitted_at"`
	AcknowledgementAt                 time.Time `json:"acknowledgement_at,omitempty"`
	CoverAt                           time.Time `json:"cover_at,omitempty"`
	ProcessedTransactionID            string    `json:"processed_transaction_id,omitempty"`
	ExpectedMaterializedDigest        string    `json:"expected_materialized_digest"`
	ObservedMaterializedDigest        string    `json:"observed_materialized_digest,omitempty"`
	ExpectedStateVersion              uint64    `json:"expected_state_version"`
	CoveredExpectedStateVersion       uint64    `json:"covered_expected_state_version,omitempty"`
	CoveredExpectedMaterializedDigest string    `json:"covered_expected_materialized_digest,omitempty"`
	ObservedStateVersion              uint64    `json:"observed_state_version,omitempty"`
	Coverage                          string    `json:"coverage"`
	RefreshBeforeAck                  bool      `json:"refresh_before_ack"`
	BufferedSnapshotAhead             bool      `json:"buffered_snapshot_ahead"`
	ConvergedAt                       time.Time `json:"converged_at,omitempty"`
	ErrorClass                        string    `json:"error_class,omitempty"`
	EvidenceRef                       string    `json:"evidence_ref,omitempty"`
}

type Frame struct {
	SchemaVersion          string      `json:"schema_version"`
	RunID                  string      `json:"run_id"`
	ClientID               string      `json:"client_id,omitempty"`
	QueryID                string      `json:"query_id,omitempty"`
	RecipientID            string      `json:"recipient_id"`
	ServerTransactionID    string      `json:"server_transaction_id,omitempty"`
	ReceivedAt             time.Time   `json:"received_at"`
	Kind                   string      `json:"kind"`
	Class                  string      `json:"class,omitempty"`
	ClientEventIDs         []string    `json:"client_event_ids,omitempty"`
	ProcessedTransactionID string      `json:"processed_transaction_id,omitempty"`
	MaterializedDigest     string      `json:"materialized_digest,omitempty"`
	PayloadBytes           Measurement `json:"payload_bytes"`
	PayloadDigest          string      `json:"payload_sha256,omitempty"`
	EvidenceRef            string      `json:"evidence_ref,omitempty"`
	ErrorClass             string      `json:"error_class,omitempty"`
}

type ProcessSample struct {
	At             time.Time   `json:"at"`
	PID            int         `json:"pid"`
	StartTime      string      `json:"start_time,omitempty"`
	ExecutableHash string      `json:"executable_hash,omitempty"`
	UserCPU        Measurement `json:"user_cpu"`
	SystemCPU      Measurement `json:"system_cpu"`
	RSS            Measurement `json:"rss"`
	PeakRSS        Measurement `json:"peak_rss"`
	Threads        Measurement `json:"threads"`
	FDs            Measurement `json:"fds"`
	ExitCode       Measurement `json:"exit_code"`
	Signal         string      `json:"signal,omitempty"`
}
type RuntimeSample struct {
	At         time.Time   `json:"at"`
	AllocBytes Measurement `json:"alloc_bytes"`
	LiveHeap   Measurement `json:"live_heap"`
	HeapGoal   Measurement `json:"heap_goal"`
	GCCycles   Measurement `json:"gc_cycles"`
	GCPause    Measurement `json:"gc_pause"`
	Goroutines Measurement `json:"goroutines"`
}
type DBSnapshot struct {
	At          time.Time   `json:"at"`
	Version     string      `json:"version,omitempty"`
	Connections Measurement `json:"connections"`
	BlockHits   Measurement `json:"block_hits"`
	BlockReads  Measurement `json:"block_reads"`
	TempBytes   Measurement `json:"temp_bytes"`
	TempFiles   Measurement `json:"temp_files"`
	Commits     Measurement `json:"commits"`
	Rollbacks   Measurement `json:"rollbacks"`
	TupleReads  Measurement `json:"tuple_reads"`
	TupleWrites Measurement `json:"tuple_writes"`
	WALBytes    Measurement `json:"wal_bytes"`
	SlotLag     Measurement `json:"slot_lag"`
	PoolActive  Measurement `json:"pool_active"`
	PoolIdle    Measurement `json:"pool_idle"`
}

type Summary struct {
	SchemaVersion     string              `json:"schema_version"`
	AggregatorVersion string              `json:"aggregator_version"`
	InputHashes       map[string]string   `json:"input_hashes"`
	Cells             []CellSummary       `json:"cells"`
	Comparisons       []ComparisonSummary `json:"comparisons,omitempty"`
	ClaimGate         ClaimGate           `json:"claim_gate"`
}
type CellSummary struct {
	ComparisonID      string                   `json:"comparison_id,omitempty"`
	BaselineID        string                   `json:"baseline_id,omitempty"`
	CandidateID       string                   `json:"candidate_id,omitempty"`
	BaselineRevision  string                   `json:"baseline_revision,omitempty"`
	CandidateRevision string                   `json:"candidate_revision,omitempty"`
	Family            string                   `json:"family"`
	Scale             int                      `json:"scale"`
	Attempts          int                      `json:"attempts"`
	Ratios            []float64                `json:"ratios,omitempty"`
	CI                CI                       `json:"ci"`
	Sign              SignResult               `json:"sign"`
	Metrics           map[string]MetricSummary `json:"metrics,omitempty"`
	Failures          []FailureClass           `json:"failures,omitempty"`
	ClaimGate         ClaimGate                `json:"claim_gate"`
	Direction         MetricDirection          `json:"direction"`
	EndpointClaims    map[string]ClaimGate     `json:"endpoint_claims,omitempty"`
}
type ComparisonSummary struct {
	ID                string        `json:"id"`
	BaselineID        string        `json:"baseline_id"`
	CandidateID       string        `json:"candidate_id"`
	BaselineRevision  string        `json:"baseline_revision,omitempty"`
	CandidateRevision string        `json:"candidate_revision,omitempty"`
	Cells             []CellSummary `json:"cells"`
	ClaimGate         ClaimGate     `json:"claim_gate"`
}
type MetricSummary struct {
	Name              string      `json:"name"`
	Unit              string      `json:"unit"`
	Denominator       string      `json:"denominator"`
	Samples           int         `json:"samples"`
	Median            Measurement `json:"median"`
	P99               Measurement `json:"p99"`
	AbsoluteV1        Measurement `json:"absolute_v1"`
	AbsoluteV2        Measurement `json:"absolute_v2"`
	AbsoluteBaseline  Measurement `json:"absolute_baseline,omitempty"`
	AbsoluteCandidate Measurement `json:"absolute_candidate,omitempty"`
}
type CI struct {
	Lower     float64 `json:"lower"`
	Upper     float64 `json:"upper"`
	Level     float64 `json:"level"`
	Resamples int     `json:"resamples"`
	Seed      int64   `json:"seed"`
}
type SignResult struct {
	Positive  int     `json:"positive"`
	Negative  int     `json:"negative"`
	Ties      int     `json:"ties"`
	PValue    float64 `json:"p_value"`
	Direction string  `json:"direction"`
}
type ClaimGate struct {
	Eligible bool     `json:"eligible"`
	Reasons  []string `json:"reasons,omitempty"`
}
type RawIndex struct {
	SchemaVersion string        `json:"schema_version"`
	Files         []IndexedFile `json:"files"`
}
type IndexedFile struct {
	Path     string `json:"path"`
	Bytes    int64  `json:"bytes"`
	SHA256   string `json:"sha256"`
	Encoding string `json:"encoding"`
}

type FailureClass string

const (
	Pass                     FailureClass = "pass"
	SetupInvalid             FailureClass = "setup_invalid"
	HarnessDefect            FailureClass = "harness_defect"
	TargetSemanticFailure    FailureClass = "target_semantic_failure"
	TargetProtocolFailure    FailureClass = "target_protocol_failure"
	TargetResourceExhaustion FailureClass = "target_resource_exhaustion"
	TargetCrash              FailureClass = "target_crash"
	TargetTimeout            FailureClass = "target_timeout"
	InfrastructureNoise      FailureClass = "infrastructure_noise"
	OperatorAbort            FailureClass = "operator_abort"
)
