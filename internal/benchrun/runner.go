package benchrun

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/instant-v2/instant-v2/internal/benchharness"
)

// TargetExecutor is the narrow WP5-A seam. It owns protocol sessions and
// workload execution; WP5-B owns attempt retention and artifact encoding.
type TargetExecutor interface {
	Qualify(context.Context, Target) (Qualification, error)
	Execute(context.Context, RunSpec) (ExecutionResult, error)
}

// TargetDriver is the deliberately thin seam for the live WP5-A driver. It
// contains no fallback behavior: an absent driver is an error, never a
// synthetic success.
type TargetDriver interface {
	QualifyTarget(context.Context, Target) (Qualification, error)
	RunTarget(context.Context, RunSpec) (ExecutionResult, error)
}
type TargetDriverAdapter struct{ Driver TargetDriver }

func (a TargetDriverAdapter) Qualify(ctx context.Context, target Target) (Qualification, error) {
	if a.Driver == nil {
		return Qualification{}, errors.New("live target driver is not configured")
	}
	return a.Driver.QualifyTarget(ctx, target)
}
func (a TargetDriverAdapter) Execute(ctx context.Context, spec RunSpec) (ExecutionResult, error) {
	if a.Driver == nil {
		return ExecutionResult{}, errors.New("live target driver is not configured")
	}
	return a.Driver.RunTarget(ctx, spec)
}

type RunSpec struct {
	Run     Run
	Target  Target
	Plan    Plan
	Attempt int
	Order   string
}
type ExecutionResult struct {
	Run      Run
	Ledger   []LedgerRow
	Frames   []Frame
	Process  []ProcessSample
	Runtime  []RuntimeSample
	DBBefore DBSnapshot
	DBAfter  DBSnapshot
	Stdout   []byte
	Stderr   []byte
}
type RunError struct {
	Class FailureClass
	Err   error
}

func (e *RunError) Error() string {
	if e.Err == nil {
		return string(e.Class)
	}
	return fmt.Sprintf("%s: %v", e.Class, e.Err)
}
func (e *RunError) Unwrap() error { return e.Err }
func classify(err error) FailureClass {
	var re *RunError
	if errors.As(err, &re) && re.Class != "" {
		return re.Class
	}
	return HarnessDefect
}

type PairRunner struct {
	Writer      *ArtifactWriter
	Executor    TargetExecutor
	Manifest    Manifest
	Plan        Plan
	Targets     []Target
	Environment Environment
	Now         func() time.Time
}

func (r *PairRunner) Run(ctx context.Context) (Summary, error) {
	if r.Writer == nil || r.Executor == nil {
		return Summary{}, errors.New("pair runner requires writer and target executor")
	}
	if len(r.Targets) != 2 {
		return Summary{}, errors.New("pair runner requires exactly two targets")
	}
	if r.Plan.Pairs != 7 {
		r.Plan.Pairs = 7
	}
	if len(r.Plan.Families) == 0 {
		r.Plan.Families = []string{r.Manifest.Family}
	}
	if len(r.Plan.Scales) == 0 {
		r.Plan.Scales = []int{r.Manifest.SubscriberScale}
	}
	if r.Now == nil {
		r.Now = time.Now
	}
	totalBudget, fileBudget, err := ContractArtifactBudget(r.Manifest.Family, r.Manifest.SubscriberScale, r.Plan)
	if err != nil {
		return Summary{}, err
	}
	if r.Manifest.ArtifactMaxTotalBytes != 0 && r.Manifest.ArtifactMaxTotalBytes != totalBudget {
		return Summary{}, errors.New("manifest artifact total budget does not match contract")
	}
	if r.Manifest.ArtifactMaxFileBytes != 0 && r.Manifest.ArtifactMaxFileBytes != fileBudget {
		return Summary{}, errors.New("manifest artifact file budget does not match contract")
	}
	r.Manifest.ArtifactMaxTotalBytes = totalBudget
	r.Manifest.ArtifactMaxFileBytes = fileBudget
	if err := ValidateManifest(r.Manifest); err != nil {
		return Summary{}, err
	}
	for _, target := range r.Targets {
		switch target.ID {
		case "v1":
			if target.Revision != "" && (r.Manifest.V1SHA == "" || r.Manifest.V1SHA != target.Revision) {
				return Summary{}, errors.New("manifest v1_sha does not match target revision")
			}
		case "v2", "v2_current", "v2-current":
			if target.Revision != "" && (r.Manifest.V2SHA == "" || r.Manifest.V2SHA != target.Revision) {
				return Summary{}, errors.New("manifest v2_sha does not match target revision")
			}
		}
	}
	if r.Plan.Seed != r.Manifest.Seed {
		return Summary{}, errors.New("manifest and plan seeds must match")
	}
	if err := r.Writer.ConfigureContractBudget(totalBudget, fileBudget); err != nil {
		return Summary{}, err
	}
	order, err := Schedule(r.Plan.Seed, r.Manifest.PairID)
	if err != nil {
		return Summary{}, err
	}
	r.Manifest.RunOrder = append([]string(nil), order.Order...)
	// Persist the deterministic fixture contract before any run artifacts so
	// offline replay can reconstruct entity IDs, query assignments, and initial
	// semantic hashes even for synthetic tests.
	if _, statErr := os.Stat(filepath.Join(r.Writer.Root, "fixture.json")); os.IsNotExist(statErr) {
		fixtureEvidence, fixtureErr := BuildFixtureEvidence(FixtureIDs{}, r.Manifest.Family, r.Manifest.SubscriberScale, r.Plan.Seed)
		if fixtureErr != nil {
			return Summary{}, fixtureErr
		}
		fixtureHash, fixtureErr := DigestJSON(fixtureEvidence)
		if fixtureErr != nil {
			return Summary{}, fixtureErr
		}
		r.Manifest.FixtureHash = fixtureHash
		if err := r.Writer.WriteJSON("fixture.json", fixtureEvidence); err != nil {
			return Summary{}, err
		}
	}
	if _, statErr := os.Stat(filepath.Join(r.Writer.Root, "config.json")); os.IsNotExist(statErr) {
		configEvidence := LiveConfig{PairID: r.Manifest.PairID, Seed: r.Manifest.Seed, Family: r.Manifest.Family, Scale: r.Manifest.SubscriberScale}
		configHash, hashErr := DigestJSON(configEvidence)
		if hashErr != nil {
			return Summary{}, hashErr
		}
		r.Manifest.ConfigHash = configHash
		if err := r.Writer.WriteJSON("config.json", configEvidence); err != nil {
			return Summary{}, err
		}
	}
	qualificationFailed := map[string]bool{}
	for _, target := range r.Targets {
		q, qerr := r.Executor.Qualify(ctx, target)
		target.Qualification = q
		qualificationFailed[target.ID] = qerr != nil || !q.Passed
		if qerr != nil && target.Qualification.Failure == "" {
			target.Qualification.Failure = qerr.Error()
		}
		if err := r.Writer.WriteJSON(filepath.Join("targets", target.ID+".json"), target); err != nil {
			return Summary{}, err
		}
	}
	if err := r.Writer.WriteJSON("manifest.json", r.Manifest); err != nil {
		return Summary{}, err
	}
	if err := r.Writer.WriteJSON("plan.json", r.Plan); err != nil {
		return Summary{}, err
	}
	if err := r.Writer.WriteJSON("run-order.json", order); err != nil {
		return Summary{}, err
	}
	environment := r.Environment
	if environment.SchemaVersion == "" {
		environment.SchemaVersion = SchemaVersion
	}
	if err := r.Writer.WriteJSON("environment.json", environment); err != nil {
		return Summary{}, err
	}
	for attempt, sideOrder := range order.Order {
		ids := []string{"v1", "v2"}
		if sideOrder == "BA" {
			ids[0], ids[1] = ids[1], ids[0]
		}
		for _, targetID := range ids {
			var target Target
			for _, candidate := range r.Targets {
				if candidate.ID == targetID || targetID == "v2" && candidate.Role == "v2_current" {
					target = candidate
				}
			}
			if target.ID == "" {
				return Summary{}, fmt.Errorf("target %s not provided", targetID)
			}
			attemptPairID := fmt.Sprintf("%s-%02d", r.Manifest.PairID, attempt+1)
			run := Run{SchemaVersion: SchemaVersion, ID: fmt.Sprintf("%s-%s", attemptPairID, targetID), PairID: attemptPairID, TargetID: targetID, Family: r.Manifest.Family, Scale: r.Manifest.SubscriberScale, Seed: r.Plan.Seed, StartedAt: r.Now().UTC(), PrimaryClass: Pass}
			var result ExecutionResult
			var executeErr error
			if qualificationFailed[target.ID] {
				run.PrimaryClass = SetupInvalid
				run.Failure = "target qualification failed"
			} else {
				result, executeErr = r.Executor.Execute(ctx, RunSpec{Run: run, Target: target, Plan: r.Plan, Attempt: attempt + 1, Order: sideOrder})
			}
			if executeErr != nil {
				run.PrimaryClass = classify(executeErr)
				run.Failure = executeErr.Error()
			}
			if result.Run.ID != "" {
				run = result.Run
				if executeErr != nil {
					run.PrimaryClass = classify(executeErr)
					run.Failure = executeErr.Error()
				} else if run.PrimaryClass == "" {
					run.PrimaryClass = Pass
				}
			}
			run.SchemaVersion = SchemaVersion
			run.ID = fmt.Sprintf("%s-%s", attemptPairID, targetID)
			run.PairID = attemptPairID
			run.TargetID = targetID
			run.Family = r.Manifest.Family
			run.Scale = r.Manifest.SubscriberScale
			if run.EndedAt.IsZero() {
				run.EndedAt = r.Now().UTC()
			}
			if err := r.writeRun(run, result); err != nil {
				return Summary{}, err
			}
		}
	}
	summary, err := reportFromArtifacts(r.Writer.Root, false)
	if err != nil {
		return Summary{}, err
	}
	if err := r.Writer.WriteJSON("summary.json", summary); err != nil {
		return Summary{}, err
	}
	if err := r.Writer.WriteText("report.md", []byte(RenderMarkdown(summary))); err != nil {
		return Summary{}, err
	}
	if err := r.Writer.Finalize(); err != nil {
		return Summary{}, err
	}
	return ReportFromArtifacts(r.Writer.Root)
}

// ContractArtifactBudget computes the deterministic raw-artifact budget for a
// frozen workload before any bundle files are written.
func ContractArtifactBudget(family string, scale int, plan Plan) (int64, int64, error) {
	workload, err := benchharness.NewWorkload(benchharness.Family(canonicalBenchmarkFamily(family)), scale, plan.Seed)
	if err != nil {
		return 0, 0, err
	}
	mutations := workload.Measured
	if workload.Family != benchharness.FamilyT && mutations <= 0 {
		mutations = plan.MeasureSeconds * 8
	}
	if mutations <= 0 {
		mutations = 1
	}
	rows, err := safeContractMultiply(int64(expectedLedgerCardinality(workload, mutations)), 1)
	if err != nil {
		return 0, 0, err
	}
	allRows, err := safeContractMultiply(rows, 14)
	if err != nil {
		return 0, 0, err
	}
	rowBytes, err := contractLedgerRowBound(scale, len(workload.Fixture.Queries))
	if err != nil {
		return 0, 0, err
	}
	ledgerBytes, err := safeContractMultiply(rows, rowBytes)
	if err != nil {
		return 0, 0, err
	}
	perFile, err := safeContractAdd(ledgerBytes, 1<<20)
	if err != nil {
		return 0, 0, err
	}
	allBytes, err := safeContractMultiply(allRows, rowBytes)
	if err != nil {
		return 0, 0, err
	}
	total, err := safeContractAdd(allBytes, 64<<20)
	if err != nil {
		return 0, 0, err
	}
	if total <= 0 || perFile <= 0 || total > MaxAbsoluteArtifactBudget || perFile > MaxAbsoluteArtifactBudget {
		return 0, 0, errors.New("contract artifact budget exceeds hard ceiling")
	}
	return total, perFile, nil
}

func configureContractArtifactBudget(w *ArtifactWriter, family string, scale int, plan Plan) error {
	total, perFile, err := ContractArtifactBudget(family, scale, plan)
	if err != nil {
		return err
	}
	return w.ConfigureContractBudget(total, perFile)
}

// contractLedgerRowBound covers the largest expected query/recipient arrays,
// IDs, digests, timestamps, and JSON punctuation. Recipient/query cardinality
// is intentionally part of the bound: repeating ExpectedRecipientSet at scale
// is the dominant cost for large H/X cells.
func contractLedgerRowBound(scale, queryCount int) (int64, error) {
	if scale <= 0 || queryCount <= 0 {
		return 0, errors.New("invalid contract ledger cardinality")
	}
	const (
		baseBytes       int64 = 4096
		idEntryMaxBytes int64 = 96
	)
	recipientBytes, err := safeContractMultiply(int64(scale), idEntryMaxBytes)
	if err != nil {
		return 0, err
	}
	queryBytes, err := safeContractMultiply(int64(queryCount), idEntryMaxBytes)
	if err != nil {
		return 0, err
	}
	row, err := safeContractAdd(baseBytes, recipientBytes)
	if err != nil {
		return 0, err
	}
	return safeContractAdd(row, queryBytes)
}

func safeContractMultiply(a, b int64) (int64, error) {
	if a < 0 || b < 0 || (b != 0 && a > (1<<63-1)/b) {
		return 0, errors.New("contract artifact budget overflow")
	}
	return a * b, nil
}

func safeContractAdd(a, b int64) (int64, error) {
	if a < 0 || b < 0 || a > (1<<63-1)-b {
		return 0, errors.New("contract artifact budget overflow")
	}
	return a + b, nil
}

func (r *PairRunner) writeRun(run Run, result ExecutionResult) error {
	base := filepath.Join("runs", run.ID)
	if err := r.Writer.WriteJSON(filepath.Join(base, "run.json"), run); err != nil {
		return err
	}
	if err := r.Writer.WriteJSONL(filepath.Join(base, "ledger.jsonl"), result.Ledger); err != nil {
		return err
	}
	if err := r.Writer.WriteJSONL(filepath.Join(base, "frames.jsonl"), result.Frames); err != nil {
		return err
	}
	if err := r.Writer.WriteJSONL(filepath.Join(base, "process.jsonl"), result.Process); err != nil {
		return err
	}
	if err := r.Writer.WriteJSONL(filepath.Join(base, "runtime-metrics.jsonl"), result.Runtime); err != nil {
		return err
	}
	if err := r.Writer.WriteJSON(filepath.Join(base, "db-before.json"), result.DBBefore); err != nil {
		return err
	}
	if err := r.Writer.WriteJSON(filepath.Join(base, "db-after.json"), result.DBAfter); err != nil {
		return err
	}
	if err := r.Writer.WriteLog(filepath.Join(base, "stdout.log"), result.Stdout); err != nil {
		return err
	}
	return r.Writer.WriteLog(filepath.Join(base, "stderr.log"), result.Stderr)
}

type SyntheticExecutor struct {
	FailAttempt int
	FailClass   FailureClass
}

func (s SyntheticExecutor) Qualify(context.Context, Target) (Qualification, error) {
	return Qualification{Passed: true, Checks: map[string]bool{"synthetic": true}}, nil
}
func (s SyntheticExecutor) Execute(_ context.Context, spec RunSpec) (ExecutionResult, error) {
	result := ExecutionResult{Run: spec.Run}
	if s.FailAttempt == spec.Attempt {
		return result, &RunError{Class: s.FailClass, Err: errors.New("synthetic failure")}
	}
	result.Run.Measurements = map[string]Measurement{"primary": {Status: StatusValue, Value: 10, Unit: "ms"}}
	now := time.Now().UTC()
	result.Run.ExpectedLedgerRows = 1
	result.Run.ExpectedMutationRecipients = 1
	result.Ledger = []LedgerRow{{SchemaVersion: SchemaVersion, RunID: spec.Run.ID, PairID: spec.Run.PairID, WriterID: "synthetic-writer", RecipientID: "synthetic-recipient", ClientEventID: spec.Run.ID + "/event-1", ExpectedQuerySet: []string{"synthetic-query"}, ExpectedRecipientSet: []string{"synthetic-recipient"}, ExpectedMaterializedDigest: "synthetic-digest", ObservedMaterializedDigest: "synthetic-digest", Coverage: "exact", SubmittedAt: now, AcknowledgementAt: now, CoverAt: now, ConvergedAt: now}}
	result.Frames = []Frame{{SchemaVersion: SchemaVersion, RunID: spec.Run.ID, RecipientID: "synthetic-recipient", ReceivedAt: now, Kind: "refresh", MaterializedDigest: "synthetic-digest", PayloadBytes: Zero("bytes")}}
	return result, nil
}
