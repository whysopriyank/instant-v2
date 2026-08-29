package benchrun

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	Fixture     FixtureIDs
	Environment Environment
	Now         func() time.Time
}

func (r *PairRunner) fixtureIDs() FixtureIDs {
	if r.Fixture == (FixtureIDs{}) {
		return DefaultFixtureIDs
	}
	return r.Fixture
}

func (r *PairRunner) Run(ctx context.Context) (Summary, error) {
	if r.Writer == nil || r.Executor == nil {
		return Summary{}, errors.New("pair runner requires writer and target executor")
	}
	if err := bindTargetKinds(r.Targets); err != nil {
		return Summary{}, err
	}
	if len(r.Targets) == 3 {
		return r.runThreeTarget(ctx)
	}
	if len(r.Targets) != 2 {
		return Summary{}, errors.New("pair runner requires exactly two targets or three-target mode")
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
	if err := bindManifestEvidenceBudget(&r.Manifest, r.Plan); err != nil {
		return Summary{}, err
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
		fixtureIDs := r.fixtureIDs()
		fixtureEvidence, fixtureErr := BuildFixtureEvidence(fixtureIDs, r.Manifest.Family, r.Manifest.SubscriberScale, r.Plan.Seed)
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
		configEvidence := LiveConfig{PairID: r.Manifest.PairID, Seed: r.Manifest.Seed, Family: r.Manifest.Family, Scale: r.Manifest.SubscriberScale, Fixture: r.fixtureIDs()}
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
			run := Run{SchemaVersion: SchemaVersion, ID: fmt.Sprintf("%s-%s", attemptPairID, targetID), PairID: attemptPairID, TargetID: targetID, Family: r.Manifest.Family, Scale: r.Manifest.SubscriberScale, Seed: r.Plan.Seed, StartedAt: r.Now().UTC(), PrimaryClass: Pass, EvidenceMaxFrames: r.Manifest.EvidenceMaxFrames, EvidenceMaxBytes: r.Manifest.EvidenceMaxBytes, EvidenceMaxRetained: r.Manifest.EvidenceMaxRetained}
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
			if run.EvidenceMaxFrames == 0 && run.EvidenceMaxBytes == 0 && run.EvidenceMaxRetained == 0 {
				run.EvidenceMaxFrames = r.Manifest.EvidenceMaxFrames
				run.EvidenceMaxBytes = r.Manifest.EvidenceMaxBytes
				run.EvidenceMaxRetained = r.Manifest.EvidenceMaxRetained
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

// runThreeTarget executes one seven-block schedule containing v1,
// v2_reference, and v2_current once per block. It deliberately shares the
// PairRunner seam so existing callers need only provide three Targets; the
// artifact shape and two-target path remain unchanged.
func (r *PairRunner) runThreeTarget(ctx context.Context) (Summary, error) {
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
	order, err := ThreeTargetSchedule(r.Plan.Seed, r.Manifest.PairID)
	if err != nil {
		return Summary{}, err
	}
	if err := ValidateThreeTargetOrder(order.Blocks); err != nil {
		return Summary{}, err
	}
	byID := make(map[string]Target, len(r.Targets))
	for _, target := range r.Targets {
		if !isThreeTargetID(target.ID) {
			return Summary{}, fmt.Errorf("unsupported three-target identity %q", target.ID)
		}
		if _, exists := byID[target.ID]; exists {
			return Summary{}, fmt.Errorf("duplicate three-target identity %q", target.ID)
		}
		byID[target.ID] = target
	}
	comparisons, err := DefaultThreeTargetComparisons(r.Targets)
	if err != nil {
		return Summary{}, err
	}
	if len(r.Manifest.Comparisons) == 0 {
		r.Manifest.Comparisons = comparisons
	} else if err := validateThreeTargetComparisons(r.Manifest.Comparisons, byID); err != nil {
		return Summary{}, err
	}
	if r.Manifest.TargetRevisions == nil {
		r.Manifest.TargetRevisions = make(map[string]string, len(byID))
	}
	for id, target := range byID {
		if revision, ok := r.Manifest.TargetRevisions[id]; ok && revision != target.Revision {
			return Summary{}, fmt.Errorf("manifest target revision for %s does not match target", id)
		}
		r.Manifest.TargetRevisions[id] = target.Revision
	}
	if len(r.Manifest.TargetRevisions) != len(byID) {
		return Summary{}, errors.New("manifest target revisions must name all three targets")
	}
	r.Manifest.TargetOrder = append([]ScheduleBlock(nil), order.Blocks...)
	r.Manifest.RunOrder = append([]string(nil), order.Order...)
	if err := bindManifestEvidenceBudget(&r.Manifest, r.Plan); err != nil {
		return Summary{}, err
	}
	totalBudget, fileBudget, err := ContractArtifactBudgetForTargets(r.Manifest.Family, r.Manifest.SubscriberScale, r.Plan, len(r.Targets))
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
	if err := validateThreeTargetManifest(r.Manifest); err != nil {
		return Summary{}, err
	}
	if r.Plan.Seed != r.Manifest.Seed {
		return Summary{}, errors.New("manifest and plan seeds must match")
	}
	if err := r.Writer.ConfigureContractBudget(totalBudget, fileBudget); err != nil {
		return Summary{}, err
	}
	if _, statErr := os.Stat(filepath.Join(r.Writer.Root, "fixture.json")); os.IsNotExist(statErr) {
		fixtureIDs := r.fixtureIDs()
		fixtureEvidence, fixtureErr := BuildFixtureEvidence(fixtureIDs, r.Manifest.Family, r.Manifest.SubscriberScale, r.Plan.Seed)
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
		configEvidence := LiveConfig{PairID: r.Manifest.PairID, Seed: r.Manifest.Seed, Family: r.Manifest.Family, Scale: r.Manifest.SubscriberScale, Fixture: r.fixtureIDs()}
		configHash, hashErr := DigestJSON(configEvidence)
		if hashErr != nil {
			return Summary{}, hashErr
		}
		r.Manifest.ConfigHash = configHash
		if err := r.Writer.WriteJSON("config.json", configEvidence); err != nil {
			return Summary{}, err
		}
	}
	qualificationFailed := make(map[string]bool, len(byID))
	for i, target := range r.Targets {
		q, qerr := r.Executor.Qualify(ctx, target)
		target.Qualification = q
		r.Targets[i] = target
		byID[target.ID] = target
		qualificationFailed[target.ID] = qerr != nil || !q.Passed
		if qerr != nil && target.Qualification.Failure == "" {
			target.Qualification.Failure = qerr.Error()
			r.Targets[i] = target
			byID[target.ID] = target
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
	for blockIndex, block := range order.Blocks {
		attemptPairID := fmt.Sprintf("%s-%02d", r.Manifest.PairID, blockIndex+1)
		for _, targetID := range block.Order {
			target := byID[targetID]
			run := Run{SchemaVersion: SchemaVersion, ID: fmt.Sprintf("%s-%s", attemptPairID, targetID), PairID: attemptPairID, TargetID: targetID, TargetRevision: target.Revision, ScheduleBlock: block.Index, Family: r.Manifest.Family, Scale: r.Manifest.SubscriberScale, Seed: r.Plan.Seed, StartedAt: r.Now().UTC(), PrimaryClass: Pass, EvidenceMaxFrames: r.Manifest.EvidenceMaxFrames, EvidenceMaxBytes: r.Manifest.EvidenceMaxBytes, EvidenceMaxRetained: r.Manifest.EvidenceMaxRetained}
			var result ExecutionResult
			var executeErr error
			if qualificationFailed[targetID] {
				run.PrimaryClass = SetupInvalid
				run.Failure = "target qualification failed"
			} else {
				result, executeErr = r.Executor.Execute(ctx, RunSpec{Run: run, Target: target, Plan: r.Plan, Attempt: blockIndex + 1, Order: strings.Join(block.Order, ">")})
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
			if run.EvidenceMaxFrames == 0 && run.EvidenceMaxBytes == 0 && run.EvidenceMaxRetained == 0 {
				run.EvidenceMaxFrames = r.Manifest.EvidenceMaxFrames
				run.EvidenceMaxBytes = r.Manifest.EvidenceMaxBytes
				run.EvidenceMaxRetained = r.Manifest.EvidenceMaxRetained
			}
			run.SchemaVersion = SchemaVersion
			run.ID = fmt.Sprintf("%s-%s", attemptPairID, targetID)
			run.PairID = attemptPairID
			run.TargetID = targetID
			if run.TargetRevision == "" {
				run.TargetRevision = target.Revision
			}
			run.ScheduleBlock = block.Index
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

func validateThreeTargetManifest(m Manifest) error {
	if m.SchemaVersion != SchemaVersion || m.BundleID == "" || m.PairID == "" || m.Family == "" || m.SubscriberScale <= 0 || m.Seed == 0 || m.StartedAt.IsZero() {
		return errors.New("three-target manifest missing required provenance")
	}
	if err := ValidateThreeTargetOrder(m.TargetOrder); err != nil {
		return err
	}
	if len(m.RunOrder) != ThreeTargetBlocks {
		return errors.New("three-target manifest run order must contain seven blocks")
	}
	for i, block := range m.TargetOrder {
		if m.RunOrder[i] != strings.Join(block.Order, ">") {
			return fmt.Errorf("three-target manifest run order does not match block %d", block.Index)
		}
	}
	if len(m.TargetRevisions) != 3 {
		return errors.New("three-target manifest requires three target revisions")
	}
	return nil
}

// ContractArtifactBudget computes the deterministic raw-artifact budget for a
// frozen workload before any bundle files are written.
func ContractArtifactBudget(family string, scale int, plan Plan) (int64, int64, error) {
	return ContractArtifactBudgetForTargets(family, scale, plan, 2)
}

// ContractEvidenceBudget computes the deterministic per-run live evidence
// budget from the same signed workload/plan inputs used by the artifact
// contract. It is persisted in the manifest so offline verification can
// reject a bundle whose run-level limit was changed after authorization.
func ContractEvidenceBudget(family string, scale int, plan Plan) (benchharness.EvidenceBudget, error) {
	workload, err := benchharness.NewWorkload(benchharness.Family(canonicalBenchmarkFamily(family)), scale, plan.Seed)
	if err != nil {
		return benchharness.EvidenceBudget{}, err
	}
	if plan.MeasureSeconds > 0 && (workload.Family == benchharness.FamilyR || workload.Family == benchharness.FamilyS) {
		// S/R behavior follows the signed run window, which is also bounded by
		// TargetDriver/Execute against the canonical workload duration.
		if err := benchharness.ValidateBehaviorWindow(workload, plan.MeasureSeconds); err != nil {
			return benchharness.EvidenceBudget{}, err
		}
		workload.DurationSeconds = plan.MeasureSeconds
	}
	mutations, err := contractMutationCount(workload, plan)
	if err != nil {
		return benchharness.EvidenceBudget{}, err
	}
	return benchharness.EvidenceBudgetForWorkloadWithWarmup(workload, mutations, plan.WarmupMutations)
}

func bindManifestEvidenceBudget(m *Manifest, plan Plan) error {
	if m == nil {
		return errors.New("manifest is required")
	}
	budget, err := ContractEvidenceBudget(m.Family, m.SubscriberScale, plan)
	if err != nil {
		return err
	}
	if (m.EvidenceMaxFrames != 0 && m.EvidenceMaxFrames != budget.MaxFrames) ||
		(m.EvidenceMaxBytes != 0 && m.EvidenceMaxBytes != budget.MaxBytes) ||
		(m.EvidenceMaxRetained != 0 && m.EvidenceMaxRetained != budget.MaxRetainedFrames) {
		return errors.New("manifest evidence budget does not match contract")
	}
	m.EvidenceMaxFrames = budget.MaxFrames
	m.EvidenceMaxBytes = budget.MaxBytes
	m.EvidenceMaxRetained = budget.MaxRetainedFrames
	return nil
}

func contractMutationCount(workload benchharness.Workload, plan Plan) (int, error) {
	mutations := workload.Measured
	if workload.Family == benchharness.FamilyT {
		return 4096, nil
	}
	if mutations <= 0 {
		if plan.MeasureSeconds <= 0 {
			return 1, nil
		}
		total, err := safeContractMultiply(int64(plan.MeasureSeconds), 8)
		if err != nil || total > int64(^uint(0)>>1) {
			return 0, errors.New("contract mutation count overflow")
		}
		mutations = int(total)
	}
	if mutations <= 0 {
		return 1, nil
	}
	return mutations, nil
}

// ContractArtifactBudgetForTargets derives limits for the number of target
// runs in one seven-block schedule. Two-target callers retain the historical
// ContractArtifactBudget behavior; three-target bundles reserve 21 runs.
func ContractArtifactBudgetForTargets(family string, scale int, plan Plan, targetCount int) (int64, int64, error) {
	if targetCount < 2 {
		return 0, 0, errors.New("at least two targets are required for an artifact budget")
	}
	workload, err := benchharness.NewWorkload(benchharness.Family(canonicalBenchmarkFamily(family)), scale, plan.Seed)
	if err != nil {
		return 0, 0, err
	}
	mutations, err := contractMutationCount(workload, plan)
	if err != nil {
		return 0, 0, err
	}
	rows, err := safeContractMultiply(int64(expectedLedgerCardinality(workload, mutations)), 1)
	if err != nil {
		return 0, 0, err
	}
	allRows, err := safeContractMultiply(rows, int64(targetCount))
	if err != nil {
		return 0, 0, err
	}
	allRows, err = safeContractMultiply(allRows, 7)
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
