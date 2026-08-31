package benchharness

import (
	"errors"
	"fmt"
)

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
	defaultEvidenceMaxRetainedFrames int   = 8192
	// SSE permits a data line up to 16 MiB; using that shared transport bound
	// keeps the derived cumulative budget valid for either live transport.
	maxEvidenceFrameBytes int64 = 16 << 20
	// With the canonical 20-second semantic-readiness timeout, the exponential
	// backoff permits at most 84 checks even if each check itself is immediate.
	maxEvidenceReadinessAttempts int = 84
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
	evidenceWriterSessions      int   = 8
	// Keep a small fixed allowance for handshake/control variants while the
	// lifecycle-specific terms account for every known bounded operation.
	evidenceLifecycleControlFrames int64 = 128
)

// MaxAbsoluteEvidenceBytes is the hard ceiling for a derived cumulative wire
// evidence budget. The counter/digest path does not retain payloads, but the
// ceiling keeps malformed workload inputs from turning a run into an
// effectively unbounded accounting operation.
const MaxAbsoluteEvidenceBytes int64 = 64 << 40

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
