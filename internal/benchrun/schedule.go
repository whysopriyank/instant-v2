package benchrun

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
)

const ThreeTargetBlocks = 7

var threeTargetIDs = []string{"v1", "v2_reference", "v2_current"}

// PairOrder returns exactly seven deterministic AB/BA attempts. A Fisher-Yates
// shuffle is used so the seed is the sole source of treatment-order variation.
func PairOrder(seed int64, pairID string) ([]string, error) {
	if pairID == "" {
		return nil, fmt.Errorf("pair id is required")
	}
	r := rand.New(rand.NewSource(seed))
	out := make([]string, 7)
	for i := range out {
		if i%2 == 0 {
			out[i] = "AB"
		} else {
			out[i] = "BA"
		}
	}
	r.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out, nil
}
func Schedule(seed int64, pairID string) (RunOrder, error) {
	order, e := PairOrder(seed, pairID)
	return RunOrder{SchemaVersion: SchemaVersion, PairID: pairID, Seed: seed, Order: order}, e
}

// ThreeTargetOrder returns seven deterministic blocks for the three-target
// benchmark. The six possible permutations are each used once, and one
// seeded permutation is repeated. Consequently every target occupies every
// position either two or three times, while the seed remains the sole source
// of order variation.
func ThreeTargetOrder(seed int64, pairID string) ([]ScheduleBlock, error) {
	if pairID == "" {
		return nil, fmt.Errorf("pair id is required")
	}
	permutations := make([][]string, 0, 6)
	base := append([]string(nil), threeTargetIDs...)
	var visit func([]string, []string)
	visit = func(prefix, remaining []string) {
		if len(remaining) == 0 {
			permutations = append(permutations, append([]string(nil), prefix...))
			return
		}
		for i, id := range remaining {
			nextRemaining := make([]string, 0, len(remaining)-1)
			nextRemaining = append(nextRemaining, remaining[:i]...)
			nextRemaining = append(nextRemaining, remaining[i+1:]...)
			visit(append(prefix, id), nextRemaining)
		}
	}
	visit(nil, base)
	// The recursive traversal is deliberately lexicographic, making the
	// complete-permutation invariant easy to audit before the seeded shuffle.
	sort.Slice(permutations, func(i, j int) bool {
		for k := range permutations[i] {
			if permutations[i][k] != permutations[j][k] {
				return permutations[i][k] < permutations[j][k]
			}
		}
		return false
	})
	r := rand.New(rand.NewSource(seed))
	r.Shuffle(len(permutations), func(i, j int) { permutations[i], permutations[j] = permutations[j], permutations[i] })
	duplicate := append([]string(nil), permutations[r.Intn(len(permutations))]...)
	permutations = append(permutations, duplicate)
	blocks := make([]ScheduleBlock, len(permutations))
	for i, permutation := range permutations {
		blocks[i] = ScheduleBlock{Index: i + 1, Order: append([]string(nil), permutation...)}
	}
	return blocks, nil
}

// ThreeTargetSchedule is the serialized schedule for a three-target run.
// Order contains a compact, human-readable representation for schema/tooling
// that only knows the legacy string field; Blocks remains authoritative and
// prevents consumers from interpreting a target permutation as AB/BA.
func ThreeTargetSchedule(seed int64, pairID string) (RunOrder, error) {
	blocks, err := ThreeTargetOrder(seed, pairID)
	order := make([]string, 0, len(blocks))
	for _, block := range blocks {
		order = append(order, strings.Join(block.Order, ">"))
	}
	return RunOrder{SchemaVersion: SchemaVersion, PairID: pairID, Seed: seed, Order: order, Blocks: blocks}, err
}

// ValidateThreeTargetOrder checks the persisted schedule independently of the
// generator. It rejects omitted/duplicated target identities and schedules
// that do not distribute each target across all three positions.
func ValidateThreeTargetOrder(blocks []ScheduleBlock) error {
	if len(blocks) != ThreeTargetBlocks {
		return fmt.Errorf("three-target schedule requires exactly %d blocks", ThreeTargetBlocks)
	}
	counts := [3]map[string]int{{}, {}, {}}
	permutations := make(map[string]int, len(blocks))
	for i, block := range blocks {
		if block.Index != i+1 {
			return fmt.Errorf("three-target block index %d is not %d", block.Index, i+1)
		}
		if len(block.Order) != len(threeTargetIDs) {
			return fmt.Errorf("three-target block %d must contain three targets", block.Index)
		}
		seen := make(map[string]bool, len(block.Order))
		key := ""
		for position, id := range block.Order {
			if !isThreeTargetID(id) || seen[id] {
				return fmt.Errorf("three-target block %d contains invalid or duplicate target %q", block.Index, id)
			}
			seen[id] = true
			counts[position][id]++
			if position > 0 {
				key += ">"
			}
			key += id
		}
		permutations[key]++
	}
	if len(permutations) != 6 {
		return fmt.Errorf("three-target schedule must contain all six permutations")
	}
	for position := range counts {
		for _, id := range threeTargetIDs {
			if counts[position][id] < 2 || counts[position][id] > 3 {
				return fmt.Errorf("target %s occupies position %d %d times, want two or three", id, position+1, counts[position][id])
			}
		}
	}
	return nil
}

// DefaultThreeTargetComparisons returns the two independent claim surfaces
// for the canonical Wave 6 target set. The reference target is never used as
// a substitute for the current target in the primary V1 comparison.
func DefaultThreeTargetComparisons(targets []Target) ([]ComparisonSpec, error) {
	byID := make(map[string]Target, len(targets))
	for _, target := range targets {
		if !isThreeTargetID(target.ID) {
			return nil, fmt.Errorf("unsupported three-target identity %q", target.ID)
		}
		if _, exists := byID[target.ID]; exists {
			return nil, fmt.Errorf("duplicate three-target identity %q", target.ID)
		}
		byID[target.ID] = target
	}
	if len(byID) != len(threeTargetIDs) {
		return nil, fmt.Errorf("three-target mode requires v1, v2_reference, and v2_current")
	}
	return []ComparisonSpec{
		{ID: "v1-v2_current", BaselineID: "v1", CandidateID: "v2_current", BaselineRevision: byID["v1"].Revision, CandidateRevision: byID["v2_current"].Revision},
		{ID: "v2_reference-v2_current", BaselineID: "v2_reference", CandidateID: "v2_current", BaselineRevision: byID["v2_reference"].Revision, CandidateRevision: byID["v2_current"].Revision},
	}, nil
}

func validateThreeTargetComparisons(comparisons []ComparisonSpec, targets map[string]Target) error {
	if len(comparisons) != 2 {
		return fmt.Errorf("three-target mode requires exactly two comparisons")
	}
	want := map[string][2]string{
		"v1-v2_current":           {"v1", "v2_current"},
		"v2_reference-v2_current": {"v2_reference", "v2_current"},
	}
	seen := make(map[string]bool, len(comparisons))
	for _, comparison := range comparisons {
		roles, ok := want[comparison.ID]
		if !ok || seen[comparison.ID] || comparison.BaselineID != roles[0] || comparison.CandidateID != roles[1] {
			return fmt.Errorf("invalid three-target comparison %q", comparison.ID)
		}
		baseline, baselineOK := targets[comparison.BaselineID]
		candidate, candidateOK := targets[comparison.CandidateID]
		if !baselineOK || !candidateOK {
			return fmt.Errorf("comparison %q references an unknown target", comparison.ID)
		}
		if comparison.BaselineRevision != baseline.Revision || comparison.CandidateRevision != candidate.Revision {
			return fmt.Errorf("comparison %q revision does not match target provenance", comparison.ID)
		}
		seen[comparison.ID] = true
	}
	for id := range want {
		if !seen[id] {
			return fmt.Errorf("missing three-target comparison %q", id)
		}
	}
	return nil
}

func isThreeTargetID(id string) bool {
	for _, want := range threeTargetIDs {
		if id == want {
			return true
		}
	}
	return false
}
