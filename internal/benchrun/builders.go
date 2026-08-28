package benchrun

import (
	"fmt"
	"github.com/instant-v2/instant-v2/internal/benchharness"
	"regexp"
	"sort"
)

// FixtureIDs are provisioned schema identifiers. Builders refuse incomplete
// identifiers rather than inventing an entity type, attribute, or protocol
// field that could make a live run appear valid.
type FixtureIDs struct {
	EntityType string `json:"entity_type"`
	ValueAttr  string `json:"value_attr"`
	BucketAttr string `json:"bucket_attr"`
	RankAttr   string `json:"rank_attr"`
	// Query labels and transaction attribute UUIDs are separate namespaces.
	// ValueAttr/BucketAttr/RankAttr are query labels only; transaction steps
	// accept the explicit UUID fields below and never fall back to labels.
	QueryEntity  string `json:"query_entity,omitempty"`
	QueryBucket  string `json:"query_bucket_attr,omitempty"`
	QueryRank    string `json:"query_rank_attr,omitempty"`
	ValueAttrID  string `json:"value_attr_id,omitempty"`
	BucketAttrID string `json:"bucket_attr_id,omitempty"`
	RankAttrID   string `json:"rank_attr_id,omitempty"`
}

// FixtureEvidence is the canonical, reproducible fixture contract persisted
// in every benchmark bundle. IDs/labels describe the wire fixture while the
// generated entity/query assignment and initial hashes prove the seed was
// resolved deterministically rather than inferred from observed rows.
type FixtureEvidence struct {
	FixtureIDs
	Family                string                   `json:"family"`
	Scale                 int                      `json:"scale"`
	Seed                  int64                    `json:"seed"`
	SeedEntities          int                      `json:"seed_entities"`
	EntityIDs             []string                 `json:"entity_ids"`
	QueryAssignments      []FixtureQueryAssignment `json:"query_assignments"`
	InitialSemanticHashes map[string]string        `json:"initial_semantic_hashes"`
}

type FixtureQueryAssignment struct {
	ID       string `json:"id"`
	Bucket   int    `json:"bucket"`
	MatchAll bool   `json:"match_all"`
	TopN     int    `json:"top_n"`
}

func canonicalBenchmarkFamily(family string) string {
	switch family {
	case "H":
		return string(benchharness.FamilyH)
	case "X":
		return string(benchharness.FamilyX)
	case "M":
		return string(benchharness.FamilyM)
	case "O":
		return string(benchharness.FamilyO)
	case "S":
		return string(benchharness.FamilyS)
	case "R":
		return string(benchharness.FamilyR)
	case "C":
		return string(benchharness.FamilyC)
	case "T":
		return string(benchharness.FamilyT)
	default:
		return family
	}
}

// BuildFixtureEvidence reconstructs the exact generated fixture from the
// frozen family/scale/seed. It is used for both live and synthetic bundles.
func BuildFixtureEvidence(ids FixtureIDs, family string, scale int, seed int64) (FixtureEvidence, error) {
	family = canonicalBenchmarkFamily(family)
	w, err := benchharness.NewWorkload(benchharness.Family(family), scale, seed)
	if err != nil {
		return FixtureEvidence{}, err
	}
	entityIDs := make([]string, 0, len(w.Fixture.Entities))
	for id := range w.Fixture.Entities {
		entityIDs = append(entityIDs, id)
	}
	sort.Strings(entityIDs)
	assignments := make([]FixtureQueryAssignment, 0, len(w.Fixture.Queries))
	hashes := make(map[string]string, len(w.Fixture.Queries))
	oracle := benchharness.NewPrefixOracle(w.Fixture)
	for _, q := range w.Fixture.Queries {
		assignments = append(assignments, FixtureQueryAssignment{ID: q.ID, Bucket: q.Bucket, MatchAll: q.MatchAll, TopN: q.TopN})
		hash, err := oracle.ExpectedDigest(q.ID, 0)
		if err != nil {
			return FixtureEvidence{}, err
		}
		hashes[q.ID] = hash
	}
	return FixtureEvidence{FixtureIDs: ids, Family: family, Scale: scale, Seed: w.Seed, SeedEntities: w.SeedEntities, EntityIDs: entityIDs, QueryAssignments: assignments, InitialSemanticHashes: hashes}, nil
}

var uuidIDRE = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func (ids FixtureIDs) validate() error {
	if err := ids.validateQuery(); err != nil {
		return err
	}
	if ids.ValueAttrID == "" {
		return fmt.Errorf("fixture value attribute UUID is required for transaction builders")
	}
	for name, value := range map[string]string{"value_attr_id": ids.ValueAttrID, "bucket_attr_id": ids.BucketAttrID, "rank_attr_id": ids.RankAttrID} {
		if value != "" && !uuidIDRE.MatchString(value) {
			return fmt.Errorf("fixture %s must be a UUID", name)
		}
	}
	return nil
}
func (ids FixtureIDs) validateQuery() error {
	if ids.queryEntity() == "" {
		return fmt.Errorf("fixture query entity label is required")
	}
	return nil
}
func (ids FixtureIDs) queryEntity() string {
	if ids.QueryEntity != "" {
		return ids.QueryEntity
	}
	return ids.EntityType
}
func (ids FixtureIDs) bucketID() string {
	return ids.BucketAttrID
}
func (ids FixtureIDs) rankID() string {
	return ids.RankAttrID
}
func (ids FixtureIDs) valueID() string {
	return ids.ValueAttrID
}
func DefaultInstaQLQuery(ids FixtureIDs, q benchharness.Query) (any, error) {
	if err := ids.validateQuery(); err != nil {
		return nil, err
	}
	body := map[string]any{}
	options := map[string]any{}
	if q.Bucket >= 0 {
		label := ids.QueryBucket
		if label == "" {
			label = ids.BucketAttr
		}
		if label == "" {
			return nil, fmt.Errorf("bucket attribute ID required for bucket query")
		}
		options["where"] = map[string]any{label: q.Bucket}
	}
	if q.TopN > 0 {
		label := ids.QueryRank
		if label == "" {
			label = ids.RankAttr
		}
		if label == "" {
			return nil, fmt.Errorf("rank attribute ID required for ordered query")
		}
		options["order"] = map[string]any{label: "asc"}
		options["limit"] = q.TopN
	}
	if len(options) > 0 {
		body["$"] = options
	}
	return map[string]any{ids.queryEntity(): body}, nil
}
func DefaultTransactionSteps(ids FixtureIDs, m benchharness.Mutation) ([]any, error) {
	if err := ids.validate(); err != nil {
		return nil, err
	}
	if m.EntityID == "" {
		return nil, fmt.Errorf("mutation entity ID is required")
	}
	switch m.Kind {
	case benchharness.MutationAppend:
		if ids.bucketID() == "" || ids.rankID() == "" {
			return nil, fmt.Errorf("append requires value, bucket, and rank attribute IDs")
		}
		return []any{[]any{"add-triple", m.EntityID, ids.valueID(), m.Marker}, []any{"add-triple", m.EntityID, ids.bucketID(), m.Bucket}, []any{"add-triple", m.EntityID, ids.rankID(), m.Rank}}, nil
	case benchharness.MutationUpdate:
		return []any{[]any{"add-triple", m.EntityID, ids.valueID(), m.Marker}}, nil
	case benchharness.MutationReorder:
		if ids.rankID() == "" {
			return nil, fmt.Errorf("reorder requires rank attribute ID")
		}
		return []any{[]any{"add-triple", m.EntityID, ids.rankID(), m.Rank}}, nil
	case benchharness.MutationRetract:
		return []any{[]any{"delete-entity", m.EntityID}}, nil
	default:
		return nil, fmt.Errorf("unsupported mutation kind %q", m.Kind)
	}
}
func ApplyDefaultBuilders(cfg *benchharness.TargetConfig, ids FixtureIDs) error {
	if cfg == nil {
		return fmt.Errorf("target config is nil")
	}
	if err := ids.validate(); err != nil {
		return err
	}
	cfg.QueryBuilder = func(q benchharness.Query) (any, error) { return DefaultInstaQLQuery(ids, q) }
	cfg.TransactionBuilder = func(m benchharness.Mutation) ([]any, error) { return DefaultTransactionSteps(ids, m) }
	return nil
}
