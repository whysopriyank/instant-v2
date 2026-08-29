package benchrun

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/instant-v2/instant-v2/internal/benchharness"
)

// FixtureIDs are provisioned schema identifiers. Builders refuse incomplete
// identifiers rather than inventing an entity type, attribute, or protocol
// field that could make a live run appear valid.
type FixtureIDs struct {
	EntityType string `json:"entity_type"`
	IDAttr     string `json:"id_attr"`
	ValueAttr  string `json:"value_attr"`
	BucketAttr string `json:"bucket_attr"`
	RankAttr   string `json:"rank_attr"`
	// Query labels and transaction attribute UUIDs are separate namespaces.
	// ValueAttr/BucketAttr/RankAttr are query labels only; transaction steps
	// accept the explicit UUID fields below and never fall back to labels.
	QueryEntity  string `json:"query_entity,omitempty"`
	QueryBucket  string `json:"query_bucket_attr,omitempty"`
	QueryRank    string `json:"query_rank_attr,omitempty"`
	IDAttrID     string `json:"id_attr_id"`
	ValueAttrID  string `json:"value_attr_id,omitempty"`
	BucketAttrID string `json:"bucket_attr_id,omitempty"`
	RankAttrID   string `json:"rank_attr_id,omitempty"`
	// Identity metadata is part of the signed fixture contract. These values
	// describe the provisioned V1 primary/identity attribute rather than being
	// inferred from observed rows or a target's schema.
	IDAttrValueType     string `json:"id_attr_value_type"`
	IDAttrValueEncoding string `json:"id_attr_value_encoding"`
	IDAttrCardinality   string `json:"id_attr_cardinality"`
	IDAttrUnique        bool   `json:"id_attr_unique"`
	IDAttrIndexed       bool   `json:"id_attr_indexed"`
	IDAttrPrimary       bool   `json:"id_attr_primary"`
	IDAttrIdentity      bool   `json:"id_attr_identity"`
	IDAttrRequired      bool   `json:"id_attr_required"`
}

// Canonical fixture UUIDs are part of the benchmark identity. Keeping them
// fixed prevents a live config from silently changing the wire schema or
// making alias matching dependent on UUID casing.
const (
	CanonicalFixtureIDAttrID     = "00000000-0000-0000-0000-000000000100"
	CanonicalFixtureValueAttrID  = "00000000-0000-0000-0000-000000000101"
	CanonicalFixtureBucketAttrID = "00000000-0000-0000-0000-000000000102"
	CanonicalFixtureRankAttrID   = "00000000-0000-0000-0000-000000000103"
)

// DefaultFixtureIDs is the deterministic schema contract used by synthetic
// runs and by tests that do not supply a provisioned live fixture. The id
// attribute is intentionally fixed alongside the value/bucket/rank UUIDs so
// V1 receives the explicit identity triple required by plain InstaQL queries.
var DefaultFixtureIDs = FixtureIDs{
	EntityType:          "bench_items",
	IDAttr:              "id",
	ValueAttr:           "value",
	BucketAttr:          "bucket",
	RankAttr:            "rank",
	QueryEntity:         "bench_items",
	QueryBucket:         "bucket",
	QueryRank:           "rank",
	IDAttrID:            CanonicalFixtureIDAttrID,
	ValueAttrID:         CanonicalFixtureValueAttrID,
	BucketAttrID:        CanonicalFixtureBucketAttrID,
	RankAttrID:          CanonicalFixtureRankAttrID,
	IDAttrValueType:     "blob",
	IDAttrValueEncoding: "string",
	IDAttrCardinality:   "one",
	IDAttrUnique:        true,
	IDAttrIndexed:       true,
	IDAttrPrimary:       true,
	IDAttrIdentity:      true,
	IDAttrRequired:      true,
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
	if err := ids.validate(); err != nil {
		return FixtureEvidence{}, err
	}
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

const maxFixtureLabelBytes = 256

func validateCanonicalUUID(value string) error {
	if !uuidIDRE.MatchString(value) {
		return fmt.Errorf("value must be a UUID")
	}
	if value != strings.ToLower(value) {
		return fmt.Errorf("value must be a lowercase canonical UUID")
	}
	return nil
}

func (ids FixtureIDs) validate() error {
	if err := ids.validateQuery(); err != nil {
		return err
	}
	if ids.IDAttr != "id" {
		return fmt.Errorf("fixture id attribute label must be exactly id")
	}
	if len(ids.IDAttr) > maxFixtureLabelBytes || strings.ContainsAny(ids.IDAttr, "\x00\r\n") {
		return fmt.Errorf("fixture id attribute label is invalid or exceeds %d bytes", maxFixtureLabelBytes)
	}
	if ids.IDAttrID == "" {
		return fmt.Errorf("fixture id attribute UUID is required")
	}
	if ids.ValueAttrID == "" {
		return fmt.Errorf("fixture value attribute UUID is required for transaction builders")
	}
	if ids.IDAttrValueType != "blob" {
		return fmt.Errorf("fixture id attribute value type must be blob")
	}
	if ids.IDAttrValueEncoding != "string" {
		return fmt.Errorf("fixture id attribute value encoding must be string")
	}
	if ids.IDAttrCardinality != "one" {
		return fmt.Errorf("fixture id attribute cardinality must be one")
	}
	if !ids.IDAttrUnique {
		return fmt.Errorf("fixture id attribute must be unique")
	}
	if !ids.IDAttrIndexed {
		return fmt.Errorf("fixture id attribute must be indexed")
	}
	if !ids.IDAttrPrimary {
		return fmt.Errorf("fixture id attribute must have primary semantics")
	}
	if !ids.IDAttrIdentity {
		return fmt.Errorf("fixture id attribute must have identity semantics")
	}
	if !ids.IDAttrRequired {
		return fmt.Errorf("fixture id attribute must be required")
	}
	seenIDs := make(map[string]string, 4)
	canonicalIDs := []struct {
		name, value, want string
	}{
		{"id_attr_id", ids.IDAttrID, CanonicalFixtureIDAttrID},
		{"value_attr_id", ids.ValueAttrID, CanonicalFixtureValueAttrID},
		{"bucket_attr_id", ids.BucketAttrID, CanonicalFixtureBucketAttrID},
		{"rank_attr_id", ids.RankAttrID, CanonicalFixtureRankAttrID},
	}
	for _, id := range canonicalIDs {
		name, value := id.name, id.value
		if value != "" && !uuidIDRE.MatchString(value) {
			return fmt.Errorf("fixture %s must be a UUID", name)
		}
		if value == "" {
			return fmt.Errorf("fixture %s is required", name)
		}
		canonical := strings.ToLower(value)
		if previous, ok := seenIDs[canonical]; ok {
			return fmt.Errorf("fixture attribute UUIDs collide case-insensitively: %s and %s", previous, name)
		}
		seenIDs[canonical] = name
		if value != id.want {
			return fmt.Errorf("fixture %s must use canonical UUID %s", name, id.want)
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
	if err := validateCanonicalUUID(m.EntityID); err != nil {
		return nil, fmt.Errorf("mutation entity ID: %w", err)
	}
	switch m.Kind {
	case benchharness.MutationAppend:
		if ids.bucketID() == "" || ids.rankID() == "" {
			return nil, fmt.Errorf("append requires value, bucket, and rank attribute IDs")
		}
		return []any{[]any{"add-triple", m.EntityID, ids.IDAttrID, m.EntityID}, []any{"add-triple", m.EntityID, ids.valueID(), m.Marker}, []any{"add-triple", m.EntityID, ids.bucketID(), m.Bucket}, []any{"add-triple", m.EntityID, ids.rankID(), m.Rank}}, nil
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
