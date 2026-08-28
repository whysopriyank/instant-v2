// Package benchharness contains the target-neutral pieces of the comparative
// benchmark driver.  It deliberately has no dependency on the product server:
// a target is supplied through a Session/Target implementation.
package benchharness

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// Family is one of the contract's fixed workload families.
type Family string

const (
	FamilyH Family = "H-append"
	FamilyX Family = "X-heterogeneous"
	FamilyM Family = "M-mixed"
	FamilyO Family = "O-reorder"
	FamilyS Family = "S-slow-reader"
	FamilyR Family = "R-reconnect"
	FamilyC Family = "C-process-cold"
	FamilyT Family = "T-saturation"
)

var contractFamilies = [...]Family{FamilyH, FamilyX, FamilyM, FamilyO, FamilyS, FamilyR, FamilyC, FamilyT}

// ContractFamilies exposes the stable family order without allowing callers
// to mutate the internal matrix used by ContractWorkloads.
var ContractFamilies = [...]Family{FamilyH, FamilyX, FamilyM, FamilyO, FamilyS, FamilyR, FamilyC, FamilyT}

// DefaultSeed is the seed used when a runner does not provide one.  It is a
// versioned constant so a workload can be reproduced from its manifest.
const DefaultSeed int64 = 0x49564f325731

// ContractScales is the complete subscriber scale matrix.
var ContractScales = [...]int{300, 1000, 2000}

// MutationKind describes the semantic operation, independently of wire shape.
type MutationKind string

const (
	MutationAppend  MutationKind = "append"
	MutationUpdate  MutationKind = "update"
	MutationRetract MutationKind = "retract"
	MutationReorder MutationKind = "reorder"
)

// Mutation is the deterministic logical operation submitted by the dedicated
// writer. Marker is fixture-only data and must not be interpreted as a server
// transaction id.
type Mutation struct {
	Sequence int64
	EventID  string
	Kind     MutationKind
	EntityID string
	Bucket   int
	Rank     int
	Marker   string
}

// Query is a stable query assignment used by the prefix oracle.
type Query struct {
	ID       string
	Bucket   int
	MatchAll bool
	TopN     int
}

// Entity is the small semantic fixture representation. Attributes are JSON
// values and are intentionally kept generic so the harness is protocol neutral.
type Entity struct {
	ID         string
	Bucket     int
	Rank       int
	Attributes map[string]any
}

// Fixture is immutable input to a PrefixOracle. Callers should use NewFixture
// and never mutate the returned maps after constructing an oracle.
type Fixture struct {
	Entities map[string]Entity
	Queries  []Query
}

// Workload is one fixed family/scale cell. The family definitions are not
// combinable dimensions; changing one field changes the manifest and therefore
// the identity of the cell.
type Workload struct {
	Family          Family
	Subscribers     int
	Seed            int64
	SeedEntities    int
	Cohorts         int
	TxRate          float64
	Warmup          bool
	Measured        int
	DurationSeconds int
	Writers         int
	Fixture         Fixture
}

// ClientBehavior is deterministic client-side pressure used by S/R families.
// PauseReads and Reconnect are instructions to the protocol driver, not
// server behavior. Backoff is recorded in the event ledger when reconnecting.
type ClientBehavior struct {
	PauseReads bool
	PauseFor   time.Duration
	Reconnect  bool
	Backoff    time.Duration
}

// NewWorkload resolves one contract cell.
func NewWorkload(family Family, subscribers int, seed int64) (Workload, error) {
	if !validFamily(family) {
		return Workload{}, fmt.Errorf("unknown workload family %q", family)
	}
	if !validScale(subscribers) {
		return Workload{}, fmt.Errorf("subscriber scale %d is not one of %v", subscribers, ContractScales)
	}
	if seed == 0 {
		seed = DefaultSeed
	}
	w := Workload{Family: family, Subscribers: subscribers, Seed: seed, TxRate: 8, Warmup: family != FamilyC, DurationSeconds: 180}
	switch family {
	case FamilyH:
		w.SeedEntities = 0
	case FamilyX, FamilyC, FamilyT:
		w.SeedEntities, w.Cohorts = 8192, 64
	case FamilyM, FamilyO:
		w.SeedEntities = 4096
	case FamilyS, FamilyR:
		w.SeedEntities = 1024
	}
	if family == FamilyC {
		w.DurationSeconds = 60
	}
	if family == FamilyT {
		w.TxRate = 0 // saturation is closed-loop, not rate controlled
		w.Measured = 4096
		w.Writers = 8
		w.DurationSeconds = 600
	}
	w.Fixture = NewFixture(w)
	return w, nil
}

// Behavior returns the fixed S/R schedule for a client at elapsed whole
// seconds. Ten percent of clients (the first deterministic cohort) are
// perturbed; all others return an inert behavior.
func (w Workload) Behavior(clientID, elapsedSeconds int) ClientBehavior {
	if clientID < 0 || clientID%10 != 0 || elapsedSeconds < 0 {
		return ClientBehavior{}
	}
	switch w.Family {
	case FamilyS:
		if elapsedSeconds > 0 && elapsedSeconds%10 == 0 {
			return ClientBehavior{PauseReads: true, PauseFor: 2 * time.Second}
		}
	case FamilyR:
		if elapsedSeconds > 0 && elapsedSeconds%30 == 0 {
			// A stable 0.5–1.5 second backoff derived from the client id and
			// reconnect epoch, with no process-global random state.
			backoff := 500 + (clientID*37+elapsedSeconds/30*53)%1001
			return ClientBehavior{Reconnect: true, Backoff: time.Duration(backoff) * time.Millisecond}
		}
	}
	return ClientBehavior{}
}

// ContractWorkloads returns the matrix in stable family/scale order.
func ContractWorkloads(seed int64) []Workload {
	out := make([]Workload, 0, len(contractFamilies)*len(ContractScales))
	for _, f := range contractFamilies {
		for _, n := range ContractScales {
			w, _ := NewWorkload(f, n, seed)
			out = append(out, w)
		}
	}
	return out
}

func validFamily(f Family) bool {
	for _, known := range contractFamilies {
		if f == known {
			return true
		}
	}
	return false
}

func validScale(n int) bool {
	for _, known := range ContractScales {
		if n == known {
			return true
		}
	}
	return false
}

// NewFixture builds stable entity ids and deterministic query assignments.
func NewFixture(w Workload) Fixture {
	f := Fixture{Entities: make(map[string]Entity, w.SeedEntities), Queries: make([]Query, 0, w.Subscribers)}
	for i := 0; i < w.SeedEntities; i++ {
		id := deterministicEntityID(w, i)
		bucket := i % max(1, w.Cohorts)
		f.Entities[id] = Entity{ID: id, Bucket: bucket, Rank: i, Attributes: map[string]any{"value": fmt.Sprintf("seed-%d", i)}}
	}
	for i := 0; i < w.Subscribers; i++ {
		q := Query{ID: fmt.Sprintf("q-%06d", i), MatchAll: true, Bucket: -1}
		if w.Family == FamilyX || w.Family == FamilyC || w.Family == FamilyT {
			q.MatchAll = false
			q.Bucket = i % max(1, w.Cohorts)
		}
		if w.Family == FamilyO {
			q.TopN = 50
		}
		f.Queries = append(f.Queries, q)
	}
	return f
}

// Mutation returns the contract-defined deterministic operation for sequence.
func (w Workload) Mutation(sequence int64) Mutation {
	if sequence < 1 {
		sequence = 1
	}
	bucket := int((sequence - 1) % int64(max(1, w.Cohorts)))
	kind := MutationAppend
	switch w.Family {
	case FamilyM:
		switch (sequence - 1) % 10 {
		case 0, 1, 2, 3:
			kind = MutationUpdate
		case 4, 5:
			kind = MutationAppend
		case 6, 7:
			kind = MutationRetract
		default:
			kind = MutationReorder
		}
	case FamilyO:
		kind = MutationReorder
	case FamilyS, FamilyR:
		kind = MutationUpdate
	case FamilyT:
		kind = MutationUpdate
	}
	entity := deterministicEntityID(w, int((sequence-1)%int64(max(1, w.SeedEntities))))
	if kind == MutationAppend {
		entity = deterministicEventID(w, sequence)
	}
	markerInput := fmt.Sprintf("%d|%s|%d|%d", w.Seed, w.Family, w.Subscribers, sequence)
	h := sha256.Sum256([]byte(markerInput))
	marker := "bench/" + hex.EncodeToString(h[:8]) + "/" + fmt.Sprintf("%06d", sequence)
	rank := int(sequence % int64(max(1, w.SeedEntities)))
	if w.Family == FamilyO {
		// Alternate between the top window and a stable out-of-window rank,
		// guaranteeing that every pair exercises entry and exit from top-50.
		if sequence%2 == 0 {
			rank = int(sequence % 50)
		} else {
			rank = 1000 + int(sequence%1000)
		}
	}
	return Mutation{Sequence: sequence, EventID: fmt.Sprintf("bench/%d/%06d", w.Seed, sequence), Kind: kind, EntityID: entity, Bucket: bucket, Rank: rank, Marker: marker}
}

// deterministicEntityID returns a UUID-shaped, reproducible id for the
// logical fixture entity. Instant stores entity ids as PostgreSQL UUIDs, so
// human-readable benchmark labels are deliberately not used on the wire.
// The seed and ordinal are the identity; family/scale are omitted so paired
// cells can share the same fixture namespace when they use the same seed.
func deterministicEntityID(w Workload, ordinal int) string {
	seed := w.Seed
	if seed == 0 {
		seed = DefaultSeed
	}
	return deterministicUUID("entity", strconv.FormatInt(seed, 10), strconv.Itoa(ordinal))
}

func deterministicEventID(w Workload, sequence int64) string {
	seed := w.Seed
	if seed == 0 {
		seed = DefaultSeed
	}
	return deterministicUUID("event", strconv.FormatInt(seed, 10), strconv.FormatInt(sequence, 10))
}

// deterministicUUID is a small UUIDv5-compatible construction using the
// existing SHA-256 dependency. It is not a random identifier: changing any
// component changes the id while repeated manifests reproduce byte-for-byte.
func deterministicUUID(namespace string, parts ...string) string {
	input := namespace
	for _, part := range parts {
		input += "\x00" + part
	}
	h := sha256.Sum256([]byte(input))
	b := h[:16]
	// Version 5 and RFC 4122 variant bits make this acceptable to UUID parsers
	// used by the product and PostgreSQL while retaining deterministic bytes.
	b[6] = (b[6] & 0x0f) | 0x50
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

var errMissingQuery = errors.New("benchmark query is not in fixture")
