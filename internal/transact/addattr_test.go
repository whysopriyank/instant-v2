package transact_test

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
	"testing"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/transact"
)

// TestAddAttrRejectsReservedNamespaces pins the tenant guard on client
// schema minting: system namespaces are server-provisioned, and a runtime
// session must not be able to create attrs inside them (e.g. $users fields
// that would surface in auth/user projections).
func TestAddAttrRejectsReservedNamespaces(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	appID, cat, _ := seed(t, db)

	for _, etype := range []string{"$users", "$files", "$default", "$streams", "$rateLimits"} {
		attrID := rand16()
		steps := parseSteps(t,
			mustJSON(t, []any{"add-attr", map[string]any{
				"id":               uuidStr(attrID),
				"forward-identity": []string{uuidStr(attrID), etype, "sneaky"},
				"value-type":       "blob",
				"cardinality":      "one",
			}}),
		)
		if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{}, nil); err == nil {
			t.Fatalf("%s: reserved namespace add-attr must be rejected", etype)
		}
	}
}

// The frozen protocol lets clients mint attr ids (instaml add-attr) and
// reference them in the SAME batch. Regression: add-attr was a stub, so every
// real SDK write died with "unknown attr <id>".
func TestAddAttrThenTripleSameBatch(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)

	appID, _, _ := seed(t, db)

	attrID := rand16()
	eid := rand16()
	steps := parseSteps(t,
		mustJSON(t, []any{"add-attr", map[string]any{
			"id":               uuidStr(attrID),
			"forward-identity": []string{uuidStr(attrID), "wireattr", "name"},
			"value-type":       "blob",
			"cardinality":      "one",
		}}),
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(attrID), "hello"}),
	)
	res, err := transact.Transact(ctx, db, mustCatalog(t, db, appID), appID, steps, transact.Options{Admin: true}, nil)
	if err != nil {
		t.Fatalf("Transact: %v", err)
	}
	if res.TxID <= 0 {
		t.Fatalf("bad tx id %d", res.TxID)
	}

	cat2, err := platform.LoadAttrCatalog(ctx, db.Pool, appID)
	if err != nil {
		t.Fatal(err)
	}
	a, ok := cat2.ByID(attrID)
	if !ok {
		t.Fatalf("attr %s missing after commit", uuidStr(attrID))
	}
	if a.ValueType != "blob" || *a.Etype != "wireattr" || *a.Label != "name" {
		t.Fatalf("stored attr wrong: %+v", a)
	}
}

func TestAddAttrReplayIsNoop(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)

	appID, _, _ := seed(t, db)

	payload := map[string]any{
		"id":               uuidStr(rand16()),
		"forward-identity": []string{uuidStr(rand16()), "wireattr", "count"},
		"value-type":       "number",
		"cardinality":      "one",
	}
	step := mustJSON(t, []any{"add-attr", payload})
	steps := parseSteps(t, step, step)
	if _, err := transact.Transact(ctx, db, mustCatalog(t, db, appID), appID, steps, transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("replayed add-attr must be a no-op, got: %v", err)
	}
}

func mustCatalog(t *testing.T, db *storage.DB, appID [16]byte) *platform.AttrCatalog {
	t.Helper()
	cat, err := platform.LoadAttrCatalog(context.Background(), db.Pool, appID)
	if err != nil {
		t.Fatal(err)
	}
	return cat
}

// The TS client always provisions the implicit <etype>/id attr. When the
// server already stores it under a different id, add-attr must ADOPT the
// stored row and rewrite same-batch triple references (client _rewriteMutations
// equivalent server-side).
func TestAddAttrAdoptsExistingIdent(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)

	appID, _, ids := seed(t, db)
	existingID := ids.name // seeded todos/name attr under a server id
	clientMinted := rand16()
	eid := rand16()

	steps := parseSteps(t,
		mustJSON(t, []any{"add-attr", map[string]any{
			"id":               uuidStr(clientMinted),
			"forward-identity": []string{uuidStr(rand16()), "todos", "name"},
			"value-type":       "blob",
			"cardinality":      "one",
		}}),
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(clientMinted), "via-alias"}),
	)
	if _, err := transact.Transact(ctx, db, mustCatalog(t, db, appID), appID, steps, transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("Transact with adopted attr: %v", err)
	}

	// The triple must be stored against the SERVER's attr id.
	rows, _ := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{eid}})
	if len(rows) == 0 {
		t.Fatalf("no triples for entity")
	}
	found := false
	for _, r := range rows {
		if r.Triple.A == existingID {
			found = true
		}
	}
	if !found {
		t.Fatalf("triple not stored under adopted attr id %s", existingID)
	}
}

// --- Security wave S2: attr metadata validation ---

func TestAddAttrRejectsUnsafeIdentNames(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	appID, cat, _ := seed(t, db)

	cases := []struct{ name, etype, label string }{
		{"oversized label", "todos", strings.Repeat("x", 300)},
		{"space in label", "todos", "my label"},
		{"newline injection", "todos", "a\nb"},
		{"unicode label", "todos", "ラベル"},
		{"oversized etype", strings.Repeat("e", 300), "ok"},
		{"space in etype", "my todos", "ok"},
	}
	for _, tc := range cases {
		attrID := rand16()
		steps := parseSteps(t,
			mustJSON(t, []any{"add-attr", map[string]any{
				"id":               uuidStr(attrID),
				"forward-identity": []string{uuidStr(attrID), tc.etype, tc.label},
				"value-type":       "blob",
				"cardinality":      "one",
			}}),
		)
		if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{}, nil); err == nil {
			t.Fatalf("%s: add-attr must be rejected", tc.name)
		}
	}
}

func TestAddAttrRejectsUnknownValueType(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	appID, cat, _ := seed(t, db)
	attrID := rand16()
	steps := parseSteps(t,
		mustJSON(t, []any{"add-attr", map[string]any{
			"id":               uuidStr(attrID),
			"forward-identity": []string{uuidStr(attrID), "todos", "weird"},
			"value-type":       "banana",
			"cardinality":      "one",
		}}),
	)
	if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{}, nil); err == nil {
		t.Fatal("unknown value-type must be rejected")
	}
}

func TestAddAttrRejectsReservedOrEmptyReverseIdentity(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	appID, cat, _ := seed(t, db)

	cases := []struct {
		name string
		rev  []string
	}{
		{"reserved reverse etype", []string{uuidStr(rand16()), "$users", "sneaky"}},
		{"empty reverse etype", []string{uuidStr(rand16()), "", "x"}},
		{"empty reverse label", []string{uuidStr(rand16()), "todos", ""}},
	}
	for _, tc := range cases {
		attrID := rand16()
		steps := parseSteps(t,
			mustJSON(t, []any{"add-attr", map[string]any{
				"id":               uuidStr(attrID),
				"forward-identity": []string{uuidStr(attrID), "links", "owner"},
				"value-type":       "ref",
				"cardinality":      "one",
				"reverse-identity": tc.rev,
			}}),
		)
		if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{Admin: true}, nil); err == nil {
			t.Fatalf("%s: must be rejected", tc.name)
		}
	}
}

func TestRequiredAttrLifecycleAndSameBatchEnforced(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	appID, _, _ := seed(t, db)

	requiredID, optionalID, eid := rand16(), rand16(), rand16()
	// Requiredness is evaluated against the transaction-local catalog. A batch
	// that creates a required attr and an entity through only an optional attr
	// must therefore reject and roll back all of its writes.
	first := parseSteps(t,
		mustJSON(t, []any{"add-attr", map[string]any{
			"id":               uuidStr(requiredID),
			"forward-identity": []string{uuidStr(rand16()), "required_batch", "title"},
			"value-type":       "blob",
			"cardinality":      "one",
			"required?":        true,
		}}),
		mustJSON(t, []any{"add-attr", map[string]any{
			"id":               uuidStr(optionalID),
			"forward-identity": []string{uuidStr(rand16()), "required_batch", "note"},
			"value-type":       "blob",
			"cardinality":      "one",
		}}),
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(optionalID), "before-required"}),
	)
	if _, err := transact.Transact(ctx, db, mustCatalog(t, db, appID), appID, first, transact.Options{Admin: true}, nil); err == nil || !strings.Contains(err.Error(), "Missing required attribute") {
		t.Fatalf("same-batch missing required value must reject, got %v", err)
	}
	if _, ok := mustCatalog(t, db, appID).ByID(requiredID); ok {
		t.Fatal("failed same-batch required validation must roll back the new attr")
	}

	// Supplying the required value in the same batch satisfies the validator,
	// so the metadata and both values commit together.
	second := parseSteps(t,
		mustJSON(t, []any{"add-attr", map[string]any{
			"id":               uuidStr(requiredID),
			"forward-identity": []string{uuidStr(rand16()), "required_batch", "title"},
			"value-type":       "blob",
			"cardinality":      "one",
			"required?":        true,
		}}),
		mustJSON(t, []any{"add-attr", map[string]any{
			"id":               uuidStr(optionalID),
			"forward-identity": []string{uuidStr(rand16()), "required_batch", "note"},
			"value-type":       "blob",
			"cardinality":      "one",
		}}),
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(requiredID), "title"}),
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(optionalID), "note"}),
	)
	if _, err := transact.Transact(ctx, db, mustCatalog(t, db, appID), appID, second, transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("same-batch add required attr with value: %v", err)
	}
	cat := mustCatalog(t, db, appID)
	if a, ok := cat.ByID(requiredID); !ok || !a.IsRequired {
		t.Fatalf("required flag not persisted: ok=%v attr=%+v", ok, a)
	}
	var wireRequired bool
	for _, raw := range cat.WireAttrs() {
		if raw["id"] == uuidStr(requiredID) {
			wireRequired, _ = raw["required?"].(bool)
		}
	}
	if !wireRequired {
		t.Fatal("required? missing from wire catalog")
	}

	// A later entity that writes only the optional attr is also rejected.
	missing := parseSteps(t, mustJSON(t, []any{"add-triple", uuidStr(rand16()), uuidStr(optionalID), "missing-title"}))
	if _, err := transact.Transact(ctx, db, cat, appID, missing, transact.Options{Admin: true}, nil); err == nil || !strings.Contains(err.Error(), "Missing required attribute") {
		t.Fatalf("missing required attr must reject, got %v", err)
	}

	// A required value makes the entity valid; adding another optional value is
	// also valid, but retracting the required value must fail while it remains
	// alive through the optional value.
	if _, err := transact.Transact(ctx, db, cat, appID,
		parseSteps(t, mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(requiredID), "title"})),
		transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("required value: %v", err)
	}
	if _, err := transact.Transact(ctx, db, cat, appID,
		parseSteps(t, mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(optionalID), "note"})),
		transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("optional value: %v", err)
	}
	if _, err := transact.Transact(ctx, db, cat, appID,
		parseSteps(t, mustJSON(t, []any{"retract-triple", uuidStr(eid), uuidStr(requiredID), "title"})),
		transact.Options{Admin: true}, nil); err == nil || !strings.Contains(err.Error(), "Missing required attribute") {
		t.Fatalf("retracting required value must reject, got %v", err)
	}

	// Full deletion removes the entity and therefore satisfies the validator.
	if _, err := transact.Transact(ctx, db, cat, appID,
		parseSteps(t, mustJSON(t, []any{"delete-entity", uuidStr(eid), "required_batch"})),
		transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("delete entity with required attr: %v", err)
	}
}

func TestRequiredAttrUpdateReservedNamespaceAlwaysDenied(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	appID, _, _ := seed(t, db)

	var forward, reverse platform.Attr
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		var err error
		forward, err = platform.GetOrCreateAttrRev(ctx, tx, appID, "$users", "secret", nil, nil, "blob", "one", false, false)
		if err != nil {
			return err
		}
		reverseEtype, reverseLabel := "$users", "owner"
		reverse, err = platform.GetOrCreateAttrRev(ctx, tx, appID, "safe_links", "user", &reverseEtype, &reverseLabel, "ref", "one", false, false)
		return err
	}); err != nil {
		t.Fatalf("seed reserved attrs: %v", err)
	}

	for _, attr := range []platform.Attr{forward, reverse} {
		steps := parseSteps(t, mustJSON(t, []any{"update-attr", map[string]any{
			"id": uuidStr(attr.ID), "required?": true,
		}}))
		if _, err := transact.Transact(ctx, db, mustCatalog(t, db, appID), appID, steps, transact.Options{Admin: true}, nil); err == nil || !strings.Contains(err.Error(), "reserved") {
			t.Fatalf("admin update of reserved attr %s must reject, got %v", uuidStr(attr.ID), err)
		}
		if got, ok := mustCatalog(t, db, appID).ByID(attr.ID); !ok || got.IsRequired {
			t.Fatalf("reserved update must roll back: ok=%v attr=%+v", ok, got)
		}
	}
}

func TestRequiredAttrUpdatePermissionsFailClosedAndUseBindings(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	appID, _, _ := seed(t, db)
	attrID := rand16()
	add := parseSteps(t, mustJSON(t, []any{"add-attr", map[string]any{
		"id": uuidStr(attrID), "forward-identity": []string{uuidStr(rand16()), "permission_attrs", "title"},
		"value-type": "blob", "cardinality": "one",
	}}))
	if _, err := transact.Transact(ctx, db, mustCatalog(t, db, appID), appID, add, transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("add attr: %v", err)
	}
	update := func(doc *perms.RuleDoc, opts transact.Options) error {
		_, err := transact.Transact(ctx, db, mustCatalog(t, db, appID), appID,
			parseSteps(t, mustJSON(t, []any{"update-attr", map[string]any{
				"id": uuidStr(attrID), "required?": true,
			}})), opts, doc)
		return err
	}
	assertNotRequired := func(label string) {
		t.Helper()
		if got, ok := mustCatalog(t, db, appID).ByID(attrID); !ok || got.IsRequired {
			t.Fatalf("%s mutated attr after failed update: ok=%v attr=%+v", label, ok, got)
		}
	}

	denyDoc, err := perms.ParseRuleDoc([]byte(`{"attrs":{"allow":{"update":"false"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := update(denyDoc, transact.Options{}); err == nil || !strings.Contains(err.Error(), "attrs.update denied") {
		t.Fatalf("denied attrs update must fail, got %v", err)
	}
	assertNotRequired("permission denial")

	evalDoc, err := perms.ParseRuleDoc([]byte(`{"attrs":{"allow":{"update":"not valid cel !!"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := update(evalDoc, transact.Options{}); err == nil || !strings.Contains(err.Error(), "attrs.update") {
		t.Fatalf("CEL error must fail closed, got %v", err)
	}
	assertNotRequired("permission evaluation error")

	// A non-admin caller is allowed only when the expression can evaluate from
	// the opts bindings supplied to the update gate.
	allowDoc, err := perms.ParseRuleDoc([]byte(`{"attrs":{"allow":{"update":"auth.id == ruleParams.owner"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := update(allowDoc, transact.Options{
		AuthUser:   map[string]any{"id": "owner-1"},
		RuleParams: map[string]any{"owner": "owner-1"},
	}); err != nil {
		t.Fatalf("bound attrs update should succeed: %v", err)
	}
	if got, ok := mustCatalog(t, db, appID).ByID(attrID); !ok || !got.IsRequired {
		t.Fatalf("bound attrs update did not persist: ok=%v attr=%+v", ok, got)
	}
}

func TestRequiredAttrUpdateWithoutRulesIsAllowed(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	appID, _, _ := seed(t, db)
	attrID := rand16()
	if _, err := transact.Transact(ctx, db, mustCatalog(t, db, appID), appID,
		parseSteps(t, mustJSON(t, []any{"add-attr", map[string]any{
			"id": uuidStr(attrID), "forward-identity": []string{uuidStr(rand16()), "no_rules_attrs", "title"},
			"value-type": "blob", "cardinality": "one",
		}})), transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("add attr: %v", err)
	}
	if _, err := transact.Transact(ctx, db, mustCatalog(t, db, appID), appID,
		parseSteps(t, mustJSON(t, []any{"update-attr", map[string]any{
			"id": uuidStr(attrID), "required?": true,
		}})), transact.Options{}, nil); err != nil {
		t.Fatalf("non-admin update without rules should be allowed: %v", err)
	}
	if got, ok := mustCatalog(t, db, appID).ByID(attrID); !ok || !got.IsRequired {
		t.Fatalf("no-rules update did not persist: ok=%v attr=%+v", ok, got)
	}
}

func TestDeleteEntityCascadeValidatesRequiredReferrer(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	appID, _, _ := seed(t, db)
	refAttrID, noteAttrID, targetAttrID := rand16(), rand16(), rand16()
	referrerID, targetID := rand16(), rand16()

	if _, err := transact.Transact(ctx, db, mustCatalog(t, db, appID), appID, parseSteps(t,
		mustJSON(t, []any{"add-attr", map[string]any{
			"id":               uuidStr(refAttrID),
			"forward-identity": []string{uuidStr(rand16()), "required_referrers", "target"},
			"value-type":       "ref",
			"cardinality":      "one",
			"required?":        true,
			"reverse-identity": []string{uuidStr(rand16()), "targets", "referrers"},
		}}),
		mustJSON(t, []any{"add-attr", map[string]any{
			"id":               uuidStr(noteAttrID),
			"forward-identity": []string{uuidStr(rand16()), "required_referrers", "note"},
			"value-type":       "blob",
			"cardinality":      "one",
		}}),
		mustJSON(t, []any{"add-attr", map[string]any{
			"id":               uuidStr(targetAttrID),
			"forward-identity": []string{uuidStr(rand16()), "targets", "name"},
			"value-type":       "blob",
			"cardinality":      "one",
		}}),
		mustJSON(t, []any{"add-triple", uuidStr(referrerID), uuidStr(refAttrID), uuidStr(targetID)}),
		mustJSON(t, []any{"add-triple", uuidStr(referrerID), uuidStr(noteAttrID), "keep-referrer-alive"}),
		mustJSON(t, []any{"add-triple", uuidStr(targetID), uuidStr(targetAttrID), "target"}),
	), transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("seed required referrer and target: %v", err)
	}

	cat := mustCatalog(t, db, appID)
	deleteTarget := parseSteps(t, mustJSON(t, []any{"delete-entity", uuidStr(targetID), "targets"}))
	if _, err := transact.Transact(ctx, db, cat, appID, deleteTarget, transact.Options{Admin: true}, nil); err == nil || !strings.Contains(err.Error(), "Missing required attribute") {
		t.Fatalf("deleting target with a live incomplete referrer must reject, got %v", err)
	}
	// The target delete and reverse cascade must be atomic when requiredness
	// rejects the now-incomplete referrer.
	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{referrerID, targetID}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("failed cascade should roll back all three triples, got %d: %+v", len(rows), rows)
	}

	// Once the optional note is retracted, deleting the target removes the
	// referrer's final live triple. An empty referrer is no longer an entity, so
	// the cascade is valid and the complete transaction commits.
	if _, err := transact.Transact(ctx, db, cat, appID,
		parseSteps(t, mustJSON(t, []any{"retract-triple", uuidStr(referrerID), uuidStr(noteAttrID), "keep-referrer-alive"})),
		transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("retract optional referrer value: %v", err)
	}
	if _, err := transact.Transact(ctx, db, cat, appID, deleteTarget, transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("delete target after referrer is no longer independently live: %v", err)
	}
	rows, err = db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{referrerID, targetID}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("final target/referrer deletion left triples: %+v", rows)
	}
}

func TestRequiredAttrAddRejectsPopulatedEtype(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	appID, _, _ := seed(t, db)
	optionalID, requiredID, eid := rand16(), rand16(), rand16()
	if _, err := transact.Transact(ctx, db, mustCatalog(t, db, appID), appID, parseSteps(t,
		mustJSON(t, []any{"add-attr", map[string]any{
			"id": uuidStr(optionalID), "forward-identity": []string{uuidStr(rand16()), "populated", "note"},
			"value-type": "blob", "cardinality": "one",
		}}),
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(optionalID), "existing"}),
	), transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("seed populated etype: %v", err)
	}
	errSteps := parseSteps(t, mustJSON(t, []any{"add-attr", map[string]any{
		"id": uuidStr(requiredID), "forward-identity": []string{uuidStr(rand16()), "populated", "title"},
		"value-type": "blob", "cardinality": "one", "required?": true,
	}}))
	if _, err := transact.Transact(ctx, db, mustCatalog(t, db, appID), appID, errSteps, transact.Options{Admin: true}, nil); err == nil || !strings.Contains(err.Error(), "already have entities") {
		t.Fatalf("required attr on populated etype must reject, got %v", err)
	}
}

func TestRequiredAttrUpdateValidationAndDisable(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	appID, _, _ := seed(t, db)
	attrID, otherID, eid := rand16(), rand16(), rand16()
	add := parseSteps(t, mustJSON(t, []any{"add-attr", map[string]any{
		"id": uuidStr(attrID), "forward-identity": []string{uuidStr(rand16()), "update_required", "title"},
		"value-type": "blob", "cardinality": "one",
	}}), mustJSON(t, []any{"add-attr", map[string]any{
		"id": uuidStr(otherID), "forward-identity": []string{uuidStr(rand16()), "update_required", "note"},
		"value-type": "blob", "cardinality": "one",
	}}))
	if _, err := transact.Transact(ctx, db, mustCatalog(t, db, appID), appID, add, transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("add attr: %v", err)
	}
	cat := mustCatalog(t, db, appID)
	if _, err := transact.Transact(ctx, db, cat, appID, parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(otherID), "existing"})),
		transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("seed entity: %v", err)
	}
	update := func(required bool) error {
		return func() error {
			_, err := transact.Transact(ctx, db, mustCatalog(t, db, appID), appID, parseSteps(t,
				mustJSON(t, []any{"update-attr", map[string]any{"id": uuidStr(attrID), "required?": required}})),
				transact.Options{Admin: true}, nil)
			return err
		}()
	}
	if err := update(true); err == nil || !strings.Contains(err.Error(), "already have entities without it") {
		t.Fatalf("incomplete required update must reject, got %v", err)
	}
	if got, ok := mustCatalog(t, db, appID).ByID(attrID); !ok || got.IsRequired {
		t.Fatalf("failed update must roll back required flag: ok=%v attr=%+v", ok, got)
	}
	if _, err := transact.Transact(ctx, db, mustCatalog(t, db, appID), appID, parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(attrID), "title"})),
		transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("fill required value: %v", err)
	}
	// Once the only live entity has a value, toggling required on and off is valid.
	if err := update(true); err != nil {
		t.Fatalf("complete required update: %v", err)
	}
	if got := mustCatalog(t, db, appID); func() bool { a, _ := got.ByID(attrID); return !a.IsRequired }() {
		t.Fatal("required update did not persist")
	}
	if err := update(false); err != nil {
		t.Fatalf("remove required flag: %v", err)
	}
}

func TestCheckedDataTypePersistedAndEnforced(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	appID, _, _ := seed(t, db)

	attrID := rand16()
	eid := rand16()
	addAttr := mustJSON(t, []any{"add-attr", map[string]any{
		"id":                uuidStr(attrID),
		"forward-identity":  []string{uuidStr(attrID), "cdtcheck", "score"},
		"value-type":        "blob",
		"cardinality":       "one",
		"checked-data-type": "number",
	}})
	steps := parseSteps(t, addAttr)
	if _, err := transact.Transact(ctx, db, mustCatalog(t, db, appID), appID, steps, transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("add-attr with checked-data-type: %v", err)
	}

	cat2 := mustCatalog(t, db, appID)
	a, ok := cat2.ByID(attrID)
	if !ok {
		t.Fatal("attr missing after commit")
	}
	if a.CheckedDataType == nil || *a.CheckedDataType != "number" {
		t.Fatalf("checked-data-type not persisted: %+v", a.CheckedDataType)
	}

	// Type-conflicting write must fail cleanly.
	bad := parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(attrID), "not-a-number"}),
	)
	if _, err := transact.Transact(ctx, db, cat2, appID, bad, transact.Options{Admin: true}, nil); err == nil {
		t.Fatal("string value into number-typed attr must be rejected")
	}

	good := parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(eid), uuidStr(attrID), float64(42)}),
	)
	if _, err := transact.Transact(ctx, db, cat2, appID, good, transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("number value into number-typed attr must succeed: %v", err)
	}
}

// Audit L1 / adversarial-review gap: tx batches are capped so permission
// probes can't pin a writer connection for O(unbounded) work.
func TestTransactRejectsExcessiveSteps(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	appID, cat, ids := seed(t, db)

	steps := make([]transact.Step, 0, 10001)
	for i := 0; i <= 10000; i++ {
		steps = append(steps, parseSteps(t,
			mustJSON(t, []any{"add-triple", uuidStr(rand16()), uuidToStr(ids.name), "x"}),
		)[0])
	}
	if _, err := transact.Transact(ctx, db, cat, appID, steps, transact.Options{}, nil); err == nil {
		t.Fatal("over-cap tx must be rejected")
	}
}

// Audit M3: value-position lookups require a unique attr — non-unique
// matches made link targets nondeterministic.
func TestValueLookupRequiresUniqueAttr(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	appID, _, ids := seed(t, db)

	var tagAttr platform.Attr
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		var e error
		tagAttr, e = platform.GetOrCreateAttr(ctx, tx, appID, "todos", "tag", "blob", "many", false, true)
		return e
	}); err != nil {
		t.Fatal(err)
	}
	cat2 := mustCatalog(t, db, appID)
	setup := parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(rand16()), uuidStr(tagAttr.ID), "dup"}),
		mustJSON(t, []any{"add-triple", uuidStr(rand16()), uuidStr(tagAttr.ID), "dup"}),
	)
	if _, err := transact.Transact(ctx, db, cat2, appID, setup, transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("setup two dup tags: %v", err)
	}

	lookupRef := []any{uuidStr(tagAttr.ID), "dup"}
	src := rand16()
	steps := parseSteps(t,
		mustJSON(t, []any{"add-triple", uuidStr(src), uuidToStr(ids.link), lookupRef}),
	)
	if _, err := transact.Transact(ctx, db, cat2, appID, steps, transact.Options{Admin: true}, nil); err == nil {
		t.Fatal("value-position lookup on non-unique attr must be rejected")
	}
}
