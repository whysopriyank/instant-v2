package platform

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func mustStr(s string) *string { return &s }

func TestFlagsForParity(t *testing.T) {
	// Pins the exact enhanced-triples CTE derivation from
	// db/model/triple.clj insert-multi-new! — any change here is a physical
	// storage contract change and needs an ADR.
	for _, tc := range []struct {
		name string
		attr Attr
		want Flags
	}{
		{
			"object blob attr (cardinality one, indexed)",
			Attr{ValueType: "blob", Cardinality: "one", IsUnique: false, IsIndexed: true},
			Flags{EA: true, EAV: false, AV: false, AVE: true, VAE: false},
		},
		{
			"unique object attr",
			Attr{ValueType: "blob", Cardinality: "one", IsUnique: true, IsIndexed: true},
			Flags{EA: true, EAV: false, AV: true, AVE: true, VAE: false},
		},
		{
			"ref many (link)",
			Attr{ValueType: "ref", Cardinality: "many", IsUnique: false, IsIndexed: false},
			Flags{EA: false, EAV: true, AV: false, AVE: false, VAE: true},
		},
		{
			"blob many",
			Attr{ValueType: "blob", Cardinality: "many", IsUnique: false, IsIndexed: false},
			Flags{EA: false, EAV: false, AV: false, AVE: false, VAE: false},
		},
	} {
		if got := FlagsFor(tc.attr); got != tc.want {
			t.Fatalf("%s: got %+v want %+v", tc.name, got, tc.want)
		}
	}
}

func TestAttrCatalogLookup(t *testing.T) {
	a := Attr{ID: newUUIDv4(), ValueType: "ref", Cardinality: "many"}
	cat := &AttrCatalog{byID: map[[16]byte]Attr{a.ID: a}}
	got, ok := cat.ByID(a.ID)
	if !ok || got.ValueType != "ref" {
		t.Fatalf("ByID miss: %+v %v", got, ok)
	}
	missing := newUUIDv4()
	if _, ok := cat.ByID(missing); ok {
		t.Fatal("unknown id resolved")
	}
	if cat.Len() != 1 {
		t.Fatalf("Len %d", cat.Len())
	}
}

func TestCheckedDataTypePointerRoundTrip(t *testing.T) {
	a := Attr{CheckedDataType: mustStr("date")}
	if *a.CheckedDataType != "date" {
		t.Fatal("pointer lost")
	}
	var nilAttr Attr
	if nilAttr.CheckedDataType != nil {
		t.Fatal("expected nil")
	}
}

// ---- indexed AttrCatalog ------------------------------------------------------

// testUUID derives a collision-free [16]byte from a counter — deterministic
// and unique within a test, unlike crypto rand which buys nothing here.
func testUUID(n int) [16]byte {
	var u [16]byte
	binary.BigEndian.PutUint64(u[:8], uint64(n+1)) // n+1 keeps the zero uuid unused
	binary.BigEndian.PutUint64(u[8:], uint64(n))
	return u
}

// randomAttrs mints n attrs with deterministic-random shapes: nil Etype/Label
// mixes, blob/ref value types, and reverse metadata on ~1/3 of attrs.
// (etype,label) pairs and reverse keys are kept unique so every indexed
// lookup has one unambiguous expected answer.
func randomAttrs(n int) []Attr {
	rng := rand.New(rand.NewSource(20240601))
	etypes := []string{"todos", "users", "comments", "folders", "posts", "tags"}
	labels := []string{"id", "title", "email", "body", "owner", "name", "size",
		"color", "done", "parent", "rank", "slug"}
	seenEL := map[string]bool{}
	seenRev := map[string]bool{}
	attrs := make([]Attr, 0, n)
	for i := 0; len(attrs) < n && i < n*50; i++ {
		a := Attr{ID: testUUID(i)}
		switch rng.Intn(6) {
		case 0: // Etype stays nil — must be excluded from every etype index
		default:
			a.Etype = mustStr(etypes[rng.Intn(len(etypes))])
		}
		if a.Etype != nil && rng.Intn(6) != 0 { // ~1/6 keep a nil Label
			a.Label = mustStr(labels[rng.Intn(len(labels))])
			if seenEL[*a.Etype+"/"+*a.Label] {
				continue // keep (etype,label) unique: index entries stay unambiguous
			}
			seenEL[*a.Etype+"/"+*a.Label] = true
		}
		if rng.Intn(2) == 0 {
			a.ValueType = "ref"
		} else {
			a.ValueType = "blob"
		}
		a.Cardinality = []string{"one", "many"}[rng.Intn(2)]
		if rng.Intn(3) == 0 {
			a.ReverseEtype = mustStr(etypes[rng.Intn(len(etypes))])
			a.ReverseLabel = mustStr(labels[rng.Intn(len(labels))])
			if seenRev[*a.ReverseEtype+"/"+*a.ReverseLabel] {
				a.ReverseEtype, a.ReverseLabel = nil, nil
			} else {
				seenRev[*a.ReverseEtype+"/"+*a.ReverseLabel] = true
			}
		}
		attrs = append(attrs, a)
	}
	return attrs
}

// bruteByEtype mirrors catalog_cache.go's ByEtype scan.
func bruteByEtype(all []Attr, etype string) map[string]bool {
	out := map[string]bool{}
	for _, a := range all {
		if a.Etype != nil && *a.Etype == etype {
			out[UUIDToStr(a.ID)] = true
		}
	}
	return out
}

// bruteFindEL mirrors catalog_cache.go's FindByEtypeLabel scan.
func bruteFindEL(all []Attr, etype, label string) *Attr {
	for _, a := range all {
		if a.Etype != nil && *a.Etype == etype && a.Label != nil && *a.Label == label {
			cp := a
			return &cp
		}
	}
	return nil
}

// bruteFindReverse mirrors instaql/query.go's findReverseAttr scan — the
// linear scan the indexed FindReverseAttr must match exactly.
func bruteFindReverse(all []Attr, childEtype, parentEtype, label string) *Attr {
	for _, a := range all {
		if a.Etype != nil && *a.Etype == childEtype &&
			a.ReverseEtype != nil && *a.ReverseEtype == parentEtype &&
			a.ReverseLabel != nil && *a.ReverseLabel == label &&
			a.ValueType == "ref" {
			cp := a
			return &cp
		}
	}
	return nil
}

// checkIndexes validates every derived index against independent brute-force
// scans over cat.Attrs() (which reads byID directly — no index involved).
func checkIndexes(t *testing.T, cat *AttrCatalog, phase string) {
	t.Helper()
	all := cat.Attrs()

	// IDStr backfill + UUID() on every stored entry.
	for _, a := range all {
		if a.IDStr == "" || a.IDStr != UUIDToStr(a.ID) || a.UUID() != UUIDToStr(a.ID) {
			t.Fatalf("%s: IDStr/UUID cache broken for %s (IDStr=%q)", phase, UUIDToStr(a.ID), a.IDStr)
		}
	}

	// Expected sets, derived by brute force only.
	wantList := map[string]map[[16]byte]Attr{}
	wantStr := map[string]map[string]Attr{}
	wantLabel := map[string]map[string]Attr{}
	etypes := map[string]bool{}
	for _, a := range all {
		if a.Etype == nil {
			continue
		}
		et := *a.Etype
		etypes[et] = true
		if wantList[et] == nil {
			wantList[et] = map[[16]byte]Attr{}
			wantStr[et] = map[string]Attr{}
		}
		wantList[et][a.ID] = a
		wantStr[et][UUIDToStr(a.ID)] = a
		if a.Label != nil {
			if wantLabel[et] == nil {
				wantLabel[et] = map[string]Attr{}
			}
			wantLabel[et][*a.Label] = a
		}
	}

	probes := make([]string, 0, len(etypes)+1)
	for et := range etypes {
		probes = append(probes, et)
	}
	probes = append(probes, "absent-etype-probe")

	for _, et := range probes {
		// AttrsOfEtype: unordered set parity, values identical to byID.
		got := cat.AttrsOfEtype(et)
		want := wantList[et]
		if len(got) != len(want) {
			t.Fatalf("%s: AttrsOfEtype(%q) len %d, want %d", phase, et, len(got), len(want))
		}
		for _, a := range got {
			wa, ok := want[a.ID]
			if !ok {
				t.Fatalf("%s: AttrsOfEtype(%q) has unexpected id %s", phase, et, UUIDToStr(a.ID))
			}
			if !reflect.DeepEqual(a, wa) {
				t.Fatalf("%s: AttrsOfEtype(%q) attr %s drifted from byID", phase, et, UUIDToStr(a.ID))
			}
		}

		// ByEtype: unchanged semantics — canonical id strings, nil when absent.
		gotIDs := cat.ByEtype(et)
		wantIDs := bruteByEtype(all, et)
		if et == "absent-etype-probe" && gotIDs != nil {
			t.Fatalf("%s: ByEtype(absent) = %v, want nil", phase, gotIDs)
		}
		if len(gotIDs) != len(wantIDs) {
			t.Fatalf("%s: ByEtype(%q) len %d, want %d", phase, et, len(gotIDs), len(wantIDs))
		}
		for _, idStr := range gotIDs {
			if !wantIDs[idStr] {
				t.Fatalf("%s: ByEtype(%q) has unexpected id %s", phase, et, idStr)
			}
		}

		// LabelIndex parity.
		gotL := cat.LabelIndex(et)
		if len(gotL) != len(wantLabel[et]) {
			t.Fatalf("%s: LabelIndex(%q) len %d, want %d", phase, et, len(gotL), len(wantLabel[et]))
		}
		for lb, a := range gotL {
			wa, ok := wantLabel[et][lb]
			if !ok || wa.ID != a.ID || !reflect.DeepEqual(a, wa) {
				t.Fatalf("%s: LabelIndex(%q)[%q] mismatch", phase, et, lb)
			}
		}

		// IDIndex parity, including key = canonical id text.
		gotI := cat.IDIndex(et)
		if len(gotI) != len(wantStr[et]) {
			t.Fatalf("%s: IDIndex(%q) len %d, want %d", phase, et, len(gotI), len(wantStr[et]))
		}
		for idStr, a := range gotI {
			wa, ok := wantStr[et][idStr]
			if !ok || wa.ID != a.ID || idStr != UUIDToStr(a.ID) {
				t.Fatalf("%s: IDIndex(%q)[%s] mismatch", phase, et, idStr)
			}
		}

		// byEtypeIDs (ByEtype's backing store) must agree with brute force.
		seen := map[string]bool{}
		for _, idStr := range cat.byEtypeIDs[et] {
			if !wantIDs[idStr] || seen[idStr] {
				t.Fatalf("%s: byEtypeIDs(%q) bad entry %s", phase, et, idStr)
			}
			seen[idStr] = true
		}
		if len(seen) != len(wantIDs) {
			t.Fatalf("%s: byEtypeIDs(%q) len %d, want %d", phase, et, len(seen), len(wantIDs))
		}
	}

	// FindByEtypeLabel parity on every labeled pair.
	for et, m := range wantLabel {
		for lb := range m {
			got, want := cat.FindByEtypeLabel(et, lb), bruteFindEL(all, et, lb)
			if got == nil || want == nil || got.ID != want.ID {
				t.Fatalf("%s: FindByEtypeLabel(%q,%q) = %v, want %v", phase, et, lb, got, want)
			}
		}
	}
	for _, p := range [][2]string{{"todos", "nope"}, {"nope-etype", "id"}, {"todos", ""}, {"", "ghost"}} {
		if cat.FindByEtypeLabel(p[0], p[1]) != nil {
			t.Fatalf("%s: FindByEtypeLabel(%q,%q) unexpectedly resolved", phase, p[0], p[1])
		}
	}

	// FindReverseAttr parity vs the instaql scan: every real reverse triple
	// (plus perturbed absent variants) must agree exactly.
	seenTr := map[[3]string]bool{}
	var triples [][3]string
	addTr := func(c, p, l string) {
		tr := [3]string{c, p, l}
		if !seenTr[tr] {
			seenTr[tr] = true
			triples = append(triples, tr)
		}
	}
	for _, a := range all {
		if a.Etype == nil || a.ReverseEtype == nil || a.ReverseLabel == nil {
			continue
		}
		c, p, l := *a.Etype, *a.ReverseEtype, *a.ReverseLabel
		addTr(c, p, l)
		addTr(c+"-x", p, l)
		addTr(c, p+"-x", l)
		addTr(c, p, l+"-x")
	}
	addTr("", "todos", "comments")
	for _, tr := range triples {
		got := cat.FindReverseAttr(tr[0], tr[1], tr[2])
		want := bruteFindReverse(all, tr[0], tr[1], tr[2])
		if (got == nil) != (want == nil) {
			t.Fatalf("%s: FindReverseAttr(%q,%q,%q) hit/miss mismatch: got %v want %v",
				phase, tr[0], tr[1], tr[2], got, want)
		}
		if got != nil && !reflect.DeepEqual(*got, *want) {
			t.Fatalf("%s: FindReverseAttr(%q,%q,%q) drifted from the linear scan",
				phase, tr[0], tr[1], tr[2])
		}
	}
}

func TestAttrCatalogIndexParityRandomized(t *testing.T) {
	attrs := randomAttrs(80)

	// Built through Add — exercises the Add+rebuildIndexes path per mutation.
	cat := &AttrCatalog{}
	for _, a := range attrs {
		cat.Add(a)
	}
	checkIndexes(t, cat, "add-built")

	// Clone — exercises the Clone+rebuildIndexes path.
	clone := cat.Clone()
	checkIndexes(t, clone, "clone")

	// Adds on the clone stay clone-local: the original's indexes never see them.
	extra := Attr{
		ID:           testUUID(1 << 20),
		Etype:        mustStr("todos"),
		Label:        mustStr("fresh-link"),
		ValueType:    "ref",
		Cardinality:  "many",
		ReverseEtype: mustStr("users"),
		ReverseLabel: mustStr("fresh-link"),
	}
	clone.Add(extra)
	checkIndexes(t, clone, "clone+add")
	checkIndexes(t, cat, "original-stays-clean")

	if got := clone.FindReverseAttr("todos", "users", "fresh-link"); got == nil || got.ID != extra.ID {
		t.Fatalf("clone reverse lookup: %+v", got)
	}
	if got := cat.FindReverseAttr("todos", "users", "fresh-link"); got != nil {
		t.Fatal("clone's add leaked into the original's reverse index")
	}
	if _, ok := clone.IDIndex("todos")[UUIDToStr(extra.ID)]; !ok {
		t.Fatal("clone IDIndex missing the added attr")
	}
	if _, ok := cat.IDIndex("todos")[UUIDToStr(extra.ID)]; ok {
		t.Fatal("original IDIndex gained the clone's attr")
	}
	if clone.Len() != len(attrs)+1 || cat.Len() != len(attrs) {
		t.Fatalf("lens: clone=%d orig=%d, want %d/%d", clone.Len(), cat.Len(), len(attrs)+1, len(attrs))
	}
	if origLen := len(cat.AttrsOfEtype("todos")); len(clone.AttrsOfEtype("todos")) != origLen+1 {
		t.Fatal("AttrsOfEtype did not track the clone-only add")
	}
}

func TestAttrCatalogZeroValueAndIDStrBackfill(t *testing.T) {
	var cat AttrCatalog
	if got := cat.AttrsOfEtype("x"); got != nil {
		t.Fatal("zero-value AttrsOfEtype must miss")
	}
	if got := cat.LabelIndex("x"); got != nil {
		t.Fatal("zero-value LabelIndex must miss")
	}
	if got := cat.IDIndex("x"); got != nil {
		t.Fatal("zero-value IDIndex must miss")
	}
	if got := cat.FindReverseAttr("a", "b", "c"); got != nil {
		t.Fatal("zero-value FindReverseAttr must miss")
	}
	if n := cat.Len(); n != 0 {
		t.Fatalf("zero-value Len = %d", n)
	}

	// Lone attr: UUID() falls back to UUIDToStr while IDStr is still empty.
	id := testUUID(7)
	lone := Attr{ID: id}
	if lone.IDStr != "" {
		t.Fatal("IDStr must start empty")
	}
	if got := lone.UUID(); got != UUIDToStr(id) {
		t.Fatalf("UUID fallback = %q, want %q", got, UUIDToStr(id))
	}

	// Add backfills IDStr on the stored copy and populates every index.
	lone.Etype = mustStr("todos")
	lone.Label = mustStr("id")
	lone.ValueType = "ref"
	lone.Cardinality = "many"
	lone.ReverseEtype = mustStr("users")
	lone.ReverseLabel = mustStr("comments")
	cat.Add(lone)
	stored, ok := cat.ByID(id)
	if !ok {
		t.Fatal("ByID lost the attr")
	}
	if stored.IDStr != UUIDToStr(id) {
		t.Fatalf("Add did not backfill IDStr: %q", stored.IDStr)
	}
	if got := cat.FindReverseAttr("todos", "users", "comments"); got == nil || got.ID != id {
		t.Fatalf("FindReverseAttr after Add: %+v", got)
	}
	if m := cat.IDIndex("todos"); m == nil || m[UUIDToStr(id)].ID != id {
		t.Fatal("IDIndex not populated by Add")
	}
	if m := cat.LabelIndex("todos"); m == nil || m["id"].ID != id {
		t.Fatal("LabelIndex not populated by Add")
	}
	if l := cat.AttrsOfEtype("todos"); len(l) != 1 || l[0].ID != id {
		t.Fatal("AttrsOfEtype not populated by Add")
	}

	// Re-Add with the same id replaces: indexes reflect the final value only.
	updated := stored
	updated.Label = mustStr("renamed")
	cat.Add(updated)
	if m := cat.LabelIndex("todos"); len(m) != 1 || m["renamed"].ID != id {
		t.Fatalf("LabelIndex after overwrite: %v", m)
	}
	if l := cat.AttrsOfEtype("todos"); len(l) != 1 {
		t.Fatalf("overwrite duplicated AttrsOfEtype: %d", len(l))
	}
}

func TestAttrCatalogCloneBackfillsIDStr(t *testing.T) {
	id := testUUID(11)
	// Hand-rolled byID with no indexes: a pre-refactor catalog image.
	src := &AttrCatalog{byID: map[[16]byte]Attr{
		id: {ID: id, Etype: mustStr("todos"), Label: mustStr("id"),
			ValueType: "blob", Cardinality: "one"},
	}}
	if pre, _ := src.ByID(id); pre.IDStr != "" {
		t.Fatal("precondition: source must start unbackfilled")
	}
	cp := src.Clone()
	got, ok := cp.ByID(id)
	if !ok {
		t.Fatal("clone lost the attr")
	}
	if got.IDStr != UUIDToStr(id) || got.UUID() != UUIDToStr(id) {
		t.Fatalf("clone did not backfill IDStr: %q", got.IDStr)
	}
	if m := cp.IDIndex("todos"); m == nil || len(m) != 1 {
		t.Fatalf("clone IDIndex: %v", m)
	}
	// The clone's backfill must not leak into the source's stored map…
	if post, _ := src.ByID(id); post.IDStr != "" {
		t.Fatal("clone backfilled the source")
	}
	// …while the source still renders the canonical uuid via the fallback.
	if srcAttr, _ := src.ByID(id); srcAttr.UUID() != UUIDToStr(id) {
		t.Fatalf("source UUID fallback = %q", srcAttr.UUID())
	}
}

func TestAttrsOfEtypeReturnsInternalSlice(t *testing.T) {
	cat := &AttrCatalog{}
	cat.Add(Attr{ID: testUUID(1), Etype: mustStr("todos"), Label: mustStr("id"),
		ValueType: "blob", Cardinality: "one"})
	cat.Add(Attr{ID: testUUID(2), Etype: mustStr("todos"), // nil Label
		ValueType: "blob", Cardinality: "many"})

	l1 := cat.AttrsOfEtype("todos")
	l2 := cat.AttrsOfEtype("todos")
	if len(l1) != 2 || &l1[0] != &l2[0] {
		t.Fatal("AttrsOfEtype must hand back the internal slice (do-not-mutate contract)")
	}
	if got := cat.AttrsOfEtype("absent"); got != nil {
		t.Fatalf("absent etype = %v, want nil", got)
	}
	// The nil-Label attr participates in the list…
	var sawLabelless bool
	for _, a := range l1 {
		if a.Label == nil {
			sawLabelless = true
		}
	}
	if !sawLabelless {
		t.Fatal("labelless attr missing from AttrsOfEtype")
	}
	// …but not in the label index.
	if m := cat.LabelIndex("todos"); len(m) != 1 {
		t.Fatalf("LabelIndex len = %d, want 1 (labelless attr excluded)", len(m))
	}
}

func TestFindReverseAttrMatchesInstaqlScan(t *testing.T) {
	// v1 create-ref-attr shape (transact/highlevel.go): the forward attr
	// comments.todo carries reverse_etype=label, reverse_label=etype.
	commentsTodo := Attr{
		ID: testUUID(22), Etype: mustStr("comments"), Label: mustStr("todo"),
		ValueType: "ref", Cardinality: "many",
		ReverseEtype: mustStr("todos"), ReverseLabel: mustStr("comments"),
	}
	// Decoys: a blob attr sharing the reverse key (must NOT resolve — the
	// ValueType=="ref" filter), a ref with a different child etype, a ref
	// with reverse metadata but nil Etype, and a ref without reverse metadata.
	blobDecoy := Attr{
		ID: testUUID(23), Etype: mustStr("comments"), Label: mustStr("todoBlob"),
		ValueType: "blob", Cardinality: "many",
		ReverseEtype: mustStr("todos"), ReverseLabel: mustStr("comments"),
	}
	otherChild := Attr{
		ID: testUUID(24), Etype: mustStr("folders"), Label: mustStr("todo"),
		ValueType: "ref", Cardinality: "many",
		ReverseEtype: mustStr("todos"), ReverseLabel: mustStr("comments"),
	}
	noEtype := Attr{
		ID: testUUID(25), ValueType: "ref", Cardinality: "many",
		ReverseEtype: mustStr("todos"), ReverseLabel: mustStr("comments"),
	}
	noReverse := Attr{
		ID: testUUID(26), Etype: mustStr("notes"), Label: mustStr("todo"),
		ValueType: "ref", Cardinality: "many",
	}
	cat := &AttrCatalog{}
	for _, a := range []Attr{commentsTodo, blobDecoy, otherChild, noEtype, noReverse} {
		cat.Add(a)
	}

	got := cat.FindReverseAttr("comments", "todos", "comments")
	if got == nil || got.ID != commentsTodo.ID {
		t.Fatalf("FindReverseAttr = %+v, want %s", got, UUIDToStr(commentsTodo.ID))
	}
	if got := cat.FindReverseAttr("folders", "todos", "comments"); got == nil || got.ID != otherChild.ID {
		t.Fatalf("other child: %+v", got)
	}
	for _, tr := range [][3]string{
		{"comments", "todos", "comments-x"}, {"comments-x", "todos", "comments"},
		{"comments", "todos-x", "comments"}, {"notes", "todos", "comments"},
		{"absent", "todos", "comments"}, {"", "todos", "comments"},
	} {
		if hit := cat.FindReverseAttr(tr[0], tr[1], tr[2]); hit != nil {
			t.Fatalf("FindReverseAttr(%q,%q,%q) resolved %s; want nil",
				tr[0], tr[1], tr[2], hit.UUID())
		}
	}

	// Heap-copy per call: mutating one result cannot poison the index, the
	// catalog, or the next lookup.
	first := cat.FindReverseAttr("comments", "todos", "comments")
	first.ValueType = "blob"
	first.Etype = mustStr("hacked")
	second := cat.FindReverseAttr("comments", "todos", "comments")
	if second == nil || second.ValueType != "ref" || *second.Etype != "comments" {
		t.Fatal("FindReverseAttr must return a defensive copy per call")
	}
	if stored := cat.byID[commentsTodo.ID]; stored.ValueType != "ref" || *stored.Etype != "comments" {
		t.Fatal("mutation leaked into the catalog")
	}

	// Full parity against the linear scan over this fixture set.
	checkIndexes(t, cat, "reverse-fixtures")
}

func TestByEtypeAndFindByEtypeLabelUnchanged(t *testing.T) {
	idLabeled, idLabelless, idOrphan, idUsers, idEmpty :=
		testUUID(31), testUUID(32), testUUID(33), testUUID(34), testUUID(35)
	cat := &AttrCatalog{}
	for _, a := range []Attr{
		{ID: idLabeled, Etype: mustStr("todos"), Label: mustStr("id"), ValueType: "blob", Cardinality: "one"},
		{ID: idLabelless, Etype: mustStr("todos"), ValueType: "blob", Cardinality: "many"}, // nil Label
		{ID: idOrphan, Label: mustStr("orphan"), ValueType: "blob", Cardinality: "one"},    // nil Etype
		{ID: idUsers, Etype: mustStr("users"), Label: mustStr("email"), ValueType: "blob", Cardinality: "one"},
		{ID: idEmpty, Etype: mustStr(""), Label: mustStr("ghost"), ValueType: "blob", Cardinality: "one"}, // etype ""
	} {
		cat.Add(a)
	}

	// ByEtype: only non-nil etype participates; canonical uuid text; nil when absent.
	got := cat.ByEtype("todos")
	if len(got) != 2 {
		t.Fatalf("ByEtype(todos) = %v, want 2 ids", got)
	}
	wantSet := map[string]bool{UUIDToStr(idLabeled): true, UUIDToStr(idLabelless): true}
	for _, s := range got {
		if !wantSet[s] {
			t.Fatalf("ByEtype(todos) has unexpected %q", s)
		}
	}
	if cat.ByEtype("absent") != nil {
		t.Fatal("ByEtype(absent) must be nil (unchanged semantics)")
	}
	if got := cat.ByEtype(""); len(got) != 1 || got[0] != UUIDToStr(idEmpty) {
		t.Fatalf("ByEtype(\"\") = %v — empty-string etype must still match", got)
	}

	// FindByEtypeLabel: matches only Etype!=nil && Label!=nil.
	hit := cat.FindByEtypeLabel("todos", "id")
	if hit == nil || hit.ID != idLabeled {
		t.Fatalf("FindByEtypeLabel(todos,id) = %+v", hit)
	}
	for _, p := range [][2]string{
		{"todos", "missing"}, {"todos", ""}, {"", "orphan"},
		{"todos", "orphan"}, {"users", "id"}, {"absent", "id"},
	} {
		if cat.FindByEtypeLabel(p[0], p[1]) != nil {
			t.Fatalf("FindByEtypeLabel(%q,%q) unexpectedly resolved", p[0], p[1])
		}
	}
	orig := *hit.Label
	hit.Label = mustStr("hacked") // reassign the copy's pointer — struct-level isolation
	if again := cat.FindByEtypeLabel("todos", "id"); again == nil || again.Label == hit.Label || *again.Label != orig {
		t.Fatal("FindByEtypeLabel must return a heap copy")
	}
	if m := cat.LabelIndex("todos"); m["id"].Label == nil || *m["id"].Label != "id" {
		t.Fatal("finder-result mutation corrupted the label index")
	}
	if stored, _ := cat.ByID(idLabeled); *stored.Label != "id" {
		t.Fatal("finder-result mutation leaked into byID")
	}
}

// ---- LoadAttrCatalog rebuild --------------------------------------------------

// scriptedQ serves a fixed attr row set; scriptedRows decodes them positionally
// in attrCols order (id, app_id, etype, label, reverse_etype, reverse_label,
// value_type, cardinality, is_unique, is_indexed, forward_ident,
// reverse_ident, checked_data_type).
type scriptedQ struct{ rows []Attr }

func (q *scriptedQ) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return &scriptedRows{rows: q.rows}, nil
}

type scriptedRows struct {
	rows []Attr
	i    int
}

func (r *scriptedRows) Close()                                       {}
func (r *scriptedRows) Next() bool                                   { return r.i < len(r.rows) }
func (r *scriptedRows) Err() error                                   { return nil }
func (r *scriptedRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *scriptedRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *scriptedRows) Values() ([]any, error)                       { return nil, nil }
func (r *scriptedRows) RawValues() [][]byte                          { return nil }
func (r *scriptedRows) Conn() *pgx.Conn                              { return nil }

func (r *scriptedRows) Scan(dest ...any) error {
	if r.i >= len(r.rows) {
		return pgx.ErrNoRows
	}
	if len(dest) != 13 {
		return fmt.Errorf("scriptedRows: want 13 dests, got %d", len(dest))
	}
	a := r.rows[r.i]
	r.i++
	*dest[0].(*[16]byte) = a.ID
	*dest[1].(*[16]byte) = a.AppID
	*dest[2].(**string) = a.Etype
	*dest[3].(**string) = a.Label
	*dest[4].(**string) = a.ReverseEtype
	*dest[5].(**string) = a.ReverseLabel
	*dest[6].(*string) = a.ValueType
	*dest[7].(*string) = a.Cardinality
	*dest[8].(*bool) = a.IsUnique
	*dest[9].(*bool) = a.IsIndexed
	*dest[10].(*[16]byte) = a.ForwardIdent
	*dest[11].(**[16]byte) = a.ReverseIdent
	*dest[12].(**string) = a.CheckedDataType
	return nil
}

func TestLoadAttrCatalogRebuildsIndexes(t *testing.T) {
	appID := testUUID(900)
	rows := randomAttrs(40)
	for i := range rows {
		rows[i].AppID = appID
	}
	cat, err := LoadAttrCatalog(context.Background(), &scriptedQ{rows: rows}, appID)
	if err != nil {
		t.Fatalf("LoadAttrCatalog: %v", err)
	}
	if cat.AppID != appID {
		t.Fatal("AppID lost")
	}
	checkIndexes(t, cat, "load")
}
