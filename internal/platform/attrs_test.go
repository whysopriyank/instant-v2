package platform

import "testing"

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
