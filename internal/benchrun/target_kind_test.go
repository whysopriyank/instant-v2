package benchrun

import "testing"

func TestTargetKindBindingCoversCanonicalAndLegacyIDs(t *testing.T) {
	for _, tc := range []struct {
		id, role, kind string
	}{
		{id: "v1", role: "v1", kind: "v1"},
		{id: "v2_reference", role: "v2_reference", kind: "v2"},
		{id: "v2_current", role: "v2_current", kind: "v2"},
		{id: "v2", role: "v2", kind: "v2"},
		{id: "v2", role: "v2_current", kind: "v2"},
		{id: "v2-current", role: "v2_current", kind: "v2"},
	} {
		t.Run(tc.id+"/"+tc.role, func(t *testing.T) {
			target := Target{ID: tc.id, Role: tc.role, Kind: tc.kind}
			if err := validateTargetBinding(target); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, target := range []Target{
		{ID: "v1", Role: "v1", Kind: "v2"},
		{ID: "v2_reference", Role: "v2_reference", Kind: "v1"},
		{ID: "v2_current", Role: "v2_current", Kind: "v1"},
	} {
		if err := validateTargetBinding(target); err == nil {
			t.Fatalf("forged target binding accepted: %+v", target)
		}
	}
}
