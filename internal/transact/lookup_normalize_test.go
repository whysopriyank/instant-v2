package transact

import (
	"encoding/json"
	"testing"
)

func TestNormalizeValueCanonicalizesExactDecimalNumbers(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "equivalent spellings", raw: `{"z":1.0,"a":[1e0,-0,1.2300]}`, want: `{"a":[1,0,1.23],"z":1}`},
		{name: "large adjacent integers", raw: `9007199254740992`, want: `9007199254740992`},
		{name: "large adjacent integer remains distinct", raw: `9007199254740993`, want: `9007199254740993`},
		{name: "small exponent", raw: `1e-7`, want: `1e-7`},
		{name: "large exponent", raw: `1e22`, want: `1e22`},
		{name: "fraction", raw: `-12.3400e-2`, want: `-0.1234`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeValue(json.RawMessage(tc.raw))
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Fatalf("normalizeValue(%s) = %s, want %s", tc.raw, got, tc.want)
			}
		})
	}
}

func TestNormalizeValueRejectsTrailingJSON(t *testing.T) {
	if _, err := normalizeValue(json.RawMessage(`1 2`)); err == nil {
		t.Fatal("normalizeValue accepted trailing JSON")
	}
}
