package sync

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// v1's collect-instaql-results emits the implicit <etype>/id triple in
// join-rows; TS clients assemble entities from it (store primaryKeys).
// Regression: omitting it rendered empty lists in every real SDK app.
func TestBuildNodeListEmitsIDTriples(t *testing.T) {
	idAttr := &platform.Attr{}
	idID := mustUUID(t, "11111111-1111-4111-8111-111111111111")
	idAttr.ID = idID
	idLabel := "id"
	idEtype := "todos"
	idAttr.Label = &idLabel
	idAttr.Etype = &idEtype
	idAttr.ValueType = "blob"
	idAttr.Cardinality = "one"
	textID := mustUUID(t, "22222222-2222-4222-8222-222222222222")
	textAttr := &platform.Attr{ID: textID, ValueType: "blob", Cardinality: "one"}
	textLabel := "text"
	textEtype := "todos"
	textAttr.Label = &textLabel
	textAttr.Etype = &textEtype
	cat := &platform.AttrCatalog{}
	cat.Add(*idAttr)
	cat.Add(*textAttr)

	result := json.RawMessage(`{"data":{"todos":[{"id":"aaa","text":"hello"}]}}`)
	out, err := BuildNodeList(cat, result)
	if err != nil {
		t.Fatal(err)
	}
	var nodes []struct {
		Data struct {
			DatalogResult struct {
				JoinRows [][][]any `json:"join-rows"`
			} `json:"datalog-result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(out, &nodes); err != nil {
		t.Fatal(err)
	}
	if len(nodes[0].Data.DatalogResult.JoinRows) == 0 {
		t.Fatal("no join rows")
	}
	rows := nodes[0].Data.DatalogResult.JoinRows[0]
	var sawID, sawText bool
	for _, arr := range rows {
		if len(arr) < 3 {
			continue
		}
		if arr[1] == platform.UUIDToStr(idID) && arr[2] == "aaa" {
			sawID = true
		}
		if arr[1] == platform.UUIDToStr(textID) && arr[2] == "hello" {
			sawText = true
		}
	}
	if !sawID || !sawText {
		t.Fatalf("join-rows missing id or text triple: sawID=%v sawText=%v rows=%v", sawID, sawText, rows)
	}
}

func TestBuildNodeListWireParity(t *testing.T) {
	cat := nodeListTestCatalog(t)
	tests := []struct {
		name   string
		result json.RawMessage
	}{
		{
			name: "numbers null nested values and refs",
			result: json.RawMessage(`{"data":{"todos":[{` +
				`"id":"todo-1","title":"\u0061 plain","rank":9007199254740993,` +
				`"ratio":1.0,"empty":null,"meta":{"z":2,"a":[true,null,{"b":"x"}]},` +
				`"owner":[{"id":"user-1"},"user-2",null,{"ID":"ignored"},{"name":"ignored"}]` +
				`}]}}`),
		},
		{
			name: "sorted etypes with page and aggregate",
			result: json.RawMessage(`{"data":{` +
				`"users":[{"id":"user-1","name":"Pri"}],` +
				`"todos":[{"id":"todo-1","title":"hello"}]` +
				`},"page-info":{"end-cursor":"c1","has-next-page?":true},` +
				`"aggregate":{"count":2,"sum":2.0}}`),
		},
		{
			name:   "null and empty entity lists",
			result: json.RawMessage(`{"data":{"todos":null,"users":[]}}`),
		},
		{
			name:   "string escaping",
			result: json.RawMessage(`{"data":{"todos":[{"id":"todo-1","title":"<>&"}]}}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildNodeList(cat, tt.result)
			if err != nil {
				t.Fatal(err)
			}
			want := nodeListWireFixture(t)
			if string(got) != string(want) {
				t.Fatalf("output differs from captured wire fixture\n got: %s\nwant: %s", got, want)
			}
			if !json.Valid(got) {
				t.Fatalf("output is invalid JSON: %s", got)
			}
		})
	}
}

func TestBuildNodeListDeepValues(t *testing.T) {
	value := "0"
	for range 129 {
		value = "[" + value + "]"
	}
	result := json.RawMessage(`{"data":{"todos":[{"id":"todo-1","meta":` + value + `}]}}`)
	cat := nodeListTestCatalog(t)

	want := nodeListWireFixture(t)
	got, err := BuildNodeList(cat, result)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("output differs from captured wire fixture\n got: %s\nwant: %s", got, want)
	}
}

func TestBuildNodeListDeterministicWireFixtures(t *testing.T) {
	const caseCount = 72
	numbers := []string{
		"0", "-0", "1.0", "-12.500", "0.000100", "1e+06", "-2.5E-3",
		"9007199254740993", "9223372036854775807", "-9223372036854775808",
	}
	rng := rand.New(rand.NewSource(0xA2C105ED))
	cat := nodeListTestCatalog(t)

	for caseIndex := range caseCount {
		// Generate every case even when -run selects only one subtest, so the
		// seeded stream matches the inputs captured in the wire fixtures.
		number := numbers[caseIndex%len(numbers)]
		result := nodeListPropertyEnvelope(t, rng, caseIndex, number)
		t.Run(fmt.Sprintf("case_%02d", caseIndex), func(t *testing.T) {
			want := nodeListWireFixture(t)
			got, err := BuildNodeList(cat, result)
			if err != nil {
				t.Fatalf("node-list encoder: %v\ninput: %s", err, result)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("output differs from captured wire fixture\ninput: %s\n got: %s\nwant: %s", result, got, want)
			}
			if !bytes.Contains(got, []byte(number)) {
				t.Fatalf("numeric spelling %q was not retained in output: %s", number, got)
			}
			if !json.Valid(got) {
				t.Fatalf("output is invalid JSON: %s", got)
			}
		})
	}
}

func TestBuildNodeListEscapingAndDepthWireFixtures(t *testing.T) {
	deepValue := "0"
	for range 129 {
		deepValue = "[" + deepValue + "]"
	}
	tests := []struct {
		name   string
		result json.RawMessage
	}{
		{
			name:   "escaped quote and backslash",
			result: json.RawMessage(`{"data":{"todos":[{"id":"todo-1","title":"quote\"slash\\tail"}]}}`),
		},
		{
			name:   "escaped control",
			result: json.RawMessage(`{"data":{"todos":[{"id":"todo-1","title":"line\nbreak"}]}}`),
		},
		{
			name:   "non ascii",
			result: json.RawMessage(`{"data":{"todos":[{"id":"todo-1","title":"café"}]}}`),
		},
		{
			name:   "html sensitive ascii",
			result: json.RawMessage(`{"data":{"todos":[{"id":"todo-1","title":"<tag>&value"}]}}`),
		},
		{
			name:   "depth bound",
			result: json.RawMessage(`{"data":{"todos":[{"id":"todo-1","meta":` + deepValue + `}]}}`),
		},
	}
	cat := nodeListTestCatalog(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := nodeListWireFixture(t)
			got, err := BuildNodeList(cat, tt.result)
			if err != nil {
				t.Fatalf("node-list encoder: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("output differs from captured wire fixture\n got: %s\nwant: %s", got, want)
			}
		})
	}
}

func nodeListPropertyEnvelope(tb testing.TB, rng *rand.Rand, caseIndex int, number string) json.RawMessage {
	tb.Helper()

	todos := make([]map[string]json.RawMessage, caseIndex%4)
	for entityIndex := range todos {
		id := fmt.Sprintf("todo_%02d_%02d", caseIndex, entityIndex)
		todos[entityIndex] = map[string]json.RawMessage{
			"id":                                   mustMarshalNodeListProperty(tb, id),
			"title":                                mustMarshalNodeListProperty(tb, fmt.Sprintf("safe title %d %d", caseIndex, entityIndex)),
			"rank":                                 json.RawMessage(number),
			"done":                                 json.RawMessage(fmt.Sprintf("%t", (caseIndex+entityIndex)%2 == 0)),
			"nullable":                             json.RawMessage("null"),
			"meta":                                 nodeListNestedProperty(tb, rng, caseIndex+entityIndex),
			fmt.Sprintf("unknown_%d", entityIndex): randomNodeListPropertyValue(tb, rng, 0, caseIndex*3+entityIndex),
			"owner":                                nodeListPropertyRef(tb, caseIndex+entityIndex),
		}
	}

	users := make([]map[string]json.RawMessage, (caseIndex/2)%3)
	for entityIndex := range users {
		users[entityIndex] = map[string]json.RawMessage{
			"id":      mustMarshalNodeListProperty(tb, fmt.Sprintf("user_%02d_%02d", caseIndex, entityIndex)),
			"name":    mustMarshalNodeListProperty(tb, fmt.Sprintf("safe user %d", entityIndex)),
			"profile": randomNodeListPropertyValue(tb, rng, 0, caseIndex+entityIndex+17),
		}
	}

	data := map[string]json.RawMessage{
		"logs":  json.RawMessage("[]"), // unknown etype and empty list
		"todos": mustMarshalNodeListProperty(tb, todos),
		"users": mustMarshalNodeListProperty(tb, users),
	}
	envelope := map[string]json.RawMessage{
		"data": mustMarshalNodeListProperty(tb, data),
	}
	if caseIndex%2 == 0 {
		envelope["page-info"] = mustMarshalNodeListProperty(tb, map[string]json.RawMessage{
			"end-cursor":     mustMarshalNodeListProperty(tb, fmt.Sprintf("cursor_%02d", caseIndex)),
			"has-next-page?": json.RawMessage(fmt.Sprintf("%t", caseIndex%4 == 0)),
			"offset":         json.RawMessage(number),
		})
	}
	if caseIndex%3 == 0 {
		envelope["aggregate"] = mustMarshalNodeListProperty(tb, map[string]json.RawMessage{
			"count":  json.RawMessage(number),
			"nested": randomNodeListPropertyValue(tb, rng, 0, caseIndex+31),
		})
	}
	return mustMarshalNodeListProperty(tb, envelope)
}

func nodeListPropertyRef(tb testing.TB, variant int) json.RawMessage {
	tb.Helper()
	id := fmt.Sprintf("user_ref_%02d", variant)
	switch variant % 4 {
	case 0:
		return mustMarshalNodeListProperty(tb, id)
	case 1:
		return mustMarshalNodeListProperty(tb, map[string]any{"id": id})
	case 2:
		return mustMarshalNodeListProperty(tb, []any{
			id,
			map[string]any{"id": id + "_object"},
			nil,
			map[string]any{"ID": "ignored"},
			map[string]any{"name": "ignored"},
		})
	default:
		return json.RawMessage("[]")
	}
}

func nodeListNestedProperty(tb testing.TB, rng *rand.Rand, salt int) json.RawMessage {
	tb.Helper()
	return mustMarshalNodeListProperty(tb, map[string]json.RawMessage{
		"array": mustMarshalNodeListProperty(tb, []json.RawMessage{
			json.RawMessage("null"),
			json.RawMessage(fmt.Sprintf("%t", salt%2 == 0)),
			randomNodeListPropertyValue(tb, rng, 1, salt+1),
		}),
		"object": mustMarshalNodeListProperty(tb, map[string]json.RawMessage{
			"large":  json.RawMessage("9007199254740993"),
			"nested": randomNodeListPropertyValue(tb, rng, 1, salt+2),
		}),
	})
}

func randomNodeListPropertyValue(tb testing.TB, rng *rand.Rand, depth, salt int) json.RawMessage {
	tb.Helper()
	numbers := []string{"0", "-0", "1.0", "0.1250", "3e+07", "9007199254740993"}
	leafKinds := 4
	kinds := leafKinds
	if depth < 4 {
		kinds += 2
	}
	switch (rng.Intn(kinds) + salt) % kinds {
	case 0:
		return json.RawMessage("null")
	case 1:
		return json.RawMessage(fmt.Sprintf("%t", (salt+depth)%2 == 0))
	case 2:
		return json.RawMessage(numbers[(rng.Intn(len(numbers))+salt)%len(numbers)])
	case 3:
		return mustMarshalNodeListProperty(tb, fmt.Sprintf("safe value %d %d", salt, depth))
	case 4:
		count := rng.Intn(4)
		values := make([]json.RawMessage, count)
		for i := range values {
			values[i] = randomNodeListPropertyValue(tb, rng, depth+1, salt+i+1)
		}
		return mustMarshalNodeListProperty(tb, values)
	default:
		count := rng.Intn(4)
		fields := make(map[string]json.RawMessage, count)
		for i := range count {
			fields[fmt.Sprintf("field_%d", i)] = randomNodeListPropertyValue(tb, rng, depth+1, salt+i+1)
		}
		return mustMarshalNodeListProperty(tb, fields)
	}
}

func mustMarshalNodeListProperty(tb testing.TB, value any) json.RawMessage {
	tb.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		tb.Fatalf("marshal node-list property fixture: %v", err)
	}
	return raw
}

func nodeListTestCatalog(tb testing.TB) *platform.AttrCatalog {
	tb.Helper()
	cat := &platform.AttrCatalog{}
	add := func(id, etype, label, valueType string) {
		attr := platform.Attr{ID: mustUUIDTB(tb, id), Etype: &etype, Label: &label, ValueType: valueType, Cardinality: "one"}
		cat.Add(attr)
	}
	add("11111111-1111-4111-8111-111111111111", "todos", "id", "blob")
	add("22222222-2222-4222-8222-222222222222", "todos", "title", "blob")
	add("33333333-3333-4333-8333-333333333333", "todos", "rank", "blob")
	add("44444444-4444-4444-8444-444444444444", "todos", "done", "blob")
	add("55555555-5555-4555-8555-555555555555", "todos", "owner", "ref")
	add("66666666-6666-4666-8666-666666666666", "users", "id", "blob")
	add("77777777-7777-4777-8777-777777777777", "users", "name", "blob")
	return cat
}

func mustUUIDTB(tb testing.TB, s string) [16]byte {
	tb.Helper()
	if strings.Count(s, "-") != 4 {
		tb.Fatalf("bad test UUID %q", s)
	}
	var u [16]byte
	hexVals := map[byte]byte{'0': 0, '1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7, '8': 8, '9': 9, 'a': 10, 'b': 11, 'c': 12, 'd': 13, 'e': 14, 'f': 15}
	idx := 0
	for i := 0; i < len(s) && idx < 32; i++ {
		if s[i] == '-' {
			continue
		}
		if idx%2 == 0 {
			u[idx/2] = hexVals[s[i]] << 4
		} else {
			u[idx/2] |= hexVals[s[i]]
		}
		idx++
	}
	return u
}

func mustUUID(t *testing.T, s string) [16]byte {
	t.Helper()
	var u [16]byte
	hex := []byte(s)
	hexVals := map[byte]byte{'0': 0, '1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7, '8': 8, '9': 9, 'a': 10, 'b': 11, 'c': 12, 'd': 13, 'e': 14, 'f': 15}
	idx := 0
	for i := 0; i < len(hex) && idx < 32; i++ {
		c := hex[i]
		if c == '-' {
			continue
		}
		if idx%2 == 0 {
			u[idx/2] = hexVals[c] << 4
		} else {
			u[idx/2] |= hexVals[c]
		}
		idx++
	}
	return u
}

// Fixtures were captured from the pre-refactor encoder after its typed/generic
// differential checks passed. Keep literal bytes independent of the encoder;
// regenerating them from BuildNodeList would hide compatibility regressions.
func nodeListWireFixture(t *testing.T) json.RawMessage {
	t.Helper()
	raw, err := os.ReadFile("testdata/nodelist-wire.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures map[string]string
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	wire, ok := fixtures[t.Name()]
	if !ok {
		t.Fatalf("missing node-list wire fixture for %s", t.Name())
	}
	return json.RawMessage(wire)
}
