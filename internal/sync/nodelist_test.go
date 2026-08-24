package sync

import (
	"encoding/json"
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
