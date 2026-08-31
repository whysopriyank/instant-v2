package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Check the actual handwritten constants, not schemagen's independent list.
func TestSchemaMatchesHandwrittenOps(t *testing.T) {
	source, err := parser.ParseFile(token.NewFileSet(), "protocol.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var ops []string
	for _, decl := range source.Decls {
		group, ok := decl.(*ast.GenDecl)
		if !ok || group.Tok != token.CONST {
			continue
		}
		for _, spec := range group.Specs {
			value := spec.(*ast.ValueSpec)
			for index, name := range value.Names {
				if !strings.HasPrefix(name.Name, "Op") {
					continue
				}
				if index >= len(value.Values) {
					t.Fatalf("%s must have an explicit wire value", name.Name)
				}
				literal, ok := value.Values[index].(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					t.Fatalf("%s must be a literal wire string", name.Name)
				}
				op, err := strconv.Unquote(literal.Value)
				if err != nil {
					t.Fatal(err)
				}
				ops = append(ops, op)
			}
		}
	}
	var schema struct {
		Definitions struct {
			WSOp struct {
				Enum []string `json:"enum"`
			} `json:"wsOp"`
		} `json:"definitions"`
	}
	if err := json.Unmarshal(schemaBytes(t), &schema); err != nil {
		t.Fatal(err)
	}
	sort.Strings(ops)
	sort.Strings(schema.Definitions.WSOp.Enum)
	if len(ops) == 0 || !reflect.DeepEqual(ops, schema.Definitions.WSOp.Enum) {
		t.Fatalf("handwritten ops %v differ from schema %v", ops, schema.Definitions.WSOp.Enum)
	}
}

func TestGeneratedSchemaHashMatchesSource(t *testing.T) {
	digest := sha256.Sum256(schemaBytes(t))
	want := "sha256:" + hex.EncodeToString(digest[:])
	if SchemaHash != want {
		t.Fatalf("generated schema hash %s != %s; run make schemagen", SchemaHash, want)
	}
}

func schemaBytes(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile("schema/protocol.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	return data
}
