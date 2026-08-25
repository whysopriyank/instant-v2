package perms

import (
	"sync"

	cel "cel.dev/cel-go/cel"
	ext "cel.dev/cel-go/ext"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// envCache is keyed by any sentinel; v1's program-cache is per (ruleVersion,
// etype, action) but Phase 2 keeps a single env compiled once and programs
// compiled per expression. Future versions add per-rule caching behind an
// explicit RuleVersion field on RuleDoc.
var (
	cachedEnv  *cel.Env
	cachedDesc protoreflect.MessageDescriptor
	onceEnv    sync.Once
	onceDesc   sync.Once
	progMemo   sync.Map // string → cel.Program (expression + wrapped binds)
)

// env returns a CEL Env with the frozen variable set and std macros, plus the
// dynamically-built instant.Request descriptor (v1 db/proto.clj).
func env() *cel.Env {
	onceEnv.Do(func() {
		desc := requestDescriptor()
		cachedDesc = desc
		var err error
		cachedEnv, err = cel.NewEnv(
			ext.Bindings(),
			cel.Variable("data", cel.MapType(cel.StringType, cel.DynType)),
			cel.Variable("auth", cel.MapType(cel.StringType, cel.DynType)),
			cel.Variable("ruleParams", cel.MapType(cel.StringType, cel.DynType)),
			cel.Variable("newData", cel.MapType(cel.StringType, cel.DynType)),
			cel.Variable("linkedData", cel.MapType(cel.StringType, cel.DynType)),
			cel.Variable("actions", cel.MapType(cel.StringType, cel.DynType)),
			cel.Types(dynamicpb.NewMessage(desc)),
			cel.Variable("request", cel.ObjectType(string(desc.FullName()))),
		)
		if err != nil {
			panic("perms: cel.NewEnv: " + err.Error())
		}
	})
	return cachedEnv
}

func requestDescriptor() protoreflect.MessageDescriptor {
	var fd protoreflect.MessageDescriptor
	_ = timestamppb.Now
	onceDesc.Do(func() {
		n := func(s string) *string { return &s }
		num := func(i int32) *int32 { return &i }
		f := &descriptorpb.FileDescriptorProto{
			Name:       n("instant/request.proto"),
			Package:    n("instant"),
			Dependency: []string{"google/protobuf/timestamp.proto"},
			MessageType: []*descriptorpb.DescriptorProto{{
				Name: n("Request"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   n("modifiedFields"),
						Number: num(1),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
					{
						Name:     n("time"),
						Number:   num(2),
						Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
						TypeName: n(".google.protobuf.Timestamp"),
					},
					{
						Name:   n("ip"),
						Number: num(3),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
					{
						Name:   n("origin"),
						Number: num(4),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
				},
			}},
			Syntax: n("proto3"),
		}
		fileDesc, err := protodesc.NewFile(f, protoregistry.GlobalFiles)
		if err != nil {
			panic("perms: protodesc request: " + err.Error())
		}
		fd = fileDesc.Messages().ByName("Request")
		if fd == nil {
			panic("perms: request descriptor missing")
		}
		cachedDesc = fd
	})
	return cachedDesc
}

// maxCELCost bounds one rule evaluation. Rules are admin-authored, but they
// run against attacker-influenced data/newData payloads on a shared process —
// an unbounded map/reduce chain must not become a tenant-to-tenant CPU
// amplifier. Sized generously above any legitimate v1-style rule.
const maxCELCost = 1_000_000

// compile produces or memoizes a program for a raw CEL expression string.
func compile(expr string) (cel.Program, error) {
	if v, ok := progMemo.Load(expr); ok {
		return v.(cel.Program), nil
	}
	e := env()
	ast, iss := e.Compile(expr)
	if iss != nil && iss.Err() != nil {
		return nil, iss.Err()
	}
	prg, err := e.Program(ast, cel.CostLimit(maxCELCost))
	if err != nil {
		return nil, err
	}
	progMemo.Store(expr, prg)
	return prg, nil
}
