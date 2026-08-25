package perms

import (
	"fmt"
	"strings"

	celtypes "cel.dev/cel-go/common/types"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Decision is the result enforced against one tx-step group.
type Decision struct {
	Allow bool
	Err   error // compile or runtime error surfaced as a permission hint
}

// ViewGate statically classifies the view rule for (etype) so read paths can
// decide WITHOUT record bindings:
//
//	ViewOpen    — no rule, or the rule is literally "true": everyone may view.
//	ViewClosed  — the rule is literally "false": nobody may view.
//	ViewDynamic — any other expression: needs auth/data bindings to decide.
//
// Read planes use this to fail closed on ViewDynamic until CEL→SQL rule-where
// pushdown exists (a dynamic rule evaluated without data bindings could
// over-allow data-dependent rules).
type ViewMode int

const (
	ViewOpen ViewMode = iota
	ViewClosed
	ViewDynamic
)

func ViewGate(doc *RuleDoc, etype string) ViewMode {
	if doc == nil {
		return ViewOpen
	}
	switch expr := doc.ResolveExpr(etype, "view"); expr {
	case "", "true":
		return ViewOpen
	case "false":
		return ViewClosed
	default:
		return ViewDynamic
	}
}

// Check evaluates the resolved program for (etype, action) with the given
// bindings. Mirrors db/cel.clj eval-program! with the patch-code semantics
// (literal strings "true"/"false" pass through) and with-binds wrapping.
func Check(
	etype, action string,
	doc *RuleDoc,
	binds Bindings,
) (bool, error) {
	expr := doc.ResolveExpr(etype, action)
	if expr == "" {
		return true, nil
	}
	if expr == "true" {
		return true, nil
	}
	if expr == "false" {
		return false, nil
	}
	wrapped, err := wrapWithBinds(doc, etype, expr)
	if err != nil {
		return false, err
	}
	prg, err := compile(wrapped)
	if err != nil {
		return false, fmt.Errorf("perms: CEL compile %s.allow.%s: %w", etype, action, err)
	}
	vars := bindingsMap(binds)
	out, _, err := prg.Eval(vars)
	if err != nil {
		return false, fmt.Errorf("perms: CEL eval %s.allow.%s: %w", etype, action, err)
	}
	if out == celtypes.True {
		return true, nil
	}
	if out == celtypes.False {
		return false, nil
	}
	if b, ok := out.Value().(bool); ok {
		return b, nil
	}
	return false, fmt.Errorf("perms: expected bool, got %v (%T)", out, out)
}

// Bindings are the explicit variable set for one evaluation.
type Bindings struct {
	Data       map[string]any
	NewData    map[string]any
	Auth       map[string]any
	RuleParams map[string]any
	LinkedData map[string]any
	Actions    map[string]any
	Request    RequestInfo
}

// RequestInfo mirrors the fields of instant.Request (db/proto.clj).
type RequestInfo struct {
	ModifiedFields []string
	IP             string
	Origin         string
	Time           *timestamppb.Timestamp // nil → now
}

func bindingsMap(b Bindings) map[string]any {
	m := map[string]any{
		"data":       anyToDyn(b.Data),
		"newData":    anyToDyn(b.NewData),
		"auth":       normalizedAuth(b.Auth),
		"ruleParams": anyToDyn(b.RuleParams),
		"request":    newRequestMessage(b.Request),
	}
	if b.LinkedData != nil {
		m["linkedData"] = anyToDyn(b.LinkedData)
	}
	if b.Actions != nil {
		m["actions"] = anyToDyn(b.Actions)
	}
	m["rateLimit"] = map[string]any{}
	return m
}

// normalizedAuth guarantees auth.id is ALWAYS bound (null when absent) so
// rules written as `auth.id != null && …` evaluate cleanly for anonymous
// callers instead of raising "no such key" — which would deny every request
// through the fail-closed eval-error path.
func normalizedAuth(auth map[string]any) map[string]any {
	out := map[string]any{"id": nil}
	for k, v := range auth {
		if k == "id" {
			continue
		}
		out[k] = v
	}
	if id, ok := auth["id"]; ok {
		out["id"] = id
	}
	return out
}

func newRequestMessage(r RequestInfo) any {
	desc := requestDescriptor()
	msg := dynamicpb.NewMessage(desc)
	if len(r.ModifiedFields) > 0 {
		fd := desc.Fields().ByName("modifiedFields")
		l := msg.Mutable(fd).List()
		for _, f := range r.ModifiedFields {
			l.Append(protoreflect.ValueOfString(f))
		}
	}
	if r.IP != "" {
		fd := desc.Fields().ByName("ip")
		msg.Set(fd, protoreflect.ValueOfString(r.IP))
	}
	if r.Origin != "" {
		fd := desc.Fields().ByName("origin")
		msg.Set(fd, protoreflect.ValueOfString(r.Origin))
	}
	if r.Time != nil {
		fd := desc.Fields().ByName("time")
		tsDesc := fd.Message()
		tsMsg := dynamicpb.NewMessage(tsDesc)
		tsMsg.Set(tsDesc.Fields().ByName("seconds"), protoreflect.ValueOfInt64(r.Time.Seconds))
		tsMsg.Set(tsDesc.Fields().ByName("nanos"), protoreflect.ValueOfInt32(r.Time.Nanos))
		msg.Set(fd, protoreflect.ValueOfMessage(tsMsg))
	}
	return msg
}

func anyToDyn(v any) any {
	if v == nil {
		return map[string]any{}
	}
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return v
}

func wrapWithBinds(doc *RuleDoc, etype, expr string) (string, error) {
	names, exprs, err := doc.BindsFor(etype)
	if err != nil {
		return "", err
	}
	if len(names) == 0 {
		return expr, nil
	}
	body := expr
	for i := len(names) - 1; i >= 0; i-- {
		body = fmt.Sprintf("cel.bind(%s, %s, %s)", names[i], exprs[i], body)
		_ = strings.Contains
	}
	return body, nil
}
