// Package perms implements the CEL-based permission system: rule-doc parsing,
// the v1 fallback chain, bind expansion, and program evaluation. Port of
// instant.model.rule + the evaluation half of instant.db.cel.
//
// Frozen surface (docs/03-protocol.md section 8):
//   - one JSONB doc per app: {<etype>|attrs|$default|$rateLimits: {...}}
//   - resolution chain etype.allow.action -> etype.allow.$default
//     -> $default.allow.action -> $default.allow.$default -> etype.fallback.action
//   - binds: [$default.bind, etype.bind] name/expr pairs wrapped as
//     cel.bind(name, expr, body) in dependency order; odd counts are errors;
//     cycles are errors.
package perms

import (
	"encoding/json"
	"fmt"
)

type EtypeRule struct {
	Bind     []any             `json:"bind,omitempty"`
	Allow    Allow             `json:"allow,omitempty"`
	Fallback Allow             `json:"fallback,omitempty"`
	Fields   map[string]string `json:"fields,omitempty"`
}

// ReservedNamespaces are rejected as user etypes (model/rule.clj).
var ReservedNamespaces = map[string]bool{
	"$users": true, "$files": true, "$default": true,
	"$streams": true, "$rateLimits": true,
}

// Allow is one etype's allow block: action -> CEL expression.
type Allow struct {
	View    *string `json:"view,omitempty"`
	Create  *string `json:"create,omitempty"`
	Update  *string `json:"update,omitempty"`
	Delete  *string `json:"delete,omitempty"`
	Link    *string `json:"link,omitempty"`
	Unlink  *string `json:"unlink,omitempty"`
	Default *string `json:"$default,omitempty"`
}

// RateLimitConfig mirrors $rateLimits entries.
type RateLimitConfig struct {
	Capacity int             `json:"capacity"`
	Refill   RateLimitRefill `json:"refill"`
}

type RateLimitRefill struct {
	Amount int    `json:"amount"`
	Period string `json:"period"` // e.g. "1m"
	Type   string `json:"type"`   // greedy | interval
}

// RuleDoc is the parsed rules.code JSONB plus derived maps.
type RuleDoc struct {
	Raw        json.RawMessage
	Etypes     map[string]*EtypeRule
	RateLimits map[string]RateLimitConfig
	Version    int64
}

// ParseRuleDoc decodes rules.code JSON bytes.
func ParseRuleDoc(raw []byte) (*RuleDoc, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return &RuleDoc{Raw: nil, Etypes: map[string]*EtypeRule{}}, nil
	}
	var generic map[string]json.RawMessage
	if err := json.Unmarshal(raw, &generic); err != nil {
		return nil, fmt.Errorf("perms: rule doc: %w", err)
	}
	etypes := make(map[string]*EtypeRule, len(generic))
	var rateLimits map[string]RateLimitConfig
	for k, v := range generic {
		if k == "$rateLimits" {
			var rl map[string]RateLimitConfig
			if err := json.Unmarshal(v, &rl); err != nil {
				return nil, fmt.Errorf("perms: $rateLimits: %w", err)
			}
			rateLimits = rl
			continue
		}
		var er EtypeRule
		if err := json.Unmarshal(v, &er); err != nil {
			return nil, fmt.Errorf("perms: %s: %w", k, err)
		}
		etypes[k] = &er
	}
	return &RuleDoc{
		Raw:        append(json.RawMessage(nil), raw...),
		Etypes:     etypes,
		RateLimits: rateLimits,
	}, nil
}

// ResolveExpr walks the frozen fallback chain and returns the CEL expression
// for (etype, action), or "" when no rule applies. Mirrors
// model/rule.clj get-program! paths:
//
//	[etype "allow" action], [etype "allow" "$default"],
//	["$default" "allow" action], ["$default" "allow" "$default"],
//	[etype "fallback" action]
func (d *RuleDoc) ResolveExpr(etype, action string) string {
	for _, pa := range [][2]string{
		{etype, action}, {etype, "$default"},
		{"$default", action}, {"$default", "$default"},
	} {
		r := d.Etypes[pa[0]]
		if r == nil {
			continue
		}
		if expr := allowAction(r.Allow, pa[1]); expr != nil {
			return *expr
		}
	}
	if r := d.Etypes[etype]; r != nil {
		if expr := allowAction(r.Fallback, action); expr != nil {
			return *expr
		}
	}
	return ""
}

func allowAction(a Allow, action string) *string {
	switch action {
	case "view":
		return a.View
	case "create":
		return a.Create
	case "update":
		return a.Update
	case "delete":
		return a.Delete
	case "link":
		return a.Link
	case "unlink":
		return a.Unlink
	case "$default":
		return a.Default
	}
	return nil
}

// FieldExpr returns the field-level CEL expression for (etype, field), or "".
// Setting rules on "id" is forbidden (model/rule.clj get-field-program!).
func (d *RuleDoc) FieldExpr(etype, field string) (string, error) {
	if field == "id" {
		return "", fmt.Errorf(
			"you cannot set field rules for `id`. Use %s -> allow -> view instead", etype)
	}
	r := d.Etypes[etype]
	if r == nil {
		return "", nil
	}
	return r.Fields[field], nil
}

// BindsFor concatenates $default.bind + etype.bind (with-binds in model/rule.clj).
func (d *RuleDoc) BindsFor(etype string) (names, exprs []string, err error) {
	var pairs []any
	if def := d.Etypes["$default"]; def != nil {
		pairs = append(pairs, def.Bind...)
	}
	if er := d.Etypes[etype]; er != nil {
		pairs = append(pairs, er.Bind...)
	}
	if len(pairs)%2 != 0 {
		return nil, nil, fmt.Errorf("bind should have an even number of elements")
	}
	names = make([]string, 0, len(pairs)/2)
	exprs = make([]string, 0, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		name, okN := pairs[i].(string)
		exprStr, okE := pairs[i+1].(string)
		if !okN || !okE {
			return nil, nil, fmt.Errorf("bind entries must be [name(string), expr(string)] pairs")
		}
		names = append(names, name)
		exprs = append(exprs, exprStr)
	}
	return names, exprs, nil
}
