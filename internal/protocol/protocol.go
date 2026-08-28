// Package protocol holds the frozen wire types that every other package shares.
// The schema at internal/protocol/schema/protocol.schema.json is authoritative;
// internal/protocol/protocol.go defines the Go surface that layers depend on;
// tools/schemagen validates that the two agree and emits protocol.d.ts.
// See docs/03-protocol.md for the human-readable contract.
package protocol

import (
	"encoding/json"
	"fmt"
)

// Op enumerates every value that the "op" field may take on the wire (kebab-case).
// The set is the enum in definitions/wsOp of the JSON Schema; tools/schemagen derives
// the corresponding d.ts string union.
const (
	OpInit              = "init"
	OpAddQuery          = "add-query"
	OpRemoveQuery       = "remove-query"
	OpTransact          = "transact"
	OpError             = "error"
	OpJoinRoom          = "join-room"
	OpLeaveRoom         = "leave-room"
	OpSetPresence       = "set-presence"
	OpRefreshPresence   = "refresh-presence"
	OpClientBroadcast   = "client-broadcast"
	OpServerBroadcast   = "server-broadcast"
	OpStartSync         = "start-sync"
	OpRemoveSync        = "remove-sync"
	OpRefreshSyncTable  = "refresh-sync-table"
	OpResyncTable       = "resync-table"
	OpStartStream       = "start-stream"
	OpAppendStream      = "append-stream"
	OpSubscribeStream   = "subscribe-stream"
	OpUnsubscribeStream = "unsubscribe-stream"
	OpInitOk            = "init-ok"
	OpAddQueryOk        = "add-query-ok"
	OpAddQueryExists    = "add-query-exists"
	OpRemoveQueryOk     = "remove-query-ok"
	OpRefreshOk         = "refresh-ok"
	OpTransactOk        = "transact-ok"
	OpPresence          = "presence"
	OpAppStatusChanged  = "app-status-changed"
)

// Frame is a single JSON-over-WebSocket envelope with a required "op".
// The remaining fields are op-dependent; Envelope preserves them via a Raw map.
type Frame map[string]json.RawMessage

// GetOp extracts the required "op" without fully decoding.
func (f Frame) GetOp() (string, error) {
	raw, ok := f["op"]
	if !ok {
		return "", fmt.Errorf("frame missing op")
	}
	var op string
	if err := json.Unmarshal(raw, &op); err != nil {
		return "", err
	}
	return op, nil
}

// ParseFrame decodes raw JSON bytes into a Frame, validating that op is present.
func ParseFrame(b []byte) (Frame, error) {
	var f Frame
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, err
	}
	if _, err := f.GetOp(); err != nil {
		return nil, err
	}
	return f, nil
}

// EncodeFrame marshals a map into canonical JSON (sorted keys) via json.Marshal.
func EncodeFrame(fields map[string]any) ([]byte, error) {
	return json.Marshal(fields)
}

// ErrorEnvelope is the ={status,type,message,hint}? value carried as op=="error".
// Field spellings must stay exactly "status","type","message","hint" (kebab would be wrong here).
type ErrorEnvelope struct {
	Status  int    `json:"status"`
	Type    string `json:"type"`
	Message string `json:"message"`
	Hint    string `json:"hint,omitempty"`
}

// Attr mirrors the underscore-free frozen JSON shape in definitions/attr.
// Hyphen/question-mark spellings are preserved via struct tags because the on-wire
// exactness is part of the frozen surface (docs/03 §7).
type Attr struct {
	ID              string     `json:"id"`
	ValueType       string     `json:"value-type"`       // "blob" | "ref"
	Cardinality     string     `json:"cardinality"`      // "one" | "many"
	ForwardIdentity [3]string  `json:"forward-identity"` // [uuid,etype,label]
	ReverseIdentity *[3]string `json:"reverse-identity,omitempty"`
	Unique          *bool      `json:"unique?,omitempty"`
	Index           *bool      `json:"index?,omitempty"`
	Required        *bool      `json:"required?,omitempty"`
	Primary         *bool      `json:"primary?,omitempty"`
	OnDelete        *string    `json:"on-delete,omitempty"`
	OnDeleteReverse *string    `json:"on-delete-reverse,omitempty"`
	CheckedDataType *string    `json:"checked-data-type,omitempty"`
}

// TxStep is one element of the frozen tx-steps array (docs/03 §4).
// It has a tuple form: first element is the op; the rest are positional args.
// The canonical on-wire repr is the bare array; TxStep is a parsed view.
type TxStep struct {
	Op   string
	Args []json.RawMessage // everything after index 0, preserved for unknown-round-trip fidelity
}

// ParseTxStep decodes one raw JSON array value into a TxStep, validating that it is non-empty.
func ParseTxStep(raw json.RawMessage) (TxStep, error) {
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil {
		return TxStep{}, err
	}
	if len(arr) == 0 {
		return TxStep{}, fmt.Errorf("tx-step must be non-empty array")
	}
	var op string
	if err := json.Unmarshal(arr[0], &op); err != nil {
		return TxStep{}, err
	}
	return TxStep{Op: op, Args: arr[1:]}, nil
}

// KnownTxStepOps is the enum backing definitions/txStep.first-item set; use it to
// reject unknown ops early in tests while preserving round-trip of forwarded bytes in prod.
var KnownTxStepOps = map[string]struct{}{
	"add-triple": {}, "deep-merge-triple": {}, "retract-triple": {},
	"delete-entity": {}, "add-attr": {}, "update-attr": {},
	"delete-attr": {}, "restore-attr": {}, "rule-params": {},
}

// InstaQLOptions carries the "$" query modifiers from definitions/instaQLOptions.
type InstaQLOptions struct {
	Where           map[string]any `json:"where,omitempty"`
	Order           map[string]any `json:"order,omitempty"`
	Limit           *int           `json:"limit,omitempty"`
	First           *int           `json:"first,omitempty"`
	Last            *int           `json:"last,omitempty"`
	Offset          *int           `json:"offset,omitempty"`
	Before          *string        `json:"before,omitempty"`
	After           *string        `json:"after,omitempty"`
	BeforeInclusive *bool          `json:"beforeInclusive,omitempty"`
	AfterInclusive  *bool          `json:"afterInclusive,omitempty"`
	Aggregate       *string        `json:"aggregate,omitempty"` // "count" only per instaql.clj:1190
	Fields          []string       `json:"fields,omitempty"`
}

// InstaqlResultEnvelope is the ={data,page-info,aggregate} shape shipped in refresh-ok.
type InstaqlResultEnvelope struct {
	Data      map[string]any `json:"data"`
	PageInfo  *PageInfo      `json:"page-info,omitempty"`
	Aggregate *Aggregate     `json:"aggregate,omitempty"`
}

// PageInfo mirrors page-info{startCursor,endCursor,hasNextPage,hasPreviousPage} (instaql.clj:924).
type PageInfo struct {
	StartCursor     *string `json:"startCursor,omitempty"`
	EndCursor       *string `json:"endCursor,omitempty"`
	HasNextPage     bool    `json:"hasNextPage"`
	HasPreviousPage bool    `json:"hasPreviousPage"`
}

// Aggregate wraps aggregate{count} (admin-only otherwise raises, instaql.clj:1190).
type Aggregate struct {
	Count int `json:"count"`
}

// WhereOperator enumerates the allowed where leaf operators.
const (
	WhereIn                 = "$in"
	WhereNot                = "$not"
	WhereNe                 = "$ne"
	WhereIsNull             = "$isNull"
	WhereGt                 = "$gt"
	WhereGte                = "$gte"
	WhereLt                 = "$lt"
	WhereLte                = "$lte"
	WhereLike               = "$like"
	WhereIlike              = "$ilike"
	WhereEntityIDStartsWith = "$entityIdStartsWith"
)
