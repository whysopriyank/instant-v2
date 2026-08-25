package platform

import (
	"context"
	"crypto/subtle"
	"errors"
	"sort"
	"sync"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/perms"
)

// ErrNoRows re-exported for callers without a pgx dependency.
var ErrNoRows = errors.New("platform: no rows")

// CatalogCache memoizes AttrCatalog + RuleDoc per app and resolves admin tokens.
type CatalogCache struct {
	Pool  Queryer
	RowQ  RowQueryer
	mu    sync.Mutex
	cache map[string]*AttrCatalog
	rules map[string]*perms.RuleDoc
}

// RowQueryer is the query surface the catalog cache needs.
type RowQueryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// NewCatalogCache builds a cache over the pool.
func NewCatalogCache(q Queryer, rq RowQueryer) *CatalogCache {
	return &CatalogCache{Pool: q, RowQ: rq, cache: map[string]*AttrCatalog{}, rules: map[string]*perms.RuleDoc{}}
}

// For returns the app's catalog, loading it on first use.
func (c *CatalogCache) For(ctx context.Context, appID string) (*AttrCatalog, error) {
	c.mu.Lock()
	if cat, ok := c.cache[appID]; ok {
		c.mu.Unlock()
		return cat, nil
	}
	c.mu.Unlock()

	// Load OUTSIDE the mutex: c.mu guards the maps, not the database. The
	// previous hold-across-query serialized EVERY app's cold load behind one
	// lock — one slow catalog load stalled all transacts and refreshes
	// process-wide (audit: lock-across-I/O). Concurrent first-touches of the
	// same app may race duplicate loads; last-writer-wins keeps both results
	// correct at the cost of one redundant query per cold key.
	var id [16]byte
	if err := ScanUUID(appID, &id); err != nil {
		return nil, err
	}
	cat, err := LoadAttrCatalog(ctx, c.Pool, id)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.cache[appID] = cat
	c.mu.Unlock()
	return cat, nil
}

// Invalidate drops one app's cached catalog and rule doc.
func (c *CatalogCache) Invalidate(appID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cache, appID)
	delete(c.rules, appID)
}

// RuleDocFor returns the app's parsed permission rules, loading on first use.
// Apps without a rules row get an empty doc — v1 default-open semantics — so
// callers can pass the result straight into perms.Check. The cache shares the
// catalog's invalidation cycle (Invalidate clears both).
func (c *CatalogCache) RuleDocFor(ctx context.Context, appID string) (*perms.RuleDoc, error) {
	c.mu.Lock()
	if d, ok := c.rules[appID]; ok {
		c.mu.Unlock()
		return d, nil
	}
	c.mu.Unlock()

	// Same lock-across-I/O discipline as For: query outside the mutex.
	var id [16]byte
	if err := ScanUUID(appID, &id); err != nil {
		return nil, err
	}
	rows, err := c.RowQ.Query(ctx,
		`SELECT code FROM rules WHERE app_id=$1::uuid`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var raw []byte
	for rows.Next() {
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	doc, err := perms.ParseRuleDoc(raw)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.rules[appID] = doc
	c.mu.Unlock()
	return doc, nil
}

// CheckAdminToken verifies an admin token belongs to the app.
//
// Timing: the candidate tokens for the app are fetched and compared in Go
// with subtle.ConstantTimeCompare over the normalized 32-hex encoding. The
// previous SQL-side `WHERE token=$1` leaked match/no-match through Postgres
// index/B-tree comparison timing; row count for an app is small and stable,
// so the residual per-row work difference is negligible while the token
// comparison itself no longer short-circuits on first differing byte.
func (c *CatalogCache) CheckAdminToken(ctx context.Context, appID, token string) (bool, error) {
	var supplied [16]byte
	if err := ScanUUID(token, &supplied); err != nil {
		return false, nil // non-uuid tokens can never match uuid-typed tokens
	}
	rows, err := c.RowQ.Query(ctx,
		`SELECT t.token FROM app_admin_tokens t WHERE t.app_id=$1::uuid`, appID)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	suppliedHex := uuidHex(supplied)
	match := 0
	for rows.Next() {
		var tok [16]byte
		if err := rows.Scan(&tok); err != nil {
			return false, err
		}
		match |= subtle.ConstantTimeCompare(uuidHex(tok), suppliedHex)
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return match == 1, nil
}

func uuidHex(u [16]byte) []byte {
	const h = "0123456789abcdef"
	out := make([]byte, 32)
	for i, b := range u {
		out[2*i] = h[b>>4]
		out[2*i+1] = h[b&0x0f]
	}
	return out
}

// ScanUUID parses canonical uuid text into [16]byte.
func ScanUUID(s string, dst *[16]byte) error {
	hex := make([]byte, 0, 32)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '-' {
			continue
		}
		v, ok := hexNibble(c)
		if !ok {
			return errors.New("bad uuid")
		}
		hex = append(hex, v)
	}
	if len(hex) != 32 {
		return errors.New("bad uuid length")
	}
	for i := range 16 {
		dst[i] = hex[2*i]<<4 | hex[2*i+1]
	}
	return nil
}

func hexNibble(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

// WireAttrs renders catalog attrs in the frozen wire shape (docs/03 §7).
func (cat *AttrCatalog) WireAttrs() []map[string]any {
	out := make([]map[string]any, 0, len(cat.byID))
	// Deterministic order: map iteration over byID is randomized, and the
	// attrs array rides the wire verbatim — corpus diffs and client caches
	// both want stable bytes. Sort by (etype, label, id).
	ordered := make([]Attr, 0, len(cat.byID))
	for _, a := range cat.byID {
		ordered = append(ordered, a)
	}
	sort.Slice(ordered, func(i, j int) bool {
		ei, ej := attrIdent(ordered[i].Etype), attrIdent(ordered[j].Etype)
		if ei != ej {
			return ei < ej
		}
		li, lj := attrIdent(ordered[i].Label), attrIdent(ordered[j].Label)
		if li != lj {
			return li < lj
		}
		return UUIDToStr(ordered[i].ID) < UUIDToStr(ordered[j].ID)
	})
	for _, a := range ordered {
		m := map[string]any{
			"id":                UUIDToStr(a.ID),
			"value-type":        a.ValueType,
			"cardinality":       a.Cardinality,
			"unique?":           a.IsUnique,
			"index?":            a.IsIndexed,
			"required?":         false,
			"checked-data-type": checkedPtr(a.CheckedDataType),
		}
		// v1 marks the implicit <etype>/id attr as the entity primary key;
		// TS clients key entity assembly off this flag (store.js primaryKeys).
		if a.Label != nil && *a.Label == "id" {
			m["primary?"] = true
		}
		if a.Etype != nil && a.Label != nil {
			m["forward-identity"] = []string{UUIDToStr(a.AppID), *a.Etype, *a.Label}
		}
		if a.ReverseEtype != nil && a.ReverseLabel != nil {
			m["reverse-identity"] = []string{UUIDToStr(a.AppID), *a.ReverseEtype, *a.ReverseLabel}
		}
		out = append(out, m)
	}
	return out
}

func checkedPtr(p *string) any {
	if p == nil {
		return nil
	}
	return *p
}

// UUIDToStr renders canonical hyphenated lowercase form.
func UUIDToStr(u [16]byte) string {
	const h = "0123456789abcdef"
	out := make([]byte, 0, 36)
	for i, b := range u {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			out = append(out, '-')
		}
		out = append(out, h[b>>4], h[b&0x0f])
	}
	return string(out)
}

// ByEtype lists attr ids belonging to an etype.
func (cat *AttrCatalog) ByEtype(etype string) []string {
	var out []string
	for _, a := range cat.byID {
		if a.Etype != nil && *a.Etype == etype {
			out = append(out, UUIDToStr(a.ID))
		}
	}
	return out
}

// FindByEtypeLabel resolves one attr by namespace + label.
func (cat *AttrCatalog) FindByEtypeLabel(etype, label string) *Attr {
	for _, a := range cat.byID {
		if a.Etype != nil && *a.Etype == etype && a.Label != nil && *a.Label == label {
			cp := a
			return &cp
		}
	}
	return nil
}

// ScanUUIDErr is ScanUUID with an error return for call sites preferring that
// shape.
func ScanUUIDErr(s string) ([16]byte, error) {
	var u [16]byte
	if err := ScanUUID(s, &u); err != nil {
		return [16]byte{}, err
	}
	return u, nil
}

func attrIdent(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
