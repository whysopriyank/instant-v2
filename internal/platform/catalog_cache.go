package platform

import (
	"context"
	"errors"
	"sync"

	"github.com/jackc/pgx/v5"
)

// ErrNoRows re-exported for callers without a pgx dependency.
var ErrNoRows = errors.New("platform: no rows")

// CatalogCache memoizes AttrCatalog per app and resolves admin tokens.
type CatalogCache struct {
	Pool  Queryer
	RowQ  RowQueryer
	mu    sync.Mutex
	cache map[string]*AttrCatalog
}

// RowQueryer is the single-row query surface.
type RowQueryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// NewCatalogCache builds a cache over the pool.
func NewCatalogCache(q Queryer, rq RowQueryer) *CatalogCache {
	return &CatalogCache{Pool: q, RowQ: rq, cache: map[string]*AttrCatalog{}}
}

// For returns the app's catalog, loading it on first use.
func (c *CatalogCache) For(ctx context.Context, appID string) (*AttrCatalog, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cat, ok := c.cache[appID]; ok {
		return cat, nil
	}
	var id [16]byte
	if err := ScanUUID(appID, &id); err != nil {
		return nil, err
	}
	cat, err := LoadAttrCatalog(ctx, c.Pool, id)
	if err != nil {
		return nil, err
	}
	c.cache[appID] = cat
	return cat, nil
}

// Invalidate drops one app's cached catalog.
func (c *CatalogCache) Invalidate(appID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cache, appID)
}

// CheckAdminToken verifies an admin token belongs to the app.
func (c *CatalogCache) CheckAdminToken(ctx context.Context, appID, token string) (bool, error) {
	var one int
	err := c.RowQ.QueryRow(ctx,
		`SELECT 1 FROM app_admin_tokens t JOIN apps a ON a.id = t.app_id
		  WHERE t.token=$1::uuid AND a.id=$2::uuid LIMIT 1`, token, appID).Scan(&one)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
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
	for _, a := range cat.byID {
		m := map[string]any{
			"id":                UUIDToStr(a.ID),
			"value-type":        a.ValueType,
			"cardinality":       a.Cardinality,
			"unique?":           a.IsUnique,
			"index?":            a.IsIndexed,
			"required?":         false,
			"checked-data-type": checkedPtr(a.CheckedDataType),
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
