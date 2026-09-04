package platform

import (
	"context"
	"crypto/rand"

	"github.com/jackc/pgx/v5"
)

// Attr is the DB-facing attribute row (modern v1 shape: etype/label live on attrs).
type Attr struct {
	ID    [16]byte
	IDStr string // cached canonical hyphenated uuid (UUIDToStr form); "" until backfilled

	AppID           [16]byte
	Etype           *string
	Label           *string
	ReverseEtype    *string
	ReverseLabel    *string
	ValueType       string // "blob" | "ref"
	Cardinality     string // "one" | "many"
	IsUnique        bool
	IsIndexed       bool
	IsRequired      bool
	ForwardIdent    [16]byte
	ReverseIdent    *[16]byte
	CheckedDataType *string // nil | "string"|"number"|"boolean"|"date"
}

// UUID returns the attr id in canonical hyphenated form. It serves the IDStr
// cache when populated (rebuildIndexes backfills every catalog entry) and
// falls back to a fresh UUIDToStr otherwise, so lone Attr values minted
// outside a catalog keep rendering correctly.
func (a Attr) UUID() string {
	if a.IDStr != "" {
		return a.IDStr
	}
	return UUIDToStr(a.ID)
}

// Flags are the five boolean flag columns on triples. Derivation is a direct
// port of v1 db/model/triple.clj enhanced-triples CTE (insert-multi-new!):
//
//	ea  = cardinality == "one"
//	eav = value_type  == "ref"
//	av  = is_unique
//	ave = is_indexed
//	vae = value_type  == "ref"   (identical to eav in v1)
func FlagsFor(a Attr) Flags {
	return Flags{
		EA:  a.Cardinality == "one",
		EAV: a.ValueType == "ref",
		AV:  a.IsUnique,
		AVE: a.IsIndexed,
		VAE: a.ValueType == "ref",
	}
}

// Flags mirrors the triples flag columns.
type Flags struct {
	EA, EAV, AV, AVE, VAE bool
}

// Queryer abstracts *pgxpool.Pool and pgx.Tx.
type Queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

const attrCols = `id, app_id, etype, label, reverse_etype, reverse_label,
	value_type, cardinality, is_unique, is_indexed,
	is_required, forward_ident, reverse_ident, checked_data_type`

// AttrCatalog is the in-memory attr set for one app.
type AttrCatalog struct {
	AppID [16]byte
	byID  map[[16]byte]Attr

	// Derived indexes over byID, rebuilt wholesale by rebuildIndexes() after
	// every mutation. They are built once at construction (LoadAttrCatalog,
	// Clone, Add) and only read afterwards, so shared cached catalogs stay
	// race-free. Zero-value catalogs miss cleanly on every lookup.
	byEtypeLabel map[string]map[string]Attr // etype -> label -> attr (Etype+Label non-nil only)
	byEtypeByID  map[string]map[string]Attr // etype -> UUIDToStr(id) -> attr
	byEtypeIDs   map[string][]string        // etype -> UUIDToStr(id) list (ByEtype backing)
	byEtypeList  map[string][]Attr          // etype -> attr list (AttrsOfEtype backing)
	byReverseKey map[string]*Attr           // child etype \0 parent etype \0 reverse label -> ref attr
}

// Clone returns a mutable copy for one transaction's add-attr overlay.
// Mutating the clone never leaks into the shared cache; post-commit cache
// refresh still flows through CatalogCache invalidation.
func (c *AttrCatalog) Clone() *AttrCatalog {
	by := make(map[[16]byte]Attr, len(c.byID)+1)
	for k, v := range c.byID {
		by[k] = v
	}
	nc := &AttrCatalog{AppID: c.AppID, byID: by}
	nc.rebuildIndexes()
	return nc
}

// Add registers an attr in this catalog view (in-tx add-attr support).
// Safe on a zero-value catalog. Callers own the catalog: Add rebuilds the
// derived indexes and backfills IDStr, so it must only run on tx-local
// clones, never on the shared cached catalog.
func (c *AttrCatalog) Add(a Attr) {
	a = normalizeRequired(a)
	if c.byID == nil {
		c.byID = make(map[[16]byte]Attr)
	}
	c.byID[a.ID] = a
	c.rebuildIndexes()
}

// rebuildIndexes recomputes every derived index from byID and backfills the
// IDStr cache on entries still missing it. It runs after each byID mutation
// (LoadAttrCatalog, Clone, Add), so constructor sites repo-wide need zero
// changes and every catalog lookup carries a populated IDStr. byID iteration
// order is randomized, which only shuffles the unordered per-etype lists —
// the same nondeterminism Attrs/ByEtype always had.
func (c *AttrCatalog) rebuildIndexes() {
	c.byEtypeLabel = make(map[string]map[string]Attr)
	c.byEtypeByID = make(map[string]map[string]Attr)
	c.byEtypeIDs = make(map[string][]string)
	c.byEtypeList = make(map[string][]Attr)
	c.byReverseKey = make(map[string]*Attr, len(c.byID))
	for id, a := range c.byID {
		if a.IDStr == "" {
			a.IDStr = UUIDToStr(id)
			c.byID[id] = a // store back so every lookup exposes the cache
		}
		if a.Etype == nil {
			continue
		}
		et := *a.Etype
		c.byEtypeList[et] = append(c.byEtypeList[et], a)
		c.byEtypeIDs[et] = append(c.byEtypeIDs[et], a.IDStr)
		idm, ok := c.byEtypeByID[et]
		if !ok {
			idm = make(map[string]Attr)
			c.byEtypeByID[et] = idm
		}
		idm[a.IDStr] = a
		if a.Label != nil {
			lb := *a.Label
			lm, ok := c.byEtypeLabel[et]
			if !ok {
				lm = make(map[string]Attr)
				c.byEtypeLabel[et] = lm
			}
			lm[lb] = a
		}
		// Reverse traversal index: only ref attrs with full reverse metadata
		// participate — exactly the predicate instaql's findReverseAttr scan
		// applied. NUL-separated key: etype/label text cannot contain NUL
		// (postgres text forbids it), so the encoding is injective.
		if a.ValueType == "ref" && a.ReverseEtype != nil && a.ReverseLabel != nil {
			key := et + "\x00" + *a.ReverseEtype + "\x00" + *a.ReverseLabel
			cp := a
			c.byReverseKey[key] = &cp
		}
	}
}

// AttrsOfEtype returns the catalog attrs whose forward etype matches, in no
// particular order. The backing array is shared with the catalog — callers
// MUST NOT mutate the returned slice (no appends, no element writes); treat
// it as a read-only view. A nil slice means "no such etype".
func (c *AttrCatalog) AttrsOfEtype(etype string) []Attr {
	return c.byEtypeList[etype]
}

// LabelIndex returns the etype's attrs keyed by label, covering only attrs
// with both Etype and Label set. The map is the catalog's internal index —
// read-only contract: callers must not add, delete, or write entries. A nil
// map means "no such etype (or no labeled attrs for it)".
func (c *AttrCatalog) LabelIndex(etype string) map[string]Attr {
	return c.byEtypeLabel[etype]
}

// IDIndex returns the etype's attrs keyed by UUIDToStr(id). Same read-only
// contract as LabelIndex; a nil map means "no such etype".
func (c *AttrCatalog) IDIndex(etype string) map[string]Attr {
	return c.byEtypeByID[etype]
}

// FindReverseAttr resolves the ref attr on childEtype whose reverse identity
// points back at parentEtype/label — the link traversal lookup instaql
// performs per nested form per query. O(1) via the reverse index; nil when no
// attr matches. The result is a heap copy: callers own it, and no mutation
// can leak back into the index.
func (c *AttrCatalog) FindReverseAttr(childEtype, parentEtype, label string) *Attr {
	if hit := c.byReverseKey[childEtype+"\x00"+parentEtype+"\x00"+label]; hit != nil {
		cp := *hit
		return &cp
	}
	return nil
}

// CreateAttrWithID inserts an attr with caller-supplied ids. The frozen wire
// protocol lets clients mint attr and ident uuids and reference them in the
// same batch, so the server adopts them verbatim (v1 semantics). Replay of a
// known attr id is a no-op.
func CreateAttrWithID(ctx context.Context, tx pgx.Tx, appID [16]byte, a Attr) error {
	a = normalizeRequired(a)
	tag, err := tx.Exec(ctx, `
		INSERT INTO attrs (id, app_id, etype, label, reverse_etype, reverse_label,
		                   value_type, cardinality, is_unique, is_indexed,
		                   is_required, forward_ident, reverse_ident, checked_data_type)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		ON CONFLICT (id) DO NOTHING`,
		a.ID, appID, a.Etype, a.Label, a.ReverseEtype, a.ReverseLabel,
		a.ValueType, a.Cardinality, a.IsUnique, a.IsIndexed, a.IsRequired,
		a.ForwardIdent, a.ReverseIdent, a.CheckedDataType)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil // replayed step; keep the stored definition
	}
	if a.Etype != nil && a.Label != nil {
		if _, err := tx.Exec(ctx, `
			INSERT INTO idents (id, app_id, attr_id, etype, label)
			VALUES ($1,$2,$3,$4,$5)
			ON CONFLICT (app_id, etype, label) DO NOTHING`,
			a.ForwardIdent, appID, a.ID, *a.Etype, *a.Label); err != nil {
			return err
		}
	}
	return nil
}

// FindAttrByIdent returns the non-deleted attr for (appID, etype, label).
func FindAttrByIdent(ctx context.Context, q Queryer, appID [16]byte, etype, label string) (Attr, bool, error) {
	var id [16]byte
	rows, err := q.Query(ctx,
		`SELECT id FROM attrs WHERE app_id=$1 AND etype=$2 AND label=$3 AND deletion_marked_at IS NULL`,
		appID, etype, label)
	if err != nil {
		return Attr{}, false, err
	}
	if !rows.Next() {
		err = rows.Err()
		rows.Close()
		if err != nil {
			return Attr{}, false, err
		}
		return Attr{}, false, nil
	}
	if err := rows.Scan(&id); err != nil {
		rows.Close()
		return Attr{}, false, err
	}
	// Close BEFORE the follow-up loadAttrByID: both run on the same tx
	// connection, and pgx rejects a second query while rows are open
	// ("conn busy").
	rows.Close()
	a, err := loadAttrByID(ctx, q, id)
	return a, true, err
}

// FindAttrByID returns the live attr owned by appID. It is intentionally
// queryer-backed so transaction callers can inspect and update metadata on
// their active pgx transaction without opening a second connection.
func FindAttrByID(ctx context.Context, q Queryer, appID, id [16]byte) (Attr, bool, error) {
	rows, err := q.Query(ctx, `
		SELECT `+attrCols+`
		  FROM attrs
		 WHERE app_id = $1 AND id = $2 AND deletion_marked_at IS NULL`, appID, id)
	if err != nil {
		return Attr{}, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return Attr{}, false, err
		}
		return Attr{}, false, nil
	}
	a, err := scanAttr(rows)
	if err != nil {
		return Attr{}, false, err
	}
	return a, true, rows.Err()
}

// LoadAttrCatalog reads all non-deleted attrs for appID.
func LoadAttrCatalog(ctx context.Context, q Queryer, appID [16]byte) (*AttrCatalog, error) {
	rows, err := q.Query(ctx, `
		SELECT `+attrCols+`
		  FROM attrs
		 WHERE app_id = $1 AND deletion_marked_at IS NULL`, appID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cat := &AttrCatalog{AppID: appID, byID: make(map[[16]byte]Attr)}
	for rows.Next() {
		a, err := scanAttr(rows)
		if err != nil {
			return nil, err
		}
		cat.byID[a.ID] = a
	}
	cat.rebuildIndexes()
	return cat, rows.Err()
}

// ByID resolves an attr; ok=false when unknown.
func (c *AttrCatalog) ByID(id [16]byte) (Attr, bool) {
	a, ok := c.byID[id]
	return a, ok
}

// Len reports catalog size.
func (c *AttrCatalog) Len() int { return len(c.byID) }

func scanAttr(r pgx.Row) (Attr, error) {
	var a Attr
	err := r.Scan(&a.ID, &a.AppID, &a.Etype, &a.Label, &a.ReverseEtype, &a.ReverseLabel,
		&a.ValueType, &a.Cardinality, &a.IsUnique, &a.IsIndexed, &a.IsRequired,
		&a.ForwardIdent, &a.ReverseIdent, &a.CheckedDataType)
	if err != nil {
		return Attr{}, err
	}
	return normalizeRequired(a), nil
}

func normalizeRequired(a Attr) Attr {
	if a.Label != nil && *a.Label == "id" {
		a.IsRequired = true
	}
	return a
}

// CreateApp inserts an app owned by creatorID.
func CreateApp(ctx context.Context, tx pgx.Tx, creatorID, appID [16]byte, title string) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO apps (id, creator_id, title) VALUES ($1,$2,$3)`,
		appID, creatorID, title)
	return err
}

// SetAdminToken registers an admin token uuid for appID.
func SetAdminToken(ctx context.Context, tx pgx.Tx, appID, token [16]byte) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO app_admin_tokens (token, app_id) VALUES ($1,$2)
		 ON CONFLICT (token) DO UPDATE SET app_id = EXCLUDED.app_id`,
		token, appID)
	return err
}

// GetOrCreateAttr finds or creates the (etype,label) attr for appID and mirrors
// it into idents. Fresh uuids on creation match v1 attr-creation semantics;
// reverse ident/etype/label mirror the forward side until the transactor phase
// defines user-facing reverse naming.
func GetOrCreateAttr(
	ctx context.Context, tx pgx.Tx, appID [16]byte,
	etype, label, valueType, cardinality string,
	isUnique, isIndexed bool,
) (Attr, error) {
	attr, _, err := GetOrCreateAttrWithStatus(ctx, tx, appID, etype, label, valueType, cardinality, isUnique, isIndexed)
	return attr, err
}

// GetOrCreateAttrWithStatus is GetOrCreateAttr plus an authoritative created
// result from the same transaction. Callers use it when cache invalidation must
// distinguish schema creation from merely resolving an existing attribute.
func GetOrCreateAttrWithStatus(
	ctx context.Context, tx pgx.Tx, appID [16]byte,
	etype, label, valueType, cardinality string,
	isUnique, isIndexed bool,
) (Attr, bool, error) {
	var existing [16]byte
	err := tx.QueryRow(ctx,
		`SELECT id FROM attrs WHERE app_id=$1 AND etype=$2 AND label=$3 AND deletion_marked_at IS NULL`,
		appID, etype, label).Scan(&existing)
	switch {
	case err == nil:
		attr, loadErr := loadAttrByID(ctx, tx, existing)
		return attr, false, loadErr
	case err != pgx.ErrNoRows:
		return Attr{}, false, err
	}

	attrID := newUUIDv4()
	fwd := newUUIDv4()
	rev := newUUIDv4()
	if _, err := tx.Exec(ctx, `
		INSERT INTO attrs (id, app_id, etype, label, reverse_etype, reverse_label,
		                   value_type, cardinality, is_unique, is_indexed,
		                   is_required, forward_ident, reverse_ident)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		attrID, appID, etype, label, etype, label,
		valueType, cardinality, isUnique, isIndexed, label == "id", fwd, rev); err != nil {
		return Attr{}, false, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO idents (id, app_id, attr_id, etype, label)
		VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (app_id, etype, label) DO NOTHING`,
		newUUIDv4(), appID, attrID, etype, label); err != nil {
		return Attr{}, false, err
	}
	attr, err := loadAttrByID(ctx, tx, attrID)
	return attr, true, err
}

func loadAttrByID(ctx context.Context, q Queryer, id [16]byte) (Attr, error) {
	rows, err := q.Query(ctx, `SELECT `+attrCols+` FROM attrs WHERE id=$1`, id)
	if err != nil {
		return Attr{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return Attr{}, err
		}
		return Attr{}, pgx.ErrNoRows
	}
	return scanAttr(rows)
}

// newUUIDv4 returns a random RFC-4122 version-4 uuid.
func newUUIDv4() [16]byte {
	var u [16]byte
	if _, err := rand.Read(u[:]); err != nil {
		panic("crypto/rand failure: " + err.Error())
	}
	u[6] = (u[6] & 0x0f) | 0x40
	u[8] = (u[8] & 0x3f) | 0x80
	return u
}

// Attrs returns all attrs in the catalog (unordered).
func (c *AttrCatalog) Attrs() []Attr {
	out := make([]Attr, 0, len(c.byID))
	for _, a := range c.byID {
		out = append(out, a)
	}
	return out
}

// GetOrCreateAttrRev is GetOrCreateAttr with explicit reverse naming — the
// shape v1's add-attr produces (forward identity + reverse identity pair).
func GetOrCreateAttrRev(
	ctx context.Context, tx pgx.Tx, appID [16]byte,
	etype, label string,
	revEtype, revLabel *string,
	valueType, cardinality string,
	isUnique, isIndexed bool,
) (Attr, error) {
	var existing [16]byte
	err := tx.QueryRow(ctx,
		`SELECT id FROM attrs WHERE app_id=$1 AND etype=$2 AND label=$3 AND deletion_marked_at IS NULL`,
		appID, etype, label).Scan(&existing)
	switch {
	case err == nil:
		return loadAttrByID(ctx, tx, existing)
	case err != pgx.ErrNoRows:
		return Attr{}, err
	}
	attrID := newUUIDv4()
	fwd := newUUIDv4()
	rev := newUUIDv4()
	if _, err := tx.Exec(ctx, `
		INSERT INTO attrs (id, app_id, etype, label, reverse_etype, reverse_label,
		                   value_type, cardinality, is_unique, is_indexed,
		                   is_required, forward_ident, reverse_ident)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		attrID, appID, etype, label, revEtype, revLabel,
		valueType, cardinality, isUnique, isIndexed, label == "id", fwd, rev); err != nil {
		return Attr{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO idents (id, app_id, attr_id, etype, label)
		VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (app_id, etype, label) DO NOTHING`,
		newUUIDv4(), appID, attrID, etype, label); err != nil {
		return Attr{}, err
	}
	return loadAttrByID(ctx, tx, attrID)
}
