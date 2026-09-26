package instaql

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/instant-v2/instant-v2/internal/platform"
)

func needsPageInfo(o *Options) bool {
	return o != nil && (o.Limit != nil || o.First != nil || o.Last != nil ||
		o.Offset != nil || o.Before != nil || o.After != nil || o.Order != nil)
}

// effectiveOrder is the order used by the complete pagination path. v1's
// paginated forms default to the creation time of the implicit <etype>/id
// triple; an unpaged form retains the legacy ID order.
func effectiveOrder(o *Options) *Order {
	if o == nil {
		return nil
	}
	if o.Order != nil {
		return o.Order
	}
	if o.fallbackIDOrder {
		return nil
	}
	if o.Limit != nil || o.First != nil || o.Last != nil || o.Offset != nil ||
		o.Before != nil || o.After != nil {
		return &Order{K: "serverCreatedAt"}
	}
	return nil
}

// paginationOptions preserves the legacy ID order when a paginated etype has
// no implicit id attr. The serverCreatedAt order is still used for explicit
// orders and for the default path when the id triple exists.
func paginationOptions(o *Options, idAttr *platform.Attr) *Options {
	if o == nil || idAttr != nil || o.Order != nil || effectiveOrder(o) == nil {
		return o
	}
	copy := *o
	copy.fallbackIDOrder = true
	return &copy
}

func effectiveLimitOffset(o *Options) (limit, offset int) {
	if o == nil {
		return 0, 0
	}
	if o.Limit != nil {
		limit = *o.Limit
	} else if o.First != nil {
		limit = *o.First
	} else if o.Last != nil {
		limit = *o.Last
	}
	if o.Offset != nil {
		offset = *o.Offset
	}
	return
}

// canUseIDFastPath preserves the cheap SQL LIMIT/OFFSET path where the
// database's entity-id order is already the complete requested order. Any
// boundary, order, backward page, or relation link needs full-set
// materialization so filtering happens before pagination.
func canUseIDFastPath(o *Options, parentRef *refLink) bool {
	if parentRef != nil {
		return false
	}
	if o == nil {
		return true
	}
	if o.fallbackIDOrder && (len(o.Before) > 0 || len(o.After) > 0 || o.Last != nil) {
		return false
	}
	return effectiveOrder(o) == nil
}

// pageSlice removes the SQL sentinel row and derives page direction metadata.
// A forward page (limit/first) has another page only when the sentinel was
// returned. A backward page (last) uses the sentinel to indicate preceding
// rows instead; it does not manufacture a next page from the page size.
func pageSlice(entities []entity, o *Options) (slice []entity, hasNext, hasPrevious bool) {
	slice = entities
	if o == nil {
		return slice, false, false
	}
	_, offset := effectiveLimitOffset(o)
	hasPrevious = offset > 0 || o.Before != nil
	limit, _ := effectiveLimitOffset(o)
	if limit <= 0 {
		return slice, false, hasPrevious
	}
	if o.Last != nil {
		if len(entities) > limit {
			hasPrevious = true
		}
		if len(slice) > limit {
			slice = slice[len(slice)-limit:]
		}
		return slice, false, hasPrevious
	}
	if len(entities) > limit {
		hasNext = true
		slice = slice[:limit]
	}
	return slice, hasNext, hasPrevious
}

// pageEntities applies cursor boundaries, then offset, then first/last/limit.
// It is intentionally independent of SQL sentinel details so relation-linked
// and explicitly ordered forms use the same observable pagination semantics.
func pageEntities(entities []entity, o *Options, etype string, sqlPaged bool, orderAttrID string) (slice []entity, hasNext, hasPrevious bool, err error) {
	if o == nil {
		return entities, false, false, nil
	}
	if sqlPaged {
		// The SQL fast path already applied the ID order, offset, and fetched
		// one sentinel row for the positive limit. Only trim that sentinel;
		// all other paths below operate on the complete filtered set.
		slice, hasNext, hasPrevious = pageSlice(entities, o)
		return slice, hasNext, hasPrevious, nil
	}
	// Keep both boundaries as indexes into the original ordered set. Applying
	// `before` to an already narrowed after-slice makes the before index refer
	// to the wrong origin and returns rows beyond the intersection.
	lo, hi := 0, len(entities)
	if len(o.After) > 0 {
		idx, e := cursorBoundary(entities, o, o.After, etype, "after", true, o.AfterInclusive, orderAttrID)
		if e != nil {
			return nil, false, false, e
		}
		lo = idx
		hasPrevious = true
	}
	beforeHasNext := false
	if len(o.Before) > 0 {
		idx, e := cursorBoundary(entities, o, o.Before, etype, "before", false, o.BeforeInclusive, orderAttrID)
		if e != nil {
			return nil, false, false, e
		}
		if o.Last != nil && idx < len(entities) {
			beforeHasNext = true
		}
		hi = idx
		hasNext = beforeHasNext
	}
	if lo > hi {
		lo = hi
	}
	slice = entities[lo:hi]
	_, offset := effectiveLimitOffset(o)
	if offset > 0 {
		hasPrevious = true
		if offset >= hi-lo {
			lo = hi
		} else {
			lo += offset
		}
		slice = entities[lo:hi]
	}
	limit, _ := effectiveLimitOffset(o)
	if limit <= 0 {
		return slice, hasNext, hasPrevious, nil
	}
	if o.Last != nil {
		if hi-lo > limit {
			hasPrevious = true
			lo = hi - limit
		}
		slice = entities[lo:hi]
		return slice, hasNext, hasPrevious, nil
	}
	if hi-lo > limit {
		hasNext = true
		hi = lo + limit
	}
	slice = entities[lo:hi]
	return slice, hasNext, hasPrevious, nil
}

func cursorID(c []any, direction string) (string, error) {
	if len(c) < 3 {
		return "", fmt.Errorf("instaql: %s cursor needs [eid, attrId, value]", direction)
	}
	id, ok := c[0].(string)
	if !ok || id == "" {
		return "", fmt.Errorf("instaql: %s cursor has invalid entity id", direction)
	}
	return id, nil
}

func entityIndex(entities []entity, id string) (int, bool) {
	for i := range entities {
		if entities[i].ID == id {
			return i, true
		}
	}
	return 0, false
}

// cursorBoundary returns the slice boundary for a cursor. For the built-in
// serverCreatedAt order, a missing entity can still be located from the
// timestamp carried in the v1 cursor tuple, which keeps pagination stable
// across deletion of the cursor entity.
func cursorBoundary(entities []entity, o *Options, cursor []any, etype, direction string, after, inclusive bool, orderAttrID string) (int, error) {
	order := effectiveOrder(o)
	if order != nil && order.K == "serverCreatedAt" {
		// Always use the encoded sort tuple, even when the cursor entity is
		// still present. Its ID triple may have been overwritten since the
		// cursor was issued, and entity lookup would then move the boundary.
		id, createdAt, err := validateServerCreatedAtCursor(cursor, orderAttrID, direction)
		if err != nil {
			return 0, err
		}
		return sort.Search(len(entities), func(i int) bool {
			cmp := compareServerCreatedAtCursor(entities[i], id, createdAt, order.Desc)
			if after {
				if inclusive {
					return cmp >= 0
				}
				return cmp > 0
			}
			if inclusive {
				return cmp > 0
			}
			return cmp >= 0
		}), nil
	}

	id, err := cursorID(cursor, direction)
	if err != nil {
		return 0, err
	}
	idx, found := entityIndex(entities, id)
	if found {
		if (after && !inclusive) || (!after && inclusive) {
			idx++
		}
		return idx, nil
	}
	if order == nil {
		return sort.Search(len(entities), func(i int) bool { return entities[i].ID >= id }), nil
	}
	return 0, &ErrCursorNotFound{Etype: etype, Direction: direction, ID: id}
}

func validateServerCreatedAtCursor(cursor []any, expectedIDAttr, direction string) (string, int64, error) {
	if len(cursor) != 4 {
		return "", 0, fmt.Errorf("instaql: %s serverCreatedAt cursor needs exactly [eid, idAttrId, eid, epochMillis]", direction)
	}
	id, ok := cursor[0].(string)
	if !ok || id == "" {
		return "", 0, fmt.Errorf("instaql: %s cursor has invalid entity id", direction)
	}
	attrID, ok := cursor[1].(string)
	if !ok || attrID == "" || expectedIDAttr == "" || attrID != expectedIDAttr {
		return "", 0, fmt.Errorf("instaql: %s serverCreatedAt cursor has wrong id attribute", direction)
	}
	value, ok := cursor[2].(string)
	if !ok || value != id {
		return "", 0, fmt.Errorf("instaql: %s serverCreatedAt cursor has mismatched id value", direction)
	}
	createdAt, ok := cursorServerCreatedAtMillis(cursor)
	if !ok {
		return "", 0, fmt.Errorf("instaql: %s cursor has invalid serverCreatedAt timestamp", direction)
	}
	return id, createdAt, nil
}

func cursorServerCreatedAtMillis(cursor []any) (int64, bool) {
	if len(cursor) != 4 {
		return 0, false
	}
	switch n := cursor[3].(type) {
	case json.Number:
		v, err := n.Int64()
		return v, err == nil
	case float64:
		if math.IsNaN(n) || math.IsInf(n, 0) || n != math.Trunc(n) || n < math.MinInt64 || n > math.MaxInt64 {
			return 0, false
		}
		return int64(n), true
	case int:
		return int64(n), true
	case int64:
		return n, true
	case uint64:
		if n > math.MaxInt64 {
			return 0, false
		}
		return int64(n), true
	default:
		return 0, false
	}
}

func compareServerCreatedAtCursor(e entity, cursorID string, cursorMillis int64, desc bool) int {
	entityMillis := e.ServerCreatedAt.UnixMilli()
	cmp := 0
	switch {
	case entityMillis < cursorMillis:
		cmp = -1
	case entityMillis > cursorMillis:
		cmp = 1
	case e.ID < cursorID:
		cmp = -1
	case e.ID > cursorID:
		cmp = 1
	}
	if desc {
		return -cmp
	}
	return cmp
}

func applyOrder(entities []entity, o *Options) error {
	order := effectiveOrder(o)
	if order == nil {
		sort.SliceStable(entities, func(i, j int) bool { return entities[i].ID < entities[j].ID })
		return nil
	}
	k := order.K
	if k == "serverCreatedAt" {
		for _, e := range entities {
			if !e.HasServerCreatedAt {
				return fmt.Errorf("instaql: cannot order by %q: entity %q has no id triple", k, e.ID)
			}
		}
		dir := 1
		if order.Desc {
			dir = -1
		}
		sort.SliceStable(entities, func(i, j int) bool {
			a, b := entities[i].ServerCreatedAt.UnixMilli(), entities[j].ServerCreatedAt.UnixMilli()
			if a == b {
				if dir > 0 {
					return entities[i].ID < entities[j].ID
				}
				return entities[i].ID > entities[j].ID
			}
			if dir > 0 {
				return a < b
			}
			return a > b
		})
		return nil
	}
	if err := validateOrderValues(entities, k); err != nil {
		return err
	}
	dir := 1
	if order.Desc {
		dir = -1
	}
	// Establish the v2 tie-breaker before sorting by the requested field.
	// Stable sorting then keeps equal field values in ID order.
	sort.SliceStable(entities, func(i, j int) bool { return entities[i].ID < entities[j].ID })
	sort.SliceStable(entities, func(i, j int) bool {
		return dir*compareValues(comparableField(entities[i], k), comparableField(entities[j], k)) < 0
	})
	return nil
}

func validateOrderValues(entities []entity, k string) error {
	kind := ""
	for _, e := range entities {
		v := comparableField(e, k)
		if v == nil {
			continue
		}
		current := ""
		if _, ok := v.(string); ok {
			current = "string"
		} else if _, ok := numberValue(v); ok {
			current = "number"
		} else {
			return fmt.Errorf("instaql: cannot order by %q: unsupported value type %T", k, v)
		}
		if kind != "" && kind != current {
			return fmt.Errorf("instaql: cannot order by %q: mixed %s and %s values", k, kind, current)
		}
		kind = current
	}
	return nil
}

func compareValues(a, b any) int {
	aNull, bNull := a == nil, b == nil
	if aNull || bNull {
		switch {
		case aNull && bNull:
			return 0
		case aNull:
			return -1 // null first ascending, last descending via direction
		default:
			return 1
		}
	}
	if af, ok := numberValue(a); ok {
		if bf, ok := numberValue(b); ok {
			switch {
			case af < bf:
				return -1
			case af > bf:
				return 1
			default:
				return 0
			}
		}
	}
	if as, ok := a.(string); ok {
		if bs, ok := b.(string); ok {
			return strings.Compare(as, bs)
		}
	}
	return 0
}

func numberValue(v any) (float64, bool) {
	var value float64
	switch n := v.(type) {
	case json.Number:
		var err error
		value, err = n.Float64()
		if err != nil {
			return 0, false
		}
	case float64:
		value = n
	default:
		return 0, false
	}
	return value, !math.IsInf(value, 0) && !math.IsNaN(value)
}

func comparableField(e entity, k string) any {
	if k == "serverCreatedAt" {
		return "" // ordering by created_at handled SQL-side later; stable no-op here
	}
	return e.Fields[k]
}

// paginateWrap adds ORDER BY/LIMIT/OFFSET to the entity-id query when possible;
// complex cursors fall back to in-memory slicing after fetch.
func paginateWrap(entitySQL string, args []any, o *Options, cat *platform.AttrCatalog, appID string) (string, []any) {
	limit, offset := effectiveLimitOffset(o)
	if limit <= 0 && offset <= 0 {
		return "SELECT entity_id FROM (" + entitySQL + ") s", args
	}
	wrapped := "SELECT entity_id FROM (" + entitySQL + ") s"
	if limit > 0 || offset > 0 {
		// Deterministic paging: without a total order LIMIT takes an
		// arbitrary subset of the matched set and OFFSET pages can overlap
		// or skip rows as the planner's emit order shifts between runs.
		wrapped += " ORDER BY s.entity_id"
	}
	if limit > 0 {
		// Keep one row beyond the requested page as a sentinel. runForm trims
		// it after ordering and uses its presence for hasNextPage.
		wrapped += fmt.Sprintf(" LIMIT %d", limit+1)
	}
	if offset > 0 {
		wrapped += fmt.Sprintf(" OFFSET %d", offset)
	}
	return wrapped, args
}
