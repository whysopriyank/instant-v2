package backup

import (
	"archive/zip"
	"context"
	"crypto/rand"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Schema materialization is separate from ZIP transport and entity decoding.
type v1AttrRef struct {
	ID          [16]byte
	Cardinality string
}

type v1Endpoint struct {
	On        string `json:"on"`
	Label     string `json:"label"`
	Has       string `json:"has"`
	Required  *bool  `json:"required"`
	RequiredQ *bool  `json:"required?"`
	Config    struct {
		Required  *bool `json:"required"`
		RequiredQ *bool `json:"required?"`
	} `json:"config"`
}

type v1Link struct {
	Forward v1Endpoint `json:"forward"`
	Reverse v1Endpoint `json:"reverse"`
}

type v1EntityDef struct {
	Attrs map[string]v1BlobDef `json:"attrs"`
}

type v1Schema struct {
	// Legacy shape used by early self-host exports.
	Blobs map[string]map[string]v1BlobDef `json:"blobs"`
	Refs  map[string]v1Link               `json:"refs"`
	// Current v1 shape emitted by schema->defs in config.json.
	Entities map[string]v1EntityDef `json:"entities"`
	Links    map[string]v1Link      `json:"links"`
}

// v1BlobDef accepts both spellings emitted by historical Instant exports.
// Older exports omitted requiredness entirely; those attrs remain optional,
// except the implicit id attr which is normalized below.
type v1BlobDef struct {
	ValueType string `json:"valueType"`
	Required  *bool  `json:"required"`
	RequiredQ *bool  `json:"required?"`
	Config    struct {
		Unique    bool  `json:"unique"`
		Indexed   bool  `json:"indexed"`
		Required  *bool `json:"required"`
		RequiredQ *bool `json:"required?"`
	} `json:"config"`
}

func v1AttrRequired(def v1BlobDef) bool {
	for _, p := range []*bool{def.Required, def.RequiredQ, def.Config.Required, def.Config.RequiredQ} {
		if p != nil {
			return *p
		}
	}
	return false
}

func v1EndpointRequired(ep v1Endpoint) bool {
	for _, p := range []*bool{ep.Required, ep.RequiredQ, ep.Config.Required, ep.Config.RequiredQ} {
		if p != nil {
			return *p
		}
	}
	return false
}

func (schema v1Schema) normalized() (v1Schema, error) {
	// Older self-host exports call these sections blobs/refs; current v1
	// exports use entities/links. Normalize both shapes before materializing
	// attrs so restore remains compatible with either producer.
	blobs := schema.Blobs
	if blobs == nil {
		blobs = make(map[string]map[string]v1BlobDef)
	}
	for etype, entity := range schema.Entities {
		if blobs[etype] == nil {
			blobs[etype] = make(map[string]v1BlobDef)
		}
		for label, def := range entity.Attrs {
			blobs[etype][label] = def
		}
	}
	refs := schema.Refs
	if refs == nil {
		refs = make(map[string]v1Link)
	}
	for name, link := range schema.Links {
		refs[name] = link
	}
	if len(blobs) == 0 && len(refs) == 0 {
		return schema, fmt.Errorf("backup: v1 %s missing schema.blobs/schema.refs", v1ConfigEntry)
	}

	schema.Blobs, schema.Refs = blobs, refs
	return schema, nil
}

func importV1Schema(ctx context.Context, tx pgx.Tx, appID [16]byte, schema v1Schema, entries []*zip.File, counts *Counts) (map[string]v1AttrRef, error) {
	attrs := map[string]v1AttrRef{} // "etype\x00label" -> attr metadata

	insertAttr := func(etype, label, reverseEtype, reverseLabel, valueType, cardinality string, cdt *string, uniq, indexed, required bool) error {
		key := etype + "\x00" + label
		if _, ok := attrs[key]; ok {
			return nil
		}
		attrID := newRandomUUID()
		fwd := newRandomUUID()
		rev := newRandomUUID()
		if _, err := tx.Exec(ctx, `
			INSERT INTO attrs (id, app_id, etype, label, reverse_etype, reverse_label,
			                   value_type, cardinality, is_unique, is_indexed,
			                   forward_ident, reverse_ident, checked_data_type, is_required)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
			attrID, appID, etype, label, reverseEtype, reverseLabel, valueType, cardinality, uniq, indexed, fwd, rev, cdt, required); err != nil {
			return fmt.Errorf("backup: insert v1 attr %s.%s: %w", etype, label, err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO idents (id, app_id, attr_id, etype, label)
			VALUES ($1,$2,$3,$4,$5)
			ON CONFLICT DO NOTHING`, newRandomUUID(), appID, attrID, etype, label); err != nil {
			return err
		}
		attrs[key] = v1AttrRef{ID: attrID, Cardinality: cardinality}
		counts.Attrs++
		return nil
	}

	// Blobs: explicit defs + implicit id attr per etype (v1 semantics).
	for etype, labels := range schema.Blobs {
		for label, def := range labels {
			cdt := v1CheckedDataType(def.ValueType)
			if err := insertAttr(etype, label, etype, label, "blob", "one", cdt, def.Config.Unique, def.Config.Indexed, v1AttrRequired(def)); err != nil {
				return nil, err
			}
		}
		if _, ok := attrs[etype+"\x00id"]; !ok {
			if err := insertAttr(etype, "id", etype, "id", "blob", "one", nil, true, true, true); err != nil {
				return nil, err
			}
		}
	}
	// Entity files can exist for an etype that has no blob declaration (for
	// example, a ref-only endpoint). Ensure its implicit id attr still exists.
	for _, entry := range entries {
		name := entry.Name
		if !strings.HasPrefix(name, v1EntitiesDir) || !strings.HasSuffix(name, v1EntitiesSufx) {
			continue
		}
		etype := name[len(v1EntitiesDir) : len(name)-len(v1EntitiesSufx)]
		if etype != "" {
			if err := insertAttr(etype, "id", etype, "id", "blob", "one", nil, true, true, true); err != nil {
				return nil, err
			}
		}
		if etype == "$files" {
			// V1 hides these attrs from config.schema but retains their stored
			// values in entities/$files. Its restore uses the system catalog;
			// v2 materializes just those builtin definitions here.
			for _, def := range []struct {
				label, valueType          string
				unique, indexed, required bool
			}{
				{"location-id", "string", true, true, true},
				{"size", "number", false, true, true},
				{"content-type", "string", false, true, false},
				{"content-disposition", "string", false, true, false},
				{"key-version", "number", false, false, false},
			} {
				if err := insertAttr(etype, def.label, etype, def.label, "blob", "one", v1CheckedDataType(def.valueType), def.unique, def.indexed, def.required); err != nil {
					return nil, err
				}
			}
		}
	}
	// Refs: forward + reverse attr pair, reverse names mirrored like v1.
	for _, link := range schema.Refs {
		fwd, rev := link.Forward, link.Reverse
		if fwd.On == "" || fwd.Label == "" || rev.On == "" || rev.Label == "" {
			return nil, fmt.Errorf("backup: v1 ref link missing endpoint names")
		}
		cardF := fwd.Has
		if cardF == "" {
			cardF = "many"
		}
		// In the v1 model, reverse `has: one` means the forward ref is unique
		// (at most one source can point at a target). Preserve that constraint;
		// reverse cardinality is metadata, not a second attrs row.
		reverseUnique := rev.Has == "one"
		// A v1 link is represented by one ref attr. Its reverse endpoint is
		// metadata on that attr, not a second row (the attrs name-collision
		// trigger intentionally rejects materializing both directions).
		if err := insertAttr(fwd.On, fwd.Label, rev.On, rev.Label, "ref", cardF, nil, reverseUnique, true, v1EndpointRequired(fwd)); err != nil {
			return nil, err
		}
	}

	return attrs, nil
}

// v1CheckedDataType maps a v1 schema valueType to the checked_data_type enum
// text; any/null map to SQL NULL (unchecked), matching v1 migration 36.
func v1CheckedDataType(valueType string) *string {
	switch valueType {
	case "string", "number", "boolean", "date":
		s := valueType
		return &s
	default:
		return nil
	}
}

func newRandomUUID() [16]byte {
	var out [16]byte
	if _, err := rand.Read(out[:]); err != nil {
		panic("crypto/rand failure: " + err.Error())
	}
	out[6] = (out[6] & 0x0f) | 0x40
	out[8] = (out[8] & 0x3f) | 0x80
	return out
}
