package backup

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
)

func importV1Entities(batch *tripleBatch, attrs map[string]v1AttrRef, etype string, body []byte) error {
	sc := newLineScanner(strings.NewReader(string(body)))
	lineNo := 0
	for sc.Next() {
		lineNo++
		line := sc.line()
		if strings.TrimSpace(line) == "" {
			continue
		}
		var row struct {
			Entity    map[string]json.RawMessage `json:"entity"`
			CreatedAt float64                    `json:"createdAt"`
		}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return fmt.Errorf("backup: v1 entities/%s line %d: %v", etype, lineNo, err)
		}
		idRaw, ok := row.Entity["id"]
		if !ok {
			return fmt.Errorf("backup: v1 entities/%s line %d: entity missing id", etype, lineNo)
		}
		var entityID string
		if err := json.Unmarshal(idRaw, &entityID); err != nil {
			return fmt.Errorf("backup: v1 entities/%s line %d: bad entity id: %v", etype, lineNo, err)
		}
		hasCreated := row.CreatedAt > 0
		created := time.Time{}
		if hasCreated {
			created = time.UnixMilli(int64(row.CreatedAt))
		}
		// The id field is a real, implicit required attr in Instant's entity
		// model. Materialize it so restored queries and required checks see the
		// same representation as native imports.
		idAttr, ok := attrs[etype+"\x00id"]
		if !ok {
			return fmt.Errorf("backup: v1 entities/%s line %d: missing implicit id attr", etype, lineNo)
		}
		idJSON, err := json.Marshal(entityID)
		if err != nil {
			return err
		}
		if err := batch.addTriple(entityID, platform.UUIDToStr(idAttr.ID), idJSON, created, hasCreated); err != nil {
			return err
		}
		for label, value := range row.Entity {
			if label == "id" {
				continue
			}
			attr, ok := attrs[etype+"\x00"+label]
			if !ok {
				return fmt.Errorf("backup: v1 entities/%s line %d: missing attr for %s.%s (schema/config.json incomplete?)", etype, lineNo, etype, label)
			}
			var decoded any
			if err := json.Unmarshal(value, &decoded); err != nil {
				return fmt.Errorf("backup: v1 entities/%s line %d: bad value for %s.%s: %v", etype, lineNo, etype, label, err)
			}
			emit := func(v json.RawMessage) error {
				return batch.addTriple(entityID, platform.UUIDToStr(attr.ID), v, created, hasCreated)
			}
			// Only many-cardinality fields use an array as a transport wrapper.
			// For one-cardinality attrs an array is the value itself and must stay
			// intact (not be silently expanded into multiple triples).
			if attr.Cardinality == "many" {
				arr, isArr := decoded.([]any)
				if !isArr {
					if err := emit(value); err != nil {
						return err
					}
					continue
				}
				for _, item := range arr {
					itemJSON, err := json.Marshal(item)
					if err != nil {
						return err
					}
					if err := emit(itemJSON); err != nil {
						return err
					}
				}
			} else if err := emit(value); err != nil {
				return err
			}
		}
	}
	return sc.err(nil)
}
