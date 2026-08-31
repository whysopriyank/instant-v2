package benchharness

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

func decodeWireResult(raw json.RawMessage, queryID string, aliases map[string]string) (Materialized, error) {
	if len(raw) == 0 || rawIsNull(raw) {
		return Materialized{}, fmt.Errorf("computation missing instaql-result")
	}
	var nodes []struct {
		Data struct {
			DatalogResult struct {
				JoinRows [][][]json.RawMessage `json:"join-rows"`
			} `json:"datalog-result"`
		} `json:"data"`
	}
	if json.Unmarshal(raw, &nodes) == nil && len(nodes) > 0 {
		out := Materialized{QueryID: queryID, Entities: map[string]Entity{}}
		identityRequired := identityAliasConfigured(aliases)
		identitySeen := make(map[string]bool)
		for _, node := range nodes {
			for _, row := range node.Data.DatalogResult.JoinRows {
				for _, triple := range row {
					if len(triple) < 3 {
						continue
					}
					id := scalarString(triple[0])
					attr := scalarString(triple[1])
					if id == "" || attr == "" {
						continue
					}
					if err := rejectNoncanonicalWireUUID(id); err != nil {
						return Materialized{}, err
					}
					if identityRequired {
						if err := validateWireEntityID(id); err != nil {
							return Materialized{}, err
						}
					}
					value, err := rawValue(triple[2])
					if err != nil {
						return Materialized{}, err
					}
					entity := out.Entities[id]
					entity.ID = id
					if entity.Attributes == nil {
						entity.Attributes = map[string]any{}
					}
					semantic := normalizeWireAttribute(attr, aliases)
					if semantic == "id" {
						if identityRequired {
							if identitySeen[id] {
								return Materialized{}, fmt.Errorf("duplicate identity triple for entity %s", id)
							}
							if err := validateWireIdentityValue(id, value); err != nil {
								return Materialized{}, err
							}
							identitySeen[id] = true
							out.Entities[id] = entity
						}
						continue
					}
					switch semantic {
					case "bucket":
						if n, ok := numericInt(value); ok {
							entity.Bucket = n
							out.Entities[id] = entity
							continue
						}
					case "rank":
						if n, ok := numericInt(value); ok {
							entity.Rank = n
							out.Entities[id] = entity
							continue
						}
					}
					entity.Attributes[semantic] = value
					out.Entities[id] = entity
				}
			}
		}
		if identityRequired {
			for id := range out.Entities {
				if !identitySeen[id] {
					return Materialized{}, fmt.Errorf("missing identity triple for entity %s", id)
				}
			}
		}
		return out, nil
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(raw, &root); err != nil {
		return Materialized{}, err
	}
	if data, ok := root["data"]; ok {
		if err := json.Unmarshal(data, &root); err != nil {
			return Materialized{}, err
		}
	}
	out := Materialized{QueryID: queryID, Entities: map[string]Entity{}}
	for _, collection := range root {
		var entities []json.RawMessage
		if json.Unmarshal(collection, &entities) != nil {
			continue
		}
		for _, rawEntity := range entities {
			id, err := wireObjectEntityID(rawEntity, aliases)
			if err != nil {
				return Materialized{}, err
			}
			if id == "" {
				continue
			}
			entity, err := wireEntity(id, rawEntity, aliases)
			if err != nil {
				return Materialized{}, err
			}
			out.Entities[entity.ID] = entity
		}
	}
	return out, nil
}

var wireUUIDRE = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func identityAliasConfigured(aliases map[string]string) bool {
	return aliases != nil && aliases["id"] != ""
}

func validateWireEntityID(id string) error {
	if !wireUUIDRE.MatchString(id) {
		return fmt.Errorf("identity entity id %q is not a UUID", id)
	}
	if id != strings.ToLower(id) {
		return fmt.Errorf("identity entity id %q must be a lowercase canonical UUID", id)
	}
	return nil
}

func rejectNoncanonicalWireUUID(value string) error {
	if wireUUIDRE.MatchString(value) && value != strings.ToLower(value) {
		return fmt.Errorf("entity UUID %q must be a lowercase canonical UUID", value)
	}
	return nil
}

func validateWireIdentityValue(entityID string, value any) error {
	identity, ok := value.(string)
	if !ok || !wireUUIDRE.MatchString(identity) || identity != strings.ToLower(identity) {
		return fmt.Errorf("identity value for entity %s must be a UUID string", entityID)
	}
	if identity != entityID {
		return fmt.Errorf("identity value for entity %s does not equal entity UUID", entityID)
	}
	return nil
}

func wireObjectEntityID(raw json.RawMessage, aliases map[string]string) (string, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return "", err
	}
	idRaw, ok := fields["id"]
	if !ok {
		if identityAliasConfigured(aliases) {
			return "", errors.New("object result entity is missing identity id")
		}
		return "", nil
	}
	value, err := rawValue(idRaw)
	if err != nil {
		return "", err
	}
	id, ok := value.(string)
	if !ok || id == "" {
		return "", errors.New("object result entity id must be a non-empty string")
	}
	if err := rejectNoncanonicalWireUUID(id); err != nil {
		return "", err
	}
	if identityAliasConfigured(aliases) {
		if err := validateWireEntityID(id); err != nil {
			return "", err
		}
	}
	return id, nil
}

func wireEntity(id string, raw json.RawMessage, aliases map[string]string) (Entity, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return Entity{}, err
	}
	entity := Entity{ID: id, Attributes: map[string]any{}}
	for key, value := range fields {
		decoded, err := rawValue(value)
		if err != nil {
			return Entity{}, err
		}
		semantic := normalizeWireAttribute(key, aliases)
		if semantic == "id" {
			// The object envelope's literal id and any explicit UUID alias
			// represent the row identity. Validate and omit both.
			if identityAliasConfigured(aliases) {
				if err := validateWireIdentityValue(id, decoded); err != nil {
					return Entity{}, err
				}
			}
			continue
		}
		switch semantic {
		case "bucket":
			if n, ok := numericInt(decoded); ok {
				entity.Bucket = n
				continue
			}
		case "rank":
			if n, ok := numericInt(decoded); ok {
				entity.Rank = n
				continue
			}
		}
		entity.Attributes[semantic] = decoded
	}
	return entity, nil
}

func normalizeWireAttribute(key string, aliases map[string]string) string {
	key = canonicalWireUUID(key)
	for _, semantic := range []string{"id", "value", "bucket", "rank"} {
		alias := ""
		if aliases != nil {
			alias = canonicalWireUUID(aliases[semantic])
		}
		if key == semantic || (alias != "" && alias == key) {
			return semantic
		}
	}
	return key
}

func canonicalWireUUID(value string) string {
	if wireUUIDRE.MatchString(value) {
		return strings.ToLower(value)
	}
	return value
}
