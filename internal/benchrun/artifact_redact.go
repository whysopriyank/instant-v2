package benchrun

import (
	"bytes"
	"encoding/json"
	"regexp"
)

// secretRE consumes a complete quoted value, including escaped quotes and
// whitespace. Keeping the value as one match is important: replacing only
// the first token would leak a quoted DSN/cookie suffix into failure logs.
var secretRE = regexp.MustCompile(`(?is)((?:"?(?:authorization|cookie|password|passwd|token|secret|dsn|auth)"?)\s*[:=]\s*)("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|[^,\s};]+)`)
var uriCredentialRE = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://)([^/:\s@]+):([^@\s/]+)@`)
var authHeaderRE = regexp.MustCompile(`(?i)(authorization\s*[:=]\s*)(?:bearer|basic)\s+[^\r\n,}]+`)
var secretJSONKeyRE = regexp.MustCompile(`(?i)^(?:authorization|cookie|password|passwd|token|secret|dsn|auth)$`)

func Redact(s string) string {
	s = authHeaderRE.ReplaceAllString(s, `${1}[REDACTED]`)
	s = uriCredentialRE.ReplaceAllString(s, `${1}[REDACTED]@`)
	return secretRE.ReplaceAllString(s, `${1}"[REDACTED]"`)
}

func redactJSONBytes(b []byte, indent bool) ([]byte, error) {
	redacted := []byte(Redact(string(b)))
	if json.Valid(redacted) {
		return redacted, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	value = redactJSONValue(value)
	if indent {
		return json.MarshalIndent(value, "", "  ")
	}
	return json.Marshal(value)
}

func redactJSONValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		for key, item := range value {
			if secretJSONKeyRE.MatchString(key) {
				value[key] = "[REDACTED]"
				continue
			}
			value[key] = redactJSONValue(item)
		}
		return value
	case []any:
		for i := range value {
			value[i] = redactJSONValue(value[i])
		}
		return value
	case string:
		return Redact(value)
	default:
		return value
	}
}
