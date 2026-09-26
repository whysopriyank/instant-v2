package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/instant-v2/instant-v2/internal/corpus"
)

// CF-003 residual-gap exception enforcement (fail closed).
//
// The release envelope carries an explicit accepted-exception table under
// "## CF-003 residual gap exceptions" (coverage rows) and
// "## CF-003 surface gap exceptions" (surfaces). Every corpus manifest
// coverage row or surface with status "gap" must be listed in its table;
// every listed entry must name an existing entry that is still "gap". Table parsing reuses the existing
// envelope text-parsing approach (read file, locate the section header,
// parse the markdown table's first column); no new envelope format is
// invented.

const (
	cf003ExceptionSection        = "## CF-003 residual gap exceptions"
	cf003SurfaceExceptionSection = "## CF-003 surface gap exceptions"
)

// ValidateCF003Exceptions composes the corpus gap inventory with the
// explicitly accepted exceptions in the release envelope.
func ValidateCF003Exceptions(corpusDir, envelopePath string) error {
	m, err := corpus.LoadManifest(corpusDir)
	if err != nil {
		return err
	}
	rows := make([][2]string, 0, len(m.Coverage))
	for _, entry := range m.Coverage {
		rows = append(rows, [2]string{entry.ID, entry.Status})
	}
	if err := validateGapExceptions(envelopePath, cf003ExceptionSection, "gap", rows); err != nil {
		return err
	}
	surfaces := make([][2]string, 0, len(m.Surfaces))
	for _, surface := range m.Surfaces {
		surfaces = append(surfaces, [2]string{surface.ID, surface.Status})
	}
	return validateGapExceptions(envelopePath, cf003SurfaceExceptionSection, "surface gap", surfaces)
}

// validateGapExceptions requires the envelope section to list exactly the
// entries (id, status) whose status is "gap": no unlisted gap, no listed
// entry that is missing or no longer gap.
func validateGapExceptions(envelopePath, section, kind string, entries [][2]string) error {
	exceptions, err := parseCF003ExceptionTable(envelopePath, section)
	if err != nil {
		return err
	}
	statusByID := make(map[string]string, len(entries))
	for _, entry := range entries {
		statusByID[entry[0]] = entry[1]
	}
	for _, id := range exceptions {
		status, ok := statusByID[id]
		if !ok {
			return fmt.Errorf("CF-003 accepted %s exception %q does not exist in the corpus manifest", kind, id)
		}
		if status != "gap" {
			return fmt.Errorf("CF-003 stale %s exception %q is not gap (status %q)", kind, id, status)
		}
	}
	var unlisted []string
	for _, entry := range entries {
		if entry[1] == "gap" && !exceptionsSet(exceptions, entry[0]) {
			unlisted = append(unlisted, entry[0])
		}
	}
	if len(unlisted) > 0 {
		sort.Strings(unlisted)
		return fmt.Errorf("CF-003 %s %q is not listed in the accepted-exception table %q", kind, strings.Join(unlisted, ", "), section)
	}
	return nil
}

func exceptionsSet(ids []string, want string) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

// parseCF003ExceptionTable reads the release envelope the same way the
// existing envelope parsing works (plain text read plus marker search) and
// extracts the first-column coverage row IDs from the markdown table under
// the CF-003 residual-gap-exceptions section.
func parseCF003ExceptionTable(path, section string) ([]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("CF-003 accepted-exception table: %w", err)
	}
	lines := strings.Split(string(b), "\n")
	header := -1
	for i, line := range lines {
		if strings.EqualFold(strings.TrimSpace(line), section) {
			header = i
			break
		}
	}
	if header < 0 {
		return nil, fmt.Errorf("CF-003 accepted-exception table missing: section %q not found", section)
	}
	var ids []string
	seen := map[string]bool{}
	for _, line := range lines[header+1:] {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "## ") {
			break
		}
		if !strings.HasPrefix(trimmed, "|") {
			continue
		}
		cells := strings.Split(trimmed, "|")
		if len(cells) < 3 {
			continue
		}
		first := strings.TrimSpace(cells[1])
		first = strings.Trim(first, "`")
		first = strings.TrimSpace(first)
		lowered := strings.ToLower(first)
		if first == "" || strings.Contains(first, "---") || lowered == "coverage row" || lowered == "surface" {
			continue
		}
		if seen[first] {
			return nil, fmt.Errorf("CF-003 accepted-exception table lists %q twice", first)
		}
		seen[first] = true
		ids = append(ids, first)
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("CF-003 accepted-exception table missing: no entries under %q", section)
	}
	return ids, nil
}
