package adminapi

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsUniqueViolationIsNarrow(t *testing.T) {
	if isUniqueViolation(errors.New("connection reset")) {
		t.Fatal("arbitrary failures must not enter the concurrent-creation retry path")
	}
	for _, constraint := range []string{"av_ignore_nulls_index", "attrs_etype_label_unique", "attrs_reverse_etype_label_unique"} {
		t.Run(constraint, func(t *testing.T) {
			err := fmt.Errorf("triple insert: %w", &pgconn.PgError{Code: "23505", ConstraintName: constraint})
			if !isUniqueViolation(err) {
				t.Fatalf("%s conflict must be retried", constraint)
			}
		})
	}
	if isUniqueViolation(fmt.Errorf("triple insert: %w", &pgconn.PgError{Code: "23505", ConstraintName: "triples_pkey"})) {
		t.Fatal("unrelated PostgreSQL unique violations must not be retried")
	}
	if !isUniqueViolation(&pgconn.PgError{Code: "P0001", Message: "trigger violation trg_attrs_unique_names"}) {
		t.Fatal("attribute-name uniqueness trigger conflicts must be retried")
	}
	if isUniqueViolation(&pgconn.PgError{Code: "P0001", Message: "different trigger failure"}) {
		t.Fatal("unrelated trigger failures must not be retried")
	}
}
