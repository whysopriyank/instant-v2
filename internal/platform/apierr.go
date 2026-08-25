// Package platform — apierr.go: one choke point for turning server errors
// into client-safe messages.
//
// Audit M4: several surfaces returned err.Error() verbatim, leaking pgx
// details (SQLSTATEs, constraint/index names, conflicting-key tuples) and
// internal package prefixes to unauthenticated callers. Domain errors
// (permission denials, validation failures) keep their text — it is ours,
// informative, and free of infrastructure detail. Anything wrapping a raw
// database error collapses to a fixed string; the full error stays in logs.
package platform

import (
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// ClientMessage renders err for HTTP/WS clients. pgx errors become a fixed
// generic message; everything else passes through with internal package
// prefixes trimmed.
func ClientMessage(err error) string {
	if err == nil {
		return ""
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return "database error"
	}
	msg := err.Error()
	if strings.Contains(msg, "SQLSTATE") || strings.Contains(msg, "ERROR:") {
		return "database error"
	}
	return msg
}
