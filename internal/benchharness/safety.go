package benchharness

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// ValidateLoopbackURL fail-closes benchmark control and protocol endpoints to
// loopback. A host name is resolved before acceptance, avoiding an accidental
// public target hidden behind a local-looking alias.
func ValidateLoopbackURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid target URL: %w", err)
	}
	if u.Hostname() == "" {
		return fmt.Errorf("target URL has no host")
	}
	host := strings.ToLower(u.Hostname())
	if host != "localhost" && net.ParseIP(host) == nil {
		return fmt.Errorf("target URL host %q is not loopback", host)
	}
	if ip := net.ParseIP(host); ip != nil && !ip.IsLoopback() {
		return fmt.Errorf("target URL host %q is not loopback", host)
	}
	if host == "localhost" {
		ips, err := net.LookupIP(host)
		if err != nil {
			return fmt.Errorf("resolve target host: %w", err)
		}
		for _, ip := range ips {
			if !ip.IsLoopback() {
				return fmt.Errorf("target host resolves outside loopback: %s", ip)
			}
		}
	}
	return nil
}

// ValidateDatabaseURL checks a DSN without opening it. It requires a database
// name beginning with instant_bench_ and a loopback host. Credentials are never
// returned or persisted.
func ValidateDatabaseURL(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return fmt.Errorf("database URL is empty")
	}
	if forbiddenDSNOverride(raw) != "" {
		return fmt.Errorf("database DSN option %q is not permitted", forbiddenDSNOverride(raw))
	}
	cfg, err := pgx.ParseConfig(raw)
	if err != nil {
		return fmt.Errorf("invalid database configuration: %w", err)
	}
	if !explicitDSNHost(raw) {
		return fmt.Errorf("database host must be explicit; environment/service host overrides are forbidden")
	}
	if !explicitDSNDatabase(raw) {
		return fmt.Errorf("database name must be explicit")
	}
	if !strings.HasPrefix(cfg.Database, "instant_bench_") {
		return fmt.Errorf("database %q is not disposable instant_bench_ database", cfg.Database)
	}
	hosts := append([]string{cfg.Host}, fallbackHosts(cfg)...)
	for _, host := range hosts {
		if err := validateResolvedHost(host); err != nil {
			return err
		}
	}
	return nil
}

func forbiddenDSNOverride(raw string) string {
	u, err := url.Parse(raw)
	if err == nil && (u.Scheme == "postgres" || u.Scheme == "postgresql") {
		for key := range u.Query() {
			switch strings.ToLower(key) {
			case "host", "hostaddr", "service", "servicefile", "fallback", "fallbacks", "target_session_attrs":
				return strings.ToLower(key)
			}
		}
	}
	for _, key := range []string{"hostaddr", "service", "servicefile", "fallback", "fallbacks", "target_session_attrs"} {
		if dsnOptionPresent(raw, key) {
			return key
		}
	}
	if strings.TrimSpace(os.Getenv("PGSERVICE")) != "" || strings.TrimSpace(os.Getenv("PGSERVICEFILE")) != "" {
		return "service"
	}
	if strings.TrimSpace(os.Getenv("PGHOSTADDR")) != "" {
		return "hostaddr"
	}
	return ""
}

func explicitDSNHost(raw string) bool {
	u, err := url.Parse(raw)
	if err == nil && (u.Scheme == "postgres" || u.Scheme == "postgresql") {
		return u.Hostname() != ""
	}
	return dsnOptionValue(raw, "host") != ""
}

func explicitDSNDatabase(raw string) bool {
	u, err := url.Parse(raw)
	if err == nil && (u.Scheme == "postgres" || u.Scheme == "postgresql") {
		if strings.TrimPrefix(u.Path, "/") != "" {
			return true
		}
		return u.Query().Get("database") != "" || u.Query().Get("dbname") != ""
	}
	return dsnOptionValue(raw, "database") != "" || dsnOptionValue(raw, "dbname") != ""
}

func dsnOptionPresent(raw, wanted string) bool {
	fields := strings.Fields(raw)
	for i, field := range fields {
		parts := strings.SplitN(field, "=", 2)
		if len(parts) == 2 && strings.EqualFold(strings.TrimSpace(parts[0]), wanted) {
			return true
		}
		if strings.EqualFold(strings.TrimSpace(field), wanted) && i+1 < len(fields) && strings.TrimSpace(fields[i+1]) == "=" {
			return true
		}
	}
	return false
}

func dsnOptionValue(raw, wanted string) string {
	fields := strings.Fields(raw)
	for i, field := range fields {
		parts := strings.SplitN(field, "=", 2)
		if len(parts) == 2 && strings.EqualFold(strings.TrimSpace(parts[0]), wanted) {
			return strings.Trim(strings.TrimSpace(parts[1]), "'\"")
		}
		if strings.EqualFold(strings.TrimSpace(field), wanted) && i+2 < len(fields) && strings.TrimSpace(fields[i+1]) == "=" {
			return strings.Trim(strings.TrimSpace(fields[i+2]), "'\"")
		}
	}
	return ""
}

func fallbackHosts(cfg *pgx.ConnConfig) []string {
	hosts := make([]string, 0, len(cfg.Fallbacks))
	for _, fallback := range cfg.Fallbacks {
		if fallback != nil {
			hosts = append(hosts, fallback.Host)
		}
	}
	return hosts
}

func validateResolvedHost(host string) error {
	host = strings.TrimSpace(strings.ToLower(host))
	if host == "" {
		return fmt.Errorf("database host is empty")
	}
	if ip := net.ParseIP(host); ip != nil {
		if !ip.IsLoopback() {
			return fmt.Errorf("database host %q is not loopback", host)
		}
		return nil
	}
	if strings.HasPrefix(host, "/") {
		return fmt.Errorf("database unix socket host %q is not an approved loopback TCP endpoint", host)
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return fmt.Errorf("resolve database host %q: %w", host, err)
	}
	if len(ips) == 0 {
		return fmt.Errorf("database host %q has no resolved addresses", host)
	}
	for _, ip := range ips {
		if !ip.IsLoopback() {
			return fmt.Errorf("database host %q resolves outside loopback: %s", host, ip)
		}
	}
	return nil
}

// DatabaseName extracts the database name for setup diagnostics without
// exposing credentials. Both URL and pgx keyword DSNs are accepted.
func DatabaseName(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err == nil && u.Hostname() != "" {
		name := strings.TrimPrefix(u.Path, "/")
		if name == "" {
			name = u.Query().Get("database")
		}
		if name == "" {
			name = u.Query().Get("dbname")
		}
		if name != "" {
			return name, nil
		}
	}
	_, name := keyValueDSNParts(raw)
	if name == "" {
		return "", fmt.Errorf("database name is missing")
	}
	return name, nil
}

func keyValueDSNParts(raw string) (host, name string) {
	for _, field := range strings.Fields(raw) {
		parts := strings.SplitN(field, "=", 2)
		if len(parts) != 2 {
			continue
		}
		switch parts[0] {
		case "host":
			host = parts[1]
		case "dbname", "database":
			name = parts[1]
		}
	}
	return host, name
}
func validateHost(host string) error {
	host = strings.ToLower(host)
	if host == "localhost" {
		ips, err := net.LookupIP(host)
		if err != nil {
			return fmt.Errorf("resolve database host: %w", err)
		}
		if len(ips) == 0 {
			return fmt.Errorf("database host %q has no resolved addresses", host)
		}
		for _, ip := range ips {
			if !ip.IsLoopback() {
				return fmt.Errorf("database host resolves outside loopback: %s", ip)
			}
		}
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("database host %q is not loopback", host)
	}
	return nil
}

// ResetGuard requires both the disposable-name rule and a marker already
// stored in the target before destructive reset. Marker equality is checked by
// the caller through VerifyMarker.
type ResetGuard struct{ Marker string }

func (g ResetGuard) Validate(databaseName string) error {
	if g.Marker == "" {
		return fmt.Errorf("explicit benchmark marker is required")
	}
	if !strings.HasPrefix(databaseName, "instant_bench_") {
		return fmt.Errorf("refusing reset of database %q", databaseName)
	}
	return nil
}
func (g ResetGuard) VerifyMarker(ctx context.Context, db *sql.DB, databaseName string) error {
	if err := g.Validate(databaseName); err != nil {
		return err
	}
	var current, got, storedDB string
	err := db.QueryRowContext(ctx, `SELECT current_database(), marker, database_name FROM instant_bench_metadata WHERE key='benchmark'`).Scan(&current, &got, &storedDB)
	if err != nil {
		return fmt.Errorf("benchmark marker missing; refusing reset: %w", err)
	}
	if current != databaseName || storedDB != databaseName {
		return fmt.Errorf("connected database identity %q does not match requested %q", current, databaseName)
	}
	if got != g.Marker {
		return fmt.Errorf("benchmark marker mismatch; refusing reset")
	}
	return nil
}

// VerifyDatabaseIdentity confirms that the connected server is the database
// named by the operator, preventing a proxy/DSN mismatch from touching the
// wrong disposable instance.
func VerifyDatabaseIdentity(ctx context.Context, db *sql.DB, databaseName string) error {
	if databaseName == "" {
		return fmt.Errorf("database name is empty")
	}
	var current string
	if err := db.QueryRowContext(ctx, `SELECT current_database()`).Scan(&current); err != nil {
		return err
	}
	if current != databaseName {
		return fmt.Errorf("connected database identity %q does not match requested %q", current, databaseName)
	}
	return nil
}

// EnsureMarker creates the tiny marker table after a safe migration. It is
// intentionally separate from VerifyMarker so setup can initialize a new DB
// without pretending it was previously provisioned.
func EnsureMarker(ctx context.Context, db *sql.DB, marker string) error {
	if marker == "" {
		return fmt.Errorf("benchmark marker is empty")
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS instant_bench_metadata (key text PRIMARY KEY, marker text NOT NULL, database_name text NOT NULL DEFAULT current_database(), created_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE instant_bench_metadata ADD COLUMN IF NOT EXISTS database_name text NOT NULL DEFAULT current_database()`); err != nil {
		return err
	}
	var current, got, storedDB string
	if err := db.QueryRowContext(ctx, `SELECT current_database(), marker, database_name FROM instant_bench_metadata WHERE key='benchmark'`).Scan(&current, &got, &storedDB); err == nil {
		if got != marker || storedDB != current {
			return fmt.Errorf("benchmark marker already belongs to another marker or database")
		}
		return nil
	} else if err != sql.ErrNoRows {
		return err
	}
	if err := db.QueryRowContext(ctx, `SELECT current_database()`).Scan(&current); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, `INSERT INTO instant_bench_metadata(key,marker,database_name) VALUES('benchmark',$1,$2) ON CONFLICT(key) DO NOTHING`, marker, current)
	if err != nil {
		return err
	}
	// The insert can lose a concurrent race without returning an error. Re-read
	// the immutable identity after every insert attempt so a caller can never
	// proceed on a marker it did not establish or verify.
	if err := db.QueryRowContext(ctx, `SELECT current_database(), marker, database_name FROM instant_bench_metadata WHERE key='benchmark'`).Scan(&current, &got, &storedDB); err != nil {
		return fmt.Errorf("benchmark marker missing after insert: %w", err)
	}
	if current != storedDB || got != marker {
		return fmt.Errorf("benchmark marker conflict: existing marker belongs to another database or marker")
	}
	return nil
}

// SafeBundlePath prevents artifact path traversal and symlink escape. The
// returned path is inside root and suitable for creating a file afterward.
func SafeBundlePath(root, relative string) (string, error) {
	if root == "" || filepath.IsAbs(relative) {
		return "", fmt.Errorf("artifact path must be relative")
	}
	clean := filepath.Clean(relative)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("artifact path escapes bundle root")
	}
	base, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	out, err := filepath.Abs(filepath.Join(base, clean))
	if err != nil {
		return "", err
	}
	prefix := base + string(os.PathSeparator)
	if out != base && !strings.HasPrefix(out, prefix) {
		return "", fmt.Errorf("artifact path escapes bundle root")
	}
	for dir := out; ; dir = filepath.Dir(dir) {
		if info, err := os.Lstat(dir); err == nil && info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("artifact path traverses symlink %q", dir)
		}
		if dir == base || dir == filepath.Dir(dir) {
			break
		}
	}
	return out, nil
}

type Qualification struct {
	Target    string
	URL       string
	Revision  string
	DirtyHash string
	Protocol  string
	Passed    bool
	Reason    string
	CheckedAt time.Time
}
type HealthChecker interface {
	Health(context.Context) error
	LiveProbe(context.Context, int) error
}

// Qualify runs the required target preflight and preserves failure reasons for
// the result artifact. It does not silently turn a failed target into zero
// delivery.
func Qualify(ctx context.Context, target string, opts SessionOptions, checker HealthChecker) Qualification {
	q := Qualification{Target: target, URL: opts.URL, Protocol: "unknown", CheckedAt: time.Now()}
	if err := ValidateLoopbackURL(opts.URL); err != nil {
		q.Reason = err.Error()
		return q
	}
	if checker == nil {
		q.Reason = "qualification checker is nil"
		return q
	}
	if err := checker.Health(ctx); err != nil {
		q.Reason = err.Error()
		return q
	}
	if err := checker.LiveProbe(ctx, 4); err != nil {
		q.Reason = err.Error()
		return q
	}
	q.Passed = true
	return q
}
