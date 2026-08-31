package benchrun

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/instant-v2/instant-v2/internal/benchharness"
	"github.com/jackc/pgx/v5"
)

func ValidateBenchmarkDSN(dsn string) error {
	lower := strings.ToLower(strings.TrimSpace(dsn))
	if lower == "" {
		return errors.New("benchmark database DSN is required")
	}
	if strings.Contains(lower, "production") || strings.Contains(lower, "prod_") || strings.Contains(lower, "public") || strings.Contains(lower, "shared") {
		return errors.New("production-looking or shared DSN is forbidden")
	}
	// The A safety helper intentionally rejects keyword overrides, but older
	// versions also misclassified the `://` in a URI as hostaddr. Validate URI
	// DSNs directly while preserving the same loopback/disposable invariants;
	// keyword DSNs continue through the shared safety helper.
	if u, err := url.Parse(dsn); err == nil && (u.Scheme == "postgres" || u.Scheme == "postgresql") {
		cfg, err := pgx.ParseConfig(dsn)
		if err != nil {
			return fmt.Errorf("unsafe benchmark database DSN: %w", err)
		}
		if !strings.HasPrefix(cfg.Database, "instant_bench_") {
			return fmt.Errorf("unsafe benchmark database DSN: database %q is not disposable", cfg.Database)
		}
		if u.Query().Get("host") != "" || u.Query().Get("hostaddr") != "" || u.Query().Get("service") != "" || u.Query().Get("fallbacks") != "" {
			return errors.New("unsafe benchmark database DSN: host override is not permitted")
		}
		if err := validateLoopbackDBHost(cfg.Host); err != nil {
			return fmt.Errorf("unsafe benchmark database DSN: %w", err)
		}
		for _, fallback := range cfg.Fallbacks {
			if fallback != nil {
				if err := validateLoopbackDBHost(fallback.Host); err != nil {
					return fmt.Errorf("unsafe benchmark database DSN: %w", err)
				}
			}
		}
		return nil
	}
	if err := benchharness.ValidateDatabaseURL(dsn); err != nil {
		return fmt.Errorf("unsafe benchmark database DSN: %w", err)
	}
	return nil
}

func validateLoopbackDBHost(host string) error {
	host = strings.TrimSpace(host)
	if host == "" {
		return errors.New("explicit loopback host is required")
	}
	if ip := net.ParseIP(host); ip != nil {
		if !ip.IsLoopback() {
			return errors.New("host is not loopback")
		}
		return nil
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return fmt.Errorf("resolve host: %w", err)
	}
	if len(ips) == 0 {
		return errors.New("host has no resolved addresses")
	}
	for _, ip := range ips {
		if !ip.IsLoopback() {
			return errors.New("host is not loopback")
		}
	}
	return nil
}
