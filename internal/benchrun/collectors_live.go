package benchrun

// Concrete collectors deliberately live in WP5-B. They report an explicit
// unsupported/failed measurement when the host or endpoint cannot provide a
// value; no collector turns an absent observation into zero.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// LiveCollectors is constructed per attempt so database handles are closed
// after each target run and collector state cannot leak across pairs.
type LiveCollectors struct {
	Process    ProcessCollector
	Runtime    RuntimeCollector
	Database   DatabaseCollector
	Interval   time.Duration
	Provenance map[string]string
}

func UnsupportedLiveCollectors(reason string) *LiveCollectors {
	return &LiveCollectors{
		Process:    UnsupportedProcessCollector{Reason: reason},
		Runtime:    UnsupportedRuntimeCollector{Reason: reason},
		Database:   UnsupportedDatabaseCollector{Reason: reason},
		Interval:   time.Second,
		Provenance: map[string]string{"process": "unsupported: " + reason, "runtime": "unsupported: " + reason, "database": "unsupported: " + reason},
	}
}

func (c *LiveCollectors) Close() {
	if c == nil {
		return
	}
	if c.Process != nil {
		_ = c.Process.Close()
	}
	if c.Runtime != nil {
		_ = c.Runtime.Close()
	}
	if c.Database != nil {
		_ = c.Database.Close()
	}
}

type FailedProcessCollector struct{ Reason string }

func (f FailedProcessCollector) Sample(context.Context) (ProcessSample, error) {
	return failedProcess(f.Reason), errors.New(f.Reason)
}
func (f FailedProcessCollector) Close() error { return nil }

type FailedRuntimeCollector struct{ Reason string }

func (f FailedRuntimeCollector) Sample(context.Context) (RuntimeSample, error) {
	return failedRuntime(f.Reason), errors.New(f.Reason)
}
func (f FailedRuntimeCollector) Close() error { return nil }

type FailedDatabaseCollector struct{ Reason string }

func (f FailedDatabaseCollector) Before(context.Context) (DBSnapshot, error) {
	return failedDBSnapshot(f.Reason), errors.New(f.Reason)
}
func (f FailedDatabaseCollector) After(context.Context) (DBSnapshot, error) {
	return failedDBSnapshot(f.Reason), errors.New(f.Reason)
}
func (f FailedDatabaseCollector) Close() error { return nil }

const procClockTicksPerSecond = 100.0

// ProcProcessCollector reads stable Linux /proc fields for an explicitly
// configured process. The executable path is optional; /proc/PID/exe is used
// when omitted.
type ProcProcessCollector struct {
	PID            int
	PIDFile        string
	ExecutablePath string
	ExpectedHash   string
	EndpointURLs   []string
	// NetworkNamespace is required when the signed process exposes a
	// wildcard listener. Exact loopback listeners do not need this additional
	// proof; wildcard listeners are accepted only in a recorded, loopback-only
	// benchmark namespace shared with the collector.
	NetworkNamespace NetworkNamespaceProvenance
}

func (c ProcProcessCollector) Sample(ctx context.Context) (ProcessSample, error) {
	pid := c.PID
	if c.PIDFile != "" {
		b, err := readBounded(c.PIDFile, 128)
		if err != nil {
			return failedProcess("process pid file unavailable"), err
		}
		pid, err = strconv.Atoi(strings.TrimSpace(string(b)))
		if err != nil {
			return failedProcess("process pid file is invalid"), err
		}
	}
	if pid <= 0 {
		return failedProcess("invalid process pid"), errors.New("process pid must be positive")
	}
	if err := pidOwnsConfiguredEndpointWithNamespace(pid, c.EndpointURLs, c.NetworkNamespace); err != nil {
		return failedProcess(err.Error()), err
	}
	select {
	case <-ctx.Done():
		return failedProcess(ctx.Err().Error()), ctx.Err()
	default:
	}
	base := filepath.Join("/proc", strconv.Itoa(pid))
	stat, err := os.ReadFile(filepath.Join(base, "stat"))
	if err != nil {
		return failedProcess(err.Error()), err
	}
	fields, err := parseProcStat(string(stat))
	if err != nil {
		return failedProcess(err.Error()), err
	}
	utime, err1 := strconv.ParseFloat(fields[11], 64)
	stime, err2 := strconv.ParseFloat(fields[12], 64)
	start := fields[19]
	if err1 != nil || err2 != nil || start == "" {
		err := errors.New("malformed process stat")
		return failedProcess(err.Error()), err
	}
	vmrss, threads, err := readProcStatus(filepath.Join(base, "status"))
	if err != nil {
		return failedProcess(err.Error()), err
	}
	fdCount, err := countProcFDs(filepath.Join(base, "fd"))
	if err != nil {
		return failedProcess(err.Error()), err
	}
	// Always resolve the executable through the sampled PID. Hashing a
	// configured path would permit a stale/replaced process to masquerade as
	// the signed target binary.
	executable, err := os.Readlink(filepath.Join(base, "exe"))
	if err != nil {
		return failedProcess(err.Error()), err
	}
	if c.ExecutablePath != "" {
		actualResolved, actualErr := filepath.EvalSymlinks(executable)
		expectedResolved, expectedErr := filepath.EvalSymlinks(c.ExecutablePath)
		if actualErr != nil || expectedErr != nil || actualResolved != expectedResolved {
			return failedProcess("sampled process executable path does not match signed provenance"), errors.New("target executable path mismatch")
		}
	}
	hash, err := hashFile(executable)
	if err != nil {
		return failedProcess(err.Error()), err
	}
	if c.ExpectedHash != "" && !strings.EqualFold(hash, c.ExpectedHash) {
		return failedProcess("target executable hash does not match signed provenance"), errors.New("target executable hash mismatch")
	}
	now := time.Now().UTC()
	return ProcessSample{
		At: now, PID: pid, StartTime: start, ExecutableHash: hash,
		UserCPU:   Measurement{Status: StatusValue, Value: utime / procClockTicksPerSecond, Unit: "cpu_seconds"},
		SystemCPU: Measurement{Status: StatusValue, Value: stime / procClockTicksPerSecond, Unit: "cpu_seconds"},
		RSS:       Measurement{Status: StatusValue, Value: float64(vmrss), Unit: "bytes"},
		PeakRSS:   Missing("bytes"),
		Threads:   Measurement{Status: StatusValue, Value: float64(threads), Unit: "threads"},
		FDs:       Measurement{Status: StatusValue, Value: float64(fdCount), Unit: "fds"},
		ExitCode:  Missing("exit_code"),
	}, nil
}

// pidOwnsConfiguredEndpoint is deliberately Linux/procfs based. A configured
// PID is not sufficient provenance for a resource claim: at least one
// configured loopback session/health/runtime endpoint must be backed by a
// listening socket owned by that exact PID. A wildcard socket is accepted only
// after the Linux namespace proof in network_provenance_linux.go. Unsupported
// platforms and missing procfs evidence fail closed.
func pidOwnsConfiguredEndpoint(pid int, endpoints []string) error {
	return pidOwnsConfiguredEndpointWithNamespace(pid, endpoints, NetworkNamespaceProvenance{})
}

func pidOwnsConfiguredEndpointWithNamespace(pid int, endpoints []string, namespace NetworkNamespaceProvenance) error {
	if runtime.GOOS != "linux" {
		return errors.New("endpoint-serving PID binding is unsupported on this platform")
	}
	return pidOwnsConfiguredEndpointAt(pid, endpoints, namespace, "/proc", "/proc/self")
}

// pidOwnsConfiguredEndpointAt is the production implementation with explicit
// proc roots so its fail-closed procfs parsing can be tested against bounded,
// deterministic evidence. The runtime path always passes the real procfs
// roots above.
func pidOwnsConfiguredEndpointAt(pid int, endpoints []string, namespace NetworkNamespaceProvenance, procRoot, selfRoot string) error {
	if len(endpoints) == 0 {
		return errors.New("no configured endpoint for process PID binding")
	}
	endpointKeys := make(map[string]bool, len(endpoints))
	for _, raw := range endpoints {
		if strings.TrimSpace(raw) == "" {
			continue
		}
		u, err := url.Parse(raw)
		if err != nil || u.Hostname() == "" {
			return errors.New("configured endpoint is invalid for process PID binding")
		}
		host := u.Hostname()
		port := u.Port()
		if port == "" {
			switch strings.ToLower(u.Scheme) {
			case "https", "wss":
				port = "443"
			default:
				port = "80"
			}
		}
		p, err := strconv.Atoi(port)
		if err != nil || p <= 0 || p > 65535 {
			return errors.New("configured endpoint has invalid port")
		}
		ips, err := endpointHostIPs(host)
		if err != nil {
			return err
		}
		for _, ip := range ips {
			endpointKeys[ip.String()+":"+strconv.Itoa(p)] = true
		}
	}
	if len(endpointKeys) == 0 {
		return errors.New("no non-empty configured endpoint for process PID binding")
	}
	inodes, err := processSocketInodesAt(pid, procRoot)
	if err != nil {
		return err
	}
	listeners, wildcards, err := processListeningInodesAt(pid, procRoot, endpointKeys)
	if err != nil {
		return err
	}
	for inode := range inodes {
		if listeners[inode] {
			return nil
		}
	}
	for inode := range inodes {
		if wildcards[inode] {
			if err := certifyWildcardListenerAt(pid, namespace, procRoot, selfRoot); err != nil {
				return err
			}
			return nil
		}
	}
	return errors.New("configured endpoint is not served by the signed process PID")
}

func endpointHostIPs(host string) ([]net.IP, error) {
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsLoopback() {
			return []net.IP{ip}, nil
		}
		return nil, errors.New("configured endpoint host is not loopback")
	}
	ips, err := net.LookupIP(host)
	if err != nil || len(ips) == 0 {
		return nil, errors.New("configured endpoint host cannot be proven loopback")
	}
	for _, ip := range ips {
		if !ip.IsLoopback() {
			return nil, errors.New("configured endpoint host is not loopback")
		}
	}
	return ips, nil
}

func processSocketInodes(pid int) (map[string]bool, error) {
	return processSocketInodesAt(pid, "/proc")
}

func processSocketInodesAt(pid int, procRoot string) (map[string]bool, error) {
	entries, err := os.ReadDir(filepath.Join(procRoot, strconv.Itoa(pid), "fd"))
	if err != nil {
		return nil, fmt.Errorf("process socket evidence unavailable: %w", err)
	}
	out := make(map[string]bool)
	for _, entry := range entries {
		link, err := os.Readlink(filepath.Join(procRoot, strconv.Itoa(pid), "fd", entry.Name()))
		if err != nil {
			continue
		}
		if strings.HasPrefix(link, "socket:[") && strings.HasSuffix(link, "]") {
			out[strings.TrimSuffix(strings.TrimPrefix(link, "socket:["), "]")] = true
		}
	}
	if len(out) == 0 {
		return out, nil
	}
	return out, nil
}

func loopbackListeningInodes(pid int, endpointKeys map[string]bool) (map[string]bool, error) {
	listeners, _, err := processListeningInodes(pid, endpointKeys)
	return listeners, err
}

func processListeningInodes(pid int, endpointKeys map[string]bool) (map[string]bool, map[string]bool, error) {
	return processListeningInodesAt(pid, "/proc", endpointKeys)
}

func processListeningInodesAt(pid int, procRoot string, endpointKeys map[string]bool) (map[string]bool, map[string]bool, error) {
	out := make(map[string]bool)
	wildcards := make(map[string]bool)
	for _, name := range []string{"tcp", "tcp6"} {
		b, err := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "net", name))
		if err != nil {
			return nil, nil, fmt.Errorf("listening socket evidence unavailable: %w", err)
		}
		for _, line := range strings.Split(string(b), "\n")[1:] {
			if strings.TrimSpace(line) == "" {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) < 10 {
				return nil, nil, errors.New("malformed listening socket evidence")
			}
			if len(fields[3]) != 2 {
				return nil, nil, errors.New("malformed listening socket state")
			}
			if _, err := strconv.ParseUint(fields[3], 16, 8); err != nil {
				return nil, nil, errors.New("malformed listening socket state")
			}
			if fields[3] != "0A" {
				continue
			}
			address := strings.SplitN(fields[1], ":", 2)
			if len(address) != 2 || len(address[0]) == 0 || len(address[1]) != 4 {
				return nil, nil, errors.New("malformed listening socket address")
			}
			portHex := address[1]
			portBytes, err := hex.DecodeString(portHex)
			if err != nil || len(portBytes) != 2 {
				return nil, nil, errors.New("malformed listening socket port")
			}
			port := int(portBytes[0])<<8 | int(portBytes[1])
			// procfs prints the port in network byte order, while the address
			// representation is kernel-specific. Only loopback listeners are
			// accepted, preventing a same-port non-loopback collision.
			ip, loopback, wildcard, ok := procListenerAddress(address[0], name == "tcp6")
			if !ok {
				return nil, nil, errors.New("malformed listening socket address")
			}
			if _, err := strconv.ParseUint(fields[9], 10, 64); err != nil {
				return nil, nil, errors.New("malformed listening socket inode")
			}
			if wildcard && endpointPort(endpointKeys, port) {
				wildcards[fields[9]] = true
				continue
			}
			if loopback && endpointKeys[ip.String()+":"+strconv.Itoa(port)] {
				out[fields[9]] = true
			}
		}
	}
	return out, wildcards, nil
}

func endpointPort(endpointKeys map[string]bool, port int) bool {
	want := strconv.Itoa(port)
	for key := range endpointKeys {
		_, got, err := net.SplitHostPort(key)
		if err == nil && got == want {
			return true
		}
	}
	return false
}

func procAddressIP(value string, ipv6 bool) (net.IP, bool) {
	ip, loopback, _, ok := procListenerAddress(value, ipv6)
	return ip, loopback && ok
}

func procListenerAddress(value string, ipv6 bool) (net.IP, bool, bool, bool) {
	b, err := hex.DecodeString(value)
	if err != nil {
		return nil, false, false, false
	}
	if !ipv6 && len(b) == 4 {
		ip := net.IPv4(b[3], b[2], b[1], b[0])
		return ip, ip.IsLoopback(), ip.IsUnspecified(), true
	}
	if ipv6 && len(b) == 16 {
		// Linux stores each 32-bit word little-endian in /proc/net/tcp6.
		for i := 0; i < len(b); i += 4 {
			b[i], b[i+1], b[i+2], b[i+3] = b[i+3], b[i+2], b[i+1], b[i]
		}
		ip := net.IP(b)
		return ip, ip.IsLoopback(), ip.IsUnspecified(), true
	}
	return nil, false, false, false
}

func (c ProcProcessCollector) Close() error { return nil }

func parseProcStat(value string) ([]string, error) {
	end := strings.LastIndex(value, ")")
	if end < 0 || end+2 > len(value) {
		return nil, errors.New("malformed /proc stat")
	}
	fields := strings.Fields(value[end+2:])
	// The suffix starts at field 3, so indexes 11/12 are utime/stime and 19 is
	// starttime (fields 14, 15, and 22 in procfs documentation).
	if len(fields) <= 19 {
		return nil, errors.New("short /proc stat")
	}
	return fields, nil
}

func readProcStatus(path string) (rssBytes, threads int64, err error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, err
	}
	for _, line := range strings.Split(string(b), "\n") {
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		switch parts[0] {
		case "VmRSS:":
			value, e := strconv.ParseInt(parts[1], 10, 64)
			if e != nil {
				return 0, 0, e
			}
			rssBytes = value * 1024
		case "Threads:":
			threads, err = strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				return 0, 0, err
			}
		}
	}
	return rssBytes, threads, nil
}

func countProcFDs(path string) (int, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return 0, err
	}
	return len(entries), nil
}

func hashFile(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Size() > DefaultMaxArtifactBytes {
		return "", errors.New("executable is non-regular or exceeds artifact budget")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.CopyN(h, f, DefaultMaxArtifactBytes+1); err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func failedProcess(reason string) ProcessSample {
	return ProcessSample{At: time.Now().UTC(), UserCPU: Measurement{Status: StatusFailed, Unit: "cpu_seconds", Error: reason}, SystemCPU: Measurement{Status: StatusFailed, Unit: "cpu_seconds", Error: reason}, RSS: Measurement{Status: StatusFailed, Unit: "bytes", Error: reason}, PeakRSS: Measurement{Status: StatusFailed, Unit: "bytes", Error: reason}, Threads: Measurement{Status: StatusFailed, Unit: "threads", Error: reason}, FDs: Measurement{Status: StatusFailed, Unit: "fds", Error: reason}, ExitCode: Measurement{Status: StatusFailed, Unit: "exit_code", Error: reason}}
}

// PrometheusRuntimeCollector maps the stable Go runtime metric names needed by
// the benchmark. Missing individual series remain missing rather than zero.
type PrometheusRuntimeCollector struct {
	URL        string
	Token      string
	HTTPClient *http.Client
}

func (c PrometheusRuntimeCollector) Sample(ctx context.Context) (RuntimeSample, error) {
	client := c.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.URL, nil)
	if err != nil {
		return failedRuntime(err.Error()), err
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return failedRuntime(err.Error()), err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		err := fmt.Errorf("runtime endpoint returned %s", resp.Status)
		return failedRuntime(err.Error()), err
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return failedRuntime(err.Error()), err
	}
	metrics := parsePrometheus(string(b))
	return RuntimeSample{At: time.Now().UTC(),
		AllocBytes: prometheusMeasurement(metrics, "go_memstats_alloc_bytes", "bytes"),
		LiveHeap:   prometheusMeasurement(metrics, "go_memstats_heap_alloc_bytes", "bytes"),
		HeapGoal:   prometheusMeasurement(metrics, "go_memstats_heap_goal_bytes", "bytes"),
		GCCycles:   prometheusMeasurement(metrics, "go_gc_duration_seconds_count", "cycles"),
		GCPause:    prometheusMeasurement(metrics, "go_gc_duration_seconds_sum", "seconds"),
		Goroutines: prometheusMeasurement(metrics, "go_goroutines", "count"),
	}, nil
}

func (c PrometheusRuntimeCollector) Close() error { return nil }

func parsePrometheus(body string) map[string]float64 {
	out := map[string]float64{}
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		name := parts[0]
		if brace := strings.IndexByte(name, '{'); brace >= 0 {
			name = name[:brace]
		}
		value, err := strconv.ParseFloat(parts[len(parts)-1], 64)
		if err == nil {
			out[name] = value
		}
	}
	return out
}

func prometheusMeasurement(metrics map[string]float64, name, unit string) Measurement {
	value, ok := metrics[name]
	if !ok {
		return Missing(unit)
	}
	return CollectorMeasurement(CollectorSupported, value, unit, "")
}

func failedRuntime(reason string) RuntimeSample {
	return RuntimeSample{At: time.Now().UTC(), AllocBytes: Measurement{Status: StatusFailed, Unit: "bytes", Error: reason}, LiveHeap: Measurement{Status: StatusFailed, Unit: "bytes", Error: reason}, HeapGoal: Measurement{Status: StatusFailed, Unit: "bytes", Error: reason}, GCCycles: Measurement{Status: StatusFailed, Unit: "cycles", Error: reason}, GCPause: Measurement{Status: StatusFailed, Unit: "seconds", Error: reason}, Goroutines: Measurement{Status: StatusFailed, Unit: "count", Error: reason}}
}

// PGDatabaseCollector captures database statistics from the target DSN. The
// query intentionally uses only PostgreSQL statistics views and is repeated
// before and after each attempt.
type PGDatabaseCollector struct{ DB *sql.DB }

func NewPGDatabaseCollector(dsn string) (*PGDatabaseCollector, error) {
	if err := ValidateBenchmarkDSN(dsn); err != nil {
		return nil, err
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	return &PGDatabaseCollector{DB: db}, nil
}

func (c *PGDatabaseCollector) Before(ctx context.Context) (DBSnapshot, error) {
	return c.snapshot(ctx)
}
func (c *PGDatabaseCollector) After(ctx context.Context) (DBSnapshot, error) {
	return c.snapshot(ctx)
}
func (c *PGDatabaseCollector) Close() error {
	if c == nil || c.DB == nil {
		return nil
	}
	return c.DB.Close()
}

const pgStatsQuery = `SELECT current_setting('server_version'), d.numbackends, d.blks_hit, d.blks_read, d.temp_bytes, d.temp_files, d.xact_commit, d.xact_rollback, d.tup_returned, d.tup_fetched, COALESCE((SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')), 0) FROM pg_stat_database d WHERE d.datname = current_database()`

func (c *PGDatabaseCollector) snapshot(ctx context.Context) (DBSnapshot, error) {
	if c == nil || c.DB == nil {
		err := errors.New("database collector is not configured")
		return failedDBSnapshot(err.Error()), err
	}
	var s DBSnapshot
	s.At = time.Now().UTC()
	var version string
	var values [10]float64
	err := c.DB.QueryRowContext(ctx, pgStatsQuery).Scan(&version, &values[0], &values[1], &values[2], &values[3], &values[4], &values[5], &values[6], &values[7], &values[8], &values[9])
	if err != nil {
		return failedDBSnapshot(Redact(err.Error())), err
	}
	s.Version = version
	s.Connections = numericMeasurement(values[0], "count")
	s.BlockHits = numericMeasurement(values[1], "count")
	s.BlockReads = numericMeasurement(values[2], "count")
	s.TempBytes = numericMeasurement(values[3], "bytes")
	s.TempFiles = numericMeasurement(values[4], "count")
	s.Commits = numericMeasurement(values[5], "count")
	s.Rollbacks = numericMeasurement(values[6], "count")
	s.TupleReads = numericMeasurement(values[7], "count")
	s.TupleWrites = numericMeasurement(values[8], "count")
	s.WALBytes = numericMeasurement(values[9], "bytes")
	s.SlotLag, s.PoolActive, s.PoolIdle = Unsupported("bytes", "slot lag is not exposed by the configured collector"), Unsupported("count", "pool metrics are not exposed by the configured collector"), Unsupported("count", "pool metrics are not exposed by the configured collector")
	return s, nil
}

func numericMeasurement(value float64, unit string) Measurement {
	return CollectorMeasurement(CollectorSupported, value, unit, "")
}

func failedDBSnapshot(reason string) DBSnapshot {
	fields := []Measurement{
		{Status: StatusFailed, Unit: "count", Error: reason}, {Status: StatusFailed, Unit: "count", Error: reason}, {Status: StatusFailed, Unit: "count", Error: reason}, {Status: StatusFailed, Unit: "bytes", Error: reason}, {Status: StatusFailed, Unit: "count", Error: reason}, {Status: StatusFailed, Unit: "count", Error: reason}, {Status: StatusFailed, Unit: "count", Error: reason}, {Status: StatusFailed, Unit: "count", Error: reason}, {Status: StatusFailed, Unit: "count", Error: reason}, {Status: StatusFailed, Unit: "bytes", Error: reason}, {Status: StatusFailed, Unit: "bytes", Error: reason}, {Status: StatusFailed, Unit: "count", Error: reason}, {Status: StatusFailed, Unit: "count", Error: reason},
	}
	return DBSnapshot{At: time.Now().UTC(), Version: "", Connections: fields[0], BlockHits: fields[1], BlockReads: fields[2], TempBytes: fields[3], TempFiles: fields[4], Commits: fields[5], Rollbacks: fields[6], TupleReads: fields[7], TupleWrites: fields[8], WALBytes: fields[9], SlotLag: fields[10], PoolActive: fields[11], PoolIdle: fields[12]}
}
