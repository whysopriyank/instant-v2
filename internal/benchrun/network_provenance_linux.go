//go:build linux

package benchrun

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// NetworkNamespaceProvenance records the signed network namespace identity
// used for a benchmark attempt. It is intentionally separate from the target
// PID: a wildcard listener is only acceptable when the collector and target
// share this recorded, non-host namespace.
type NetworkNamespaceProvenance struct {
	NamespaceID        string `json:"network_namespace_id,omitempty"`
	InitialNamespaceID string `json:"initial_network_namespace_id,omitempty"`
}

// certifyWildcardListener is the runtime entry point used before a process
// resource sample is accepted. The caller's namespace is intentionally read
// from /proc/self rather than inferred from a host setting.
func certifyWildcardListener(pid int, provenance NetworkNamespaceProvenance) error {
	return certifyWildcardListenerAt(pid, provenance, "/proc", "/proc/self")
}

// certifyWildcardListenerAt proves the narrow exception for a target that
// binds 0.0.0.0/::: the collector and target must share the signed namespace,
// that namespace must differ from the recorded initial namespace, and procfs
// must show loopback as its only interface and route surface. The explicit
// roots make every parser testable without depending on the host network.
func certifyWildcardListenerAt(pid int, provenance NetworkNamespaceProvenance, procRoot, selfRoot string) error {
	if pid <= 0 {
		return errors.New("wildcard listener namespace PID is invalid")
	}
	namespaceID, err := normalizeNetworkNamespaceID(provenance.NamespaceID)
	if err != nil {
		return fmt.Errorf("recorded target network namespace is invalid: %w", err)
	}
	initialID, err := normalizeNetworkNamespaceID(provenance.InitialNamespaceID)
	if err != nil {
		return fmt.Errorf("recorded initial network namespace is invalid: %w", err)
	}
	if namespaceID == initialID {
		return errors.New("target network namespace is the initial namespace")
	}
	targetRoot := filepath.Join(procRoot, strconv.Itoa(pid))
	targetID, err := readNetworkNamespaceID(filepath.Join(targetRoot, "ns", "net"))
	if err != nil {
		return fmt.Errorf("target network namespace evidence unavailable: %w", err)
	}
	if targetID != namespaceID {
		return errors.New("target network namespace does not match signed provenance")
	}
	collectorID, err := readNetworkNamespaceID(filepath.Join(selfRoot, "ns", "net"))
	if err != nil {
		return fmt.Errorf("collector network namespace evidence unavailable: %w", err)
	}
	if collectorID != targetID {
		return errors.New("collector and target network namespaces differ")
	}
	if err := validateLoopbackOnlyNetwork(filepath.Join(targetRoot, "net")); err != nil {
		return err
	}
	return nil
}

func normalizeNetworkNamespaceID(value string) (string, error) {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, "net:[") || !strings.HasSuffix(value, "]") {
		return "", errors.New("expected net:[inode] identity")
	}
	digits := strings.TrimSuffix(strings.TrimPrefix(value, "net:["), "]")
	if digits == "" {
		return "", errors.New("namespace inode is empty")
	}
	for _, r := range digits {
		if r < '0' || r > '9' {
			return "", errors.New("namespace inode is not decimal")
		}
	}
	if _, err := strconv.ParseUint(digits, 10, 64); err != nil {
		return "", errors.New("namespace inode is out of range")
	}
	return "net:[" + digits + "]", nil
}

func readNetworkNamespaceID(path string) (string, error) {
	value, err := os.Readlink(path)
	if err != nil {
		return "", err
	}
	return normalizeNetworkNamespaceID(value)
}

func validateLoopbackOnlyNetwork(netRoot string) error {
	dev, err := os.ReadFile(filepath.Join(netRoot, "dev"))
	if err != nil {
		return fmt.Errorf("network interface evidence unavailable: %w", err)
	}
	if err := parseLoopbackOnlyDev(string(dev)); err != nil {
		return err
	}
	route4, err := os.ReadFile(filepath.Join(netRoot, "route"))
	if err != nil {
		return fmt.Errorf("IPv4 route evidence unavailable: %w", err)
	}
	if err := parseLoopbackOnlyRoute(string(route4)); err != nil {
		return err
	}
	route6, err := os.ReadFile(filepath.Join(netRoot, "ipv6_route"))
	if err != nil {
		return fmt.Errorf("IPv6 route evidence unavailable: %w", err)
	}
	if err := parseLoopbackOnlyIPv6Route(string(route6)); err != nil {
		return err
	}
	return nil
}

func parseLoopbackOnlyDev(value string) error {
	seenLoopback := false
	headerLines := 0
	for _, line := range strings.Split(value, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if isProcNetDevHeader(line) {
			headerLines++
			continue
		}
		colon := strings.IndexByte(line, ':')
		if colon <= 0 {
			return errors.New("malformed network interface evidence")
		}
		name := strings.TrimSpace(line[:colon])
		if name == "" || strings.ContainsAny(name, " \t") {
			return errors.New("malformed network interface name")
		}
		if len(strings.Fields(line[colon+1:])) < 8 {
			return errors.New("malformed network interface counters")
		}
		if name != "lo" {
			return fmt.Errorf("externally reachable network interface %q is present", name)
		}
		if seenLoopback {
			return errors.New("duplicate loopback network interface evidence")
		}
		seenLoopback = true
	}
	if headerLines < 2 {
		return errors.New("malformed network interface header")
	}
	if !seenLoopback {
		return errors.New("loopback network interface evidence is missing")
	}
	return nil
}

func parseLoopbackOnlyRoute(value string) error {
	seenHeader := false
	for _, line := range strings.Split(value, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if isProcRouteHeader(line) {
			seenHeader = true
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 11 {
			return errors.New("malformed IPv4 route evidence")
		}
		if fields[0] != "lo" {
			return fmt.Errorf("externally reachable IPv4 route uses %q", fields[0])
		}
		for _, index := range []int{1, 2, 7} {
			if len(fields[index]) != 8 {
				return errors.New("malformed IPv4 route address field")
			}
			if _, err := strconv.ParseUint(fields[index], 16, 32); err != nil {
				return errors.New("malformed IPv4 route address field")
			}
		}
		// The Linux proc ABI renders route flags as a four-digit hexadecimal
		// bitmask (for example 0001), unlike the eight-digit IPv4 fields above.
		if len(fields[3]) != 4 {
			return errors.New("malformed IPv4 route flags field")
		}
		if _, err := strconv.ParseUint(fields[3], 16, 16); err != nil {
			return errors.New("malformed IPv4 route flags field")
		}
		for _, index := range []int{4, 5, 6, 8, 9, 10} {
			if _, err := strconv.ParseUint(fields[index], 10, 64); err != nil {
				return errors.New("malformed IPv4 route counter")
			}
		}
		if fields[2] != "00000000" {
			return errors.New("loopback IPv4 route has a gateway")
		}
	}
	if !seenHeader {
		return errors.New("malformed IPv4 route header")
	}
	return nil
}

func isProcNetDevHeader(line string) bool {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return false
	}
	if fields[0] == "Inter-|" {
		return strings.Contains(line, "Receive") && strings.Contains(line, "Transmit")
	}
	if len(fields) < 2 || fields[0] != "face" || !strings.HasPrefix(fields[1], "|bytes") {
		return false
	}
	for _, label := range []string{"bytes", "packets", "errs", "drop", "fifo", "frame", "compressed", "multicast", "colls", "carrier"} {
		if !strings.Contains(line, label) {
			return false
		}
	}
	return true
}

func isProcRouteHeader(line string) bool {
	fields := strings.Fields(line)
	want := []string{"Iface", "Destination", "Gateway", "Flags", "RefCnt", "Use", "Metric", "Mask", "MTU", "Window", "IRTT"}
	if len(fields) < len(want) {
		return false
	}
	for i, expected := range want {
		if fields[i] != expected {
			return false
		}
	}
	return true
}

func parseLoopbackOnlyIPv6Route(value string) error {
	for _, line := range strings.Split(value, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 10 {
			return errors.New("malformed IPv6 route evidence")
		}
		for _, index := range []int{0, 2, 4} {
			if len(fields[index]) != 32 {
				return errors.New("malformed IPv6 route address")
			}
			if _, err := hex.DecodeString(fields[index]); err != nil {
				return errors.New("malformed IPv6 route address")
			}
		}
		for _, index := range []int{1, 3, 5, 6, 7, 8} {
			if _, err := strconv.ParseUint(fields[index], 16, 32); err != nil {
				return errors.New("malformed IPv6 route counter")
			}
		}
		if fields[9] != "lo" {
			return fmt.Errorf("externally reachable IPv6 route uses %q", fields[9])
		}
	}
	return nil
}
