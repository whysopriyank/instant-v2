package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

type namespaceConfig struct {
	Targets []struct {
		ID               string `json:"id"`
		NetworkNamespace string `json:"network_namespace_id"`
		InitialNamespace string `json:"initial_network_namespace_id"`
	} `json:"targets"`
}

func validateDedicatedNamespace(configPath string) error {
	if runtime.GOOS != "linux" {
		return errors.New("dedicated benchmark namespace validation requires Linux procfs")
	}
	if err := noSymlinkComponents(configPath); err != nil {
		return err
	}
	b, err := readBounded(configPath, 8<<20)
	if err != nil {
		return err
	}
	var cfg namespaceConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		return fmt.Errorf("namespace config is malformed: %w", err)
	}
	wantIDs := []string{"v1", "v2_current", "v2_reference"}
	if len(cfg.Targets) != len(wantIDs) {
		return errors.New("namespace config must contain exactly the Wave 6 triad")
	}
	byID := make(map[string]struct{}, len(cfg.Targets))
	for _, target := range cfg.Targets {
		if _, exists := byID[target.ID]; exists {
			return fmt.Errorf("namespace config duplicates target %q", target.ID)
		}
		byID[target.ID] = struct{}{}
	}
	for _, id := range wantIDs {
		if _, exists := byID[id]; !exists {
			return fmt.Errorf("namespace config is missing target %q", id)
		}
	}
	collectorID, err := readNamespaceID("/proc/self/ns/net")
	if err != nil {
		return fmt.Errorf("collector namespace evidence unavailable: %w", err)
	}
	initial := ""
	for _, target := range cfg.Targets {
		targetID, err := normalizeNamespaceID(target.NetworkNamespace)
		if err != nil {
			return fmt.Errorf("target %s namespace: %w", target.ID, err)
		}
		initialID, err := normalizeNamespaceID(target.InitialNamespace)
		if err != nil {
			return fmt.Errorf("target %s initial namespace: %w", target.ID, err)
		}
		if targetID != collectorID {
			return fmt.Errorf("target %s namespace %s does not match collector %s", target.ID, targetID, collectorID)
		}
		if targetID == initialID {
			return fmt.Errorf("target %s namespace is the initial/host namespace", target.ID)
		}
		if initial == "" {
			initial = initialID
		} else if initial != initialID {
			return errors.New("triad target initial namespace identities differ")
		}
	}
	if err := validateLoopbackOnlyProcNetwork("/proc/self/net"); err != nil {
		return err
	}
	return nil
}

func normalizeNamespaceID(value string) (string, error) {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, "net:[") || !strings.HasSuffix(value, "]") {
		return "", errors.New("expected net:[inode] identity")
	}
	digits := strings.TrimSuffix(strings.TrimPrefix(value, "net:["), "]")
	if digits == "" {
		return "", errors.New("namespace inode is empty")
	}
	if _, err := strconv.ParseUint(digits, 10, 64); err != nil {
		return "", errors.New("namespace inode is invalid")
	}
	return "net:[" + digits + "]", nil
}

func readNamespaceID(path string) (string, error) {
	value, err := os.Readlink(path)
	if err != nil {
		return "", err
	}
	return normalizeNamespaceID(value)
}

func validateLoopbackOnlyProcNetwork(netRoot string) error {
	dev, err := os.ReadFile(filepath.Join(netRoot, "dev"))
	if err != nil {
		return fmt.Errorf("network interface evidence unavailable: %w", err)
	}
	if err := validateProcDev(string(dev)); err != nil {
		return err
	}
	route4, err := os.ReadFile(filepath.Join(netRoot, "route"))
	if err != nil {
		return fmt.Errorf("IPv4 route evidence unavailable: %w", err)
	}
	if err := validateProcRoute(string(route4)); err != nil {
		return err
	}
	route6, err := os.ReadFile(filepath.Join(netRoot, "ipv6_route"))
	if err != nil {
		return fmt.Errorf("IPv6 route evidence unavailable: %w", err)
	}
	if err := validateProcIPv6Route(string(route6)); err != nil {
		return err
	}
	return nil
}

func validateProcDev(value string) error {
	seenLoopback, headers := false, 0
	for _, line := range strings.Split(value, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if procDevHeader(line) {
			headers++
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
			return fmt.Errorf("external network interface %q is present", name)
		}
		if seenLoopback {
			return errors.New("duplicate loopback network interface evidence")
		}
		seenLoopback = true
	}
	if headers < 2 || !seenLoopback {
		return errors.New("loopback-only network interface evidence is incomplete")
	}
	return nil
}

func procDevHeader(line string) bool {
	fields := strings.Fields(line)
	if len(fields) > 0 && fields[0] == "Inter-|" {
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

func procRouteHeader(line string) bool {
	want := []string{"Iface", "Destination", "Gateway", "Flags", "RefCnt", "Use", "Metric", "Mask", "MTU", "Window", "IRTT"}
	fields := strings.Fields(line)
	if len(fields) < len(want) {
		return false
	}
	for i, value := range want {
		if fields[i] != value {
			return false
		}
	}
	return true
}

func validateProcRoute(value string) error {
	header := false
	for _, line := range strings.Split(value, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if procRouteHeader(line) {
			header = true
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 11 || fields[0] != "lo" {
			return errors.New("external or malformed IPv4 route evidence")
		}
		for _, index := range []int{1, 2, 7} {
			if len(fields[index]) != 8 {
				return errors.New("malformed IPv4 route address field")
			}
			if _, err := strconv.ParseUint(fields[index], 16, 32); err != nil {
				return errors.New("malformed IPv4 route address field")
			}
		}
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
		// The dedicated namespace has only the kernel's canonical local route:
		// an all-zero destination/gateway with the loopback mask.  Merely
		// requiring the interface name would allow an external destination to
		// hide behind lo and would not prove the namespace is isolated.
		if fields[3] != "0001" || fields[7] != "000000FF" ||
			(fields[1] != "00000000" && fields[1] != "0000007F") {
			return errors.New("non-canonical IPv4 loopback route evidence")
		}
	}
	if !header {
		return errors.New("malformed IPv4 route header")
	}
	return nil
}

func validateProcIPv6Route(value string) error {
	for _, line := range strings.Split(value, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 10 {
			return errors.New("malformed IPv6 route evidence")
		}
		for _, index := range []int{0, 2, 4} {
			if len(fields[index]) != 32 {
				return errors.New("malformed IPv6 route address")
			}
			for _, r := range fields[index] {
				if (r < '0' || r > '9') && (r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
					return errors.New("malformed IPv6 route address")
				}
			}
		}
		for _, index := range []int{1, 3} {
			if len(fields[index]) != 2 {
				return errors.New("malformed IPv6 route prefix")
			}
			if _, err := strconv.ParseUint(fields[index], 16, 8); err != nil {
				return errors.New("malformed IPv6 route prefix")
			}
		}
		for _, index := range []int{5, 6, 7, 8} {
			if len(fields[index]) != 8 {
				return errors.New("malformed IPv6 route counter")
			}
			if _, err := strconv.ParseUint(fields[index], 16, 32); err != nil {
				return errors.New("malformed IPv6 route counter")
			}
		}
		if fields[9] != "lo" {
			return fmt.Errorf("external IPv6 route uses %q", fields[9])
		}
		zero := strings.Repeat("0", 32)
		loopback := strings.Repeat("0", 31) + "1"
		defaultRoute := fields[0] == zero && fields[1] == "00"
		localRoute := fields[0] == loopback && fields[1] == "80"
		// Linux's proc ABI reports distinct flags for the null/default and
		// local ::1 routes. Match the complete known values; accepting
		// arbitrary bits or crossing route types would weaken the namespace
		// proof.
		canonicalFlags := (defaultRoute && fields[8] == "00200200") ||
			(localRoute && fields[8] == "80200001")
		if (!defaultRoute && !localRoute) ||
			fields[2] != zero || fields[3] != "00" ||
			fields[4] != zero ||
			!canonicalFlags {
			return errors.New("non-canonical IPv6 loopback route evidence")
		}
	}
	return nil
}
