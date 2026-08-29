//go:build linux

package benchrun

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWildcardListenerRequiresDedicatedRecordedNamespace(t *testing.T) {
	root := writeNamespaceFixture(t, "net:[4026533000]", "net:[4026533001]", true, true)
	provenance := NetworkNamespaceProvenance{
		NamespaceID:        "net:[4026533000]",
		InitialNamespaceID: "net:[4026533001]",
	}
	if err := certifyWildcardListenerAt(42, provenance, root, filepath.Join(root, "self")); err != nil {
		t.Fatalf("isolated loopback-only namespace was rejected: %v", err)
	}
	if err := pidOwnsConfiguredEndpointAt(42, []string{"http://127.0.0.1:29101/health"}, provenance, root, filepath.Join(root, "self")); err != nil {
		t.Fatalf("production endpoint ownership path rejected isolated wildcard listener: %v", err)
	}
}

func TestWildcardListenerRejectsHostNamespace(t *testing.T) {
	root := writeNamespaceFixture(t, "net:[4026533001]", "net:[4026533001]", true, true)
	err := certifyWildcardListenerAt(42, NetworkNamespaceProvenance{
		NamespaceID:        "net:[4026533001]",
		InitialNamespaceID: "net:[4026533001]",
	}, root, filepath.Join(root, "self"))
	if err == nil || !strings.Contains(err.Error(), "initial") {
		t.Fatalf("host namespace wildcard was accepted or misclassified: %v", err)
	}
}

func TestWildcardListenerRejectsSelfTargetNamespaceMismatch(t *testing.T) {
	root := writeNamespaceFixture(t, "net:[4026533000]", "net:[4026533001]", true, true)
	self := filepath.Join(root, "self")
	if err := os.Remove(filepath.Join(self, "ns", "net")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("net:[4026533002]", filepath.Join(self, "ns", "net")); err != nil {
		t.Fatal(err)
	}
	err := certifyWildcardListenerAt(42, NetworkNamespaceProvenance{
		NamespaceID:        "net:[4026533000]",
		InitialNamespaceID: "net:[4026533001]",
	}, root, self)
	if err == nil || !strings.Contains(err.Error(), "collector") {
		t.Fatalf("collector/target namespace mismatch was accepted or misclassified: %v", err)
	}
}

func TestWildcardListenerRejectsMissingOrMalformedRecordedNamespace(t *testing.T) {
	root := writeNamespaceFixture(t, "net:[4026533000]", "net:[4026533001]", true, true)
	for name, provenance := range map[string]NetworkNamespaceProvenance{
		"missing target":    {InitialNamespaceID: "net:[4026533001]"},
		"missing initial":   {NamespaceID: "net:[4026533000]"},
		"malformed target":  {NamespaceID: "4026533000", InitialNamespaceID: "net:[4026533001]"},
		"malformed initial": {NamespaceID: "net:[4026533000]", InitialNamespaceID: "net:bad"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := certifyWildcardListenerAt(42, provenance, root, filepath.Join(root, "self")); err == nil {
				t.Fatal("invalid recorded namespace was accepted")
			}
		})
	}
}

func TestWildcardListenerRejectsExternalInterfaceOrRoute(t *testing.T) {
	for name, mutate := range map[string]func(string){
		"external interface": func(root string) {
			writeProcNetDev(t, filepath.Join(root, "42", "net", "dev"), "lo: 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0\neth0: 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0\n")
		},
		"external route": func(root string) {
			writeProcRoute(t, filepath.Join(root, "42", "net", "route"), "Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT\neth0 00000000 0100000A 0001 0 0 0 00000000 0 0 0\n")
		},
	} {
		t.Run(name, func(t *testing.T) {
			root := writeNamespaceFixture(t, "net:[4026533000]", "net:[4026533001]", true, true)
			mutate(root)
			err := certifyWildcardListenerAt(42, NetworkNamespaceProvenance{
				NamespaceID:        "net:[4026533000]",
				InitialNamespaceID: "net:[4026533001]",
			}, root, filepath.Join(root, "self"))
			if err == nil {
				t.Fatal("externally reachable namespace evidence was accepted")
			}
		})
	}
}

func TestWildcardListenerRejectsMalformedProcEvidence(t *testing.T) {
	root := writeNamespaceFixture(t, "net:[4026533000]", "net:[4026533001]", true, true)
	writeProcNetDev(t, filepath.Join(root, "42", "net", "dev"), "lo malformed\n")
	if err := certifyWildcardListenerAt(42, NetworkNamespaceProvenance{
		NamespaceID:        "net:[4026533000]",
		InitialNamespaceID: "net:[4026533001]",
	}, root, filepath.Join(root, "self")); err == nil {
		t.Fatal("malformed proc evidence was accepted")
	}
	root = writeNamespaceFixture(t, "net:[4026533000]", "net:[4026533001]", true, true)
	if err := os.WriteFile(filepath.Join(root, "42", "net", "tcp"), []byte("header\nmalformed\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := pidOwnsConfiguredEndpointAt(42, []string{"http://127.0.0.1:29101/health"}, NetworkNamespaceProvenance{
		NamespaceID:        "net:[4026533000]",
		InitialNamespaceID: "net:[4026533001]",
	}, root, filepath.Join(root, "self")); err == nil {
		t.Fatal("malformed listening proc evidence was accepted")
	}
}

func TestProcEvidenceHeaderLikeRecordsAreNotHeaders(t *testing.T) {
	dev := "Inter-| Receive | Transmit\n face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed\n" +
		"lo: 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0\n" +
		"Inter-evil: 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0\n"
	if err := parseLoopbackOnlyDev(dev); err == nil {
		t.Fatal("interface record with Inter- prefix was accepted as a proc header")
	}

	route := "Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\tMTU\tWindow\tIRTT\n" +
		"lo 00000000 00000000 0001 0 0 0 000000FF 0 0 0\n" +
		"IfaceNet 00000000 00000000 0001 0 0 0 000000FF 0 0 0\n"
	if err := parseLoopbackOnlyRoute(route); err == nil {
		t.Fatal("route record with Iface prefix was accepted as a proc header")
	}
}

func TestParseProcRouteAcceptsKernelColumnWidths(t *testing.T) {
	route := "Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\tMTU\tWindow\tIRTT\n" +
		"lo 00000000 00000000 0001 0 0 0 000000FF 0 0 0\n"
	if err := parseLoopbackOnlyRoute(route); err != nil {
		t.Fatalf("valid Linux /proc/net/route row was rejected: %v", err)
	}
}

func TestWildcardListenerRejectsNonLoopbackConfiguredEndpoint(t *testing.T) {
	root := writeNamespaceFixture(t, "net:[4026533000]", "net:[4026533001]", true, true)
	err := pidOwnsConfiguredEndpointAt(42, []string{"http://192.0.2.1:29101/health"}, NetworkNamespaceProvenance{
		NamespaceID:        "net:[4026533000]",
		InitialNamespaceID: "net:[4026533001]",
	}, root, filepath.Join(root, "self"))
	if err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("non-loopback endpoint was accepted or misclassified: %v", err)
	}
}

func writeNamespaceFixture(t *testing.T, targetNS, selfNS string, loopback, listener bool) string {
	t.Helper()
	root := t.TempDir()
	target := filepath.Join(root, "42")
	self := filepath.Join(root, "self")
	for _, dir := range []string{filepath.Join(target, "ns"), filepath.Join(target, "net"), filepath.Join(self, "ns")} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(targetNS, filepath.Join(target, "ns", "net")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(targetNS, filepath.Join(self, "ns", "net")); err != nil {
		t.Fatal(err)
	}
	dev := "Inter-| Receive                                                |  Transmit\n face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed\n"
	if loopback {
		dev += "lo: 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0\n"
	}
	writeProcNetDev(t, filepath.Join(target, "net", "dev"), dev)
	writeProcRoute(t, filepath.Join(target, "net", "route"), "Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT\nlo 00000000 00000000 0001 0 0 0 000000FF 0 0 0\n")
	if err := os.WriteFile(filepath.Join(target, "net", "ipv6_route"), []byte("00000000000000000000000000000000 00 00000000000000000000000000000000 00 00000000000000000000000000000000 00000000 00000000 00000000 00000001 lo\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if listener {
		if err := os.WriteFile(filepath.Join(target, "net", "tcp"), []byte("  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n   0: 00000000:71AD 00000000:0000 0A 00000000:00000000 00:00000000 00000000   100        0 12345 1 0000000000000000 100 0 0 10 0\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(target, "net", "tcp6"), []byte("  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(target, "fd"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("socket:[12345]", filepath.Join(target, "fd", "1")); err != nil {
		t.Fatal(err)
	}
	return root
}

func writeProcNetDev(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value), 0600); err != nil {
		t.Fatal(err)
	}
}

func writeProcRoute(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value), 0600); err != nil {
		t.Fatal(err)
	}
}
