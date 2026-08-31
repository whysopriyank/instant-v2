package main

import (
	"strings"
	"testing"
)

func TestNamespaceEvidenceRejectsExternalInterfacesAndRoutes(t *testing.T) {
	dev := "Inter-| Receive                        | Transmit\n face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed\n    lo: 1 2 0 0 0 0 0 0 3 4 0 0 0 0 0 0 0 0\n"
	if err := validateProcDev(dev); err != nil {
		t.Fatal(err)
	}
	if err := validateProcDev(strings.Replace(dev, "    lo:", "  eth0:", 1)); err == nil {
		t.Fatal("external interface evidence was accepted")
	}
	route := "Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT\nlo 00000000 00000000 0001 0 0 0 000000FF 0 0 0\n"
	if err := validateProcRoute(route); err != nil {
		t.Fatal(err)
	}
	localV4 := strings.Replace(route, "lo 00000000", "lo 0000007F", 1)
	if err := validateProcRoute(localV4); err != nil {
		t.Fatalf("canonical IPv4 loopback route was rejected: %v", err)
	}
	if err := validateProcRoute(strings.Replace(route, "lo 00000000", "eth0 00000000", 1)); err == nil {
		t.Fatal("external IPv4 route evidence was accepted")
	}
	ipv6 := "00000000000000000000000000000000 00 00000000000000000000000000000000 00 00000000000000000000000000000000 ffffffff 00000001 00000000 00200200 lo\n"
	if err := validateProcIPv6Route(ipv6); err != nil {
		t.Fatal(err)
	}
	if err := validateProcIPv6Route(ipv6 + ipv6); err != nil {
		t.Fatalf("duplicated kernel loopback IPv6 route rows were rejected: %v", err)
	}
	localV6 := "00000000000000000000000000000001 80 00000000000000000000000000000000 00 00000000000000000000000000000000 00000000 00000000 00000000 80200001 lo\n"
	if err := validateProcIPv6Route(localV6); err != nil {
		t.Fatalf("canonical IPv6 loopback route was rejected: %v", err)
	}
	if err := validateProcIPv6Route(strings.Replace(ipv6, " lo", " eth0", 1)); err == nil {
		t.Fatal("external IPv6 route evidence was accepted")
	}
}

func TestNamespaceEvidenceRejectsExternalLoopbackRoutes(t *testing.T) {
	v4 := "Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT\nlo 0100007F 00000000 0001 0 0 0 000000FF 0 0 0\n"
	if err := validateProcRoute(v4); err == nil {
		t.Fatal("external IPv4 destination on lo was accepted")
	}
	v4 = "Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT\nlo 00000000 00000000 0001 0 0 0 FFFFFFFF 0 0 0\n"
	if err := validateProcRoute(v4); err == nil {
		t.Fatal("arbitrary IPv4 loopback mask was accepted")
	}
	v6 := "00000000000000000000000000000002 80 00000000000000000000000000000000 00 00000000000000000000000000000000 00000000 00000000 00000000 80200001 lo\n"
	if err := validateProcIPv6Route(v6); err == nil {
		t.Fatal("external IPv6 destination on lo was accepted")
	}
	v6 = "00000000000000000000000000000000 00 00000000000000000000000000000000 00 00000000000000000000000000000000 00000000 00000000 00000000 00200201 lo\n"
	if err := validateProcIPv6Route(v6); err == nil {
		t.Fatal("non-canonical IPv6 route flags were accepted")
	}
	v6 = "00000000000000000000000000000000 00 00000000000000000000000000000000 00 00000000000000000000000000000001 00000000 00000000 00000000 00200200 lo\n"
	if err := validateProcIPv6Route(v6); err == nil {
		t.Fatal("non-zero IPv6 route gateway was accepted")
	}
	v6 = "00000000000000000000000000000000 00 00000000000000000000000000000000 00 00000000000000000000000000000000 ffffff 00000000 00000000 00200200 lo\n"
	if err := validateProcIPv6Route(v6); err == nil {
		t.Fatal("short IPv6 route counter was accepted")
	}
	v6 = "00000000000000000000000000000001 80 00000000000000000000000000000000 00 00000000000000000000000000000000 00000000 00000000 00000000 00200200 lo\n"
	if err := validateProcIPv6Route(v6); err == nil {
		t.Fatal("default-route flags were accepted for the IPv6 local route")
	}
}
