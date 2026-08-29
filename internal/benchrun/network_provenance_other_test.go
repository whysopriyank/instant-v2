//go:build !linux

package benchrun

import (
	"strings"
	"testing"
)

func TestWildcardListenerNamespaceProvenanceFailsClosedOnNonLinux(t *testing.T) {
	err := certifyWildcardListener(1, NetworkNamespaceProvenance{
		NamespaceID:        "net:[4026533000]",
		InitialNamespaceID: "net:[4026533001]",
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("non-Linux wildcard listener was not rejected explicitly: %v", err)
	}
}
