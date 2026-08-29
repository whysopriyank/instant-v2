//go:build !linux

package benchrun

import "errors"

// NetworkNamespaceProvenance is retained on non-Linux builds so the
// collector's contract remains explicit, but namespace certification is not
// available without Linux procfs evidence.
type NetworkNamespaceProvenance struct {
	NamespaceID        string `json:"network_namespace_id,omitempty"`
	InitialNamespaceID string `json:"initial_network_namespace_id,omitempty"`
}

func certifyWildcardListener(int, NetworkNamespaceProvenance) error {
	return errors.New("wildcard listener namespace provenance is unsupported on this platform")
}

func certifyWildcardListenerAt(int, NetworkNamespaceProvenance, string, string) error {
	return errors.New("wildcard listener namespace provenance is unsupported on this platform")
}
