//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd

package main

import (
	"errors"
	"os"
)

func readPostmasterIdentityDescriptor(target string, expected os.FileInfo) (postmasterProcessIdentity, error) {
	return postmasterProcessIdentity{}, errors.New("descriptor-relative postmaster validation is unsupported on this platform")
}
