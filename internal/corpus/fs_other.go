//go:build !darwin && !linux

package corpus

import (
	"errors"
)

func reserveOutputDirPlatform(dir string) (*ReservedDir, error) {
	return nil, errors.New("descriptor-relative directory reservation is unsupported on this platform")
}

func reserveOutputDirWithHooks(dir string, hooks reserveHooks) (*ReservedDir, error) {
	return nil, errors.New("descriptor-relative directory reservation is unsupported on this platform")
}

func checkFreshOutputDirPlatform(dir string) error {
	return errors.New("descriptor-relative directory checking is unsupported on this platform")
}

func writeEvidenceInDir(d *ReservedDir, target string, value any, hooks writeHooks) error {
	return errors.New("descriptor-relative evidence writing is unsupported on this platform")
}

func writeEvidenceWithHooks(path string, value any, hooks writeHooks) error {
	return errors.New("descriptor-relative evidence writing is unsupported on this platform")
}
