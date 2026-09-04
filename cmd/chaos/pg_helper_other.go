//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd

package main

func pgHelperAvailable() bool { return false }

func maybeRunPGHelper() (bool, error) { return false, nil }
