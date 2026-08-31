//go:build !linux

package main

import (
	"errors"
	"os"
)

func openOutputLock(string) (*os.File, error) {
	return nil, errors.New("output lock acquisition requires Linux dirfd/O_NOFOLLOW support")
}

func runLockedCommand(string, []string) error {
	return errors.New("output lock acquisition requires Linux dirfd/O_NOFOLLOW support")
}
