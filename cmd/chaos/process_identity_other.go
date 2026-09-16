//go:build !darwin && !linux

package main

import "errors"

func processStartToken(int) (int64, error) {
	return 0, errors.New("kernel process start identity is unsupported on this platform")
}
