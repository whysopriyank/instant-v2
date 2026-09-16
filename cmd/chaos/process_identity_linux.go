//go:build linux

package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// processStartToken returns Linux's /proc starttime-in-clock-ticks token.
func processStartToken(pid int) (int64, error) {
	if pid <= 0 {
		return 0, fmt.Errorf("invalid process pid %d", pid)
	}
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, fmt.Errorf("read kernel process identity for %d: %w", pid, err)
	}
	close := strings.LastIndexByte(string(b), ')')
	if close < 0 || close+2 >= len(b) {
		return 0, fmt.Errorf("malformed /proc/%d/stat", pid)
	}
	fields := strings.Fields(string(b)[close+2:])
	// The suffix starts with field 3 (state), so field 22 is index 19.
	if len(fields) <= 19 {
		return 0, fmt.Errorf("short /proc/%d/stat", pid)
	}
	token, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil || token <= 0 {
		return 0, fmt.Errorf("invalid /proc/%d start token %q", pid, fields[19])
	}
	return token, nil
}
