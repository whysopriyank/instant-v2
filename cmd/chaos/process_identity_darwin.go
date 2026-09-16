//go:build darwin

package main

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// processStartToken returns the kernel start-instance token for pid. It is
// read after exec, so PID reuse cannot satisfy a later identity check.
func processStartToken(pid int) (int64, error) {
	if pid <= 0 {
		return 0, fmt.Errorf("invalid process pid %d", pid)
	}
	proc, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return 0, fmt.Errorf("read kernel process identity for %d: %w", pid, err)
	}
	if int(proc.Proc.P_pid) != pid {
		return 0, fmt.Errorf("kernel process identity pid mismatch: expected %d, got %d", pid, proc.Proc.P_pid)
	}
	return proc.Proc.P_starttime.Sec*1_000_000 + int64(proc.Proc.P_starttime.Usec), nil
}
