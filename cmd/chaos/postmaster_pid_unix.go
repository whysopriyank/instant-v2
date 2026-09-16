//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

func readPostmasterIdentityDescriptor(target string, expected os.FileInfo) (postmasterProcessIdentity, error) {
	parentFD, err := unix.Open(filepath.Dir(target), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return postmasterProcessIdentity{}, fmt.Errorf("open pg-data parent: %w", err)
	}
	defer func() { _ = unix.Close(parentFD) }()

	targetName := filepath.Base(target)
	dirFD, err := unix.Openat(parentFD, targetName, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return postmasterProcessIdentity{}, fmt.Errorf("open pg-data directory: %w", err)
	}
	defer func() { _ = unix.Close(dirFD) }()

	var st unix.Stat_t
	if err := unix.Fstat(dirFD, &st); err != nil {
		return postmasterProcessIdentity{}, fmt.Errorf("stat pg-data directory: %w", err)
	}
	if expected == nil || !sameUnixStat(expected, &st) {
		return postmasterProcessIdentity{}, errors.New("--pg-data changed during validation")
	}

	pidFD, err := unix.Openat(dirFD, "postmaster.pid", unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, unix.ENOENT) {
			return postmasterProcessIdentity{}, nil
		}
		return postmasterProcessIdentity{}, fmt.Errorf("open postmaster pid: %w", err)
	}
	defer func() { _ = unix.Close(pidFD) }()

	var pidSt unix.Stat_t
	if err := unix.Fstat(pidFD, &pidSt); err != nil {
		return postmasterProcessIdentity{}, fmt.Errorf("stat postmaster pid: %w", err)
	}
	if pidSt.Mode&unix.S_IFMT != unix.S_IFREG {
		return postmasterProcessIdentity{}, errors.New("refusing non-regular postmaster pid file")
	}
	if pidSt.Uid != uint32(os.Getuid()) {
		return postmasterProcessIdentity{}, fmt.Errorf("refusing postmaster pid owned by uid %d (current %d)", pidSt.Uid, os.Getuid())
	}

	buf := make([]byte, 4096)
	n, err := unix.Read(pidFD, buf)
	if err != nil {
		return postmasterProcessIdentity{}, fmt.Errorf("read postmaster pid: %w", err)
	}
	content := string(buf[:n])
	lines := strings.Split(content, "\n")
	if len(lines) < 3 {
		return postmasterProcessIdentity{}, fmt.Errorf("postmaster pid file has fewer than 3 lines")
	}

	pidStr := strings.TrimSpace(lines[0])
	pid, err := strconv.Atoi(pidStr)
	if err != nil || pid <= 0 {
		return postmasterProcessIdentity{}, fmt.Errorf("invalid postmaster pid %q", pidStr)
	}

	dataDir := strings.TrimSpace(lines[1])
	canonicalTarget, _ := filepath.EvalSymlinks(target)
	canonicalDataDir, _ := filepath.EvalSymlinks(dataDir)
	if canonicalTarget != "" && canonicalDataDir != "" {
		if filepath.Clean(canonicalTarget) != filepath.Clean(canonicalDataDir) {
			return postmasterProcessIdentity{}, fmt.Errorf("postmaster.pid data directory mismatch (expected %s, got %s)", canonicalTarget, canonicalDataDir)
		}
	} else if filepath.Clean(target) != filepath.Clean(dataDir) {
		return postmasterProcessIdentity{}, fmt.Errorf("postmaster.pid data directory mismatch (expected %s, got %s)", target, dataDir)
	}

	startTimeStr := strings.TrimSpace(lines[2])
	startTime, err := strconv.ParseInt(startTimeStr, 10, 64)
	if err != nil || startTime <= 0 {
		return postmasterProcessIdentity{}, fmt.Errorf("invalid postmaster start time %q", startTimeStr)
	}
	kernelStart, err := processStartToken(pid)
	if err != nil {
		return postmasterProcessIdentity{}, err
	}

	port := 0
	if len(lines) >= 4 {
		port, _ = strconv.Atoi(strings.TrimSpace(lines[3]))
	}

	return postmasterProcessIdentity{
		PID:       pid,
		StartTime: kernelStart,
		DataDir:   dataDir,
		Port:      port,
	}, nil
}
