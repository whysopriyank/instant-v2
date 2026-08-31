//go:build linux

package main

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// openOutputLock opens or creates the lock through a held parent directory
// descriptor. The final component is never followed, including when an
// attacker replaces an absent lock with a symlink between the existence check
// and creation.
func openOutputLock(path string) (*os.File, error) {
	if err := noSymlinkComponents(path); err != nil {
		return nil, err
	}
	clean := filepath.Clean(path)
	directory, err := openStableDir(filepath.Dir(clean))
	if err != nil {
		return nil, fmt.Errorf("open output lock directory: %w", err)
	}
	defer directory.Close()
	name := filepath.Base(clean)
	for attempt := 0; attempt < 8; attempt++ {
		var stat unix.Stat_t
		err := unix.Fstatat(int(directory.Fd()), name, &stat, unix.AT_SYMLINK_NOFOLLOW)
		if err == nil {
			if stat.Mode&unix.S_IFMT != unix.S_IFREG || !rootOrTaskOwned(stat.Uid) {
				return nil, errors.New("output lock is not a root/task-owned regular file")
			}
		} else if !errors.Is(err, unix.ENOENT) {
			return nil, fmt.Errorf("inspect output lock: %w", err)
		}
		flags := unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW
		if errors.Is(err, unix.ENOENT) {
			flags |= unix.O_CREAT | unix.O_EXCL
		}
		fd, openErr := unix.Openat(int(directory.Fd()), name, flags, 0o600)
		if errors.Is(openErr, unix.EEXIST) {
			continue
		}
		if openErr != nil {
			return nil, fmt.Errorf("open output lock: %w", openErr)
		}
		var opened unix.Stat_t
		if statErr := unix.Fstat(fd, &opened); statErr != nil {
			_ = unix.Close(fd)
			return nil, statErr
		}
		if opened.Mode&unix.S_IFMT != unix.S_IFREG || !rootOrTaskOwned(opened.Uid) {
			_ = unix.Close(fd)
			return nil, errors.New("output lock is not a root/task-owned regular file")
		}
		if flockErr := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); flockErr != nil {
			_ = unix.Close(fd)
			return nil, fmt.Errorf("benchmark output is already owned by another controller: %w", flockErr)
		}
		file := os.NewFile(uintptr(fd), clean)
		if file == nil {
			_ = unix.Close(fd)
			return nil, errors.New("open output lock: invalid descriptor")
		}
		return file, nil
	}
	return nil, errors.New("output lock changed during secure acquisition")
}

func runLockedCommand(path string, command []string) error {
	lock, err := openOutputLock(path)
	if err != nil {
		return err
	}
	fd := int(lock.Fd())
	if len(command) > 0 {
		if fd != 3 {
			if err := unix.Dup3(fd, 3, 0); err != nil {
				_ = lock.Close()
				return fmt.Errorf("publish output lock descriptor: %w", err)
			}
			_ = lock.Close()
		}
		if _, err := unix.FcntlInt(3, unix.F_SETFD, 0); err != nil {
			return fmt.Errorf("retain output lock descriptor: %w", err)
		}
		env := os.Environ()
		seenFD := false
		for index, value := range env {
			if strings.HasPrefix(value, "WAVE6_LOCK_FD=") {
				env[index] = "WAVE6_LOCK_FD=3"
				seenFD = true
			}
		}
		if !seenFD {
			env = append(env, "WAVE6_LOCK_FD=3")
		}
		return unix.Exec(command[0], command, env)
	}
	defer lock.Close()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	defer signal.Stop(stop)
	<-stop
	return nil
}
