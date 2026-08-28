//go:build linux

package benchrun

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// openProvisionExecutable opens a non-symlink, non-writable executable with
// O_NOFOLLOW, hashes that descriptor, and returns it for ExtraFiles-based
// execution. Keeping the descriptor alive closes the hash/execute TOCTOU
// window: the child runs the exact object that was measured.
func openProvisionExecutable(path string) (*os.File, string, error) {
	if path == "" {
		return nil, "", errors.New("provision executable path is empty")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, "", err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, "", errors.New("provision executable must be a regular non-symlink file")
	}
	if info.Mode().Perm()&0222 != 0 {
		return nil, "", errors.New("provision executable must not be writable")
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, "", err
	}
	f := os.NewFile(uintptr(fd), path)
	if f == nil {
		_ = syscall.Close(fd)
		return nil, "", errors.New("could not create executable descriptor")
	}
	opened, err := f.Stat()
	if err != nil || !opened.Mode().IsRegular() || opened.Size() > DefaultMaxArtifactBytes || opened.Mode().Perm()&0222 != 0 {
		_ = f.Close()
		if err == nil {
			err = errors.New("provision executable is not a bounded regular file")
		}
		return nil, "", err
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, "", err
	}
	data, err := io.ReadAll(io.LimitReader(f, DefaultMaxArtifactBytes+1))
	if err != nil {
		_ = f.Close()
		return nil, "", err
	}
	if int64(len(data)) > DefaultMaxArtifactBytes {
		_ = f.Close()
		return nil, "", errors.New("provision executable exceeds artifact budget")
	}
	// Copy the bytes into a sealed anonymous executable object. The source
	// path may be replaced or chmod/write-mutated after this point; execution
	// is bound to the immutable memfd, not the mutable source inode.
	memfd, err := unix.MemfdCreate("instant-bench-provision", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		_ = f.Close()
		return nil, "", err
	}
	sealed := os.NewFile(uintptr(memfd), "instant-bench-provision")
	if sealed == nil {
		_ = unix.Close(memfd)
		_ = f.Close()
		return nil, "", errors.New("could not create sealed executable descriptor")
	}
	if _, err = sealed.Write(data); err != nil {
		_ = sealed.Close()
		_ = f.Close()
		return nil, "", err
	}
	if _, err = sealed.Seek(0, io.SeekStart); err != nil {
		_ = sealed.Close()
		_ = f.Close()
		return nil, "", err
	}
	if err = unix.Fchmod(memfd, 0500); err != nil {
		_ = sealed.Close()
		_ = f.Close()
		return nil, "", err
	}
	seals := unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	if _, err = unix.FcntlInt(uintptr(memfd), unix.F_ADD_SEALS, seals); err != nil {
		_ = sealed.Close()
		_ = f.Close()
		return nil, "", err
	}
	_ = f.Close()
	sum := sha256.Sum256(data)
	return sealed, hex.EncodeToString(sum[:]), nil
}
