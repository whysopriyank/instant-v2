//go:build linux

package corpus

import (
	"golang.org/x/sys/unix"
)

func renameNoReplace(dirFD int, from, to string) error {
	return unix.Renameat2(dirFD, from, dirFD, to, unix.RENAME_NOREPLACE)
}
