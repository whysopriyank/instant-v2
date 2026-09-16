//go:build darwin

package corpus

import (
	"golang.org/x/sys/unix"
)

func renameNoReplace(dirFD int, from, to string) error {
	return unix.RenameatxNp(dirFD, from, dirFD, to, unix.RENAME_EXCL)
}
