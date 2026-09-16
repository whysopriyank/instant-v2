//go:build !darwin && !linux

package corpus

import (
	"errors"
)

func renameNoReplace(dirFD int, from, to string) error {
	return errors.New("atomic rename with no-replace is not supported on this platform")
}
