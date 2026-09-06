//go:build !darwin && !linux

package state

import (
	"errors"
	"fmt"
	"os"
	"time"
)

func acquireFileLock(file *os.File, _ time.Time) error {
	return fmt.Errorf("state: file locking is unsupported on this platform: %s", file.Name())
}

func releaseFileLock(file *os.File) error {
	return fmt.Errorf("state: file locking is unsupported on this platform: %s", file.Name())
}

func isUnsupportedDirectorySync(err error) bool {
	return errors.Is(err, os.ErrInvalid)
}
