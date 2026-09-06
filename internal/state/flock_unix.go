//go:build darwin || linux

package state

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"
)

func acquireFileLock(file *os.File, deadline time.Time) error {
	for {
		// Always attempt once before consulting the deadline. A gate that used the
		// full budget must still succeed when the cross-process lock is free.
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			return fmt.Errorf("state: lock %s: %w", file.Name(), err)
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("%w: %s", ErrLockTimeout, file.Name())
		}
		if remaining > 10*time.Millisecond {
			remaining = 10 * time.Millisecond
		}
		time.Sleep(remaining)
	}
}

func releaseFileLock(file *os.File) error {
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_UN); err != nil {
		return fmt.Errorf("state: unlock %s: %w", file.Name(), err)
	}
	return nil
}

func isUnsupportedDirectorySync(err error) bool {
	return errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP)
}
