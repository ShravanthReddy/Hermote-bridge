// Package update holds Hermote Bridge's release checks and what the bridge
// knows about its own installation (docs/specs/bridge-status-updates.md).
package update

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
)

// Identity is the executable this daemon process started from, captured
// before it serves anything (D7 step 1). The path can be replaced while the
// process runs, and os.Executable then names the replacement, so later
// checks compare against this digest and file identity, never a fresh
// lookup. The descriptor stays open for the life of the process.
type Identity struct {
	// Incarnation is 16 random bytes in hex, new for every process.
	Incarnation string
	// Path is the executable path as the process was started.
	Path   string
	Device uint64
	Inode  uint64
	// SHA256 is the hex digest of the file the process started from.
	SHA256 string
	file   *os.File
}

// CaptureIdentity records the running executable's identity.
func CaptureIdentity() (*Identity, error) {
	path, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("locate running executable: %w", err)
	}
	return captureIdentity(path)
}

func captureIdentity(path string) (*Identity, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open running executable: %w", err)
	}
	identity, err := identityOf(file, path)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return identity, nil
}

func identityOf(file *os.File, path string) (*Identity, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, errors.New("running executable has no device and inode")
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return nil, fmt.Errorf("hash running executable: %w", err)
	}
	incarnation := make([]byte, 16)
	if _, err := rand.Read(incarnation); err != nil {
		return nil, err
	}
	return &Identity{
		Incarnation: hex.EncodeToString(incarnation),
		Path:        path,
		Device:      uint64(stat.Dev),
		Inode:       stat.Ino,
		SHA256:      hex.EncodeToString(hash.Sum(nil)),
		file:        file,
	}, nil
}

// SameFile reports whether path, following symlinks, names the file this
// process started from.
func (i *Identity) SameFile(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && uint64(stat.Dev) == i.Device && stat.Ino == i.Inode
}

// Close releases the start-up descriptor; the daemon calls it at exit.
func (i *Identity) Close() error {
	if i.file == nil {
		return nil
	}
	return i.file.Close()
}
