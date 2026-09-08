// Package state persists hermote-bridge's small on-disk state under
// $HERMES_HOME/remote (default ~/.hermes/remote): the bridge identity, the
// trusted phones and the chosen configuration. Everything is 0600/0700.
package state

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ShravanthReddy/Hermote-bridge/internal/protocol"
)

const defaultLockTimeout = 5 * time.Second

// ErrLockTimeout means another process held the state transaction lock past
// the bounded acquisition deadline. Callers must fail rather than write a
// snapshot that may already be stale.
var ErrLockTimeout = errors.New("state: timed out acquiring state lock")

// ErrMutationCommitted means a state mutation crossed its atomic rename commit
// point, but a later durability or unlock step failed. The new pathname
// contents are visible even though the returned diagnostic must be reported.
var ErrMutationCommitted = errors.New("state: mutation committed with post-commit diagnostic")

// Dir resolves the state directory.
func Dir() (string, error) {
	home := os.Getenv("HERMES_HOME")
	if home == "" {
		h, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		home = filepath.Join(h, ".hermes")
	}
	return filepath.Join(home, "remote"), nil
}

// HermesHome returns the Hermes home directory ($HERMES_HOME or ~/.hermes).
func HermesHome() (string, error) {
	d, err := Dir()
	if err != nil {
		return "", err
	}
	return filepath.Dir(d), nil
}

// Config is the operator's configuration (config.json).
type Config struct {
	Transport  protocol.Transport `json:"transport"`
	RelayURL   string             `json:"relay_url,omitempty"`
	HTTPSPort  int                `json:"https_port"`  // tailscale serve port (direct)
	BridgePort int                `json:"bridge_port"` // loopback listener the bridge binds
	Name       string             `json:"name"`        // shown on the phone after pairing
	Python     string             `json:"python"`      // Hermes venv interpreter
}

// Device is a trusted phone.
type Device struct {
	ID        string    `json:"id"` // base64url(Ed25519 public key)
	Name      string    `json:"name,omitempty"`
	FirstSeen time.Time `json:"first_seen"`
	LastSeen  time.Time `json:"last_seen"`
}

type writeFileFunc func(path string, data []byte, mode os.FileMode) (committed bool, err error)
type acquireLockFunc func(file *os.File, deadline time.Time) error
type releaseLockFunc func(file *os.File) error
type syncDirectoryFunc func(dir string) error

type storeOptions struct {
	lockTimeout   time.Duration
	writeFile     writeFileFunc
	acquireLock   acquireLockFunc
	releaseLock   releaseLockFunc
	syncDirectory syncDirectoryFunc
	beforeGate    func(deadline time.Time)
}

// Store is the on-disk state. Each complete operation is serialized within
// this Store and against other Store instances/processes sharing the directory.
// Lock order is the per-Store gate followed by the private file lock.
type Store struct {
	dir         string
	gate        chan struct{}
	lockFile    *os.File
	lockTimeout time.Duration
	writeFile   writeFileFunc
	acquireLock acquireLockFunc
	releaseLock releaseLockFunc
	beforeGate  func(deadline time.Time)
	closed      bool // guarded by gate
}

// Open creates the default state directory if needed and returns a Store.
func Open() (*Store, error) {
	dir, err := Dir()
	if err != nil {
		return nil, err
	}
	return OpenAt(dir)
}

// OpenAt creates an isolated Store rooted at dir. It is primarily useful to
// tests and tools that must not depend on process-global environment state.
func OpenAt(dir string) (*Store, error) {
	return openAt(dir, storeOptions{})
}

func openAt(dir string, opts storeOptions) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	lockPath := filepath.Join(dir, ".state.lock")
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := lockFile.Chmod(0o600); err != nil {
		_ = lockFile.Close()
		return nil, err
	}
	if opts.lockTimeout <= 0 {
		opts.lockTimeout = defaultLockTimeout
	}
	if opts.acquireLock == nil {
		opts.acquireLock = acquireFileLock
	}
	if opts.releaseLock == nil {
		opts.releaseLock = releaseFileLock
	}
	if opts.syncDirectory == nil {
		opts.syncDirectory = syncDirectory
	}
	if opts.writeFile == nil {
		opts.writeFile = func(path string, data []byte, mode os.FileMode) (bool, error) {
			return writeFile(path, data, mode, opts.syncDirectory)
		}
	}
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	return &Store{
		dir: dir, gate: gate, lockFile: lockFile, lockTimeout: opts.lockTimeout,
		writeFile: opts.writeFile, acquireLock: opts.acquireLock,
		releaseLock: opts.releaseLock, beforeGate: opts.beforeGate,
	}, nil
}

// Close releases this Store's lock-file descriptor. It is idempotent, waits
// for active Store work within the same bounded acquisition deadline, and
// causes later operations to fail with os.ErrClosed.
func (s *Store) Close() error {
	deadline := time.Now().Add(s.lockTimeout)
	if s.beforeGate != nil {
		s.beforeGate(deadline)
	}
	if err := acquireGate(s.gate, deadline); err != nil {
		return err
	}
	defer releaseGate(s.gate)
	if s.closed {
		return nil
	}
	s.closed = true
	return s.lockFile.Close()
}

// Path returns the absolute path of a file inside the state directory.
func (s *Store) Path(name string) string { return filepath.Join(s.dir, name) }

// Identity loads the bridge identity, creating one on first use.
func (s *Store) Identity() (id *protocol.Identity, err error) {
	err = s.locked(func() (bool, error) {
		p := s.Path("identity.key")
		raw, readErr := os.ReadFile(p)
		switch {
		case readErr == nil:
			seed, decodeErr := hex.DecodeString(strings.TrimSpace(string(raw)))
			if decodeErr != nil {
				return false, fmt.Errorf("state: corrupt %s: %w", p, decodeErr)
			}
			id, decodeErr = protocol.IdentityFromSeed(seed)
			return false, decodeErr
		case errors.Is(readErr, os.ErrNotExist):
			var createErr error
			id, createErr = protocol.NewIdentity(nil)
			if createErr != nil {
				return false, createErr
			}
			return s.writeFile(p, []byte(hex.EncodeToString(id.Seed())+"\n"), 0o600)
		default:
			return false, readErr
		}
	})
	return id, err
}

// Config loads config.json (zero value when absent).
func (s *Store) Config() (c Config, err error) {
	err = s.locked(func() (bool, error) {
		readErr := s.readJSONUnlocked("config.json", &c)
		if errors.Is(readErr, os.ErrNotExist) {
			c = Config{}
			return false, nil
		}
		return false, readErr
	})
	return c, err
}

// SaveConfig writes config.json atomically under the shared state lock.
func (s *Store) SaveConfig(c Config) error {
	return s.locked(func() (bool, error) { return s.writeJSONUnlocked("config.json", c) })
}

// Devices lists trusted phones from disk under the shared state lock.
func (s *Store) Devices() (devices []Device, err error) {
	err = s.locked(func() (bool, error) {
		readErr := s.readJSONUnlocked("devices.json", &devices)
		if errors.Is(readErr, os.ErrNotExist) {
			devices = nil
			return false, nil
		}
		return false, readErr
	})
	return devices, err
}

// IsTrusted reports whether a phone public key is trusted and persists its
// last_seen stamp. A read, lock or write failure is an admission failure.
func (s *Store) IsTrusted(phoneID []byte) bool {
	id := protocol.DeviceID(phoneID)
	trusted := false
	err := s.locked(func() (bool, error) {
		devices, err := s.devicesUnlocked()
		if err != nil {
			return false, err
		}
		for i := range devices {
			if devices[i].ID != id {
				continue
			}
			devices[i].LastSeen = time.Now().UTC()
			committed, writeErr := s.writeJSONUnlocked("devices.json", devices)
			trusted = committed || writeErr == nil
			return committed, writeErr
		}
		return false, nil
	})
	return err == nil && trusted
}

// AddTrusted records a phone whose one-time pairing proof was consumed. It is
// the only mutation that may insert an absent device.
func (s *Store) AddTrusted(phoneID []byte, name string) error {
	id := protocol.DeviceID(phoneID)
	return s.locked(func() (bool, error) {
		devices, err := s.devicesUnlocked()
		if err != nil {
			return false, err
		}
		now := time.Now().UTC()
		for i := range devices {
			if devices[i].ID == id {
				devices[i].LastSeen = now
				if name != "" {
					devices[i].Name = name
				}
				return s.writeJSONUnlocked("devices.json", devices)
			}
		}
		devices = append(devices, Device{ID: id, Name: name, FirstSeen: now, LastSeen: now})
		return s.writeJSONUnlocked("devices.json", devices)
	})
}

// Trust updates an existing trusted phone. It deliberately cannot insert an
// absent record, so a late name/last-seen update cannot undo revocation.
func (s *Store) Trust(phoneID []byte, name string) error {
	id := protocol.DeviceID(phoneID)
	return s.locked(func() (bool, error) {
		devices, err := s.devicesUnlocked()
		if err != nil {
			return false, err
		}
		for i := range devices {
			if devices[i].ID != id {
				continue
			}
			devices[i].LastSeen = time.Now().UTC()
			if name != "" {
				devices[i].Name = name
			}
			return s.writeJSONUnlocked("devices.json", devices)
		}
		return false, fmt.Errorf("state: device %q is not trusted", id)
	})
}

// Revoke removes a trusted phone by id (or unique id prefix).
func (s *Store) Revoke(idOrPrefix string) (removed Device, err error) {
	err = s.locked(func() (bool, error) {
		devices, readErr := s.devicesUnlocked()
		if readErr != nil {
			return false, readErr
		}
		idx := -1
		for i, d := range devices {
			if d.ID == idOrPrefix || strings.HasPrefix(d.ID, idOrPrefix) {
				if idx >= 0 {
					return false, fmt.Errorf("state: %q matches more than one device", idOrPrefix)
				}
				idx = i
			}
		}
		if idx < 0 {
			return false, fmt.Errorf("state: no device matches %q", idOrPrefix)
		}
		removed = devices[idx]
		devices = append(devices[:idx], devices[idx+1:]...)
		return s.writeJSONUnlocked("devices.json", devices)
	})
	if err != nil && !errors.Is(err, ErrMutationCommitted) {
		removed = Device{}
	}
	return removed, err
}

func (s *Store) locked(fn func() (committed bool, err error)) (err error) {
	deadline := time.Now().Add(s.lockTimeout)
	if s.beforeGate != nil {
		s.beforeGate(deadline)
	}
	if err := acquireGate(s.gate, deadline); err != nil {
		return err
	}
	defer releaseGate(s.gate)
	if s.closed {
		return os.ErrClosed
	}
	if err := s.acquireLock(s.lockFile, deadline); err != nil {
		return err
	}
	committed := false
	defer func() {
		combined := errors.Join(err, s.releaseLock(s.lockFile))
		if committed && combined != nil {
			err = fmt.Errorf("%w: %w", ErrMutationCommitted, combined)
			return
		}
		err = combined
	}()
	committed, err = fn()
	return err
}

func acquireGate(gate <-chan struct{}, deadline time.Time) error {
	select {
	case <-gate:
		return nil
	default:
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return ErrLockTimeout
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-gate:
		return nil
	case <-timer.C:
		// Give a gate released at the deadline boundary one final immediate try.
		select {
		case <-gate:
			return nil
		default:
			return ErrLockTimeout
		}
	}
}

func releaseGate(gate chan<- struct{}) {
	gate <- struct{}{}
}

func (s *Store) devicesUnlocked() ([]Device, error) {
	var devices []Device
	err := s.readJSONUnlocked("devices.json", &devices)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return devices, err
}

func (s *Store) readJSONUnlocked(name string, v any) error {
	p := s.Path(name)
	raw, err := os.ReadFile(p)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("state: corrupt %s: %w", p, err)
	}
	return nil
}

func (s *Store) writeJSONUnlocked(name string, v any) (bool, error) {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return false, err
	}
	return s.writeFile(s.Path(name), append(raw, '\n'), 0o600)
}

// writeFile writes atomically and durably with a unique temporary file.
func writeFile(path string, data []byte, mode os.FileMode, syncDir syncDirectoryFunc) (bool, error) {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return false, err
	}
	tmpPath := tmp.Name()
	closed := false
	defer func() {
		if !closed {
			_ = tmp.Close()
		}
		_ = os.Remove(tmpPath)
	}()
	if err := tmp.Chmod(mode); err != nil {
		return false, err
	}
	if _, err := tmp.Write(data); err != nil {
		return false, err
	}
	if err := tmp.Sync(); err != nil {
		return false, err
	}
	if err := tmp.Close(); err != nil {
		return false, err
	}
	closed = true
	if err := os.Rename(tmpPath, path); err != nil {
		return false, err
	}
	return true, syncDir(dir)
}

func syncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Sync(); err != nil && !isUnsupportedDirectorySync(err) {
		return err
	}
	return nil
}
