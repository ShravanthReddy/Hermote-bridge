// Package state persists hermes-remote's small on-disk state under
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
	"sync"
	"time"

	"github.com/shravanthreddy/hermes-ios/remote/internal/protocol"
)

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

// Store is the on-disk state with in-memory caching. Safe for concurrent use.
type Store struct {
	dir string
	mu  sync.Mutex
}

// Open creates the directory if needed and returns a Store.
func Open() (*Store, error) {
	dir, err := Dir()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &Store{dir: dir}, nil
}

// Path returns the absolute path of a file inside the state directory.
func (s *Store) Path(name string) string { return filepath.Join(s.dir, name) }

// Identity loads the bridge identity, creating one on first use.
func (s *Store) Identity() (*protocol.Identity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.Path("identity.key")
	raw, err := os.ReadFile(p)
	switch {
	case err == nil:
		seed, err := hex.DecodeString(strings.TrimSpace(string(raw)))
		if err != nil {
			return nil, fmt.Errorf("state: corrupt %s: %w", p, err)
		}
		return protocol.IdentityFromSeed(seed)
	case errors.Is(err, os.ErrNotExist):
		id, err := protocol.NewIdentity(nil)
		if err != nil {
			return nil, err
		}
		if err := writeFile(p, []byte(hex.EncodeToString(id.Seed())+"\n"), 0o600); err != nil {
			return nil, err
		}
		return id, nil
	default:
		return nil, err
	}
}

// Config loads config.json (zero value when absent).
func (s *Store) Config() (Config, error) {
	var c Config
	err := s.readJSON("config.json", &c)
	if errors.Is(err, os.ErrNotExist) {
		return Config{}, nil
	}
	return c, err
}

// SaveConfig writes config.json.
func (s *Store) SaveConfig(c Config) error { return s.writeJSON("config.json", c) }

// Devices lists trusted phones.
func (s *Store) Devices() ([]Device, error) {
	var d []Device
	err := s.readJSON("devices.json", &d)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return d, err
}

// IsTrusted reports whether a phone public key is trusted and stamps last_seen.
func (s *Store) IsTrusted(phoneID []byte) bool {
	id := protocol.DeviceID(phoneID)
	devices, err := s.Devices()
	if err != nil {
		return false
	}
	for i := range devices {
		if devices[i].ID == id {
			devices[i].LastSeen = time.Now().UTC()
			_ = s.writeJSON("devices.json", devices)
			return true
		}
	}
	return false
}

// Trust records a newly paired phone.
func (s *Store) Trust(phoneID []byte, name string) error {
	devices, err := s.Devices()
	if err != nil {
		return err
	}
	id := protocol.DeviceID(phoneID)
	now := time.Now().UTC()
	for i := range devices {
		if devices[i].ID == id {
			devices[i].LastSeen = now
			if name != "" {
				devices[i].Name = name
			}
			return s.writeJSON("devices.json", devices)
		}
	}
	devices = append(devices, Device{ID: id, Name: name, FirstSeen: now, LastSeen: now})
	return s.writeJSON("devices.json", devices)
}

// Revoke removes a trusted phone by id (or unique id prefix).
func (s *Store) Revoke(idOrPrefix string) (Device, error) {
	devices, err := s.Devices()
	if err != nil {
		return Device{}, err
	}
	idx := -1
	for i, d := range devices {
		if d.ID == idOrPrefix || strings.HasPrefix(d.ID, idOrPrefix) {
			if idx >= 0 {
				return Device{}, fmt.Errorf("state: %q matches more than one device", idOrPrefix)
			}
			idx = i
		}
	}
	if idx < 0 {
		return Device{}, fmt.Errorf("state: no device matches %q", idOrPrefix)
	}
	removed := devices[idx]
	devices = append(devices[:idx], devices[idx+1:]...)
	return removed, s.writeJSON("devices.json", devices)
}

func (s *Store) readJSON(name string, v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := os.ReadFile(s.Path(name))
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}

func (s *Store) writeJSON(name string, v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return writeFile(s.Path(name), append(raw, '\n'), 0o600)
}

// writeFile writes atomically (temp + rename) with the given mode.
func writeFile(path string, data []byte, mode os.FileMode) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, mode); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
