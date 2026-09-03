// Package push notifies paired phones through APNs while they are not
// connected to the bridge: a watcher polls the gateway for the moments that
// need a person (approvals, questions, finished turns) and a token-auth APNs
// client delivers them. Nothing here runs unless `hermes-remote push setup`
// has stored an APNs key.
package push

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrNotConfigured means no APNs key has been set up on this Mac.
var ErrNotConfigured = errors.New("push not configured — run `hermes-remote push setup`")

// Config is push.json in the state directory.
type Config struct {
	// Apple Developer team (10 characters).
	TeamID string `json:"team_id"`
	// Key ID of the APNs authentication key (10 characters).
	KeyID string `json:"key_id"`
	// Path of the .p8 key; `push setup` copies it into the state directory.
	KeyPath string `json:"key_path"`
	// The app's bundle identifier (the apns-topic).
	BundleID string `json:"bundle_id"`
}

const configFile = "push.json"

// LoadConfig reads push.json from dir; ErrNotConfigured when absent.
func LoadConfig(dir string) (Config, error) {
	raw, err := os.ReadFile(filepath.Join(dir, configFile))
	if errors.Is(err, os.ErrNotExist) {
		return Config{}, ErrNotConfigured
	}
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return Config{}, fmt.Errorf("push.json: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Save writes push.json (owner-only).
func (c Config) Save(dir string) error {
	if err := c.Validate(); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, configFile), raw, 0o600)
}

// Validate checks the identifiers Apple issues have their expected shape.
func (c Config) Validate() error {
	if len(c.TeamID) != 10 {
		return errors.New("team id must be 10 characters")
	}
	if len(c.KeyID) != 10 {
		return errors.New("key id must be 10 characters")
	}
	if c.KeyPath == "" {
		return errors.New("key path is required")
	}
	if c.BundleID == "" {
		return errors.New("bundle id is required")
	}
	return nil
}
