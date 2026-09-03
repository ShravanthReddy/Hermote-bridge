package push

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/ShravanthReddy/hermes-remote/internal/protocol"
)

// Registration is one phone's push details, keyed by its device id.
type Registration struct {
	protocol.PushRegistration
	UpdatedAt time.Time `json:"updated_at"`
}

// Wants reports whether the phone asked for alerts of this kind.
func (r Registration) Wants(kind Kind) bool {
	for _, k := range r.Kinds {
		if k == string(kind) {
			return true
		}
	}
	return false
}

// Registry persists registrations in push-devices.json (owner-only).
type Registry struct {
	path string

	mu      sync.Mutex
	devices map[string]Registration
}

// OpenRegistry loads (or starts) the registry in dir.
func OpenRegistry(dir string) (*Registry, error) {
	r := &Registry{path: filepath.Join(dir, "push-devices.json"), devices: map[string]Registration{}}
	raw, err := os.ReadFile(r.path)
	if errors.Is(err, os.ErrNotExist) {
		return r, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, &r.devices); err != nil {
		return nil, err
	}
	return r, nil
}

// Set stores a phone's registration; an empty token withdraws it.
func (r *Registry) Set(deviceID string, reg protocol.PushRegistration, now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if reg.Token == "" {
		delete(r.devices, deviceID)
	} else {
		r.devices[deviceID] = Registration{PushRegistration: reg, UpdatedAt: now}
	}
	return r.persist()
}

// Remove drops a phone (revoked, or APNs reported its token unregistered).
func (r *Registry) Remove(deviceID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.devices, deviceID)
	return r.persist()
}

// All returns registrations keyed by device id, in a stable order.
func (r *Registry) All() []Entry {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Entry, 0, len(r.devices))
	for id, reg := range r.devices {
		out = append(out, Entry{DeviceID: id, Registration: reg})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DeviceID < out[j].DeviceID })
	return out
}

// Entry pairs a device id with its registration.
type Entry struct {
	DeviceID string
	Registration
}

func (r *Registry) persist() error {
	raw, err := json.MarshalIndent(r.devices, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(r.path, raw, 0o600)
}
