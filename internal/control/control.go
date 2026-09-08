// Package control is the local admin API between the hermote-bridge CLI and
// the running daemon: a small HTTP API over a unix socket in the state
// directory (0600), never exposed on any TCP port.
package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/ShravanthReddy/Hermote-bridge/internal/state"
)

// Status is what `hermote-bridge status` shows.
type Status struct {
	LaunchID    string            `json:"launch_id,omitempty"`
	SessionID   string            `json:"session_id"`
	Transport   string            `json:"transport"`
	Name        string            `json:"name"`
	Gateway     string            `json:"gateway"` // starting | ready | down
	GatewayPort int               `json:"gateway_port"`
	BridgeAddr  string            `json:"bridge_addr"`
	PublicURL   string            `json:"public_url,omitempty"`
	Connections int               `json:"connections"`
	Pending     int               `json:"pending_pairings"`
	Devices     []state.Device    `json:"devices"`
	StartedAt   time.Time         `json:"started_at"`
	Version     string            `json:"version"`
	Warnings    []string          `json:"warnings,omitempty"`
	Extra       map[string]string `json:"extra,omitempty"`
}

// PairResult is the answer to a pair request.
type PairResult struct {
	URL     string    `json:"url"` // hermes://pair?...
	Expires time.Time `json:"expires"`
}

// Backend is what the daemon exposes to the control API.
type Backend interface {
	Status(ctx context.Context) Status
	Pair(ctx context.Context) (PairResult, error)
	Revoke(ctx context.Context, idOrPrefix string) (state.Device, error)
}

// Serve listens on the unix socket until ctx ends.
func Serve(ctx context.Context, socketPath string, b Backend) error {
	_ = os.Remove(socketPath)
	l, err := net.Listen("unix", socketPath)
	if err != nil {
		return err
	}
	// UnixListener otherwise unlinks socketPath when it closes, even if a
	// replacement daemon has already bound a new socket at the same path.
	if unixListener, ok := l.(*net.UnixListener); ok {
		unixListener.SetUnlinkOnClose(false)
	}
	socketInfo, err := os.Lstat(socketPath)
	if err != nil {
		_ = l.Close()
		return err
	}
	removeOwnedSocket := func() {
		current, statErr := os.Lstat(socketPath)
		if statErr == nil && os.SameFile(socketInfo, current) {
			_ = os.Remove(socketPath)
		}
	}
	defer func() {
		_ = l.Close()
		removeOwnedSocket()
	}()
	if err := os.Chmod(socketPath, 0o600); err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, b.Status(r.Context()))
	})
	mux.HandleFunc("POST /pair", func(w http.ResponseWriter, r *http.Request) {
		res, err := b.Pair(r.Context())
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, res)
	})
	mux.HandleFunc("DELETE /devices/{id}", func(w http.ResponseWriter, r *http.Request) {
		d, err := b.Revoke(r.Context(), r.PathValue("id"))
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, d)
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	if err := srv.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// Client talks to the daemon.
type Client struct {
	http *http.Client
}

// NewClient dials the unix socket lazily.
func NewClient(socketPath string) *Client {
	return &Client{http: &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socketPath)
		}},
	}}
}

// CloseIdleConnections prevents a response from an exiting daemon generation
// from pinning later readiness polls to its already-open Unix connection.
func (c *Client) CloseIdleConnections() { c.http.CloseIdleConnections() }

// ErrNotRunning means the daemon's socket is not answering.
var ErrNotRunning = errors.New("hermote-bridge is not running (start it with: hermote-bridge up)")

func (c *Client) do(ctx context.Context, method, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, "http://hermote-bridge"+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return ErrNotRunning
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(body, &e)
		if e.Error == "" {
			e.Error = string(bytes.TrimSpace(body))
		}
		return fmt.Errorf("%s", e.Error)
	}
	if out != nil {
		return json.Unmarshal(body, out)
	}
	return nil
}

// Status fetches the daemon status.
func (c *Client) Status(ctx context.Context) (Status, error) {
	var s Status
	err := c.do(ctx, http.MethodGet, "/status", &s)
	return s, err
}

// Pair asks for a fresh pairing QR.
func (c *Client) Pair(ctx context.Context) (PairResult, error) {
	var p PairResult
	err := c.do(ctx, http.MethodPost, "/pair", &p)
	return p, err
}

// Revoke removes a device.
func (c *Client) Revoke(ctx context.Context, id string) (state.Device, error) {
	var d state.Device
	err := c.do(ctx, http.MethodDelete, "/devices/"+id, &d)
	return d, err
}
