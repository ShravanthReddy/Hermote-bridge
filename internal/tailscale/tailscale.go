// Package tailscale wraps the Tailscale CLI for the direct transport.
package tailscale

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

// appCLI is where the Mac app ships its CLI. It must be exec'd in place — a
// symlink breaks its bundle-identifier lookup.
const appCLI = "/Applications/Tailscale.app/Contents/MacOS/Tailscale"

// DownloadURL is where to send people who don't have Tailscale yet.
const DownloadURL = "https://tailscale.com/download/mac"

// ErrNotInstalled means no CLI was found.
var ErrNotInstalled = errors.New("Tailscale is not installed")

// CLI is a located tailscale binary.
type CLI struct{ path string }

// Find locates the CLI on PATH or inside the Mac app bundle.
func Find() (*CLI, error) {
	if p, err := exec.LookPath("tailscale"); err == nil {
		return &CLI{path: p}, nil
	}
	if st, err := os.Stat(appCLI); err == nil && !st.IsDir() {
		return &CLI{path: appCLI}, nil
	}
	return nil, ErrNotInstalled
}

// Status is the subset of `tailscale status --json` we use.
type Status struct {
	BackendState string
	DNSName      string // MagicDNS name without the trailing dot
	TailnetName  string
}

// Status queries the local tailscaled.
func (c *CLI) Status(ctx context.Context) (Status, error) {
	out, err := exec.CommandContext(ctx, c.path, "status", "--json").Output()
	if err != nil {
		return Status{}, fmt.Errorf("tailscale status: %w", err)
	}
	var raw struct {
		BackendState   string
		Self           struct{ DNSName string }
		CurrentTailnet struct {
			Name           string
			MagicDNSSuffix string
		}
	}
	if err := json.Unmarshal(out, &raw); err != nil {
		return Status{}, err
	}
	return Status{
		BackendState: raw.BackendState,
		DNSName:      strings.TrimSuffix(raw.Self.DNSName, "."),
		TailnetName:  raw.CurrentTailnet.Name,
	}, nil
}

// WaitRunning polls until the backend is Running or the timeout passes.
// onWait is called once if the first check fails, so the caller can tell the
// user what to do.
func (c *CLI) WaitRunning(ctx context.Context, timeout time.Duration, onWait func(state string)) (Status, error) {
	deadline := time.Now().Add(timeout)
	warned := false
	for {
		st, err := c.Status(ctx)
		if err == nil && st.BackendState == "Running" && st.DNSName != "" {
			return st, nil
		}
		if !warned {
			state := st.BackendState
			if err != nil {
				state = err.Error()
			}
			onWait(state)
			warned = true
		}
		if time.Now().After(deadline) {
			return st, errors.New("Tailscale did not connect in time")
		}
		select {
		case <-ctx.Done():
			return st, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// ServeMapped reports whether `tailscale serve` already maps publicURL.
func (c *CLI) ServeMapped(ctx context.Context, publicURL string) bool {
	out, err := exec.CommandContext(ctx, c.path, "serve", "status").CombinedOutput()
	return err == nil && strings.Contains(string(out), publicURL)
}

// ServeOn maps https://<self>:<port> → target in the background.
func (c *CLI) ServeOn(ctx context.Context, httpsPort int, target string) error {
	out, err := exec.CommandContext(ctx, c.path, "serve", "--bg", fmt.Sprintf("--https=%d", httpsPort), target).CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if strings.Contains(strings.ToLower(msg), "https") || strings.Contains(strings.ToLower(msg), "cert") || strings.Contains(msg, "MagicDNS") {
			return fmt.Errorf("%s\n\nTailscale needs HTTPS certificates for your tailnet: open https://login.tailscale.com/admin/dns, enable MagicDNS and HTTPS Certificates, then run `hermes-remote up` again", msg)
		}
		return fmt.Errorf("tailscale serve: %s", msg)
	}
	return nil
}

// ServeOff removes the mapping on httpsPort.
func (c *CLI) ServeOff(ctx context.Context, httpsPort int) error {
	out, err := exec.CommandContext(ctx, c.path, "serve", fmt.Sprintf("--https=%d", httpsPort), "off").CombinedOutput()
	if err != nil {
		return fmt.Errorf("tailscale serve off: %s", strings.TrimSpace(string(out)))
	}
	return nil
}
