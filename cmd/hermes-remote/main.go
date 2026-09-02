// hermes-remote makes a Mac's Hermes reachable from the Hermes iPhone app:
// one command installs a background bridge, and a QR code pairs the phone.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/ShravanthReddy/hermes-remote/internal/control"
	"github.com/ShravanthReddy/hermes-remote/internal/gateway"
	"github.com/ShravanthReddy/hermes-remote/internal/launchd"
	"github.com/ShravanthReddy/hermes-remote/internal/protocol"
	"github.com/ShravanthReddy/hermes-remote/internal/qr"
	"github.com/ShravanthReddy/hermes-remote/internal/state"
	"github.com/ShravanthReddy/hermes-remote/internal/tailscale"
)

// version is set by the release build (-ldflags "-X main.version=…").
var version = "dev"

const usage = `hermes-remote — connect the Hermes iPhone app to this Mac

Usage:
  hermes-remote up [--transport direct|relay] [--relay wss://…] [--name "My Mac"]
                   Install (or update) the background bridge and show a pairing QR code.
  hermes-remote pair            Show a new pairing QR code (5-minute validity).
  hermes-remote status          Show what is running and who is paired.
  hermes-remote devices         List paired phones.
  hermes-remote devices revoke <id-prefix>
  hermes-remote restart | stop | logs
  hermes-remote selftest        Pair a throw-away test client over loopback and exercise the tunnel.
  hermes-remote uninstall       Stop and remove the background bridge (keeps pairing state).
  hermes-remote version

Transports:
  direct  (recommended) the phone reaches this Mac over your Tailscale network;
          needs the Tailscale app on the phone, signed into the same account.
  relay   both sides connect to a relay; the relay only ever sees encrypted bytes.
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fail("%s", err.Error())
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return cmdUp(nil)
	}
	switch args[0] {
	case "up":
		return cmdUp(args[1:])
	case "pair", "qr":
		return cmdPair(args[1:])
	case "status":
		return cmdStatus()
	case "devices":
		return cmdDevices(args[1:])
	case "restart":
		return withCtx(launchd.Restart)
	case "stop":
		return withCtx(launchd.Stop)
	case "logs":
		return cmdLogs()
	case "uninstall":
		return cmdUninstall()
	case "selftest":
		return cmdSelftest()
	case "daemon":
		return runDaemon(args[1:])
	case "version", "--version", "-v":
		fmt.Println("hermes-remote", version)
		return nil
	case "help", "-h", "--help":
		fmt.Print(usage)
		return nil
	}
	fmt.Print(usage)
	return fmt.Errorf("unknown command %q", args[0])
}

func withCtx(f func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return f(ctx)
}

// ── output helpers ──────────────────────────────────────────────────────────

func bold(s string) { fmt.Printf("\033[1m%s\033[0m\n", s) }
func ok(format string, a ...any) {
	fmt.Printf("  \033[32m✓\033[0m %s\n", fmt.Sprintf(format, a...))
}
func warn(format string, a ...any) {
	fmt.Printf("  \033[33m!\033[0m %s\n", fmt.Sprintf(format, a...))
}
func fail(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "  \033[31m✗\033[0m %s\n", fmt.Sprintf(format, a...))
}

// ── up ──────────────────────────────────────────────────────────────────────

func cmdUp(args []string) error {
	fs := flag.NewFlagSet("up", flag.ContinueOnError)
	transport := fs.String("transport", "", "direct (default) or relay")
	relay := fs.String("relay", "", "relay URL (wss://…) for --transport relay")
	name := fs.String("name", "", "name shown on the phone (default: this Mac's hostname)")
	httpsPort := fs.Int("https-port", 0, "Tailscale Serve HTTPS port (default 8443)")
	bridgePort := fs.Int("bridge-port", 0, "loopback port for the bridge (default 9120)")
	lightTerminal := fs.Bool("light-terminal", false, "render the QR for a light terminal background")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	bold("Setting up remote access to Hermes on this Mac")
	store, err := state.Open()
	if err != nil {
		return err
	}
	cfg, err := store.Config()
	if err != nil {
		return err
	}
	hermesHome, _ := state.HermesHome()

	// Hermes.
	python, err := gateway.FindPython(hermesHome)
	if err != nil {
		return err
	}
	cfg.Python = python
	ok("Hermes Agent at %s", filepath.Dir(filepath.Dir(filepath.Dir(python))))

	// Config from flags with sticky defaults.
	if *transport != "" {
		cfg.Transport = protocol.Transport(*transport)
	}
	if cfg.Transport == "" {
		cfg.Transport = protocol.TransportDirect
	}
	if cfg.Transport != protocol.TransportDirect && cfg.Transport != protocol.TransportRelay {
		return fmt.Errorf("unknown transport %q", cfg.Transport)
	}
	if *relay != "" {
		cfg.RelayURL = strings.TrimRight(*relay, "/")
	}
	if cfg.Transport == protocol.TransportRelay && cfg.RelayURL == "" {
		return errors.New("--transport relay needs --relay wss://host")
	}
	if *name != "" {
		cfg.Name = *name
	}
	if cfg.Name == "" {
		host, _ := os.Hostname()
		cfg.Name = strings.TrimSuffix(host, ".local")
	}
	if *httpsPort != 0 {
		cfg.HTTPSPort = *httpsPort
	}
	if cfg.HTTPSPort == 0 {
		cfg.HTTPSPort = 8443
	}
	if *bridgePort != 0 {
		cfg.BridgePort = *bridgePort
	}
	if cfg.BridgePort == 0 {
		cfg.BridgePort = 9120
	}

	// Tailscale (direct only).
	var ts *tailscale.CLI
	if cfg.Transport == protocol.TransportDirect {
		ts, err = tailscale.Find()
		if err != nil {
			warn("Tailscale is not installed. It gives your phone a private, encrypted path to this Mac.")
			fmt.Printf("    1. Install it: %s  (opening now)\n", tailscale.DownloadURL)
			fmt.Println("    2. Sign in (same account you'll use on the phone), then run `hermes-remote up` again.")
			_ = exec.Command("open", tailscale.DownloadURL).Run()
			return errors.New("Tailscale required for the direct transport")
		}
		st, err := ts.WaitRunning(ctx, 2*time.Minute, func(state string) {
			warn("Tailscale is installed but not connected (%s). Open the Tailscale menu bar item and sign in — waiting…", state)
		})
		if err != nil {
			return err
		}
		ok("Tailscale connected as %s", st.DNSName)
	}
	if err := store.SaveConfig(cfg); err != nil {
		return err
	}

	// LaunchAgent.
	bin, err := os.Executable()
	if err != nil {
		return err
	}
	bin, _ = filepath.EvalSymlinks(bin)
	pathEnv := strings.Join([]string{
		filepath.Dir(python), "/usr/local/bin", "/opt/homebrew/bin", filepath.Join(os.Getenv("HOME"), "homebrew", "bin"),
		"/usr/bin", "/bin", "/usr/sbin", "/sbin",
	}, ":")
	if err := launchd.Install(ctx, launchd.Spec{
		Binary: bin, HermesHome: hermesHome, LogDir: filepath.Join(hermesHome, "logs"), Path: pathEnv,
	}); err != nil {
		return err
	}
	client := control.NewClient(store.Path("control.sock"))
	st, err := waitForDaemon(ctx, client, 2*time.Minute)
	if err != nil {
		return fmt.Errorf("%w — see `hermes-remote logs`", err)
	}
	ok("Bridge running (LaunchAgent %s), Hermes gateway %s on 127.0.0.1:%d", launchd.Label, st.Gateway, st.GatewayPort)

	// Tailscale Serve.
	if cfg.Transport == protocol.TransportDirect {
		public := strings.TrimSuffix(strings.Replace(st.PublicURL, "wss://", "https://", 1), "/v1/bridge")
		target := fmt.Sprintf("http://%s", st.BridgeAddr)
		if ts.ServeMapped(ctx, public) {
			ok("Tailscale Serve already maps %s → %s", public, target)
		} else {
			if err := ts.ServeOn(ctx, cfg.HTTPSPort, target); err != nil {
				return err
			}
			ok("Tailscale Serve maps %s → %s (tailnet only)", public, target)
		}
	}
	return showPair(ctx, client, cfg, *lightTerminal)
}

func waitForDaemon(ctx context.Context, c *control.Client, timeout time.Duration) (control.Status, error) {
	deadline := time.Now().Add(timeout)
	var last control.Status
	var lastErr error
	printed := false
	for time.Now().Before(deadline) {
		st, err := c.Status(ctx)
		if err == nil {
			last = st
			if st.Gateway == "ready" {
				return st, nil
			}
			if !printed {
				fmt.Printf("  … starting the Hermes gateway (%s)\n", st.Gateway)
				printed = true
			}
		} else {
			lastErr = err
		}
		time.Sleep(time.Second)
	}
	if lastErr != nil && last.SessionID == "" {
		return last, lastErr
	}
	return last, fmt.Errorf("the Hermes gateway did not become ready (state %s)", last.Gateway)
}

// ── pair ────────────────────────────────────────────────────────────────────

func cmdPair(args []string) error {
	fs := flag.NewFlagSet("pair", flag.ContinueOnError)
	lightTerminal := fs.Bool("light-terminal", false, "render the QR for a light terminal background")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store, err := state.Open()
	if err != nil {
		return err
	}
	cfg, err := store.Config()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return showPair(ctx, control.NewClient(store.Path("control.sock")), cfg, *lightTerminal)
}

func showPair(ctx context.Context, c *control.Client, cfg state.Config, lightTerminal bool) error {
	res, err := c.Pair(ctx)
	if err != nil {
		return err
	}
	fmt.Println()
	bold("Pair your iPhone")
	if cfg.Transport == protocol.TransportDirect {
		fmt.Println("  1. On the phone, install Tailscale and sign in with the same account as this Mac.")
		fmt.Println("  2. Open Hermes → “Scan setup code” and point it at this code.")
	} else {
		fmt.Println("  Open Hermes on the phone → “Scan setup code” and point it at this code.")
	}
	fmt.Println()
	code, err := qr.Render(res.URL, !lightTerminal)
	if err != nil {
		return err
	}
	for _, line := range strings.Split(strings.TrimRight(code, "\n"), "\n") {
		fmt.Println("  " + line)
	}
	fmt.Println()
	fmt.Printf("  Or open this link on the phone:\n  %s\n\n", res.URL)
	fmt.Printf("  The code expires %s. Print a new one any time: hermes-remote pair\n", res.Expires.Local().Format("15:04"))
	fmt.Println("  Anyone who scans it within that window becomes a paired phone — keep it to yourself.")
	return nil
}

// ── status / devices / logs / uninstall ─────────────────────────────────────

func cmdStatus() error {
	store, err := state.Open()
	if err != nil {
		return err
	}
	cfg, _ := store.Config()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if launchd.Loaded(ctx) {
		ok("LaunchAgent %s loaded", launchd.Label)
	} else {
		warn("LaunchAgent %s not loaded (run `hermes-remote up`)", launchd.Label)
	}
	st, err := control.NewClient(store.Path("control.sock")).Status(ctx)
	if err != nil {
		return err
	}
	ok("Bridge %s, transport %s, session %s, up since %s", st.Version, st.Transport, st.SessionID, st.StartedAt.Local().Format(time.Stamp))
	if st.Gateway == "ready" {
		ok("Hermes gateway ready on 127.0.0.1:%d", st.GatewayPort)
	} else {
		warn("Hermes gateway %s", st.Gateway)
	}
	if cfg.Transport == protocol.TransportDirect {
		if ts, err := tailscale.Find(); err == nil && st.PublicURL != "" {
			public := strings.TrimSuffix(strings.Replace(st.PublicURL, "wss://", "https://", 1), "/v1/bridge")
			if ts.ServeMapped(ctx, public) {
				ok("Tailscale Serve: %s → http://%s", public, st.BridgeAddr)
			} else {
				warn("Tailscale Serve is not mapping %s (run `hermes-remote up`)", public)
			}
		}
	}
	if relayURL := st.Extra["relay"]; relayURL != "" && st.Extra["relay_attached"] == "true" {
		ok("Attached to relay %s", relayURL)
	}
	for _, w := range st.Warnings {
		warn("%s", w)
	}
	ok("%d phone(s) connected, %d pairing code(s) outstanding", st.Connections, st.Pending)
	printDevices(st.Devices)
	return nil
}

func printDevices(devices []state.Device) {
	if len(devices) == 0 {
		fmt.Println("  No paired phones yet — run `hermes-remote pair`.")
		return
	}
	fmt.Println("  Paired phones:")
	for _, d := range devices {
		name := d.Name
		if name == "" {
			name = "(unnamed)"
		}
		fmt.Printf("    %-10s %-24s paired %s, last seen %s\n", d.ID[:8]+"…", name,
			d.FirstSeen.Local().Format("2006-01-02"), d.LastSeen.Local().Format("2006-01-02 15:04"))
	}
}

func cmdDevices(args []string) error {
	store, err := state.Open()
	if err != nil {
		return err
	}
	if len(args) >= 2 && args[0] == "revoke" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		d, err := control.NewClient(store.Path("control.sock")).Revoke(ctx, args[1])
		if errors.Is(err, control.ErrNotRunning) {
			// Daemon down: edit the file directly.
			d, err = store.Revoke(args[1])
		}
		if err != nil {
			return err
		}
		ok("Revoked %s (%s). It must scan a new code to connect again.", d.ID[:8]+"…", d.Name)
		return nil
	}
	devices, err := store.Devices()
	if err != nil {
		return err
	}
	printDevices(devices)
	return nil
}

func cmdLogs() error {
	hermesHome, err := state.HermesHome()
	if err != nil {
		return err
	}
	cmd := exec.Command("tail", "-n", "100", "-f", filepath.Join(hermesHome, "logs", "remote.log"))
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	return cmd.Run()
}

func cmdUninstall() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := state.Open()
	if err != nil {
		return err
	}
	cfg, _ := store.Config()
	if err := launchd.Uninstall(ctx); err != nil {
		return err
	}
	ok("Stopped and removed LaunchAgent %s", launchd.Label)
	if cfg.Transport == protocol.TransportDirect && cfg.HTTPSPort != 0 {
		if ts, err := tailscale.Find(); err == nil {
			if err := ts.ServeOff(ctx, cfg.HTTPSPort); err == nil {
				ok("Removed the Tailscale Serve mapping on :%d", cfg.HTTPSPort)
			}
		}
	}
	fmt.Printf("  Kept %s (identity, paired phones, config). Delete it for a clean slate.\n", store.Path(""))
	return nil
}
