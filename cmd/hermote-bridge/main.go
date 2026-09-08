// hermote-bridge makes a Mac's Hermes reachable from the Hermote iPhone app:
// one command installs a background bridge, and a QR code pairs the phone.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/ShravanthReddy/Hermote-bridge/internal/control"
	"github.com/ShravanthReddy/Hermote-bridge/internal/gateway"
	"github.com/ShravanthReddy/Hermote-bridge/internal/launchd"
	"github.com/ShravanthReddy/Hermote-bridge/internal/protocol"
	"github.com/ShravanthReddy/Hermote-bridge/internal/qr"
	"github.com/ShravanthReddy/Hermote-bridge/internal/state"
	"github.com/ShravanthReddy/Hermote-bridge/internal/tailscale"
)

// version is set by the release build (-ldflags "-X main.version=…").
var version = "dev"

const usage = `hermote-bridge — connect the Hermote iPhone app to this Mac

Usage:
  hermote-bridge up [--transport direct|relay] [--relay wss://…] [--name "My Mac"]
                   Install (or update) the background bridge and show a pairing QR code.
                   Asks which transport to use the first time; the choice sticks.
  hermote-bridge pair            Show a new pairing QR code (5-minute validity).
  hermote-bridge status          Show what is running and who is paired.
  hermote-bridge devices         List paired phones.
  hermote-bridge devices revoke <id-prefix>
  hermote-bridge restart | stop | logs
  hermote-bridge selftest        Pair a throw-away test client over loopback and exercise the tunnel.
  hermote-bridge push …          Notifications for phones that are away: setup (APNs key), status, test.
  hermote-bridge uninstall       Stop and remove the background bridge (keeps pairing state).
  hermote-bridge version

Transports:
  direct  (recommended) the phone reaches this Mac over your Tailscale network;
          needs the Tailscale app on the phone, signed into the same account.
  relay   both sides connect to a relay; the relay only ever sees encrypted bytes.
          Uses the hosted relay unless --relay names your own.
`

// hostedRelayURL is the relay run for Hermote (see docs/REMOTE-ACCESS.md §6).
// The legacy sslip.io endpoint remains live for existing pairings. Stored relay
// URLs are sticky, so this default only applies without a saved relay URL.
const hostedRelayURL = "wss://relay.hermote.app"

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
		if err := requireMacOS("restart"); err != nil {
			return err
		}
		return withCtx(launchd.Restart)
	case "stop":
		if err := requireMacOS("stop"); err != nil {
			return err
		}
		return withCtx(launchd.Stop)
	case "logs":
		return cmdLogs()
	case "uninstall":
		if err := requireMacOS("uninstall"); err != nil {
			return err
		}
		return cmdUninstall()
	case "selftest":
		return cmdSelftest()
	case "push":
		return cmdPush(args[1:])
	case "daemon":
		return runDaemon(args[1:])
	case "version", "--version", "-v":
		fmt.Println("hermote-bridge", version)
		return nil
	case "help", "-h", "--help":
		fmt.Print(usage)
		return nil
	}
	fmt.Print(usage)
	return fmt.Errorf("unknown command %q", args[0])
}

func requireMacOS(command string) error {
	return requireMacOSOn(command, runtime.GOOS)
}

func requireMacOSOn(command, goos string) error {
	if goos == "darwin" {
		return nil
	}
	return fmt.Errorf("hermote-bridge %s manages a macOS LaunchAgent and is unavailable on %s; see the README for manual foreground operation", command, goos)
}

func withCtx(f func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return f(ctx)
}

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

func cmdUp(args []string) error {
	if err := requireMacOS("up"); err != nil {
		return err
	}
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

	// Config from flags with sticky defaults. The first `up` on a Mac asks which
	// transport to use (when run in a terminal); after that the answer is stored.
	if *transport != "" {
		cfg.Transport = protocol.Transport(*transport)
	}
	if cfg.Transport == "" {
		_, tsErr := tailscale.Find()
		cfg.Transport = chooseTransport(os.Stdin, os.Stdout, stdinIsTerminal(), tsErr == nil)
	}
	if cfg.Transport != protocol.TransportDirect && cfg.Transport != protocol.TransportRelay {
		return fmt.Errorf("unknown transport %q", cfg.Transport)
	}
	if *relay != "" {
		cfg.RelayURL = strings.TrimRight(*relay, "/")
	}
	switch cfg.Transport {
	case protocol.TransportRelay:
		if cfg.RelayURL == "" {
			cfg.RelayURL = hostedRelayURL
		}
		if cfg.RelayURL == hostedRelayURL {
			ok("Transport: hosted relay %s (pass --relay wss://… to use your own)", cfg.RelayURL)
		} else {
			ok("Transport: relay %s", cfg.RelayURL)
		}
	default:
		ok("Transport: direct over Tailscale")
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
			fmt.Println("    2. Sign in (same account you'll use on the phone), then run `hermote-bridge up` again.")
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
	bin, err := stableServiceExecutable()
	if err != nil {
		return err
	}
	pathEnv := strings.Join([]string{
		filepath.Dir(python), "/usr/local/bin", "/opt/homebrew/bin", filepath.Join(os.Getenv("HOME"), "homebrew", "bin"),
		"/usr/bin", "/bin", "/usr/sbin", "/sbin",
	}, ":")
	launchID := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	installation, err := launchd.Install(ctx, launchd.Spec{
		Binary: bin, LaunchID: launchID, HermesHome: hermesHome,
		LogDir: filepath.Join(hermesHome, "logs"), Path: pathEnv,
	})
	if err != nil {
		return err
	}
	client := control.NewClient(store.Path("control.sock"))
	st, err := waitForDaemon(ctx, client, launchID, 2*time.Minute)
	if err != nil {
		rollbackCtx, cancelRollback := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelRollback()
		if rollbackErr := installation.Rollback(rollbackCtx); rollbackErr != nil {
			return errors.Join(
				fmt.Errorf("%w — see `hermote-bridge logs`", err),
				fmt.Errorf("restore previous bridge service: %w", rollbackErr),
			)
		}
		return fmt.Errorf("%w — LaunchAgent state was rolled back; this does not replace the executable at its configured path", err)
	}
	installation.Commit()
	if migrated, migrationErr := migrateLegacyCommand(bin); migrationErr != nil {
		warn("Bridge is ready, but the hermes-remote compatibility command was not migrated: %v", migrationErr)
	} else if migrated {
		ok("Compatibility command: %s → hermote-bridge", filepath.Join(filepath.Dir(bin), "hermes-remote"))
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

// stableServiceExecutable chooses a path that survives Homebrew keg cleanup.
// It verifies every symlink candidate resolves to this process before storing
// it in launchd and prefers the canonical name over the compatibility alias.
func stableServiceExecutable() (string, error) {
	executable, err := os.Executable()
	if err != nil {
		return "", err
	}
	return stableServiceExecutableAt(executable, os.Args[0], exec.LookPath)
}

func stableServiceExecutableAt(
	executable string,
	invoked string,
	lookPath func(string) (string, error),
) (string, error) {
	executable, err := filepath.Abs(executable)
	if err != nil {
		return "", err
	}
	var candidates []string
	if path, err := lookPath("hermote-bridge"); err == nil {
		candidates = append(candidates, path)
	}
	if strings.ContainsRune(invoked, os.PathSeparator) {
		path, absErr := filepath.Abs(invoked)
		if absErr == nil {
			if filepath.Base(path) == "hermes-remote" {
				candidates = append(candidates, filepath.Join(filepath.Dir(path), "hermote-bridge"))
			}
			candidates = append(candidates, path)
		}
	} else if path, lookupErr := lookPath(invoked); lookupErr == nil {
		if filepath.Base(path) == "hermes-remote" {
			candidates = append(candidates, filepath.Join(filepath.Dir(path), "hermote-bridge"))
		}
		candidates = append(candidates, path)
	}
	candidates = append(candidates, executable)
	for _, candidate := range candidates {
		candidate, err = filepath.Abs(candidate)
		if err != nil {
			continue
		}
		candidateInfo, candidateErr := os.Stat(candidate)
		executableInfo, executableErr := os.Stat(executable)
		if candidateErr == nil && executableErr == nil && os.SameFile(candidateInfo, executableInfo) {
			return filepath.Clean(candidate), nil
		}
	}
	return "", fmt.Errorf("cannot resolve a stable path for %s", executable)
}

var legacyVersionOutput = regexp.MustCompile(`^hermes-remote (?:v?[0-9]+\.[0-9]+\.[0-9]+|dev)$`)

// migrateLegacyCommand replaces only a recognized old bridge executable next
// to the stable canonical command. It runs after daemon readiness so a failed
// upgrade retains the old command and binary as a recovery path.
func migrateLegacyCommand(canonical string) (bool, error) {
	canonical, err := filepath.Abs(canonical)
	if err != nil {
		return false, err
	}
	if filepath.Base(canonical) != "hermote-bridge" {
		return false, nil
	}
	legacy := filepath.Join(filepath.Dir(canonical), "hermes-remote")
	info, err := os.Lstat(legacy)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		target, readErr := os.Readlink(legacy)
		if readErr != nil {
			return false, readErr
		}
		if target == "hermote-bridge" {
			return false, nil
		}
		info, err = os.Stat(legacy)
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return false, nil
	}
	checkCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	output, err := exec.CommandContext(checkCtx, legacy, "version").CombinedOutput()
	if err != nil || !legacyVersionOutput.MatchString(strings.TrimSpace(string(output))) {
		return false, nil
	}
	stagedAlias := fmt.Sprintf("%s.alias.%d", legacy, os.Getpid())
	if err := os.Symlink("hermote-bridge", stagedAlias); err != nil {
		return false, err
	}
	if err := os.Rename(stagedAlias, legacy); err != nil {
		_ = os.Remove(stagedAlias)
		return false, err
	}
	return true, nil
}

// stdinIsTerminal reports whether `up` can ask the user a question.
func stdinIsTerminal() bool {
	fi, err := os.Stdin.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// chooseTransport asks how the phone should reach this Mac. Non-interactive runs
// (piped stdin, scripts) get the direct transport, which is also the default
// answer. Three unrecognised answers fall back to direct rather than loop.
func chooseTransport(in io.Reader, out io.Writer, interactive, tailscaleInstalled bool) protocol.Transport {
	if !interactive {
		return protocol.TransportDirect
	}
	tsNote := ""
	if !tailscaleInstalled {
		tsNote = " Not installed yet; `up` walks you through it."
	}
	fmt.Fprintln(out)
	fmt.Fprintln(out, "  How should your phone reach this Mac?")
	fmt.Fprintln(out, "    1) Tailscale (recommended) — a private network between your own devices. Needs the")
	fmt.Fprintf(out, "       Tailscale app on the Mac and the phone, signed into the same account.%s\n", tsNote)
	fmt.Fprintln(out, "    2) Hosted relay — works anywhere, nothing extra to install. The relay forwards")
	fmt.Fprintln(out, "       encrypted bytes it cannot read; you can run your own with --relay.")
	reader := bufio.NewReader(in)
	for attempt := 0; attempt < 3; attempt++ {
		fmt.Fprint(out, "  Choose 1 or 2 [1]: ")
		line, err := reader.ReadString('\n')
		if t, ok := parseTransportChoice(line); ok {
			fmt.Fprintln(out)
			return t
		}
		if err != nil {
			break
		}
		fmt.Fprintln(out, "  Please answer 1 or 2.")
	}
	fmt.Fprintln(out)
	return protocol.TransportDirect
}

// parseTransportChoice maps an answer to a transport; empty means the default.
func parseTransportChoice(answer string) (protocol.Transport, bool) {
	switch strings.ToLower(strings.TrimSpace(answer)) {
	case "", "1", "direct", "tailscale", "t":
		return protocol.TransportDirect, true
	case "2", "relay", "r":
		return protocol.TransportRelay, true
	}
	return "", false
}

func waitForDaemon(ctx context.Context, c *control.Client, launchID string, timeout time.Duration) (control.Status, error) {
	deadline := time.Now().Add(timeout)
	var last control.Status
	var lastErr error
	printed := false
	for time.Now().Before(deadline) {
		st, err := c.Status(ctx)
		if err == nil {
			last = st
			if st.LaunchID != launchID {
				lastErr = fmt.Errorf("control socket answered from launch generation %q while waiting for %q", st.LaunchID, launchID)
				c.CloseIdleConnections()
			} else {
				if st.Gateway == "ready" {
					return st, nil
				}
				if !printed {
					fmt.Printf("  … starting the Hermes gateway (%s)\n", st.Gateway)
					printed = true
				}
			}
		} else {
			lastErr = err
		}
		pause := time.Second
		if remaining := time.Until(deadline); remaining < pause {
			pause = remaining
		}
		if pause > 0 {
			timer := time.NewTimer(pause)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return last, ctx.Err()
			}
		}
	}
	if lastErr != nil && (last.SessionID == "" || last.LaunchID != launchID) {
		return last, lastErr
	}
	return last, fmt.Errorf("the Hermes gateway did not become ready (state %s)", last.Gateway)
}

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
		fmt.Println("  2. Open Hermote → “Scan setup code” and point it at this code.")
	} else {
		fmt.Println("  Open Hermote on the phone → “Scan setup code” and point it at this code.")
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
	fmt.Printf("  The code expires %s. Print a new one any time: hermote-bridge pair\n", res.Expires.Local().Format("15:04"))
	fmt.Println("  Anyone who scans it within that window becomes a paired phone — keep it to yourself.")
	return nil
}

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
		warn("LaunchAgent %s not loaded (run `hermote-bridge up`)", launchd.Label)
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
				warn("Tailscale Serve is not mapping %s (run `hermote-bridge up`)", public)
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
		fmt.Println("  No paired phones yet — run `hermote-bridge pair`.")
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
