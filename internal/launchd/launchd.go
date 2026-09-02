// Package launchd installs hermes-remote as a per-user LaunchAgent.
package launchd

import (
	"context"
	"fmt"
	"html"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Label is the LaunchAgent label.
const Label = "ai.hermes.remote"

// PlistPath is ~/Library/LaunchAgents/<Label>.plist.
func PlistPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "Library", "LaunchAgents", Label+".plist"), nil
}

// Spec describes the daemon invocation.
type Spec struct {
	Binary     string // absolute path to hermes-remote
	HermesHome string
	LogDir     string
	Path       string // PATH for the child (must include the tailscale CLI dir and the Hermes venv)
}

func plist(s Spec) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>` + Label + `</string>
    <!-- Installed by hermes-remote: the paired, end-to-end-encrypted bridge
         between the Hermes iPhone app and this Mac's Hermes gateway. -->
    <key>ProgramArguments</key>
    <array>
        <string>` + html.EscapeString(s.Binary) + `</string>
        <string>daemon</string>
    </array>
    <key>WorkingDirectory</key><string>` + html.EscapeString(s.HermesHome) + `</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>HERMES_HOME</key><string>` + html.EscapeString(s.HermesHome) + `</string>
        <key>PATH</key><string>` + html.EscapeString(s.Path) + `</string>
    </dict>
    <key>LimitLoadToSessionType</key>
    <array><string>Aqua</string><string>Background</string></array>
    <key>RunAtLoad</key><true/>
    <key>KeepAlive</key><true/>
    <key>ThrottleInterval</key><integer>10</integer>
    <key>ExitTimeOut</key><integer>20</integer>
    <key>StandardOutPath</key><string>` + html.EscapeString(filepath.Join(s.LogDir, "remote.log")) + `</string>
    <key>StandardErrorPath</key><string>` + html.EscapeString(filepath.Join(s.LogDir, "remote.log")) + `</string>
</dict>
</plist>
`)
	return b.String()
}

func domain() string { return fmt.Sprintf("gui/%d", os.Getuid()) }

// Install writes the plist and (re)starts the agent.
func Install(ctx context.Context, s Spec) error {
	p, err := PlistPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(s.LogDir, 0o755); err != nil {
		return err
	}
	content := []byte(plist(s))
	if existing, err := os.ReadFile(p); err == nil && string(existing) == string(content) && Loaded(ctx) {
		// Same definition already loaded: a restart is enough (and avoids the
		// bootout/bootstrap race below).
		return Restart(ctx)
	}
	if err := os.WriteFile(p, content, 0o644); err != nil {
		return err
	}
	if Loaded(ctx) {
		_ = exec.CommandContext(ctx, "launchctl", "bootout", domain()+"/"+Label).Run()
		// launchd tears the job down asynchronously; bootstrapping too early
		// fails with "Bootstrap failed: 5: Input/output error".
		for i := 0; i < 50 && Loaded(ctx); i++ {
			time.Sleep(200 * time.Millisecond)
		}
	}
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		out, err := exec.CommandContext(ctx, "launchctl", "bootstrap", domain(), p).CombinedOutput()
		if err == nil {
			lastErr = nil
			break
		}
		lastErr = fmt.Errorf("launchctl bootstrap: %s", strings.TrimSpace(string(out)))
		time.Sleep(time.Second)
	}
	if lastErr != nil {
		return lastErr
	}
	return Restart(ctx)
}

// Restart kickstarts the running agent.
func Restart(ctx context.Context) error {
	out, err := exec.CommandContext(ctx, "launchctl", "kickstart", "-k", domain()+"/"+Label).CombinedOutput()
	if err != nil {
		return fmt.Errorf("launchctl kickstart: %s", strings.TrimSpace(string(out)))
	}
	return nil
}

// Stop unloads the agent (it will not restart until Install/`up`).
func Stop(ctx context.Context) error {
	out, err := exec.CommandContext(ctx, "launchctl", "bootout", domain()+"/"+Label).CombinedOutput()
	if err != nil && !strings.Contains(string(out), "No such process") && !strings.Contains(string(out), "not find") {
		return fmt.Errorf("launchctl bootout: %s", strings.TrimSpace(string(out)))
	}
	return nil
}

// Uninstall stops the agent and removes the plist.
func Uninstall(ctx context.Context) error {
	if err := Stop(ctx); err != nil {
		return err
	}
	p, err := PlistPath()
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Loaded reports whether launchd knows the agent.
func Loaded(ctx context.Context) bool {
	return exec.CommandContext(ctx, "launchctl", "print", domain()+"/"+Label).Run() == nil
}
