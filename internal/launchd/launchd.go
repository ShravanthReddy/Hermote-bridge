// Package launchd installs the Hermote bridge as a per-user LaunchAgent.
package launchd

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"html"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Label is a compatibility identity. Existing installations use this label and
// plist path, so changing it would allow two bridge daemons to run at once.
const Label = "ai.hermes.remote"

type commandRunner func(context.Context, string, ...string) ([]byte, error)

type serviceManager struct {
	homeDir      func() (string, error)
	uid          func() int
	run          commandRunner
	sleep        func(context.Context, time.Duration) error
	processAlive func(int) bool
}

var systemManager = serviceManager{
	homeDir: os.UserHomeDir,
	uid:     os.Getuid,
	run: func(ctx context.Context, name string, args ...string) ([]byte, error) {
		return exec.CommandContext(ctx, name, args...).CombinedOutput()
	},
	sleep: sleepContext,
	processAlive: func(pid int) bool {
		err := syscall.Kill(pid, 0)
		return err == nil || errors.Is(err, syscall.EPERM)
	},
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PlistPath is ~/Library/LaunchAgents/<Label>.plist.
func PlistPath() (string, error) { return systemManager.plistPath() }

func (m serviceManager) plistPath() (string, error) {
	home, err := m.homeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "Library", "LaunchAgents", Label+".plist"), nil
}

// Spec describes the daemon invocation.
type Spec struct {
	Binary     string // absolute path to hermote-bridge
	LaunchID   string // unique ID used to prove readiness belongs to this launch
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
    <!-- Installed by hermote-bridge: the paired, end-to-end-encrypted bridge
         between Hermote and this Mac's Hermes gateway. -->
    <key>ProgramArguments</key>
    <array>
        <string>` + html.EscapeString(s.Binary) + `</string>
        <string>daemon</string>
        <string>--launch-id</string>
        <string>` + html.EscapeString(s.LaunchID) + `</string>
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

func (m serviceManager) domain() string { return fmt.Sprintf("gui/%d", m.uid()) }

type serviceSnapshot struct {
	path    string
	content []byte
	mode    os.FileMode
	exists  bool
	loaded  bool
	pid     int
}

// programExecutable returns ProgramArguments[0] from a launchd plist. The
// decoder handles the XML escaping used by plist(), including binary paths
// containing '&' or '<'.
func programExecutable(content []byte) (string, error) {
	decoder := xml.NewDecoder(strings.NewReader(string(content)))
	for {
		token, err := decoder.Token()
		if err != nil {
			return "", fmt.Errorf("read saved LaunchAgent: %w", err)
		}
		start, ok := token.(xml.StartElement)
		if !ok || start.Name.Local != "key" {
			continue
		}
		var key string
		if err := decoder.DecodeElement(&key, &start); err != nil {
			return "", fmt.Errorf("read saved LaunchAgent key: %w", err)
		}
		if key != "ProgramArguments" {
			continue
		}
		for {
			token, err := decoder.Token()
			if err != nil {
				return "", fmt.Errorf("read saved LaunchAgent ProgramArguments: %w", err)
			}
			argument, ok := token.(xml.StartElement)
			if !ok {
				continue
			}
			if argument.Name.Local != "array" {
				return "", errors.New("saved LaunchAgent ProgramArguments is not an array")
			}
			for {
				token, err := decoder.Token()
				if err != nil {
					return "", fmt.Errorf("read saved LaunchAgent arguments: %w", err)
				}
				switch value := token.(type) {
				case xml.StartElement:
					if value.Name.Local != "string" {
						continue
					}
					var executable string
					if err := decoder.DecodeElement(&executable, &value); err != nil {
						return "", fmt.Errorf("read saved LaunchAgent executable: %w", err)
					}
					if strings.TrimSpace(executable) == "" {
						return "", errors.New("saved LaunchAgent executable is empty")
					}
					return executable, nil
				case xml.EndElement:
					if value.Name.Local == "array" {
						return "", errors.New("saved LaunchAgent ProgramArguments is empty")
					}
				}
			}
		}
	}
}

// Installation is a service transition that can be rolled back until the new
// daemon has passed its caller-owned readiness check.
type Installation struct {
	manager  serviceManager
	previous serviceSnapshot
	active   bool
}

// Commit accepts the replacement after its daemon is ready.
func (i *Installation) Commit() { i.active = false }

// Rollback restores the prior plist and loaded state. It is idempotent.
func (i *Installation) Rollback(ctx context.Context) error {
	if i == nil || !i.active {
		return nil
	}
	i.active = false
	return i.manager.restore(ctx, i.previous)
}

func (m serviceManager) snapshot(ctx context.Context) (serviceSnapshot, error) {
	path, err := m.plistPath()
	if err != nil {
		return serviceSnapshot{}, err
	}
	loaded, pid := m.job(ctx)
	snapshot := serviceSnapshot{path: path, loaded: loaded, pid: pid}
	info, statErr := os.Stat(path)
	switch {
	case statErr == nil:
		snapshot.exists = true
		snapshot.mode = info.Mode().Perm()
		snapshot.content, err = os.ReadFile(path)
		if err != nil {
			return serviceSnapshot{}, err
		}
	case errors.Is(statErr, os.ErrNotExist):
		if snapshot.loaded {
			return serviceSnapshot{}, fmt.Errorf("launchd job %s is loaded but %s is missing; refusing an upgrade that cannot roll back", Label, path)
		}
	default:
		return serviceSnapshot{}, statErr
	}
	return snapshot, nil
}

// Install writes the stable-label plist and starts it. The returned transition
// remains rollback-capable until Commit is called after daemon readiness.
func Install(ctx context.Context, spec Spec) (*Installation, error) {
	return systemManager.install(ctx, spec)
}

func (m serviceManager) install(ctx context.Context, spec Spec) (*Installation, error) {
	if strings.TrimSpace(spec.LaunchID) == "" {
		return nil, errors.New("launch ID is required")
	}
	previous, err := m.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(previous.path), 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(spec.LogDir, 0o755); err != nil {
		return nil, err
	}
	content := []byte(plist(spec))
	transition := &Installation{manager: m, previous: previous, active: true}
	if err := os.WriteFile(previous.path, content, 0o644); err != nil {
		return nil, transition.failAndRestore(ctx, err)
	}
	if previous.loaded {
		if err := m.stop(ctx); err != nil {
			return nil, transition.failAndRestore(ctx, err)
		}
		if err := m.waitUnloaded(ctx); err != nil {
			return nil, transition.failAndRestore(ctx, err)
		}
		if err := m.waitProcessExit(ctx, previous.pid); err != nil {
			return nil, transition.failAndRestore(ctx, err)
		}
	}
	if err := m.bootstrap(ctx, previous.path); err != nil {
		return nil, transition.failAndRestore(ctx, err)
	}
	return transition, nil
}

func (i *Installation) failAndRestore(ctx context.Context, installErr error) error {
	restoreErr := i.Rollback(ctx)
	if restoreErr == nil {
		return installErr
	}
	return errors.Join(installErr, fmt.Errorf("restore previous LaunchAgent: %w", restoreErr))
}

func (m serviceManager) restore(ctx context.Context, snapshot serviceSnapshot) error {
	var result error
	if loaded, pid := m.job(ctx); loaded {
		if err := m.stop(ctx); err != nil {
			result = errors.Join(result, err)
		} else if err := m.waitUnloaded(ctx); err != nil {
			result = errors.Join(result, err)
		} else if err := m.waitProcessExit(ctx, pid); err != nil {
			result = errors.Join(result, err)
		}
	}
	if snapshot.exists {
		mode := snapshot.mode
		if mode == 0 {
			mode = 0o644
		}
		if err := os.WriteFile(snapshot.path, snapshot.content, mode); err != nil {
			return errors.Join(result, err)
		}
	} else if err := os.Remove(snapshot.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.Join(result, err)
	}
	if !snapshot.loaded {
		return result
	}
	// A failed replacement can leave the previously loaded process alive after
	// launchd has forgotten the job. Never bootstrap its saved definition until
	// that captured process has actually exited.
	if err := m.waitProcessExit(ctx, snapshot.pid); err != nil {
		result = errors.Join(result, err)
	}
	if result != nil {
		return result
	}
	executable, err := programExecutable(snapshot.content)
	if err != nil {
		return fmt.Errorf("saved LaunchAgent was restored but remains unloaded: %w", err)
	}
	info, err := os.Stat(executable)
	if err != nil {
		return fmt.Errorf("saved LaunchAgent was restored but remains unloaded because its executable %q is unavailable: %w", executable, err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return fmt.Errorf("saved LaunchAgent was restored but remains unloaded because its executable %q is not an executable regular file", executable)
	}
	if err := m.bootstrap(ctx, snapshot.path); err != nil {
		return errors.Join(result, err)
	}
	return result
}

func (m serviceManager) bootstrap(ctx context.Context, path string) error {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		out, err := m.run(ctx, "launchctl", "bootstrap", m.domain(), path)
		if err == nil {
			return nil
		}
		lastErr = fmt.Errorf("launchctl bootstrap: %s", strings.TrimSpace(string(out)))
		if attempt < 4 {
			if err := m.sleep(ctx, time.Second); err != nil {
				return errors.Join(lastErr, err)
			}
		}
	}
	return lastErr
}

func (m serviceManager) waitUnloaded(ctx context.Context) error {
	for attempt := 0; attempt < 50; attempt++ {
		if !m.loaded(ctx) {
			return nil
		}
		if err := m.sleep(ctx, 200*time.Millisecond); err != nil {
			return err
		}
	}
	return fmt.Errorf("launchd job %s did not stop", Label)
}

func (m serviceManager) waitProcessExit(ctx context.Context, pid int) error {
	if pid <= 0 {
		return nil
	}
	for attempt := 0; attempt < 150; attempt++ {
		if !m.processAlive(pid) {
			return nil
		}
		if err := m.sleep(ctx, 200*time.Millisecond); err != nil {
			return err
		}
	}
	return fmt.Errorf("previous launchd process %d did not exit", pid)
}

// Restart kickstarts the running agent.
func Restart(ctx context.Context) error { return systemManager.restart(ctx) }

func (m serviceManager) restart(ctx context.Context) error {
	out, err := m.run(ctx, "launchctl", "kickstart", "-k", m.domain()+"/"+Label)
	if err != nil {
		return fmt.Errorf("launchctl kickstart: %s", strings.TrimSpace(string(out)))
	}
	return nil
}

// Stop unloads the agent (it will not restart until Install/`up`).
func Stop(ctx context.Context) error { return systemManager.stop(ctx) }

func (m serviceManager) stop(ctx context.Context) error {
	out, err := m.run(ctx, "launchctl", "bootout", m.domain()+"/"+Label)
	if err != nil && !strings.Contains(string(out), "No such process") && !strings.Contains(string(out), "not find") {
		return fmt.Errorf("launchctl bootout: %s", strings.TrimSpace(string(out)))
	}
	return nil
}

// Uninstall stops the agent and removes the stable compatibility plist.
func Uninstall(ctx context.Context) error {
	if err := systemManager.stop(ctx); err != nil {
		return err
	}
	path, err := systemManager.plistPath()
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// Loaded reports whether launchd knows the agent.
func Loaded(ctx context.Context) bool { return systemManager.loaded(ctx) }

func (m serviceManager) loaded(ctx context.Context) bool {
	loaded, _ := m.job(ctx)
	return loaded
}

func (m serviceManager) job(ctx context.Context) (bool, int) {
	output, err := m.run(ctx, "launchctl", "print", m.domain()+"/"+Label)
	if err != nil {
		return false, 0
	}
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 && fields[0] == "pid" && fields[1] == "=" {
			pid, parseErr := strconv.Atoi(fields[2])
			if parseErr == nil {
				return true, pid
			}
		}
	}
	return true, 0
}
