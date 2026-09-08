package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ShravanthReddy/Hermote-bridge/internal/control"
	"github.com/ShravanthReddy/Hermote-bridge/internal/protocol"
	"github.com/ShravanthReddy/Hermote-bridge/internal/state"
)

type readyBackend struct{ launchID string }

func (b readyBackend) Status(context.Context) control.Status {
	return control.Status{LaunchID: b.launchID, SessionID: "session", Gateway: "ready"}
}

func (readyBackend) Pair(context.Context) (control.PairResult, error) {
	return control.PairResult{}, errors.New("not implemented")
}

func (readyBackend) Revoke(context.Context, string) (state.Device, error) {
	return state.Device{}, errors.New("not implemented")
}

func TestWaitForDaemonRejectsReadyPreviousGeneration(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "hermote-ready-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socketPath := filepath.Join(dir, "control.sock")
	serverCtx, stopServer := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() { serverDone <- control.Serve(serverCtx, socketPath, readyBackend{launchID: "old"}) }()

	client := control.NewClient(socketPath)
	deadline := time.Now().Add(2 * time.Second)
	for {
		probeCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_, probeErr := client.Status(probeCtx)
		cancel()
		if probeErr == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("control server never became ready: %v", probeErr)
		}
		time.Sleep(10 * time.Millisecond)
	}

	status, err := waitForDaemon(context.Background(), client, "new", 25*time.Millisecond)
	if err == nil {
		t.Fatal("accepted ready status from previous launch generation")
	}
	if status.LaunchID != "old" || !strings.Contains(err.Error(), `generation "old"`) {
		t.Fatalf("status = %+v, err = %v", status, err)
	}

	stopServer()
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestChooseTransport(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  protocol.Transport
	}{
		{"default is direct", "\n", protocol.TransportDirect},
		{"1 is direct", "1\n", protocol.TransportDirect},
		{"word direct", "Direct\n", protocol.TransportDirect},
		{"2 is relay", "2\n", protocol.TransportRelay},
		{"word relay", " relay \n", protocol.TransportRelay},
		{"retries after a bad answer", "maybe\n2\n", protocol.TransportRelay},
		{"three bad answers fall back to direct", "a\nb\nc\n", protocol.TransportDirect},
		{"EOF falls back to direct", "", protocol.TransportDirect},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			got := chooseTransport(strings.NewReader(tc.input), &out, true, true)
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
			if !strings.Contains(out.String(), "Hosted relay") {
				t.Fatalf("prompt should describe the relay option:\n%s", out.String())
			}
		})
	}
}

func TestChooseTransportNonInteractiveIgnoresInput(t *testing.T) {
	got := chooseTransport(strings.NewReader("2\n"), io.Discard, false, true)
	if got != protocol.TransportDirect {
		t.Fatalf("non-interactive run must default to direct, got %q", got)
	}
}

func TestChooseTransportMentionsMissingTailscale(t *testing.T) {
	var out bytes.Buffer
	_ = chooseTransport(strings.NewReader("\n"), &out, true, false)
	if !strings.Contains(out.String(), "Not installed yet") {
		t.Fatalf("prompt should say Tailscale is missing:\n%s", out.String())
	}
}

func TestLaunchdCommandsAreMacOSOnly(t *testing.T) {
	for _, command := range []string{"up", "restart", "stop", "uninstall"} {
		if err := requireMacOSOn(command, "darwin"); err != nil {
			t.Fatalf("%s refused on darwin: %v", command, err)
		}
		err := requireMacOSOn(command, "linux")
		if err == nil || !strings.Contains(err.Error(), command) || !strings.Contains(err.Error(), "manual foreground operation") {
			t.Fatalf("%s linux error = %v, want command and manual-operation guidance", command, err)
		}
	}
}

func TestStableServiceExecutablePrefersCanonicalSymlink(t *testing.T) {
	dir := t.TempDir()
	realBinary := filepath.Join(dir, "Cellar", "hermote-bridge", "0.13.0", "bin", "hermote-bridge")
	if err := os.MkdirAll(filepath.Dir(realBinary), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(realBinary, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	stableDir := filepath.Join(dir, "bin")
	if err := os.MkdirAll(stableDir, 0o755); err != nil {
		t.Fatal(err)
	}
	canonical := filepath.Join(stableDir, "hermote-bridge")
	legacy := filepath.Join(stableDir, "hermes-remote")
	if err := os.Symlink(realBinary, canonical); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("hermote-bridge", legacy); err != nil {
		t.Fatal(err)
	}
	lookPath := func(name string) (string, error) {
		switch name {
		case "hermote-bridge":
			return canonical, nil
		case "hermes-remote":
			return legacy, nil
		default:
			return "", os.ErrNotExist
		}
	}

	got, err := stableServiceExecutableAt(realBinary, "hermes-remote", lookPath)
	if err != nil {
		t.Fatal(err)
	}
	if got != canonical {
		t.Fatalf("service executable = %q, want stable canonical path %q", got, canonical)
	}
}

func TestStableServiceExecutableRejectsUnrelatedPathCandidate(t *testing.T) {
	dir := t.TempDir()
	executable := filepath.Join(dir, "actual", "hermote-bridge")
	unrelated := filepath.Join(dir, "other", "hermote-bridge")
	for _, path := range []string{executable, unrelated} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(path), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	lookPath := func(string) (string, error) { return unrelated, nil }

	got, err := stableServiceExecutableAt(executable, executable, lookPath)
	if err != nil {
		t.Fatal(err)
	}
	if got != executable {
		t.Fatalf("service executable = %q, want executing path %q", got, executable)
	}
}

func TestMigrateLegacyCommandAfterReadiness(t *testing.T) {
	dir := t.TempDir()
	canonical := filepath.Join(dir, "hermote-bridge")
	legacy := filepath.Join(dir, "hermes-remote")
	if err := os.WriteFile(canonical, []byte("canonical"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(legacy, []byte("#!/bin/sh\necho 'hermes-remote 0.12.0'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	migrated, err := migrateLegacyCommand(canonical)
	if err != nil {
		t.Fatal(err)
	}
	if !migrated {
		t.Fatal("recognized legacy executable was not migrated")
	}
	target, err := os.Readlink(legacy)
	if err != nil || target != "hermote-bridge" {
		t.Fatalf("legacy alias = %q, %v", target, err)
	}
}

func TestMigrateRecognizedLegacySymlinkAfterReadiness(t *testing.T) {
	dir := t.TempDir()
	canonical := filepath.Join(dir, "hermote-bridge")
	oldBinary := filepath.Join(dir, "old-hermes-remote")
	legacy := filepath.Join(dir, "hermes-remote")
	if err := os.WriteFile(canonical, []byte("canonical"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(oldBinary, []byte("#!/bin/sh\necho 'hermes-remote 0.12.0'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(oldBinary, legacy); err != nil {
		t.Fatal(err)
	}
	migrated, err := migrateLegacyCommand(canonical)
	if err != nil || !migrated {
		t.Fatalf("recognized legacy symlink was not migrated: migrated=%v err=%v", migrated, err)
	}
	target, err := os.Readlink(legacy)
	if err != nil || target != "hermote-bridge" {
		t.Fatalf("legacy alias = %q, %v", target, err)
	}
}

func TestMigrateLegacyCommandPreservesUnrelatedFileAndSymlink(t *testing.T) {
	for _, fixture := range []struct {
		name  string
		setup func(t *testing.T, path string)
	}{
		{
			name: "unrelated executable",
			setup: func(t *testing.T, path string) {
				t.Helper()
				if err := os.WriteFile(path, []byte("#!/bin/sh\necho unrelated\n"), 0o755); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "user symlink",
			setup: func(t *testing.T, path string) {
				t.Helper()
				target := filepath.Join(filepath.Dir(path), "user-tool")
				if err := os.WriteFile(target, []byte("#!/bin/sh\necho unrelated\n"), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("user-tool", path); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			dir := t.TempDir()
			canonical := filepath.Join(dir, "hermote-bridge")
			legacy := filepath.Join(dir, "hermes-remote")
			if err := os.WriteFile(canonical, []byte("canonical"), 0o755); err != nil {
				t.Fatal(err)
			}
			fixture.setup(t, legacy)
			before, err := os.Lstat(legacy)
			if err != nil {
				t.Fatal(err)
			}
			migrated, err := migrateLegacyCommand(canonical)
			if err != nil || migrated {
				t.Fatalf("unrelated path changed: migrated=%v err=%v", migrated, err)
			}
			after, err := os.Lstat(legacy)
			if err != nil {
				t.Fatal(err)
			}
			if before.Mode() != after.Mode() {
				t.Fatalf("mode changed from %v to %v", before.Mode(), after.Mode())
			}
			if after.Mode()&os.ModeSymlink != 0 {
				target, readErr := os.Readlink(legacy)
				if readErr != nil || target != "user-tool" {
					t.Fatalf("user symlink changed to %q: %v", target, readErr)
				}
			} else {
				content, readErr := os.ReadFile(legacy)
				if readErr != nil || string(content) != "#!/bin/sh\necho unrelated\n" {
					t.Fatalf("unrelated executable changed: %q, %v", content, readErr)
				}
			}
		})
	}
}
