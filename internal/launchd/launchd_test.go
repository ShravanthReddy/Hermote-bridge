package launchd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type fakeLaunchctl struct {
	loaded           bool
	pid              int
	failNewBootstrap bool
	commands         []string
}

func (f *fakeLaunchctl) run(_ context.Context, name string, args ...string) ([]byte, error) {
	f.commands = append(f.commands, name+" "+strings.Join(args, " "))
	if name != "launchctl" || len(args) == 0 {
		return nil, errors.New("unexpected command")
	}
	switch args[0] {
	case "print":
		if f.loaded {
			if f.pid > 0 {
				return []byte("pid = " + fmt.Sprint(f.pid) + "\n"), nil
			}
			return nil, nil
		}
		return []byte("not found"), errors.New("not loaded")
	case "bootout":
		f.loaded = false
		return nil, nil
	case "bootstrap":
		content, err := os.ReadFile(args[len(args)-1])
		if err != nil {
			return []byte(err.Error()), err
		}
		if f.failNewBootstrap && strings.Contains(string(content), "/new/hermote-bridge") {
			return []byte("new service rejected"), errors.New("bootstrap failed")
		}
		f.loaded = true
		return nil, nil
	case "kickstart":
		if !f.loaded {
			return []byte("not loaded"), errors.New("kickstart failed")
		}
		return nil, nil
	default:
		return nil, errors.New("unexpected launchctl operation")
	}
}

func testManager(home string, launchctl *fakeLaunchctl) serviceManager {
	return serviceManager{
		homeDir:      func() (string, error) { return home, nil },
		uid:          func() int { return 501 },
		run:          launchctl.run,
		sleep:        func(context.Context, time.Duration) error { return nil },
		processAlive: func(int) bool { return false },
	}
}

func testSpec(binary, home string) Spec {
	return Spec{
		Binary: binary, LaunchID: "test-launch", HermesHome: filepath.Join(home, ".hermes"),
		LogDir: filepath.Join(home, ".hermes", "logs"), Path: "/usr/bin:/bin",
	}
}

func TestInstallWaitsForPreviousProcessExit(t *testing.T) {
	home := t.TempDir()
	fake := &fakeLaunchctl{loaded: true, pid: 42}
	manager := testManager(home, fake)
	checks := 0
	manager.processAlive = func(pid int) bool {
		if pid != 42 {
			t.Fatalf("checked unexpected pid %d", pid)
		}
		checks++
		return checks < 3
	}
	writeOldPlist(t, manager, home)

	if _, err := manager.install(context.Background(), testSpec("/new/hermote-bridge", home)); err != nil {
		t.Fatal(err)
	}
	if checks != 3 {
		t.Fatalf("process exit checks = %d, want 3", checks)
	}
	bootstrapIndex := -1
	bootoutIndex := -1
	for index, command := range fake.commands {
		if strings.Contains(command, " bootout ") {
			bootoutIndex = index
		}
		if strings.Contains(command, " bootstrap ") && bootstrapIndex == -1 {
			bootstrapIndex = index
		}
	}
	if bootoutIndex == -1 || bootstrapIndex <= bootoutIndex {
		t.Fatalf("bootstrap did not follow bootout and process-exit wait: %v", fake.commands)
	}
}

func TestRestoreDoesNotBootstrapWhilePreviousProcessIsAlive(t *testing.T) {
	home := t.TempDir()
	fake := &fakeLaunchctl{loaded: true, pid: 42}
	manager := testManager(home, fake)
	manager.processAlive = func(pid int) bool { return pid == 42 }
	writeOldPlist(t, manager, home)

	ctx, cancel := context.WithCancel(context.Background())
	manager.sleep = func(context.Context, time.Duration) error {
		cancel()
		return context.Canceled
	}
	if _, err := manager.install(ctx, testSpec("/new/hermote-bridge", home)); err == nil {
		t.Fatal("upgrade unexpectedly succeeded while previous process remained alive")
	}
	for _, command := range fake.commands {
		if strings.Contains(command, " bootstrap ") {
			t.Fatalf("restored service bootstrapped over live previous process: %v", fake.commands)
		}
	}
}

func writeOldPlist(t *testing.T, manager serviceManager, home string) (string, []byte) {
	t.Helper()
	path, err := manager.plistPath()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	oldExecutable := filepath.Join(home, "old", "hermes-remote")
	if err := os.MkdirAll(filepath.Dir(oldExecutable), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(oldExecutable, []byte("old bridge"), 0o755); err != nil {
		t.Fatal(err)
	}
	old := []byte(plist(testSpec(oldExecutable, home)))
	if err := os.WriteFile(path, old, 0o644); err != nil {
		t.Fatal(err)
	}
	return path, old
}

func TestInstallReusesStableLabelAndCanCommit(t *testing.T) {
	home := t.TempDir()
	fake := &fakeLaunchctl{loaded: true}
	manager := testManager(home, fake)
	path, _ := writeOldPlist(t, manager, home)

	transition, err := manager.install(context.Background(), testSpec("/new/hermote-bridge", home))
	if err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(content), "/new/hermote-bridge") || !strings.Contains(string(content), Label) {
		t.Fatalf("new plist did not retain label and canonical binary:\n%s", content)
	}
	if !fake.loaded {
		t.Fatal("replacement service is not loaded")
	}
	transition.Commit()
	if err := transition.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	content, _ = os.ReadFile(path)
	if strings.Contains(string(content), filepath.Join(home, "old", "hermes-remote")) {
		t.Fatal("committed transition rolled back")
	}
}

func TestInstallBootstrapStartsReplacementOnce(t *testing.T) {
	home := t.TempDir()
	fake := &fakeLaunchctl{loaded: true}
	manager := testManager(home, fake)
	writeOldPlist(t, manager, home)

	if _, err := manager.install(context.Background(), testSpec("/new/hermote-bridge", home)); err != nil {
		t.Fatal(err)
	}
	bootstraps, kickstarts := 0, 0
	for _, command := range fake.commands {
		if strings.Contains(command, " bootstrap ") {
			bootstraps++
		}
		if strings.Contains(command, " kickstart ") {
			kickstarts++
		}
	}
	if bootstraps != 1 || kickstarts != 0 {
		t.Fatalf("replacement starts = bootstrap %d, kickstart %d; commands: %v", bootstraps, kickstarts, fake.commands)
	}
}

func TestReadinessRollbackRestoresOldPlistAndLoadedState(t *testing.T) {
	home := t.TempDir()
	fake := &fakeLaunchctl{loaded: true}
	manager := testManager(home, fake)
	path, old := writeOldPlist(t, manager, home)

	transition, err := manager.install(context.Background(), testSpec("/new/hermote-bridge", home))
	if err != nil {
		t.Fatal(err)
	}
	if err := transition.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != string(old) || !fake.loaded {
		t.Fatalf("old service not restored: loaded=%v\n%s", fake.loaded, content)
	}
	bootstraps, kickstarts := 0, 0
	for _, command := range fake.commands {
		if strings.Contains(command, " bootstrap ") {
			bootstraps++
		}
		if strings.Contains(command, " kickstart ") {
			kickstarts++
		}
	}
	if bootstraps != 2 || kickstarts != 0 {
		t.Fatalf("upgrade and rollback starts = bootstrap %d, kickstart %d; commands: %v", bootstraps, kickstarts, fake.commands)
	}
}

func TestBootstrapFailureRestoresOldService(t *testing.T) {
	home := t.TempDir()
	fake := &fakeLaunchctl{loaded: true, failNewBootstrap: true}
	manager := testManager(home, fake)
	path, old := writeOldPlist(t, manager, home)

	if _, err := manager.install(context.Background(), testSpec("/new/hermote-bridge", home)); err == nil {
		t.Fatal("bootstrap failure unexpectedly succeeded")
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != string(old) || !fake.loaded {
		t.Fatalf("bootstrap failure did not restore old service: loaded=%v\n%s", fake.loaded, content)
	}
}

func TestRollbackRestoresMissingExecutablePlistButLeavesJobUnloaded(t *testing.T) {
	home := t.TempDir()
	fake := &fakeLaunchctl{loaded: true}
	manager := testManager(home, fake)
	path, old := writeOldPlist(t, manager, home)
	oldExecutable := filepath.Join(home, "old", "hermes-remote")

	transition, err := manager.install(context.Background(), testSpec("/new/hermote-bridge", home))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(oldExecutable); err != nil {
		t.Fatal(err)
	}
	err = transition.Rollback(context.Background())
	if err == nil || !strings.Contains(err.Error(), "was restored but remains unloaded") || !strings.Contains(err.Error(), oldExecutable) {
		t.Fatalf("rollback error = %v, want explicit missing-executable recovery message", err)
	}
	content, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(content) != string(old) {
		t.Fatal("rollback did not restore the previous plist")
	}
	if fake.loaded {
		t.Fatal("rollback loaded a LaunchAgent whose executable was missing")
	}
}

func TestProgramExecutableDecodesEscapedPath(t *testing.T) {
	want := "/Applications/Hermes & Hermote/bin/hermote-bridge"
	got, err := programExecutable([]byte(plist(testSpec(want, t.TempDir()))))
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("programExecutable = %q, want %q", got, want)
	}
}

func TestFreshInstallRollbackRemovesPlist(t *testing.T) {
	home := t.TempDir()
	fake := &fakeLaunchctl{}
	manager := testManager(home, fake)
	path, _ := manager.plistPath()

	transition, err := manager.install(context.Background(), testSpec("/new/hermote-bridge", home))
	if err != nil {
		t.Fatal(err)
	}
	if err := transition.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fresh-install plist still exists after rollback: %v", err)
	}
	if fake.loaded {
		t.Fatal("fresh-install service still loaded after rollback")
	}
}

func TestLoadedJobWithoutPlistRefusesUnsafeUpgrade(t *testing.T) {
	home := t.TempDir()
	fake := &fakeLaunchctl{loaded: true}
	manager := testManager(home, fake)
	if _, err := manager.install(context.Background(), testSpec("/new/hermote-bridge", home)); err == nil {
		t.Fatal("unsafe upgrade unexpectedly succeeded")
	}
	if len(fake.commands) != 1 || !strings.Contains(fake.commands[0], "launchctl print") {
		t.Fatalf("unsafe upgrade mutated launchd: %v", fake.commands)
	}
}

func TestInstallRequiresLaunchID(t *testing.T) {
	home := t.TempDir()
	fake := &fakeLaunchctl{}
	manager := testManager(home, fake)
	spec := testSpec("/new/hermote-bridge", home)
	spec.LaunchID = ""
	if _, err := manager.install(context.Background(), spec); err == nil {
		t.Fatal("install without a launch ID unexpectedly succeeded")
	}
	if len(fake.commands) != 0 {
		t.Fatalf("invalid install mutated launchd: %v", fake.commands)
	}
}
