package update

import (
	"os"
	"path/filepath"
	"testing"
)

const testInstallURL = "https://raw.githubusercontent.com/ShravanthReddy/Hermote-bridge/main/install.sh"

// installBinary writes an executable at dir/hermote-bridge inside a 0755 directory.
func installBinary(t *testing.T, dir string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "hermote-bridge")
	if err := os.WriteFile(path, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func capture(t *testing.T, path string) *Identity {
	t.Helper()
	id, err := captureIdentity(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = id.Close() })
	return id
}

func service(executable string) Environment {
	return Environment{Version: "0.15.0", GOOS: "darwin", UID: os.Getuid(), Managed: true, ServiceExecutable: executable}
}

func TestHomebrewDetectedFromCellarPath(t *testing.T) {
	prefix := filepath.Join(t.TempDir(), "homebrew")
	cellar := installBinary(t, filepath.Join(prefix, "Cellar", "hermote-bridge", "0.15.0", "bin"))
	if err := os.MkdirAll(filepath.Join(prefix, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(prefix, "bin", "brew"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	linked := filepath.Join(prefix, "bin", "hermote-bridge")
	if err := os.Symlink(cellar, linked); err != nil {
		t.Fatal(err)
	}
	got := DetectInstall(capture(t, linked), service(linked))
	if got.Method != MethodHomebrew || got.Blocker != ReasonHomebrew {
		t.Fatalf("a Cellar executable with brew beside it is %+v", got)
	}
	if got.UpdateCommand != "brew upgrade hermote-bridge && hermote-bridge restart" {
		t.Fatalf("homebrew command %q", got.UpdateCommand)
	}
	if got.Path != linked {
		t.Fatalf("path %q, want the service's %q", got.Path, linked)
	}

	// Without brew under the prefix the Cellar layout alone is not Homebrew.
	if err := os.Remove(filepath.Join(prefix, "bin", "brew")); err != nil {
		t.Fatal(err)
	}
	if got := DetectInstall(capture(t, linked), service(linked)); got.Method == MethodHomebrew {
		t.Fatalf("no brew, still reported Homebrew: %+v", got)
	}
}

func TestManualInstallInLocalBin(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "home", ".local", "bin")
	path := installBinary(t, dir)
	got := DetectInstall(capture(t, path), service(path))
	if got.Method != MethodManual || got.Blocker != "" || got.Path != path {
		t.Fatalf("a script install in a safe directory is %+v", got)
	}
	if canApply, reason := got.Availability(); canApply || reason != ReasonNotSupported {
		t.Fatalf("phase 1 reports %v %q, want no update engine", canApply, reason)
	}
}

func TestServiceMismatchWhenPlistRunsAnotherFile(t *testing.T) {
	running := installBinary(t, filepath.Join(t.TempDir(), "old"))
	configured := installBinary(t, filepath.Join(t.TempDir(), "new"))
	got := DetectInstall(capture(t, running), service(configured))
	if got.Blocker != ReasonServiceMismatch {
		t.Fatalf("the LaunchAgent runs another file, got %+v", got)
	}
}

func TestSymlinkedStablePathCannotApply(t *testing.T) {
	root := t.TempDir()
	real := installBinary(t, filepath.Join(root, "real"))
	stable := filepath.Join(root, "bin", "hermote-bridge")
	if err := os.MkdirAll(filepath.Dir(stable), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real, stable); err != nil {
		t.Fatal(err)
	}
	got := DetectInstall(capture(t, stable), service(stable))
	if got.Method != MethodManual || got.Blocker != ReasonSymlinked {
		t.Fatalf("a symlinked stable path is %+v", got)
	}
}

func TestGroupWritableDirectoryIsUnsafe(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "shared")
	path := installBinary(t, dir)
	if err := os.Chmod(dir, 0o775); err != nil {
		t.Fatal(err)
	}
	got := DetectInstall(capture(t, path), service(path))
	if got.Blocker != ReasonUnsafeDirectory {
		t.Fatalf("a group-writable directory is %+v", got)
	}
}

func TestDevelopmentBuild(t *testing.T) {
	path := installBinary(t, filepath.Join(t.TempDir(), "bin"))
	env := service(path)
	env.Version = "dev"
	got := DetectInstall(capture(t, path), env)
	if got.Method != MethodDevelopment || got.Blocker != ReasonDevelopmentBuild {
		t.Fatalf("a dev build is %+v", got)
	}
}

func TestUpdateCommandQuotesInstallDirOnBashSide(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "it's my bin")
	path := installBinary(t, dir)
	got := DetectInstall(capture(t, path), service(path))
	quoted := "'" + filepath.Dir(dir) + "/it'\\''s my bin'"
	want := "curl -fsSL " + testInstallURL + " | HERMOTE_BRIDGE_INSTALL_DIR=" + quoted + " bash"
	if got.UpdateCommand != want {
		t.Fatalf("command\n%s\nwant\n%s", got.UpdateCommand, want)
	}
}

func TestUpdateCommandNamesStandardDirectoriesToo(t *testing.T) {
	// install.sh prefers a writable /usr/local/bin, so a bare command would
	// install a second copy there instead of replacing ~/.local/bin's.
	dir := filepath.Join(t.TempDir(), "Users", "x", ".local", "bin")
	path := installBinary(t, dir)
	got := DetectInstall(capture(t, path), service(path))
	want := "curl -fsSL " + testInstallURL + " | HERMOTE_BRIDGE_INSTALL_DIR='" + dir + "' bash"
	if got.UpdateCommand != want {
		t.Fatalf("command %q, want %q", got.UpdateCommand, want)
	}
}
