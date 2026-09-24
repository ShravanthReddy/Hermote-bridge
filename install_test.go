package remote_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const canonicalVersion = "0.13.0"

type installerFixture struct {
	dir          string
	binDir       string
	installDir   string
	homeDir      string
	canonical    string
	legacy       string
	checksums    string
	upLog        string
	installer    string
	originalPath string
}

func newInstallerFixture(t *testing.T, version, archiveBinary string) installerFixture {
	t.Helper()
	root := t.TempDir()
	fixture := installerFixture{
		dir: root, binDir: filepath.Join(root, "bin"), installDir: filepath.Join(root, "install"),
		homeDir: filepath.Join(root, "home"), upLog: filepath.Join(root, "up.log"),
		installer: filepath.Join(".", "install.sh"), originalPath: os.Getenv("PATH"),
	}
	for _, dir := range []string{fixture.binDir, fixture.installDir, fixture.homeDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	fixture.canonical = fmt.Sprintf("hermote-bridge_%s_darwin_universal.tar.gz", version)
	fixture.legacy = fmt.Sprintf("hermes-remote_%s_darwin_universal.tar.gz", version)
	fixture.checksums = filepath.Join(root, "checksums.txt")
	writeExecutable(t, filepath.Join(fixture.binDir, "uname"), `#!/bin/sh
if [ "$1" = "-s" ]; then printf '%s\n' "${FAKE_OS:-Darwin}"; else printf '%s\n' "${FAKE_ARCH:-arm64}"; fi
`)
	writeExecutable(t, filepath.Join(fixture.binDir, "curl"), `#!/bin/sh
out=""
url=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    -o) out="$2"; shift 2 ;;
    -*) shift ;;
    *) url="$1"; shift ;;
  esac
done
case "$url" in
  */releases/latest)
    [ -n "$HERMOTE_TEST_LATEST_TAG" ] || exit 22
    printf '{"tag_name":"%s"}\n' "$HERMOTE_TEST_LATEST_TAG" ;;
  */checksums.txt) cp "$HERMOTE_TEST_FIXTURE/checksums.txt" "$out" ;;
  */hermote-bridge_*.tar.gz)
    [ -f "$HERMOTE_TEST_FIXTURE/$(basename "$url")" ] || exit 22
    cp "$HERMOTE_TEST_FIXTURE/$(basename "$url")" "$out" ;;
  */hermes-remote_*.tar.gz)
    [ -f "$HERMOTE_TEST_FIXTURE/$(basename "$url")" ] || exit 22
    cp "$HERMOTE_TEST_FIXTURE/$(basename "$url")" "$out" ;;
  *) exit 22 ;;
esac
`)
	archive := filepath.Join(root, map[bool]string{true: fixture.legacy, false: fixture.canonical}[archiveBinary == "hermes-remote"])
	writeArchive(t, archive, archiveBinary, version)
	digest, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(digest)
	if err := os.WriteFile(fixture.checksums, []byte(fmt.Sprintf("%x  %s\n", sum, filepath.Base(archive))), 0o644); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func writeArchive(t *testing.T, path, binary, version string) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	gzipWriter := gzip.NewWriter(file)
	tarWriter := tar.NewWriter(gzipWriter)
	script := fmt.Sprintf(`#!/bin/sh
case "$1" in
  version) echo "%s %s" ;;
  up)
    if [ "${HERMOTE_TEST_UP_FAIL:-0}" = 1 ]; then exit 42; fi
    echo up >> "$HERMOTE_TEST_UP_LOG" ;;
  restart) echo restart >> "$HERMOTE_TEST_UP_LOG" ;;
  *) echo "%s %s" ;;
esac
`, binary, version, binary, version)
	if err := tarWriter.WriteHeader(&tar.Header{Name: binary, Mode: 0o755, Size: int64(len(script))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tarWriter.Write([]byte(script)); err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeExecutable(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
}

func (fixture installerFixture) run(t *testing.T, extra ...string) (string, error) {
	t.Helper()
	command := exec.Command("bash", fixture.installer)
	command.Dir = "."
	command.Env = append(os.Environ(),
		"PATH="+fixture.binDir+string(os.PathListSeparator)+fixture.originalPath,
		"HOME="+fixture.homeDir,
		"HERMOTE_TEST_FIXTURE="+fixture.dir,
		"HERMOTE_TEST_UP_LOG="+fixture.upLog,
		"HERMOTE_BRIDGE_INSTALL_DIR="+fixture.installDir,
	)
	command.Env = append(command.Env, extra...)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	err := command.Run()
	return output.String(), err
}

func (fixture installerFixture) runTTY(t *testing.T, extra ...string) (string, error) {
	t.Helper()
	command := exec.Command("script", "-q", "/dev/null", "bash", fixture.installer)
	command.Dir = "."
	command.Env = append(os.Environ(),
		"PATH="+fixture.binDir+string(os.PathListSeparator)+fixture.originalPath,
		"HOME="+fixture.homeDir,
		"HERMOTE_TEST_FIXTURE="+fixture.dir,
		"HERMOTE_TEST_UP_LOG="+fixture.upLog,
		"HERMOTE_BRIDGE_INSTALL_DIR="+fixture.installDir,
	)
	command.Env = append(command.Env, extra...)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	err := command.Run()
	return output.String(), err
}

func TestInstallerFreshInstallAndCompatibilityAlias(t *testing.T) {
	fixture := newInstallerFixture(t, canonicalVersion, "hermote-bridge")
	output, err := fixture.run(t, "HERMOTE_BRIDGE_VERSION=v"+canonicalVersion)
	if err != nil {
		t.Fatalf("installer failed: %v\n%s", err, output)
	}
	canonical := filepath.Join(fixture.installDir, "hermote-bridge")
	if info, err := os.Stat(canonical); err != nil || info.Mode()&0o111 == 0 {
		t.Fatalf("canonical executable not installed: %v", err)
	}
	legacy := filepath.Join(fixture.installDir, "hermes-remote")
	target, err := os.Readlink(legacy)
	if err != nil || target != "hermote-bridge" {
		t.Fatalf("legacy alias = %q, %v", target, err)
	}
	if _, err := os.Stat(fixture.upLog); !os.IsNotExist(err) {
		t.Fatalf("non-TTY install unexpectedly ran up: %v", err)
	}
	if !strings.Contains(output, "Next: "+canonical+" up") {
		t.Fatalf("non-TTY next step missing:\n%s", output)
	}
}

func TestInstallerNoUpAndNewEnvironmentWins(t *testing.T) {
	fixture := newInstallerFixture(t, canonicalVersion, "hermote-bridge")
	output, err := fixture.run(t,
		"HERMOTE_BRIDGE_VERSION=v"+canonicalVersion,
		"HERMES_REMOTE_VERSION=v0.12.0",
		"HERMOTE_BRIDGE_NO_UP=1",
		"HERMES_REMOTE_NO_UP=0",
	)
	if err != nil {
		t.Fatalf("installer failed: %v\n%s", err, output)
	}
	if _, err := os.Stat(fixture.upLog); !os.IsNotExist(err) {
		t.Fatalf("NO_UP unexpectedly ran up: %v", err)
	}
}

func TestInstallerPinnedLegacyArchiveFallback(t *testing.T) {
	fixture := newInstallerFixture(t, "0.12.0", "hermes-remote")
	output, err := fixture.run(t, "HERMES_REMOTE_VERSION=v0.12.0", "HERMES_REMOTE_NO_UP=1")
	if err != nil {
		t.Fatalf("legacy fallback failed: %v\n%s", err, output)
	}
	if !strings.Contains(output, "archive published for pinned release") {
		t.Fatalf("legacy fallback was not disclosed:\n%s", output)
	}
	if _, err := os.Stat(filepath.Join(fixture.installDir, "hermote-bridge")); err != nil {
		t.Fatal(err)
	}
}

func TestInstallerLatestReleaseDoesNotFallBackToLegacyArchive(t *testing.T) {
	fixture := newInstallerFixture(t, canonicalVersion, "hermes-remote")
	output, err := fixture.run(t, "HERMOTE_TEST_LATEST_TAG=v"+canonicalVersion)
	if err == nil || !strings.Contains(output, "canonical release asset is missing") {
		t.Fatalf("latest release unexpectedly accepted a legacy archive: %v\n%s", err, output)
	}
	if _, statErr := os.Stat(filepath.Join(fixture.installDir, "hermote-bridge")); !os.IsNotExist(statErr) {
		t.Fatalf("latest release installed a binary after canonical asset failure: %v", statErr)
	}
}

func TestInstallerPinnedRenamedReleaseDoesNotFallBackToLegacyArchive(t *testing.T) {
	fixture := newInstallerFixture(t, canonicalVersion, "hermes-remote")
	output, err := fixture.run(t, "HERMOTE_BRIDGE_VERSION=v"+canonicalVersion)
	if err == nil || !strings.Contains(output, "canonical release asset is missing") {
		t.Fatalf("renamed release unexpectedly accepted a legacy archive: %v\n%s", err, output)
	}
}

func TestInstallerSetupFailureRestoresRecognizedPreviousBinary(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("the guided setup path is macOS-only")
	}
	fixture := newInstallerFixture(t, canonicalVersion, "hermote-bridge")
	canonical := filepath.Join(fixture.installDir, "hermote-bridge")
	writeExecutable(t, canonical, `#!/bin/sh
case "$1" in
  version) echo "hermote-bridge 0.12.0" ;;
  restart) echo previous-restart >> "$HERMOTE_TEST_UP_LOG" ;;
esac
`)
	output, err := fixture.runTTY(t,
		"HERMOTE_BRIDGE_VERSION=v"+canonicalVersion,
		"HERMOTE_TEST_UP_FAIL=1",
	)
	if err == nil || !strings.Contains(output, "restored the previous hermote-bridge executable") {
		t.Fatalf("setup failure did not report restoration: %v\n%s", err, output)
	}
	versionOutput, versionErr := exec.Command(canonical, "version").CombinedOutput()
	if versionErr != nil || strings.TrimSpace(string(versionOutput)) != "hermote-bridge 0.12.0" {
		t.Fatalf("previous binary was not restored: %v, %q", versionErr, versionOutput)
	}
	restartLog, readErr := os.ReadFile(fixture.upLog)
	if readErr != nil || !strings.Contains(string(restartLog), "previous-restart") {
		t.Fatalf("previous service restart not attempted: %v, %q", readErr, restartLog)
	}
}

func TestInstallerSuccessfulSetupMigratesRecognizedLegacyCommand(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("the guided setup path is macOS-only")
	}
	fixture := newInstallerFixture(t, canonicalVersion, "hermote-bridge")
	legacy := filepath.Join(fixture.installDir, "hermes-remote")
	writeExecutable(t, legacy, "#!/bin/sh\necho 'hermes-remote 0.12.0'\n")
	output, err := fixture.runTTY(t, "HERMOTE_BRIDGE_VERSION=v"+canonicalVersion)
	if err != nil {
		t.Fatalf("installer failed: %v\n%s", err, output)
	}
	target, readErr := os.Readlink(legacy)
	if readErr != nil || target != "hermote-bridge" {
		t.Fatalf("recognized legacy command was not migrated: target=%q err=%v\n%s", target, readErr, output)
	}
}

func TestInstallerChecksumFailureLeavesExistingBinary(t *testing.T) {
	fixture := newInstallerFixture(t, canonicalVersion, "hermote-bridge")
	canonical := filepath.Join(fixture.installDir, "hermote-bridge")
	writeExecutable(t, canonical, "previous binary\n")
	if err := os.WriteFile(fixture.checksums, []byte(strings.Repeat("0", 64)+"  "+fixture.canonical+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	output, err := fixture.run(t, "HERMOTE_BRIDGE_VERSION=v"+canonicalVersion, "HERMOTE_BRIDGE_NO_UP=1")
	if err == nil || !strings.Contains(output, "checksum mismatch") {
		t.Fatalf("checksum failure not reported: %v\n%s", err, output)
	}
	content, readErr := os.ReadFile(canonical)
	if readErr != nil || string(content) != "previous binary\n" {
		t.Fatalf("existing binary changed after checksum failure: %q, %v", content, readErr)
	}
}

func TestInstallerPreservesUnrelatedLegacyFile(t *testing.T) {
	fixture := newInstallerFixture(t, canonicalVersion, "hermote-bridge")
	legacy := filepath.Join(fixture.installDir, "hermes-remote")
	writeExecutable(t, legacy, "unrelated\n")
	output, err := fixture.run(t, "HERMOTE_BRIDGE_VERSION=v"+canonicalVersion, "HERMOTE_BRIDGE_NO_UP=1")
	if err != nil {
		t.Fatalf("installer failed: %v\n%s", err, output)
	}
	content, readErr := os.ReadFile(legacy)
	if readErr != nil || string(content) != "unrelated\n" {
		t.Fatalf("unrelated legacy file changed: %q, %v", content, readErr)
	}
	if !strings.Contains(output, "Preserved existing") {
		t.Fatalf("preservation warning missing:\n%s", output)
	}
}

func TestInstallerRejectsUnsupportedArchitectureBeforeDownload(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("bash installer is not supported on Windows")
	}
	fixture := newInstallerFixture(t, canonicalVersion, "hermote-bridge")
	output, err := fixture.run(t, "FAKE_OS=Linux", "FAKE_ARCH=mips64", "HERMOTE_BRIDGE_VERSION=v"+canonicalVersion)
	if err == nil || !strings.Contains(output, "unsupported Linux architecture") {
		t.Fatalf("unsupported architecture not rejected: %v\n%s", err, output)
	}
}
