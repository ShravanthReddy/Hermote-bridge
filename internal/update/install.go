package update

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
)

// InstallScriptURL is the installer the manual update command runs
// (remote/install.sh on the public mirror).
const InstallScriptURL = "https://raw.githubusercontent.com/ShravanthReddy/Hermote-bridge/main/install.sh"

// HomebrewUpdateCommand is shown for Homebrew installs; the bridge never runs
// brew itself (owner decision 3, D6).
const HomebrewUpdateCommand = "brew upgrade hermote-bridge && hermote-bridge restart"

// Method is how Hermote Bridge was installed (§3, install.method).
type Method string

const (
	MethodManual      Method = "manual"
	MethodHomebrew    Method = "homebrew"
	MethodDevelopment Method = "development"
	MethodUnknown     Method = "unknown"
)

// Reason says why an install cannot be updated from the phone (§3,
// update.unavailable).
type Reason string

const (
	// ReasonNotSupported: this bridge has no update engine enabled.
	ReasonNotSupported        Reason = "not_supported"
	ReasonHomebrew            Reason = "homebrew"
	ReasonUnsupportedPlatform Reason = "unsupported_platform"
	ReasonNoService           Reason = "no_service"
	ReasonServiceMismatch     Reason = "service_mismatch"
	ReasonSymlinked           Reason = "symlinked"
	ReasonUnsafeDirectory     Reason = "unsafe_directory"
	ReasonDevelopmentBuild    Reason = "development_build"
)

// Environment is what the daemon knows about how it was started.
type Environment struct {
	// Version is main.version; "dev" for a build without a release version.
	Version string
	GOOS    string
	UID     int
	// Managed is true when the service manager started this process
	// (`daemon --launch-id`), false for a foreground daemon.
	Managed bool
	// ServiceExecutable is the LaunchAgent's ProgramArguments[0]; empty when
	// no LaunchAgent is installed.
	ServiceExecutable string
}

// Install describes the running bridge's installation (D6).
type Install struct {
	Method Method
	// Path is the stable path the service runs, else the running executable.
	Path string
	// Blocker is D6's reason this install could not be replaced from the
	// phone; empty when nothing about the install stands in the way.
	Blocker Reason
	// UpdateCommand is what the person runs on the Mac to update.
	UpdateCommand string
}

// cellarExecutable matches a Homebrew keg's binary; the first group is the prefix.
var cellarExecutable = regexp.MustCompile(`^(.+)/Cellar/hermote-bridge/[^/]+/bin/hermote-bridge$`)

// DetectInstall classifies the running bridge's installation per D6.
func DetectInstall(identity *Identity, env Environment) Install {
	resolved, err := filepath.EvalSymlinks(identity.Path)
	if err != nil {
		resolved = identity.Path
	}
	install := Install{Method: MethodManual, Path: identity.Path}
	if env.ServiceExecutable != "" {
		install.Path = env.ServiceExecutable
	}
	switch {
	case env.Version == "dev":
		install.Method = MethodDevelopment
	case isHomebrewKeg(resolved):
		install.Method = MethodHomebrew
	}
	install.Blocker = blocker(identity, install, env)
	if install.Method == MethodHomebrew {
		install.UpdateCommand = HomebrewUpdateCommand
	} else {
		install.UpdateCommand = ManualUpdateCommand(filepath.Dir(install.Path))
	}
	return install
}

// Availability is whether the phone may update this install now and, if
// not, why. Phase 1 bridges have no update engine, so every install reports
// not_supported whatever its Blocker (§4, B1.1).
func (Install) Availability() (canApply bool, unavailable Reason) {
	return false, ReasonNotSupported
}

// ManualUpdateCommand runs the installer into dir. The directory is always
// named because install.sh prefers a writable /usr/local/bin and would
// otherwise install a second copy (remote/install.sh:85-92); the assignment
// sits on bash's side of the pipe so the installer sees it.
func ManualUpdateCommand(dir string) string {
	return "curl -fsSL " + InstallScriptURL + " | HERMOTE_BRIDGE_INSTALL_DIR=" + shellQuote(dir) + " bash"
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

func isHomebrewKeg(resolved string) bool {
	match := cellarExecutable.FindStringSubmatch(resolved)
	if match == nil {
		return false
	}
	info, err := os.Stat(filepath.Join(match[1], "bin", "brew"))
	return err == nil && info.Mode().IsRegular()
}

func blocker(identity *Identity, install Install, env Environment) Reason {
	switch {
	case env.GOOS != "darwin":
		return ReasonUnsupportedPlatform
	case install.Method == MethodDevelopment:
		return ReasonDevelopmentBuild
	case install.Method == MethodHomebrew:
		return ReasonHomebrew
	case !env.Managed || env.ServiceExecutable == "":
		return ReasonNoService
	case !identity.SameFile(env.ServiceExecutable):
		return ReasonServiceMismatch
	}
	info, err := os.Lstat(env.ServiceExecutable)
	if err != nil {
		return ReasonServiceMismatch
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return ReasonSymlinked
	}
	if !safeLocation(env.ServiceExecutable, info, env.UID) {
		return ReasonUnsafeDirectory
	}
	return ""
}

// safeLocation applies the privilege rules: a regular file owned by the
// user, in a directory the user owns and can write, that group and others
// cannot write.
func safeLocation(path string, info os.FileInfo, uid int) bool {
	if !info.Mode().IsRegular() || !ownedBy(info, uid) {
		return false
	}
	dir := filepath.Dir(path)
	dirInfo, err := os.Stat(dir)
	if err != nil || !dirInfo.IsDir() || !ownedBy(dirInfo, uid) || dirInfo.Mode().Perm()&0o022 != 0 {
		return false
	}
	return syscall.Access(dir, 0o2) == nil
}

func ownedBy(info os.FileInfo, uid int) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(stat.Uid) == uid
}
