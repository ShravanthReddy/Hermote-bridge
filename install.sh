#!/usr/bin/env bash
# hermote-bridge installer — the one line Hermote shows:
#   curl -fsSL https://raw.githubusercontent.com/ShravanthReddy/Hermote-bridge/main/install.sh | bash
#
# Downloads a release, verifies its SHA-256, stages the canonical executable,
# and uses a controlling terminal for interactive macOS setup when one exists.
#
# Options (new names take precedence over compatibility names):
#   HERMOTE_BRIDGE_VERSION=vX.Y.Z  HERMOTE_BRIDGE_NO_UP=1
#   HERMOTE_BRIDGE_REPO=owner/repo HERMOTE_BRIDGE_INSTALL_DIR=/absolute/path
#   HERMES_REMOTE_VERSION / HERMES_REMOTE_NO_UP / HERMES_REMOTE_REPO (legacy)
set -euo pipefail

REPO="${HERMOTE_BRIDGE_REPO:-${HERMES_REMOTE_REPO:-ShravanthReddy/Hermote-bridge}}"
PINNED_VERSION="${HERMOTE_BRIDGE_VERSION:-${HERMES_REMOTE_VERSION:-}}"
NO_UP="${HERMOTE_BRIDGE_NO_UP:-${HERMES_REMOTE_NO_UP:-0}}"
BIN=hermote-bridge
LEGACY_BIN=hermes-remote

say() { printf '  \033[32m✓\033[0m %s\n' "$*"; }
warn() { printf '  \033[33m!\033[0m %s\n' "$*" >&2; }
die() { printf '  \033[31m✗\033[0m %s\n' "$*" >&2; exit 1; }

os=$(uname -s | tr '[:upper:]' '[:lower:]')
arch=$(uname -m)
case "$os" in
    darwin) asset_arch=universal ;;
    linux) case "$arch" in x86_64) asset_arch=amd64 ;; aarch64 | arm64) asset_arch=arm64 ;; *) die "unsupported Linux architecture $arch" ;; esac ;;
    *) die "hermote-bridge releases support macOS and Linux downloads (got $os)" ;;
esac

command -v curl >/dev/null || die "curl is required"
sha() { if command -v shasum >/dev/null; then shasum -a 256 "$1" | cut -d' ' -f1; else sha256sum "$1" | cut -d' ' -f1; fi; }

api="https://api.github.com/repos/$REPO/releases"
if [[ -n "$PINNED_VERSION" ]]; then
    tag="$PINNED_VERSION"
else
    tag=$(curl -fsSL "$api/latest" | sed -nE 's/.*"tag_name": *"([^"]+)".*/\1/p' | head -1)
    [[ -n "$tag" ]] || die "could not determine the latest release of $REPO"
fi
[[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "release version must look like v1.2.3 (got $tag)"
version="${tag#v}"
base="https://github.com/$REPO/releases/download/$tag"

tmp=$(mktemp -d)
stage=""
backup=""
previous_binary_recognized=0
cleanup() {
    if [[ -n "$stage" ]]; then rm -f "$stage"; fi
    if [[ -n "$backup" && ( -e "$backup" || -L "$backup" ) ]]; then
        if [[ ! -e "$dest/$BIN" && ! -L "$dest/$BIN" ]]; then mv "$backup" "$dest/$BIN"; else rm -f "$backup"; fi
    fi
    rm -rf "$tmp"
}
trap cleanup EXIT

asset="${BIN}_${version}_${os}_${asset_arch}.tar.gz"
archive_binary="$BIN"
if ! curl -fsSL -o "$tmp/$asset" "$base/$asset"; then
    rm -f "$tmp/$asset"
    legacy_archive_allowed=0
    if [[ -n "$PINNED_VERSION" && "$version" =~ ^0\.([0-9]+)\.[0-9]+$ ]] && (( 10#${BASH_REMATCH[1]} < 13 )); then
        legacy_archive_allowed=1
    fi
    if [[ "$legacy_archive_allowed" != 1 ]]; then
        die "canonical release asset is missing: $base/$asset"
    fi
    legacy_asset="${LEGACY_BIN}_${version}_${os}_${asset_arch}.tar.gz"
    warn "Pinned release $tag predates the rename; using its legacy archive name."
    curl -fsSL -o "$tmp/$legacy_asset" "$base/$legacy_asset" || die "download failed: $base/$legacy_asset"
    asset="$legacy_asset"
    archive_binary="$LEGACY_BIN"
fi
curl -fsSL -o "$tmp/checksums.txt" "$base/checksums.txt" || die "checksums.txt missing from the release"
expected=$(awk -v file="$asset" '$2 == file { print $1; exit }' "$tmp/checksums.txt")
[[ -n "$expected" ]] || die "no checksum listed for $asset"
actual=$(sha "$tmp/$asset")
[[ "$expected" == "$actual" ]] || die "checksum mismatch for $asset (expected $expected, got $actual)"
say "Checksum verified"

tar -xzf "$tmp/$asset" -C "$tmp"
[[ -x "$tmp/$archive_binary" ]] || die "archive did not contain $archive_binary"

if [[ -n "${HERMOTE_BRIDGE_INSTALL_DIR:-}" ]]; then
    case "$HERMOTE_BRIDGE_INSTALL_DIR" in /*) dest="$HERMOTE_BRIDGE_INSTALL_DIR" ;; *) die "HERMOTE_BRIDGE_INSTALL_DIR must be absolute" ;; esac
    mkdir -p "$dest"
elif [[ -w /usr/local/bin ]]; then
    dest=/usr/local/bin
else
    dest="$HOME/.local/bin"
    mkdir -p "$dest"
fi

stage="$dest/.${BIN}.new.$$"
install -m 0755 "$tmp/$archive_binary" "$stage"
"$stage" version >/dev/null || die "downloaded executable failed its version check"
if [[ -e "$dest/$BIN" || -L "$dest/$BIN" ]]; then
    previous_version=$("$dest/$BIN" version 2>/dev/null || true)
    if [[ "$previous_version" =~ ^(hermote-bridge|hermes-remote)[[:space:]](v?[0-9]+\.[0-9]+\.[0-9]+|dev)$ ]]; then
        previous_binary_recognized=1
    fi
    backup="$dest/.${BIN}.previous.$$"
    mv "$dest/$BIN" "$backup"
fi
mv "$stage" "$dest/$BIN"
stage=""
say "Installed $dest/$BIN ($("$dest/$BIN" version))"

case ":$PATH:" in
    *":$dest:"*) ;;
    *)
        echo "  Add $dest to your PATH, e.g. in ~/.zshrc:"
        echo "      export PATH=\"$dest:\$PATH\""
        ;;
esac

interactive_tty=0
setup_completed=0
if [[ "$os" == darwin && "$NO_UP" != 1 && -t 1 ]] && (: </dev/tty) 2>/dev/null; then
    interactive_tty=1
fi
if [[ "$interactive_tty" == 1 ]]; then
    echo
    if ! "$dest/$BIN" up </dev/tty; then
        if [[ -n "$backup" && ( -e "$backup" || -L "$backup" ) ]]; then
            failed_binary="$tmp/$BIN.failed"
            mv "$dest/$BIN" "$failed_binary"
            mv "$backup" "$dest/$BIN"
            backup=""
            if [[ "$previous_binary_recognized" == 1 ]] && "$dest/$BIN" restart >/dev/null 2>&1; then
                warn "Setup failed; restored the previous $BIN executable and restarted its service."
            else
                warn "Setup failed; restored the previous $BIN executable, but did not verify that its service restarted."
            fi
            die "setup did not finish; fix the reported issue before retrying '$dest/$BIN up'"
        fi
        die "setup did not finish; $dest/$BIN remains installed so you can fix the reported issue and run '$BIN up' again"
    fi
	setup_completed=1
elif [[ "$os" == darwin ]]; then
    echo "  Next: $dest/$BIN up"
else
    echo "  Linux archive installed. Automatic background-service setup is macOS-only; no systemd unit is installed."
fi

if [[ -n "$backup" ]]; then
    rm -f "$backup"
    backup=""
fi

legacy="$dest/$LEGACY_BIN"
if [[ ! -e "$legacy" && ! -L "$legacy" ]]; then
    ln -s "$BIN" "$legacy"
    say "Compatibility command: $legacy → $BIN"
elif [[ -L "$legacy" && "$(readlink "$legacy")" == "$BIN" ]]; then
    :
elif [[ "$setup_completed" == 1 && -f "$legacy" && -x "$legacy" ]]; then
    legacy_version=$("$legacy" version 2>/dev/null || true)
    if [[ "$legacy_version" =~ ^hermes-remote[[:space:]](v?[0-9]+\.[0-9]+\.[0-9]+|dev)$ ]]; then
        alias_stage="$dest/.${LEGACY_BIN}.alias.$$"
        ln -s "$BIN" "$alias_stage"
        mv -f "$alias_stage" "$legacy"
        say "Migrated compatibility command: $legacy → $BIN"
    else
        warn "Preserved existing $legacy; it was not recognized as the previous bridge executable."
    fi
else
    warn "Preserved existing $legacy; it was not replaced. Use $dest/$BIN as the canonical command."
fi
