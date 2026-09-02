#!/usr/bin/env bash
# hermes-remote installer — the one line the Hermes iPhone app shows:
#   curl -fsSL https://raw.githubusercontent.com/ShravanthReddy/hermes-remote/main/install.sh | bash
#
# Downloads the latest release for this machine, verifies its SHA-256 against
# the release's checksums.txt, installs to ~/.local/bin (or /usr/local/bin when
# writable), then runs `hermes-remote up`, which prints the pairing QR code.
#
# Options (env): HERMES_REMOTE_VERSION=vX.Y.Z  HERMES_REMOTE_NO_UP=1  HERMES_REMOTE_REPO=owner/repo
set -euo pipefail

REPO="${HERMES_REMOTE_REPO:-ShravanthReddy/hermes-remote}"
BIN=hermes-remote

say() { printf '  \033[32m✓\033[0m %s\n' "$*"; }
die() { printf '  \033[31m✗\033[0m %s\n' "$*" >&2; exit 1; }

os=$(uname -s | tr '[:upper:]' '[:lower:]')
arch=$(uname -m)
case "$os" in
    darwin) asset_arch=universal ;;
    linux) case "$arch" in x86_64) asset_arch=amd64 ;; aarch64 | arm64) asset_arch=arm64 ;; *) die "unsupported Linux architecture $arch" ;; esac ;;
    *) die "hermes-remote supports macOS and Linux (got $os)" ;;
esac

command -v curl >/dev/null || die "curl is required"
sha() { if command -v shasum >/dev/null; then shasum -a 256 "$1" | cut -d' ' -f1; else sha256sum "$1" | cut -d' ' -f1; fi; }

api="https://api.github.com/repos/$REPO/releases"
if [[ -n "${HERMES_REMOTE_VERSION:-}" ]]; then
    tag="$HERMES_REMOTE_VERSION"
else
    tag=$(curl -fsSL "$api/latest" | sed -nE 's/.*"tag_name": *"([^"]+)".*/\1/p' | head -1)
    [[ -n "$tag" ]] || die "could not determine the latest release of $REPO"
fi
version="${tag#v}"
asset="${BIN}_${version}_${os}_${asset_arch}.tar.gz"
base="https://github.com/$REPO/releases/download/$tag"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
printf 'Downloading %s %s for %s/%s…\n' "$BIN" "$tag" "$os" "$asset_arch"
curl -fsSL -o "$tmp/$asset" "$base/$asset" || die "download failed: $base/$asset"
curl -fsSL -o "$tmp/checksums.txt" "$base/checksums.txt" || die "checksums.txt missing from the release"
expected=$(grep " $asset\$" "$tmp/checksums.txt" | cut -d' ' -f1)
[[ -n "$expected" ]] || die "no checksum listed for $asset"
actual=$(sha "$tmp/$asset")
[[ "$expected" == "$actual" ]] || die "checksum mismatch for $asset (expected $expected, got $actual)"
say "Checksum verified"

tar -xzf "$tmp/$asset" -C "$tmp"
[[ -x "$tmp/$BIN" ]] || die "archive did not contain $BIN"

if [[ -w /usr/local/bin ]]; then dest=/usr/local/bin; else dest="$HOME/.local/bin"; mkdir -p "$dest"; fi
install -m 0755 "$tmp/$BIN" "$dest/$BIN"
say "Installed $dest/$BIN ($("$dest/$BIN" version))"

case ":$PATH:" in
    *":$dest:"*) ;;
    *)
        echo "  Add $dest to your PATH, e.g. in ~/.zshrc:"
        echo "      export PATH=\"$dest:\$PATH\""
        ;;
esac

if [[ "${HERMES_REMOTE_NO_UP:-0}" != "1" && -t 1 ]]; then
    echo
    exec "$dest/$BIN" up
fi
echo "  Next: $dest/$BIN up"
