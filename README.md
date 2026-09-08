# hermote-bridge

The bridge that connects the Hermote iPhone app to a Hermes Agent installation on a Mac or Linux host. Version 0.13 includes negotiated file uploads up to 256 MiB per file and up to eight terminals per phone connection.

Source: [github.com/ShravanthReddy/Hermote-bridge](https://github.com/ShravanthReddy/Hermote-bridge)

## macOS quickstart

Install the checksum-verified release binary:

```bash
curl -fsSL https://raw.githubusercontent.com/ShravanthReddy/Hermote-bridge/main/install.sh | bash
```

In an interactive terminal, the installer runs guided setup automatically. Otherwise, run the `up` command at the path printed by the installer. If installed into `~/.local/bin`, add that directory to your PATH before using the short command.

`up` finds the Hermes Agent Python environment, installs a per-user LaunchAgent, starts the gateway and bridge on loopback, and prints a pairing QR valid for five minutes. Scan it with **Scan Setup Code** in Hermote. The first run asks how the phone should reach this Mac and remembers the choice.

| Transport | Requirements | Switch later |
| --- | --- | --- |
| Direct | Tailscale on the Mac and phone, signed into the same account | `hermote-bridge up --transport direct` |
| Relay | A relay reachable by both sides; the relay forwards encrypted bytes | `hermote-bridge up --transport relay --relay wss://your-relay.example` |

Relay mode uses Hermote's hosted relay when `up` is run without `--relay`. Run your own with the files in [`deploy/relay/`](deploy/relay/).

## What it does

- Pairing uses a one-time QR code and device identities; revoke one with `hermote-bridge devices revoke <id-prefix>`.
- The gateway listens on `127.0.0.1` with a session token. The bridge exposes only the JSON-RPC and REST routes used by Hermote.
- Frames are encrypted with AES-256-GCM and sequential counters. Relay operators forward encrypted bytes.
- Optional APNs notifications are configured with `hermote-bridge push setup` for approvals, questions, and completed turns while a paired phone is offline.
- The PTY channel opens the user's login shell in the requested folder. Each phone connection can own at most eight terminals; they end when that connection closes.

## Commands

```text
hermote-bridge up [--transport direct|relay] [--relay wss://…] [--name "My Mac"]
hermote-bridge pair
hermote-bridge status
hermote-bridge devices [revoke <id-prefix>]
hermote-bridge selftest
hermote-bridge push setup --team-id TEAM --key-id KEY --p8 AuthKey_KEY.p8
hermote-bridge push status | test
hermote-bridge restart | stop | logs | uninstall
hermote-bridge daemon
hermote-bridge version
```

State is stored in `~/.hermes/remote/` (or `$HERMES_HOME/remote/` when `HERMES_HOME` is set). Logs are in `~/.hermes/logs/remote.log` (or `$HERMES_HOME/logs/remote.log`).

## Linux foreground setup

Linux archives support a manually configured foreground daemon. There is no guided `up` path, LaunchAgent, or bundled systemd unit. Create `$HERMES_HOME/remote/config.json` (or `~/.hermes/remote/config.json`):

```json
{
  "transport": "relay",
  "relay_url": "wss://your-relay.example",
  "https_port": 8443,
  "bridge_port": 9120,
  "name": "My Linux host",
  "python": "/absolute/path/to/hermes/.venv/bin/python"
}
```

Relay-mode `daemon` will not connect without an explicit operator relay URL; unlike macOS `up`, it does not fill an empty `relay_url` with the hosted default. Keep the state and config private, then run pairing separately:

```bash
state_dir="${HERMES_HOME:-$HOME/.hermes}/remote"
chmod 700 "$state_dir"
chmod 600 "$state_dir/config.json"
hermote-bridge daemon   # terminal 1; keep it running or supervise it yourself
hermote-bridge pair    # terminal 2
```

`up`, `restart`, `stop`, and `uninstall` are macOS-only service commands. A non-TTY macOS install, a Linux install, or an install with `HERMOTE_BRIDGE_NO_UP=1` preserves an existing `hermes-remote` executable with a warning. Run `hermote-bridge up` later on macOS to complete guided setup; an existing recognized legacy executable is migrated only after that setup succeeds. An unrelated existing file is always preserved.

## Attachment limits

The bridge has three distinct attachment paths:

- **Legacy inline fallback:** 12 MiB raw as a preflight heuristic. A 12 MiB source may still exceed the JSON, data URL, or envelope budget.
- **Protocol frame guard:** 16 MiB encoded per frame. Oversized frames are rejected.
- **Negotiated blob upload:** 256 MiB per file, advertised by the attachment capability. It is available only when the phone negotiates that capability.

The blob path reserves up to 512 MiB of process-wide spool space and writes 512 KiB chunks. Its channel accepts frames up to 1 MiB of plaintext, including chunk metadata and base64 encoding. It allows one active upload per phone connection and two process-wide. These are operational limits, not additional per-file capacity.

## Existing installs and migration

The installer verifies `checksums.txt` before replacing the canonical binary and keeps the existing state, pairing identities, logs, and `ai.hermes.remote` service identity. `hermes-remote` remains a compatibility command. A new alias is created when the name is free; a recognized old executable is replaced only after guided macOS setup succeeds. Run `hermote-bridge up` after an upgrade so the LaunchAgent uses the stable canonical command without starting a second daemon.

For a pinned pre-v0.13 release whose canonical archive is unavailable, the installer may use that release's explicitly named legacy archive. It does not use legacy archives as a fallback for current releases.

If a release fails after its immutable tag is pushed, keep the tag. Repair or recreate only the incomplete GitHub release from that exact tag, verify the canonical archives against `checksums.txt`, then advance the public installer from the verified release source. Until the installer is updated, the previous installer may still request a legacy archive name. Do not delete or reuse the tag.

## Development and support

From the repository root:

```bash
go test ./...
go build -o ~/.local/bin/hermote-bridge ./cmd/hermote-bridge
```

GoReleaser produces the universal macOS binary, Linux `amd64`/`arm64` archives, relay archives, and `checksums.txt`. Homebrew users can install from the companion tap:

```bash
brew install ShravanthReddy/hermes/hermote-bridge
hermote-bridge up
```

- Issues: [github.com/ShravanthReddy/Hermote-bridge/issues](https://github.com/ShravanthReddy/Hermote-bridge/issues)
- Community: [Discord](https://discord.gg/S7HfRnwG7n)
- Self-hosted relay: [`deploy/relay/`](deploy/relay/)

## License

MIT — see [LICENSE](LICENSE).
