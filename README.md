# hermes-remote

The Mac side of the Hermes iPhone app: one command that makes the Hermes agent on your Mac
reachable from your phone, privately, with a QR code to pair.

```bash
curl -fsSL https://raw.githubusercontent.com/ShravanthReddy/hermes-remote/main/install.sh | bash
```

or

```bash
brew install ShravanthReddy/hermes/hermes-remote
hermes-remote up
```

`up` installs a background service (launchd), starts a private copy of the Hermes gateway bound
to loopback with a random token, exposes the bridge on your Tailscale network, and prints a QR
code. Scan it with **Scan Setup Code** in the Hermes app on the phone. That's the whole setup.

## What it does

- **Pairing, not passwords.** The QR carries the bridge's public key and a one-time code. The
  phone and the Mac sign an X25519 handshake with Ed25519 identities; after the first scan the
  phone is trusted and reconnects on its own. `hermes-remote devices revoke <id>` cuts one off.
- **End-to-end encryption.** Every frame between phone and bridge is AES-256-GCM with per-
  direction keys and strictly sequential counters, on top of Tailscale's WireGuard tunnel.
- **Nothing exposed.** The Hermes gateway listens only on 127.0.0.1 with a token the bridge mints
  at start-up. The bridge forwards exactly the JSON-RPC stream and the handful of REST routes the
  app uses; everything else is refused.
- **Self-contained.** Standard-library crypto only; one pinned dependency (WebSocket). A single
  static binary; nothing is installed into or changed in your Hermes checkout or `config.yaml`.

## Commands

```
hermes-remote up [--transport direct|relay] [--relay wss://…] [--name "My Mac"]
hermes-remote pair            new QR (5-minute validity)
hermes-remote status
hermes-remote devices [revoke <id-prefix>]
hermes-remote selftest        pair a throw-away client over loopback and exercise the tunnel
hermes-remote restart | stop | logs | uninstall
```

State: `~/.hermes/remote/` (identity, paired devices, config). Logs: `~/.hermes/logs/remote.log`.

## Requirements

- macOS (Linux: foreground/systemd support to follow) with [Hermes Agent](https://hermes-agent.nousresearch.com) installed.
- [Tailscale](https://tailscale.com/download/mac), signed in, with MagicDNS and HTTPS certificates
  enabled for your tailnet (`up` tells you if they aren't). The phone runs the Tailscale app on the
  same account.

## Protocol

Documented in the app repository's `docs/REMOTE-ACCESS.md`. Golden test vectors under
`internal/protocol` are shared with the iOS client so both sides are checked byte for byte.

## Development

```bash
go test ./...
go build -o ~/.local/bin/hermes-remote ./cmd/hermes-remote
```

Releases are cut with GoReleaser from tags (`.goreleaser.yaml`): universal macOS binary, Linux
amd64/arm64, `checksums.txt`, SBOM, Homebrew formula.

## License

MIT — see [LICENSE](LICENSE).
