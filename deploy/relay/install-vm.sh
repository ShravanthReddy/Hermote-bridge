#!/usr/bin/env bash
# Install hermes-relay + Caddy on a fresh Ubuntu/Debian VM (no Docker).
#
#   scp hermes-relay-linux-<arch> ubuntu@HOST:hermes-relay
#   ssh ubuntu@HOST 'sudo RELAY_DOMAIN=relay.example.com bash -s' < deploy/relay/install-vm.sh
#
# Result: /usr/local/bin/hermes-relay as the `hermes-relay` systemd service on
# 127.0.0.1:8080, Caddy terminating TLS for $RELAY_DOMAIN and proxying to it.
# Caddy obtains and renews the Let's Encrypt certificate itself.
set -euo pipefail
: "${RELAY_DOMAIN:?set RELAY_DOMAIN}"
BIN="${RELAY_BIN:-$HOME/hermes-relay}"
[[ -f "$BIN" ]] || BIN="/home/${SUDO_USER:-ubuntu}/hermes-relay"
[[ -f "$BIN" ]] || { echo "hermes-relay binary not found (scp it to the login user's home first)" >&2; exit 1; }

install -m 0755 "$BIN" /usr/local/bin/hermes-relay
id -u hermes-relay >/dev/null 2>&1 || useradd --system --home /nonexistent --shell /usr/sbin/nologin hermes-relay

cat >/etc/systemd/system/hermes-relay.service <<'EOF'
[Unit]
Description=hermes-relay (Hermes iPhone app relay)
After=network-online.target
Wants=network-online.target

[Service]
User=hermes-relay
ExecStart=/usr/local/bin/hermes-relay -listen 127.0.0.1:8080
Restart=always
RestartSec=2
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable --now hermes-relay
systemctl is-active hermes-relay

# Caddy from the official apt repo.
if ! command -v caddy >/dev/null; then
    apt-get update -qq
    apt-get install -y -qq debian-keyring debian-archive-keyring apt-transport-https curl gnupg >/dev/null
    curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
    curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' >/etc/apt/sources.list.d/caddy-stable.list
    apt-get update -qq
    apt-get install -y -qq caddy >/dev/null
fi
cat >/etc/caddy/Caddyfile <<EOF
$RELAY_DOMAIN {
    encode zstd gzip
    reverse_proxy 127.0.0.1:8080
    log {
        output file /var/log/caddy/relay-access.log {
            roll_size 50mb
            roll_keep 7
            roll_keep_for 168h
        }
    }
}
EOF
mkdir -p /var/log/caddy && chown caddy:caddy /var/log/caddy
systemctl reload caddy 2>/dev/null || systemctl restart caddy
systemctl enable caddy >/dev/null

# Oracle/Ubuntu images ship iptables rules that drop 80/443 even when the
# cloud security list allows them.
if command -v iptables >/dev/null; then
    for p in 80 443; do
        iptables -C INPUT -p tcp --dport $p -j ACCEPT 2>/dev/null || iptables -I INPUT 5 -p tcp --dport $p -j ACCEPT
    done
    command -v netfilter-persistent >/dev/null && netfilter-persistent save >/dev/null 2>&1 || true
fi

echo "✓ hermes-relay $(hermes-relay -version | awk '{print $2}') on 127.0.0.1:8080; Caddy serving https://$RELAY_DOMAIN"
echo "  check: curl -s https://$RELAY_DOMAIN/healthz"
