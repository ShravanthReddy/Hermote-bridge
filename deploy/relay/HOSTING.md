# Hermote Oracle hosting

`Caddyfile.hermote` is the complete configuration for the shared website/relay VM.
All configured relay hostnames proxy to the relay process on `127.0.0.1:8080`.
The public endpoint is `wss://relay.hermote.app`. Keep port 8080 bound to loopback.

`install-vm.sh` is for a fresh VM only. It now refuses when a Caddyfile or relay
installation already exists, before changing binaries or services. A partial first
installation also requires manual inspection and recovery; blindly rerunning this
fresh-provisioning script is intentionally refused. Use the deployment steps below for this shared server.
Do not remove the existing Caddyfile to bypass that guard on this shared VM.
For application binary updates, separately stage and validate the binary and plan
the needed relay maintenance; adding a hostname does not require a relay restart.

## Deployment

1. Confirm `hermote.app`, `www.hermote.app`, and `relay.hermote.app` resolve to the
   VM from at least two public resolvers. Compare Caddy's running admin config to
   the adapted disk configuration and reconcile any unrelated differences.
2. Build the website as a static export. Require `index.html`, `privacy.html`,
   and every referenced local asset. Stage only the exported public directory to
   a new `/srv/hermote-site/releases/<release-id>` directory, owned by root, with
   directories readable/traversable and files readable by Caddy. Never stage the
   repository, `.env` files, source maps, credentials or server bundles.
3. Record the old `current` symlink target (if any), back up `/etc/caddy/Caddyfile`,
   and record the relay process start time and `/healthz` bridge/phone counts.
4. Create a temporary symlink alongside `current` and atomically rename it to
   `current` on the VM. Preserve previous release directories for rollback.
5. Validate the candidate with `caddy validate --config <candidate> --adapter caddyfile`.
   Install the accepted file as `/etc/caddy/Caddyfile` and run
   `caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile`.
   Do not restart the relay, Caddy, or unrelated services.
6. A Caddy reload closes proxied WebSockets. Notify users of a brief interruption;
   bridge and phone connections retry with their existing identities/pairings.
   Open terminals must be restarted. Require the previous bridge count to recover
   within 60 seconds, with trusted HTTPS and `/healthz` working on both relay hosts.
   Only after the original bridge count recovers, run the opt-in interoperability test
   and compare counts again after it exits. Exercise encrypted phone-side traffic, including an already-trusted
   phone reconnect without a pairing code. Verify the relay process start time is unchanged.
7. Verify the apex, `/privacy`, `/privacy/` redirect, asset URLs, missing-route 404, and `www` redirect
   through public HTTPS. Explicit relay acceptance: `curl -fsS https://relay.hermote.app/healthz`
   and each configured relay hostname must succeed without certificate bypass. For website-only content updates, atomically switch the
   symlink without reloading Caddy, so relay sockets are unaffected.

## Rollback

If validation fails, do not install or reload the candidate. If post-reload relay
recovery fails, restore the saved Caddyfile, validate and reload it, then verify
bridge recovery again. Rollback itself causes one brief WebSocket interruption.
Restore the previous site symlink atomically for a bad content release. On the
first site deployment, remove only the new symlink if no prior site target existed.
Do not delete prior releases or restart the relay to roll back website content.

If both relay names are healthy but website certificate issuance is delayed, inspect
Caddy's ACME errors before making another change. Do not reload repeatedly for a
certificate rate limit; respect the retry time. Keep the healthy relay configuration
while investigating the website. Withdrawing site blocks, if needed, requires a
reviewed relay-only configuration and one further announced reconnect.

## Domain ownership

Shravanth Reddy owns the Namecheap registration. Current term ends September 8,
2027; auto-renew is off. Renew before expiry to preserve stored phone endpoints.
Do not silently enable recurring billing as part of a server deployment.
