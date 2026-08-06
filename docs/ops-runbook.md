# FRP Panel Operations Runbook

## Scope

This runbook describes the offline deployment baseline for the FRP Panel fork. It does not authorize a production deployment. Do not use the upstream `install.sh` for the Master; deploy a repository-built binary only.

## Public access gate

Do not open the public management endpoint until all of the following pass on the target host:

1. Nginx configuration validation.
2. Certbot 5.7 or newer short-lived IP certificate issuance with the HTTP-01 hooks.
3. `certbot renew --dry-run`.
4. One real renewal and a successful Nginx reload.
5. Alert verification for a certificate with less than 72 hours remaining.

The intended management endpoint is `https://PUBLIC_IP_OR_DNS:9443`. Master port 9100 remains bound to `127.0.0.1`; Agent RPC port 9101 is the only public application listener.

## Installation order

1. Build the forked Master binary and verify its checksum.
2. Create the `frp-panel` system user and directories:
   - `/opt/frp-panel/bin`
   - `/opt/frp-panel/data/log`
   - `/opt/frp-panel/secrets`
   - `/etc/frp-panel`
3. Install the environment file as `/etc/frp-panel/frp-panel.env`, owned by `root:frp-panel`, mode `0600`.
4. Generate random high-entropy values for `[REDACTED]` entries outside the repository.
5. Create the snapshot key file with mode `0600`.
6. Install systemd, Nginx, Certbot hook, and logrotate templates.
7. Validate each configuration offline before enabling services.

`APP_ENABLE_REGISTER` is `false` by default. It may be temporarily set to `true` only for the initial administrator creation and must be returned to `false` immediately afterward.

## Certificate workflow

Set `CERTBOT_WEBROOT` if `/var/www/frp-panel` is not used. Configure Certbot 5.7 or newer with the short-lived IP certificate profile and these hooks:

- `deploy/certbot/auth-hook.sh`
- `deploy/certbot/cleanup-hook.sh`

Run the renewal wrapper with `--dry-run` first. Never print `CERTBOT_VALIDATION`, passwords, tokens, or private keys. The timer runs every 12 hours and its `ExecStartPost` invokes `check-expiry.sh`; a certificate with less than 72 hours remaining exits non-zero and records a `daemon.err` syslog alert. Connect systemd unit failures and this syslog tag to the site's real notification channel before opening the public endpoint. Enable `frp-panel-cert-renew.timer` only after the full public access gate passes.

## Emergency access

Use an SSH local tunnel instead of public HTTP:

```text
ssh -L 19100:127.0.0.1:9100 -p SSH_PORT root@SERVER
```

Then browse to `http://127.0.0.1:19100`. SSH supplies transport encryption; do not expose port 9100 publicly.

## Backup, rollback, and migration

Before migration, stop writes and back up:

- the SQLite database and WAL files;
- `/opt/frp-panel/secrets`;
- deployed binary and environment file;
- existing FRP configuration and service units.

Create a manifest with SHA-256 checksums and verify it before changing services. A rollback restores the previous binary, database/WAL set, configuration, and units as one reviewed operation. Do not report success until the old service is healthy and existing rules are verified.

### Scripts (spec section 12, implemented; offline-tested only)

All scripts are strict bash (`set -euo pipefail`), accept `--dry-run` (validate
and print, zero side effects) and `--fs-root DIR` (absolute chroot-style root,
offline tests only). Exit codes: 0 OK; 1 operation/verification failure;
2 usage.

| Script | Purpose |
|---|---|
| `deploy/backup-aliyun.sh` | Timestamped backup of `frps-phone.toml`, the `frps-phone.service` unit, the frps binary (parsed from the unit `ExecStart`, overridable with `--frps-binary`), the FRP Panel env file, the snapshot key's SHA-256 fingerprint, and the SQLite database. Per spec 12.1 the snapshot key file itself is NEVER copied into the backup: only `snapshot-key.fingerprint` (64-hex SHA-256, covered by `manifest.sha256` with NO `manifest.meta` restore entry) is recorded, so plaintext never leaves the host and rollback can never restore a stale key; live key recovery remains a Bitwarden Secrets Manager step. All restore targets are validated against the same absolute-prefix whitelist rollback enforces BEFORE any meta is generated (e.g. `--frps-binary /usr/local/bin/frps` is rejected up front). The DB is snapshotted online with `sqlite3 .backup` when available (wal/shm are not copied); otherwise the panel service is stopped, db + wal + shm are copied, and the service is reliably restarted via an EXIT trap (`--force-stop` forces this path). Writes `manifest.meta` (`name<TAB>abs-target<TAB>mode<TAB>uid:gid`) and `manifest.sha256` (sha256sum -c format, includes the manifest.meta hash) and self-verifies with `sha256sum -c` before reporting success. |
| `deploy/backup-k30pro.sh` | Backup of `/data/frp/frpc`, `/data/frp/frpc.toml`, `/data/adb/service.d/frpc.sh` with mode/owner/hash. Runs on the phone or under `--fs-root`. |
| `deploy/rollback-aliyun.sh` | Verifies the manifest (`sha256sum -c`) and validates every restore target against an absolute-prefix whitelist BEFORE restoring anything; restores files with best-effort chmod/chown; `systemctl daemon-reload && systemctl start frps-phone`; waits for `is-active` (15s) and the control port 7002 (15s). Any failure exits 1 and never reports success. |
| `deploy/rollback-k30pro.sh` | Verifies the manifest, restores the three phone files (frpc.toml forced 0600), `pkill`s the old frpc, re-runs the idempotent `service.d/frpc.sh`, then delegates the FINAL success verdict to `verify-k30pro-rollback.sh` (frpc process alive + `frpc.toml` SHA-256 matching the manifest + local port listening; `--verify-hook` overrides the default sibling hook; dry-run never executes the hook). |
| `deploy/migrate.sh` | Five-phase cutover with automatic one-click rollback: 0 preflight (backup dir + manifest + tools; `--k30-backup-dir` REQUIRED as an absolute timestamped phone backup dir such as `/data/frp/backups/20260805_000000` — the `/data/frp/backups` container is rejected and the remote path is never stat'ed locally; `--stop-units` REQUIRED unless the `--no-stop-units` test seam), 1 cutover (stop `frps-phone`, confirm 7002 released), 2 takeover hook (`--takeover-hook`, receives `MIGRATE_OLD_PORT`/`MIGRATE_PUBLIC_PORTS`/`MIGRATE_BACKUP_DIR`), 3 verification (public ports 24/8000, `verify-aliyun-rollback.sh` SSH-root + MCP 401/200), 4 auto-rollback from the PRE-migration backup: stop `--stop-units` and CONFIRM the public ports release (any stop failure or port-not-released is an immediate `evidence/rollback_failed` — a still-serving new listener is never mistaken for the old service), `rollback-aliyun.sh`, then recovered ONLY when the old frpc reconnects (public 24/8000 listening) AND `verify-aliyun-rollback.sh` passes (ssh root + MCP 401/200). Only if ADB is reachable does it fall back to `rollback-k30pro.sh` via `adb shell` and re-verify both; otherwise it writes `evidence/rollback_failed`. Preflight also saves the verified `manifest.sha256`/`manifest.meta` plus one `sha256sum -c` run into the evidence dir (spec 12.3.7). Exit codes: 0 success; 1 preflight/cutover failure; 3 rolled back; 4 rollback failed. The main frps on `:7000` is never touched. Real rollback elapsed time is recorded honestly in `evidence/rto.txt` (the ≤60s RTO target is a live-gate check, never faked). Requires `PP_MCP_URL`, `PP_MCP_TOKEN`/`PP_MCP_TOKEN_FILE`, `--stop-units` and `--k30-backup-dir` at runtime. |
| `deploy/hooks/verify-aliyun-rollback.sh` | `ssh <host> id -u` returns 0; MCP without token = 401; MCP with token = 200. The token is injected via `PP_MCP_TOKEN[_FILE]` and passed to curl through a header file (`-H @file`), so it never appears on a command line, in process listings, or in logs/evidence. |
| `deploy/hooks/verify-k30pro-rollback.sh` | frpc process alive, current `frpc.toml` SHA-256 matches the backup manifest, local port 24 listening. |

**Non-atomic restore honesty:** rollback restores are verify-before-write,
per-file copies — they are NOT a globally atomic transaction across all
targets. If any single-file write fails, the script exits 1; the rollback must
be re-run (safe, idempotent) or inspected manually. Partial success is never
claimed. All ports are validated as 1..65535 before any command runs
(`--port`/`--old-port`/`--public-ports`), so operator input can never be
interpolated into a listening-regex.

### Offline testing vs live rehearsal

`deploy/tests/test-backup-rollback.sh` runs the whole suite offline with PATH
stubs (356 assertions + 1 Windows-symlink SKIP, read-only for the repo). Passing it means the scripts
are safe to review and safe to run with `--dry-run`; it does NOT prove the
production cutover. The spec section 15 gates — real systemd,
Magisk/toybox/SELinux on the phone, ADB, MCP endpoints, sqlite3 `.backup`
against the live WAL database, and a real backup/rollback rehearsal with the
60s RTO — must each be exercised on the live hosts and reported separately
before any production migration.

## Resource observation

`MemoryHigh=300M` and `MemoryMax=512M` are initial, deliberately loose safety limits, not measured capacity claims. During a 24-hour shadow run record RSS, Swap, CPU, database size, log growth, and RPC reconnect count. Adjust limits only after reviewing that evidence.

Install logrotate and verify rotation does not lose active logs. The memory alert unit is a placeholder and must not be enabled until a real monitored command and delivery path are configured and tested.

## Android ARM64 client

Build the Client with the upstream build entry point for Android ARM64:

```text
build.sh --platform android --bintype client --arch arm64
```

Validate startup, RPC TLS, socket inspection, Docker detection, Magisk startup, and SELinux Enforcing before changing the phone. Do not change Docker daemon `iptables=false`. If host inspection is unreliable, keep the official Android frpc and use a separate read-only probe Agent after a new review.
