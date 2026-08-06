# Offline deployment tests

Run from the repository root:

```bash
bash deploy/tests/validate-baseline.sh
bash deploy/tests/validate-baseline.sh --root /absolute/path/to/checkout
bash deploy/tests/test-backup-rollback.sh
bash deploy/tests/test-backup-rollback.sh --root /absolute/path/to/checkout
```

Both validators only read repository files. They do not invoke Nginx, Certbot,
systemd, sqlite3, ADB, SSH, or any network endpoint.

## validate-baseline.sh

Checks that every required deployment file exists and contains the expected
safety markers (environment hardening, certificate workflow, backup manifest,
rollback verification, migration exit codes).

## test-backup-rollback.sh

Offline TDD harness for the I2 backup / rollback / migration scripts (spec
section 12). It is read-only for the repository: every write happens under
`mktemp -d` and the trap removes it on exit. All host commands
(`systemctl`, `ss`, `sqlite3`, `pkill`, `pgrep`, `ssh`, `curl`, `adb`) are
PATH stubs from `deploy/tests/fixtures/` that record invocations to
`$STUB_LOG` and return scriptable results via `STUB_*` environment variables:

| Stub | Scriptable behavior |
|---|---|
| `stub-systemctl` | `STUB_SYSTEMCTL_STOP_EXIT`, `STUB_SYSTEMCTL_START_EXIT`, `STUB_IS_ACTIVE` |
| `stub-ss` | `STUB_SS_OUTPUT` (static), `STUB_SS_SEQUENCE_FILE` (one line consumed per call) |
| `stub-sqlite3` | simulates `.backup 'FILE'` by writing a real file; `STUB_SQLITE3_FAIL` |
| `stub-pkill` / `stub-pgrep` | `STUB_PKILL_EXIT`, `STUB_PGREP_OUTPUT`/`STUB_PGREP_EXIT` |
| `stub-ssh` | `STUB_SSH_OUTPUT` (default `0` = root), `STUB_SSH_EXIT` |
| `stub-curl` | `STUB_CURL_CODE_NO_TOKEN` (default 401), `STUB_CURL_CODE_TOKEN` (default 200) |
| `stub-adb` | `STUB_ADB_DEVICES` (device list), `STUB_ADB_SHELL_OUTPUT`/`STUB_ADB_SHELL_EXIT` |

The harness sets small `WAIT_ACTIVE`/`WAIT_PORT`/`WAIT_PUBLIC`/`WAIT_INTERVAL`
values so timeout tests run in milliseconds.

### Coverage (T01-T32)

- **T01-T03** bash -n on every script, presence of the whole deliverable set,
  usage exit codes (unknown flag -> 2, `--help` -> 0, relative `--fs-root` -> 2).
- **T04-T08** backup-aliyun: online `sqlite3 .backup` (no wal/shm, no
  systemctl), stop-copy fallback (`--force-stop` and sqlite3-absent) with
  correct stop->copy->start order and EXIT-trap restart, dry-run with zero
  side effects, manifest format/self-verification, missing-file fail-fast
  (service never stopped when the DB is absent), stop-failure trap restart.
- **T09** backup-k30pro: files + mode/owner/hash metadata, dry-run.
- **T10-T15** rollback-aliyun: verify->restore->daemon-reload->start->port,
  tampered backup rejected with zero restore, malicious manifests (traversal,
  `..`, absolute names, `/opt/frp-evil` boundary, `/etc/cron.d` outside
  whitelist, bad mode/owner) rejected, is-active timeout, port timeout,
  dry-run.
- **T16-T18** rollback-k30pro: restore + pkill + idempotent frpc.sh +
  process/port verification, port-timeout failure, dry-run.
- **T19-T24b** migrate: preflight failures (missing backup dir, bad manifest,
  missing `PP_MCP_URL`) with zero side effects, dry-run plan, full success
  (cutover -> takeover hook with env injection -> public ports + SSH + MCP
  verify, `:7000` never touched, token never in evidence), auto-rollback from
  the PRE-migration backup (exit 3 + RTO evidence), rollback-OK-but-no-reconnect
  with ADB unreachable (exit 4 + `rollback_failed`), ADB-reachable fallback
  invoking rollback-k30pro (exit 3), rollback-aliyun failure (exit 4).
- **T26-T27** hooks: verify-aliyun SSH-root + MCP 401/200 semantics with the
  token never printed or logged (header passed via `-H @file`); verify-k30
  process + config hash + port.
- **T28** secret scan: no hardcoded secrets, hooks never echo the token,
  manifests/output contain no env secrets or key material.
- **T29** `.gitignore`: `deploy/systemd/frp-panel.service`,
  `deploy/logrotate/frp-panel`, `deploy/env/frp-panel.env.example` and every
  new script are NOT ignored; the root `frp-panel`/`frp-panel.exe` build
  outputs remain ignored.
- **T30-T32** repo read-only (deploy/ file set unchanged), static sample
  manifests self-verify and restore, baseline validator passes.

### Fixtures

- `stub-*` — the PATH command stubs described above.
- `aliyun-src/`, `k30-src/` — realistic source trees mounted under the
  `--fs-root` fixture root (systemd unit with `ExecStart`, WAL-mode SQLite db,
  env file and snapshot key containing secret-like values that must never
  leak, phone frpc + idempotent `frpc.sh`, `/proc/net/tcp`).
- `sample-aliyun-backup/`, `sample-k30-backup/` — pre-made manifests with
  real SHA-256 hashes; `sha256sum -c` self-verifies and rollback works from
  them (documentation + restore-without-backup coverage).

**Honesty boundary:** all 356 assertions pass offline with PATH stubs (1 SKIP:
the dynamic symlink fixture is unavailable on this Windows/MSYS host — the
static `! -L` gates are strict in both `validate_checksum_manifest` and
`parse_manifest`). Real
host behaviors — systemd, Magisk/toybox, ADB, MCP endpoints, sqlite3
`.backup` against the live WAL database, and the 60-second RTO rehearsal
(spec section 15) — are live-gate work and are NOT covered by this harness.
