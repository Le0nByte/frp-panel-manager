#!/usr/bin/env bash
# rollback-aliyun.sh - one-click restore of the Aliyun host from a backup
# directory produced by backup-aliyun.sh (spec section 12.3).
#
# Order: verify manifest (sha256sum -c) -> parse/validate manifest.meta ->
# restore files (best-effort chmod/chown) -> daemon-reload + start -> wait for
# is-active (15s) -> wait for the frps-phone control port (15s). Any failure
# exits 1 and never reports success.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/lib/backup-common.sh"

usage() {
  cat <<'EOF'
rollback-aliyun.sh - restore the Aliyun host from a backup directory

Usage:
  rollback-aliyun.sh [options] BACKUP_DIR
  rollback-aliyun.sh [options] --backup-dir BACKUP_DIR

Options:
  --dry-run           verify and print actions, restore nothing, start nothing
  --fs-root DIR       chroot-style absolute root for all paths (tests only)
  --service NAME      frps-phone systemd unit (default frps-phone)
  --port PORT         frps-phone control port to verify (default 7002)
  --wait-active SEC   is-active timeout (default 15)
  --wait-port SEC     port-listen timeout (default 15)
  --wait-interval SEC polling interval; tests override with a fraction
  --help              show this help

The manifest is verified and every restore target is validated against the
absolute-prefix whitelist BEFORE anything is written, so a tampered or
malicious backup cannot modify the host.

The restore is verify-before-write and per-file (not a globally atomic
transaction): if any single-file write fails the script exits 1 and the
rollback must be re-run (safe, idempotent) or inspected manually — partial
success is never claimed.
EOF
}

BACKUP_DIR=""
SERVICE="frps-phone"
PORT=7002
WAIT_ACTIVE="${WAIT_ACTIVE:-15}"
WAIT_PORT="${WAIT_PORT:-15}"

parse_common_flags "$@"
set -- "${COMMON_REMAINING[@]}"
while (($# > 0)); do
  case "$1" in
    --backup-dir) [[ $# -ge 2 ]] || usage_err "--backup-dir requires a path"; BACKUP_DIR="$2"; shift 2 ;;
    --service) [[ $# -ge 2 ]] || usage_err "--service requires a name"; SERVICE="$2"; shift 2 ;;
    --port) [[ $# -ge 2 ]] || usage_err "--port requires a number"; PORT="$2"; shift 2 ;;
    --wait-active) [[ $# -ge 2 ]] || usage_err "--wait-active requires seconds"; WAIT_ACTIVE="$2"; shift 2 ;;
    --wait-port) [[ $# -ge 2 ]] || usage_err "--wait-port requires seconds"; WAIT_PORT="$2"; shift 2 ;;
    --wait-interval) [[ $# -ge 2 ]] || usage_err "--wait-interval requires seconds"; WAIT_INTERVAL="$2"; shift 2 ;;
    -*) usage_err "unknown option: $1" ;;
    *) [[ -z "$BACKUP_DIR" ]] && BACKUP_DIR="$1" || usage_err "unexpected argument: $1"; shift ;;
  esac
done

[[ -n "$BACKUP_DIR" ]] || usage_err "backup directory required"
validate_port "--port" "$PORT"
[[ -d "$BACKUP_DIR" ]] || die "backup directory not found: $BACKUP_DIR"

# 1. tamper chain first: sha256sum -c inside the backup dir
verify_manifest "$BACKUP_DIR"
# 2. parse + validate every meta line against the whitelist (verify-then-restore)
parse_manifest "$BACKUP_DIR" ALIYUN_PREFIXES

# 3. restore
n="${#RESTORE_ENTRIES[@]}"
for ((i = 0; i < n; i += 4)); do
  name="${RESTORE_ENTRIES[i]}"
  target="${RESTORE_ENTRIES[i + 1]}"
  mode="${RESTORE_ENTRIES[i + 2]}"
  owner="${RESTORE_ENTRIES[i + 3]}"
  printf 'restore %s -> %s\n' "$name" "$target"
  restore_entry "$BACKUP_DIR" "$name" "$target" "$mode" "$owner"
done

# 4. service bring-up + verification
dry_or_run systemctl daemon-reload
dry_or_run systemctl start "$SERVICE" || { printf 'ERROR: failed to start %s\n' "$SERVICE" >&2; exit 1; }
if (( DRY_RUN )); then
  printf '[dry-run] would verify %s active and port %s listening\n' "$SERVICE" "$PORT"
  exit 0
fi
if ! wait_is_active "$SERVICE" "$WAIT_ACTIVE"; then
  printf 'ERROR: %s not active within %ds\n' "$SERVICE" "$WAIT_ACTIVE" >&2
  exit 1
fi
if ! wait_port_listening "$PORT" "$WAIT_PORT"; then
  printf 'ERROR: port %s not listening within %ds\n' "$PORT" "$WAIT_PORT" >&2
  exit 1
fi
printf 'rollback complete: %s restored, %s active, port %s listening\n' "$BACKUP_DIR" "$SERVICE" "$PORT"
