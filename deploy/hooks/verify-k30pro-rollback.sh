#!/usr/bin/env bash
# verify-k30pro-rollback.sh - post-rollback verification of the K30 Pro client:
#   1. the frpc process is running
#   2. the current frpc.toml SHA-256 matches the backup manifest
#   3. the local port (default 24) is listening (/proc/net/tcp)
# Runs on the phone, or under --fs-root in offline tests.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/backup-common.sh"

usage() {
  cat <<'EOF'
verify-k30pro-rollback.sh - verify frpc process, config hash, and port

Usage:
  verify-k30pro-rollback.sh [options] BACKUP_DIR

Options:
  --fs-root DIR   chroot-style absolute root for all paths (tests only)
  --frpc PATH     frpc binary (default /data/frp/frpc)
  --frpc-toml PATH frpc config (default /data/frp/frpc.toml)
  --port PORT     local port to verify (default 24)
  --wait-port SEC port-listen timeout (default 24)
  --help          show this help

Exits 0 only when the process is alive, the config hash matches the manifest,
and the port is listening.
EOF
}

BACKUP_DIR=""
FRPC="/data/frp/frpc"
FRPC_TOML="/data/frp/frpc.toml"
PORT=24
WAIT_PORT="${WAIT_PORT:-24}"

parse_common_flags "$@"
set -- "${COMMON_REMAINING[@]}"
while (($# > 0)); do
  case "$1" in
    --backup-dir) [[ $# -ge 2 ]] || usage_err "--backup-dir requires a path"; BACKUP_DIR="$2"; shift 2 ;;
    --frpc) [[ $# -ge 2 ]] || usage_err "--frpc requires a path"; FRPC="$2"; shift 2 ;;
    --frpc-toml) [[ $# -ge 2 ]] || usage_err "--frpc-toml requires a path"; FRPC_TOML="$2"; shift 2 ;;
    --port) [[ $# -ge 2 ]] || usage_err "--port requires a number"; PORT="$2"; shift 2 ;;
    --wait-port) [[ $# -ge 2 ]] || usage_err "--wait-port requires seconds"; WAIT_PORT="$2"; shift 2 ;;
    -*) usage_err "unknown option: $1" ;;
    *) [[ -z "$BACKUP_DIR" ]] && BACKUP_DIR="$1" || usage_err "unexpected argument: $1"; shift ;;
  esac
done

[[ -n "$BACKUP_DIR" ]] || usage_err "backup directory required"
validate_port "--port" "$PORT"
[[ -d "$BACKUP_DIR" ]] || die "backup directory not found: $BACKUP_DIR"
[[ -f "$BACKUP_DIR/manifest.sha256" ]] || die "backup has no manifest.sha256: $BACKUP_DIR"

if ! pgrep -f '/data/frp/frpc' >/dev/null 2>&1; then
  printf 'ERROR: frpc process not running\n' >&2
  exit 1
fi

expected="$(awk '{ n = $2; sub(/^\*/, "", n); if (n == "frpc.toml") { print $1; exit } }' "$BACKUP_DIR/manifest.sha256")"
[[ -n "$expected" ]] || die "manifest.sha256 has no frpc.toml entry"
current_cfg="$(resolve_target "$FRPC_TOML")"
actual="$(sha256sum "$current_cfg" 2>/dev/null | awk '{print $1}' || true)"
if [[ "$actual" != "$expected" ]]; then
  printf 'ERROR: frpc.toml hash mismatch (manifest %s != current %s)\n' "$expected" "$actual" >&2
  exit 1
fi

if ! wait_k30_port "$PORT" "$WAIT_PORT"; then
  printf 'ERROR: port %s not listening within %ds\n' "$PORT" "$WAIT_PORT" >&2
  exit 1
fi

printf 'verify-k30pro-rollback OK: frpc running, config hash verified, port %s listening\n' "$PORT"
