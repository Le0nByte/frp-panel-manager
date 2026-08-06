#!/usr/bin/env bash
# rollback-k30pro.sh - restore the K30 Pro frpc client from a backup directory
# produced by backup-k30pro.sh (spec section 12.3.5). Runs locally on the
# phone (or under --fs-root in offline tests).
#
# Restores frpc / frpc.toml / service.d/frpc.sh, forces frpc.toml to 0600,
# kills any old frpc, re-runs the (idempotent) service.d/frpc.sh, then lets
# the verify hook (verify-k30pro-rollback.sh by default, --verify-hook to
# override) decide the FINAL success: frpc process alive + frpc.toml SHA-256
# matching the backup manifest + local port listening. Never fakes success:
# ADB unreachability is reported by migrate.sh as rollback_failed, not here.
# --dry-run restores nothing and does NOT execute the verify hook.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/lib/backup-common.sh"

usage() {
  cat <<'EOF'
rollback-k30pro.sh - restore the K30 Pro frpc client from a backup directory

Usage:
  rollback-k30pro.sh [options] BACKUP_DIR
  rollback-k30pro.sh [options] --backup-dir BACKUP_DIR

Options:
  --dry-run           verify and print actions, restore nothing, run nothing
  --fs-root DIR       chroot-style absolute root for all paths (tests only)
  --frpc PATH         frpc binary (default /data/frp/frpc)
  --frpc-toml PATH    frpc config (default /data/frp/frpc.toml)
  --frpc-sh PATH      startup script (default /data/adb/service.d/frpc.sh)
  --port PORT         local port to verify (default 24)
  --verify-hook PATH  post-restore verification hook
                      (default sibling hooks/verify-k30pro-rollback.sh);
                      its exit code is the FINAL success verdict
  --wait-port SEC     port-listen timeout forwarded to the verify hook (default 24)
  --wait-interval SEC polling interval; tests override with a fraction
  --help              show this help
EOF
}

BACKUP_DIR=""
FRPC="/data/frp/frpc"
FRPC_TOML="/data/frp/frpc.toml"
FRPC_SH="/data/adb/service.d/frpc.sh"
PORT=24
VERIFY_HOOK="$SCRIPT_DIR/hooks/verify-k30pro-rollback.sh"
WAIT_PORT="${WAIT_PORT:-24}"

parse_common_flags "$@"
set -- "${COMMON_REMAINING[@]}"
while (($# > 0)); do
  case "$1" in
    --backup-dir) [[ $# -ge 2 ]] || usage_err "--backup-dir requires a path"; BACKUP_DIR="$2"; shift 2 ;;
    --frpc) [[ $# -ge 2 ]] || usage_err "--frpc requires a path"; FRPC="$2"; shift 2 ;;
    --frpc-toml) [[ $# -ge 2 ]] || usage_err "--frpc-toml requires a path"; FRPC_TOML="$2"; shift 2 ;;
    --frpc-sh) [[ $# -ge 2 ]] || usage_err "--frpc-sh requires a path"; FRPC_SH="$2"; shift 2 ;;
    --port) [[ $# -ge 2 ]] || usage_err "--port requires a number"; PORT="$2"; shift 2 ;;
    --verify-hook) [[ $# -ge 2 ]] || usage_err "--verify-hook requires a path"; VERIFY_HOOK="$2"; shift 2 ;;
    --wait-port) [[ $# -ge 2 ]] || usage_err "--wait-port requires seconds"; WAIT_PORT="$2"; shift 2 ;;
    --wait-interval) [[ $# -ge 2 ]] || usage_err "--wait-interval requires seconds"; WAIT_INTERVAL="$2"; shift 2 ;;
    -*) usage_err "unknown option: $1" ;;
    *) [[ -z "$BACKUP_DIR" ]] && BACKUP_DIR="$1" || usage_err "unexpected argument: $1"; shift ;;
  esac
done

[[ -n "$BACKUP_DIR" ]] || usage_err "backup directory required"
validate_port "--port" "$PORT"
[[ -d "$BACKUP_DIR" ]] || die "backup directory not found: $BACKUP_DIR"
[[ -f "$VERIFY_HOOK" ]] || die "verify hook missing: $VERIFY_HOOK"

verify_manifest "$BACKUP_DIR"
parse_manifest "$BACKUP_DIR" K30_PREFIXES

n="${#RESTORE_ENTRIES[@]}"
for ((i = 0; i < n; i += 4)); do
  name="${RESTORE_ENTRIES[i]}"
  target="${RESTORE_ENTRIES[i + 1]}"
  mode="${RESTORE_ENTRIES[i + 2]}"
  owner="${RESTORE_ENTRIES[i + 3]}"
  printf 'restore %s -> %s\n' "$name" "$target"
  restore_entry "$BACKUP_DIR" "$name" "$target" "$mode" "$owner"
done

# config/token files are forced 0600 on restore (spec: phone-side hardening)
toml_resolved="$(resolve_target "$FRPC_TOML")"
dry_or_run chmod 600 "$toml_resolved"

# restart: kill old frpc, then re-run the idempotent service.d/frpc.sh
printf 'restarting frpc via %s\n' "$FRPC_SH"
dry_or_run pkill -f '/data/frp/frpc' || true
sh_resolved="$(resolve_target "$FRPC_SH")"
dry_or_run sh "$sh_resolved"

if (( DRY_RUN )); then
  printf '[dry-run] would verify via %s (process + config hash + port %s)\n' "$VERIFY_HOOK" "$PORT"
  exit 0
fi

# FINAL success is decided by the verify hook (frpc process + frpc.toml SHA-256
# vs the backup manifest + local port). The hook receives the REAL paths so no
# weaker re-implementation can drift from the actual post-restore state.
if ! bash "$VERIFY_HOOK" --backup-dir "$BACKUP_DIR" --fs-root "$FS_ROOT" \
    --frpc "$FRPC" --frpc-toml "$FRPC_TOML" --port "$PORT" --wait-port "$WAIT_PORT"; then
  printf 'ERROR: post-restore verification failed (verify hook: %s)\n' "$VERIFY_HOOK" >&2
  exit 1
fi
printf 'rollback complete: %s restored, verify hook passed\n' "$BACKUP_DIR"
