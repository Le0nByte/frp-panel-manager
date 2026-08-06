#!/usr/bin/env bash
# backup-k30pro.sh - backup of the K30 Pro frpc client (spec section 12.1).
# Runs locally on the phone (bash + toybox) or under --fs-root in offline
# tests. Records mode / owner / SHA-256 for each file in the manifest.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/lib/backup-common.sh"

usage() {
  cat <<'EOF'
backup-k30pro.sh - back up the K30 Pro frpc client (spec 12.1)

Usage:
  backup-k30pro.sh [options]

Options:
  --dry-run           validate and print actions, perform nothing
  --fs-root DIR       chroot-style absolute root for all paths (tests only)
  --out DIR           backup output directory
                      (default: /data/frp/backups/<UTC timestamp>)
  --frpc PATH         frpc binary (default /data/frp/frpc)
  --frpc-toml PATH    frpc config (default /data/frp/frpc.toml)
  --frpc-sh PATH      startup script (default /data/adb/service.d/frpc.sh)
  --help              show this help
EOF
}

FRPC="/data/frp/frpc"
FRPC_TOML="/data/frp/frpc.toml"
FRPC_SH="/data/adb/service.d/frpc.sh"
OUT_BASE="/data/frp/backups"
OUT_DIR=""

parse_common_flags "$@"
set -- "${COMMON_REMAINING[@]}"
while (($# > 0)); do
  case "$1" in
    --out) [[ $# -ge 2 ]] || usage_err "--out requires a path"; OUT_DIR="$2"; shift 2 ;;
    --frpc) [[ $# -ge 2 ]] || usage_err "--frpc requires a path"; FRPC="$2"; shift 2 ;;
    --frpc-toml) [[ $# -ge 2 ]] || usage_err "--frpc-toml requires a path"; FRPC_TOML="$2"; shift 2 ;;
    --frpc-sh) [[ $# -ge 2 ]] || usage_err "--frpc-sh requires a path"; FRPC_SH="$2"; shift 2 ;;
    *) usage_err "unknown option: $1" ;;
  esac
done

[[ -n "$OUT_DIR" ]] || OUT_DIR="$OUT_BASE/$(date -u +%Y%m%d_%H%M%S)"

frpc_src="$(resolve_target "$FRPC")"
toml_src="$(resolve_target "$FRPC_TOML")"
sh_src="$(resolve_target "$FRPC_SH")"

# shared whitelist gate: phone restore targets must be inside K30_PREFIXES
require_target_allowed "$FRPC" K30_PREFIXES
require_target_allowed "$FRPC_TOML" K30_PREFIXES
require_target_allowed "$FRPC_SH" K30_PREFIXES

[[ -e "$frpc_src" ]] || die "required file missing: $FRPC"
[[ -e "$toml_src" ]] || die "required file missing: $FRPC_TOML"
[[ -e "$sh_src" ]] || die "required file missing: $FRPC_SH"

BACKUP_DIR="$OUT_DIR"
backup_file "$frpc_src" frpc "$FRPC" || die "failed to back up $FRPC"
backup_file "$toml_src" frpc.toml "$FRPC_TOML" || die "failed to back up $FRPC_TOML"
backup_file "$sh_src" frpc.sh "$FRPC_SH" || die "failed to back up $FRPC_SH"

if (( DRY_RUN )); then
  printf '[dry-run] would write manifest.meta + manifest.sha256 and self-verify\n'
  printf '[dry-run] backup plan OK: %d file(s) -> %s\n' "${#BACKUP_NAMES[@]}" "$OUT_DIR"
  exit 0
fi
finalize_backup "$OUT_DIR"
printf 'backup complete: %s (%d files, manifest verified)\n' "$OUT_DIR" "${#BACKUP_NAMES[@]}"
