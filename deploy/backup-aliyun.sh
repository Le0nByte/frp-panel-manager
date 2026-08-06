#!/usr/bin/env bash
# backup-aliyun.sh - offline-safe backup of the frps-phone relay and FRP Panel
# state on the Aliyun host (spec section 12.1). Writes a timestamped directory
# with manifest.meta + manifest.sha256 and self-verifies before success.
#
# Spec 12.1: the snapshot key is NEVER copied into the backup. If the key
# exists, only its SHA-256 fingerprint is recorded as snapshot-key.fingerprint
# (a tamper-evident checksum-coverage evidence file inside manifest.sha256,
# with NO manifest.meta restore entry), so rollback can never restore a stale
# key and the plaintext never leaves the host. Key content is never printed.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/lib/backup-common.sh"

usage() {
  cat <<'EOF'
backup-aliyun.sh - back up frps-phone relay and FRP Panel state (spec 12.1)

Usage:
  backup-aliyun.sh [options]

Options:
  --dry-run               validate and print actions, perform nothing
  --fs-root DIR           chroot-style absolute root for all paths (tests only)
  --out DIR               backup output directory
                          (default: /opt/frp-panel/migration/<UTC timestamp>)
  --frps-toml PATH        frps-phone config
                          (default /opt/frp/frp_0.65.0_linux_amd64/frps-phone.toml)
  --unit PATH             frps-phone systemd unit
                          (default /etc/systemd/system/frps-phone.service)
  --frps-binary PATH      frps binary; default parsed from the unit ExecStart
  --db PATH               FRP Panel SQLite database
                          (default /opt/frp-panel/data/data.db)
  --env PATH               FRP Panel env file (default /etc/frp-panel/frp-panel.env)
  --snapshot-key PATH      snapshot key file (fingerprinted, never copied)
                          (default /opt/frp-panel/secrets/snapshot.key)
  --service NAME          FRP Panel service for stop-copy DB backup
                          (default frp-panel.service)
  --force-stop            stop-copy DB backup even when sqlite3 is available
  --help                  show this help

The SQLite database is snapshotted online with `sqlite3 .backup` when the CLI
is available; otherwise (or with --force-stop) the panel service is stopped,
the db + wal + shm set is copied, and the service is reliably restarted via a
trap. --dry-run never stops services and never creates the output directory.
EOF
}

FRPS_TOML="/opt/frp/frp_0.65.0_linux_amd64/frps-phone.toml"
FRPS_UNIT="/etc/systemd/system/frps-phone.service"
FRPS_BINARY=""
PANEL_DB="/opt/frp-panel/data/data.db"
PANEL_ENV="/etc/frp-panel/frp-panel.env"
SNAPSHOT_KEY="/opt/frp-panel/secrets/snapshot.key"
PANEL_SERVICE="frp-panel.service"
OUT_BASE="/opt/frp-panel/migration"
OUT_DIR=""
FORCE_STOP=0

parse_common_flags "$@"
set -- "${COMMON_REMAINING[@]}"
while (($# > 0)); do
  case "$1" in
    --out) [[ $# -ge 2 ]] || usage_err "--out requires a path"; OUT_DIR="$2"; shift 2 ;;
    --frps-toml) [[ $# -ge 2 ]] || usage_err "--frps-toml requires a path"; FRPS_TOML="$2"; shift 2 ;;
    --unit) [[ $# -ge 2 ]] || usage_err "--unit requires a path"; FRPS_UNIT="$2"; shift 2 ;;
    --frps-binary) [[ $# -ge 2 ]] || usage_err "--frps-binary requires a path"; FRPS_BINARY="$2"; shift 2 ;;
    --db) [[ $# -ge 2 ]] || usage_err "--db requires a path"; PANEL_DB="$2"; shift 2 ;;
    --env) [[ $# -ge 2 ]] || usage_err "--env requires a path"; PANEL_ENV="$2"; shift 2 ;;
    --snapshot-key) [[ $# -ge 2 ]] || usage_err "--snapshot-key requires a path"; SNAPSHOT_KEY="$2"; shift 2 ;;
    --service) [[ $# -ge 2 ]] || usage_err "--service requires a name"; PANEL_SERVICE="$2"; shift 2 ;;
    --force-stop) FORCE_STOP=1; shift ;;
    *) usage_err "unknown option: $1" ;;
  esac
done

[[ -n "$OUT_DIR" ]] || OUT_DIR="$OUT_BASE/$(date -u +%Y%m%d_%H%M%S)"
BACKUP_DIR="$OUT_DIR"

# ---- resolve sources under FS_ROOT ------------------------------------------
frps_toml_src="$(resolve_target "$FRPS_TOML")"
unit_src="$(resolve_target "$FRPS_UNIT")"
db_src="$(resolve_target "$PANEL_DB")"
env_src="$(resolve_target "$PANEL_ENV")"
key_src="$(resolve_target "$SNAPSHOT_KEY")"

frps_target="$FRPS_BINARY"
if [[ -z "$frps_target" ]]; then
  [[ -f "$unit_src" ]] || die "unit not found (needed to resolve frps binary): $FRPS_UNIT"
  parsed="$(awk '/^ExecStart=/{sub(/^ExecStart=[[:space:]]*/,""); print; exit}' "$unit_src" || true)"
  frps_target="${parsed%%[[:space:]]*}"
  [[ -n "$frps_target" ]] || die "cannot resolve frps binary from $FRPS_UNIT ExecStart; pass --frps-binary"
fi
frps_src="$(resolve_target "$frps_target")"

# ---- shared whitelist gate (same as rollback enforces) ----------------------
# Reject any target outside the restore whitelist BEFORE generating meta, so a
# backup is never produced that rollback would refuse (e.g. --frps-binary
# /usr/local/bin/frps). This mirrors M1 and keeps backup/rollback in lockstep.
require_target_allowed "$FRPS_TOML" ALIYUN_PREFIXES
require_target_allowed "$FRPS_UNIT" ALIYUN_PREFIXES
require_target_allowed "$frps_target" ALIYUN_PREFIXES
require_target_allowed "$PANEL_DB" ALIYUN_PREFIXES
require_target_allowed "$PANEL_ENV" ALIYUN_PREFIXES

# ---- required files ----------------------------------------------------------
[[ -e "$frps_toml_src" ]] || die "required file missing: $FRPS_TOML"
[[ -e "$unit_src" ]] || die "required file missing: $FRPS_UNIT"
[[ -e "$frps_src" ]] || die "required file missing: $frps_target"
[[ -e "$db_src" ]] || die "required file missing: $PANEL_DB"

# ---- optional files (warn, skip) ---------------------------------------------
if [[ -e "$env_src" ]]; then
  backup_file "$env_src" frp-panel.env "$PANEL_ENV" || die "failed to back up $PANEL_ENV"
else
  printf 'WARNING: env file not found, skipped: %s\n' "$PANEL_ENV" >&2
fi
if [[ -e "$key_src" ]]; then
  # Spec 12.1: only the fingerprint belongs in the backup. The key file itself
  # is never copied (no manifest.meta restore entry), so rollback can never
  # restore a stale key and no plaintext leaves the host. The fingerprint IS
  # covered by manifest.sha256, making it tamper-evident.
  key_fp="$(sha256_of "$key_src")"
  [[ "$key_fp" =~ ^[0-9A-Fa-f]{64}$ ]] || die "cannot compute snapshot key fingerprint for $SNAPSHOT_KEY"
  if (( ! DRY_RUN )); then
    mkdir -p "$OUT_DIR"
    printf '%s\n' "$key_fp" > "$OUT_DIR/snapshot-key.fingerprint"
    BACKUP_NAMES+=("snapshot-key.fingerprint")
  else
    printf '[dry-run] would record snapshot key fingerprint (key file not copied, spec 12.1)\n'
  fi
  printf 'snapshot key fingerprint recorded (key file not copied, spec 12.1)\n' >&2
else
  printf 'WARNING: snapshot key not found, skipped: %s\n' "$SNAPSHOT_KEY" >&2
fi

# ---- required files ----------------------------------------------------------
backup_file "$frps_toml_src" frps-phone.toml "$FRPS_TOML" || die "failed to back up $FRPS_TOML"
backup_file "$unit_src" frps-phone.service "$FRPS_UNIT" || die "failed to back up $FRPS_UNIT"
backup_file "$frps_src" frps "$frps_target" || die "failed to back up $frps_target"

# ---- database ----------------------------------------------------------------
db_backed_up=0
if (( ! FORCE_STOP )) && command -v sqlite3 >/dev/null 2>&1; then
  # Online consistent snapshot; wal/shm are NOT copied (spec 12.1).
  if (( ! DRY_RUN )) && [[ ! -e "$db_src" ]]; then
    die "required file missing: $PANEL_DB"
  fi
  dry_or_run sqlite3 "$db_src" ".backup '$OUT_DIR/data.db'" || die "sqlite3 .backup failed for $PANEL_DB"
  dry_or_run chmod "$(stat_mode "$db_src")" "$OUT_DIR/data.db"
  record_backup data.db "$PANEL_DB" "$(stat_mode "$db_src")" "$(stat_uid "$db_src"):$(stat_gid "$db_src")"
  db_backed_up=1
fi

if (( ! db_backed_up )); then
  # Stop-copy fallback: stop -> copy db + wal + shm -> start, reliably restarted.
  printf 'INFO: using stop-copy database backup (sqlite3 unavailable or --force-stop)\n' >&2
  STOPPED=0
  restart_panel() {
    if (( STOPPED )); then
      STOPPED=0
      if ! systemctl start "$PANEL_SERVICE" >/dev/null 2>&1; then
        printf 'ERROR: %s restart failed after backup; service may be down!\n' "$PANEL_SERVICE" >&2
        return 1
      fi
    fi
    return 0
  }
  if (( ! DRY_RUN )); then
    trap 'restart_panel || true' EXIT
    STOPPED=1
  fi
  dry_or_run systemctl stop "$PANEL_SERVICE" || die "failed to stop $PANEL_SERVICE"
  backup_file "$db_src" data.db "$PANEL_DB" 640 || die "failed to back up $PANEL_DB"
  for ext in -wal -shm; do
    if [[ -e "$db_src$ext" ]]; then
      backup_file "$db_src$ext" "data.db$ext" "$PANEL_DB$ext" 640 || die "failed to back up $PANEL_DB$ext"
    fi
  done
  if (( DRY_RUN )); then
    dry_or_run systemctl start "$PANEL_SERVICE"
  else
    restart_panel || die "failed to restart $PANEL_SERVICE after backup"
  fi
fi

# ---- manifest + self verification ---------------------------------------------
frps_digest="$(sha256_of "$frps_src")"
if (( DRY_RUN )); then
  printf '[dry-run] would write manifest.meta + manifest.sha256 and self-verify\n'
  printf '[dry-run] backup plan OK: %d file(s) -> %s\n' "${#BACKUP_NAMES[@]}" "$OUT_DIR"
  exit 0
fi
finalize_backup "$OUT_DIR" "image digest (frps-phone binary): $frps_digest"
printf 'backup complete: %s (%d files, manifest verified)\n' "$OUT_DIR" "${#BACKUP_NAMES[@]}"
