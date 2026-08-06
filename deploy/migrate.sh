#!/usr/bin/env bash
# migrate.sh - five-phase cutover orchestration (spec section 12.2/12.3) with
# automatic one-click rollback. Requires a PRE-MIGRATION backup directory
# (produced by backup-aliyun.sh) so the rollback always restores the exact
# pre-cutover state.
#
# Phases: 0 preflight -> 1 cutover (stop old unit, confirm port release) ->
# 2 takeover hook (new system, env-injected) -> 3 verify (public ports, SSH,
# MCP) -> 4 auto-rollback on any post-cutover failure.
#
# Preflight REQUIRES (spec 12.3.1/12.3.5): --stop-units naming the new-stack
# units that hold the public ports (--no-stop-units is a test seam only), and
# --k30-backup-dir pointing at an absolute TIMESTAMPED phone backup directory
# used by the ADB fallback (never the /data/frp/backups container itself; the
# remote path is never stat'ed locally).
#
# Exit codes: 0 success; 1 preflight/cutover failure (before takeover);
# 3 post-takeover failure, rolled back; 4 rollback also failed (evidence
# `rollback_failed` written). The main frps on :7000 is never touched.
#
# Rollback is only ever declared complete (exit 3) when BOTH the old frpc has
# reconnected (public ports listening) AND the verify-aliyun hook (ssh root +
# MCP 401/200, spec 12.3.6) passes. Any stop-unit failure or port-not-released
# is an immediate rollback_failed (exit 4) — a still-serving new listener is
# never mistaken for the old service.
#
# RTO timing is recorded honestly in <evidence>/rto.txt from real wall-clock
# timestamps; the 60s target is a live-gate check, never faked offline.
# The preflight-verified manifest.sha256 / manifest.meta and one `sha256sum -c`
# run (filenames + OK, no key/token content) are saved as evidence.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/lib/backup-common.sh"

usage() {
  cat <<'EOF'
migrate.sh - cutover orchestration with automatic one-click rollback (spec 12)

Usage:
  migrate.sh [options] --backup-dir BACKUP_DIR

Environment (required):
  PP_MCP_URL            MCP probe URL for post-cutover verification
  PP_MCP_TOKEN | PP_MCP_TOKEN_FILE
                        MCP token for the 200-with-token probe

Options:
  --dry-run             validate and print the full plan, perform nothing
  --fs-root DIR         chroot-style absolute root for all paths (tests only)
  --backup-dir DIR      pre-migration backup directory (required)
  --old-unit NAME       old frps-phone unit (default frps-phone)
  --old-port PORT       old control port to release (default 7002)
  --takeover-hook CMD   command that brings up the new system
                        (default: systemctl start frp-panel)
                        receives MIGRATE_OLD_PORT / MIGRATE_PUBLIC_PORTS /
                        MIGRATE_BACKUP_DIR in the environment
  --stop-units "A B C"  new-stack units stopped before rollback
                        (REQUIRED for real use; --no-stop-units is a
                        test seam only)
  --no-stop-units       skip stopping new units during rollback (tests only)
  --public-ports "P.."  public ports the new system must serve (default "24 8000")
  --rollback-aliyun PATH  rollback script (default sibling rollback-aliyun.sh)
  --rollback-k30pro PATH  phone-side rollback script path invoked via adb
                        (default /data/frp/bin/rollback-k30pro.sh)
  --k30-backup-dir DIR  phone-side K30 backup dir used by the ADB fallback
                        (REQUIRED; absolute TIMESTAMPED directory such as
                        /data/frp/backups/20260805_000000 — the /data/frp/backups
                        container itself is rejected; never stat'ed locally)
  --verify-aliyun PATH  verify hook (default sibling hooks/verify-aliyun-rollback.sh)
  --ssh-host HOST       SSH host for verification (default k30pro)
  --adb PATH            adb binary (default adb from PATH)
  --evidence-dir DIR    evidence output (default BACKUP_DIR/evidence)
  --required-entries S  backup entries required by preflight
                        (default "frps-phone.toml frps-phone.service frps data.db")
  --wait-active/--wait-port/--wait-public/--wait-interval SEC  timeouts/polling

Exit codes: 0 success; 1 preflight/cutover failure; 2 usage;
3 rolled back after a post-takeover failure; 4 rollback failed (rollback_failed).
EOF
}

BACKUP_DIR=""
OLD_UNIT="frps-phone"
OLD_PORT=7002
TAKEOVER_HOOK="systemctl start frp-panel"
STOP_UNITS=""
PUBLIC_PORTS="24 8000"
ROLLBACK_ALIYUN="$SCRIPT_DIR/rollback-aliyun.sh"
ROLLBACK_K30="/data/frp/bin/rollback-k30pro.sh"
K30_BACKUP_DIR=""
VERIFY_ALIYUN="$SCRIPT_DIR/hooks/verify-aliyun-rollback.sh"
SSH_HOST="k30pro"
ADB_BIN="${ADB_BIN:-adb}"
NO_STOP_UNITS=0
EVIDENCE_DIR=""
REQUIRED_ENTRIES="frps-phone.toml frps-phone.service frps data.db"
WAIT_ACTIVE="${WAIT_ACTIVE:-15}"
WAIT_PORT="${WAIT_PORT:-15}"
WAIT_PUBLIC="${WAIT_PUBLIC:-30}"

parse_common_flags "$@"
set -- "${COMMON_REMAINING[@]}"
while (($# > 0)); do
  case "$1" in
    --backup-dir) [[ $# -ge 2 ]] || usage_err "--backup-dir requires a path"; BACKUP_DIR="$2"; shift 2 ;;
    --old-unit) [[ $# -ge 2 ]] || usage_err "--old-unit requires a name"; OLD_UNIT="$2"; shift 2 ;;
    --old-port) [[ $# -ge 2 ]] || usage_err "--old-port requires a number"; OLD_PORT="$2"; shift 2 ;;
    --takeover-hook) [[ $# -ge 2 ]] || usage_err "--takeover-hook requires a command"; TAKEOVER_HOOK="$2"; shift 2 ;;
    --stop-units) [[ $# -ge 2 ]] || usage_err "--stop-units requires unit names"; STOP_UNITS="$2"; shift 2 ;;
    --no-stop-units) NO_STOP_UNITS=1; shift ;;
    --public-ports) [[ $# -ge 2 ]] || usage_err "--public-ports requires ports"; PUBLIC_PORTS="$2"; shift 2 ;;
    --rollback-aliyun) [[ $# -ge 2 ]] || usage_err "--rollback-aliyun requires a path"; ROLLBACK_ALIYUN="$2"; shift 2 ;;
    --rollback-k30pro) [[ $# -ge 2 ]] || usage_err "--rollback-k30pro requires a path"; ROLLBACK_K30="$2"; shift 2 ;;
    --k30-backup-dir) [[ $# -ge 2 ]] || usage_err "--k30-backup-dir requires a path"; K30_BACKUP_DIR="$2"; shift 2 ;;
    --verify-aliyun) [[ $# -ge 2 ]] || usage_err "--verify-aliyun requires a path"; VERIFY_ALIYUN="$2"; shift 2 ;;
    --ssh-host) [[ $# -ge 2 ]] || usage_err "--ssh-host requires a host"; SSH_HOST="$2"; shift 2 ;;
    --adb) [[ $# -ge 2 ]] || usage_err "--adb requires a path"; ADB_BIN="$2"; shift 2 ;;
    --evidence-dir) [[ $# -ge 2 ]] || usage_err "--evidence-dir requires a path"; EVIDENCE_DIR="$2"; shift 2 ;;
    --required-entries) [[ $# -ge 2 ]] || usage_err "--required-entries requires names"; REQUIRED_ENTRIES="$2"; shift 2 ;;
    --wait-active) [[ $# -ge 2 ]] || usage_err "--wait-active requires seconds"; WAIT_ACTIVE="$2"; shift 2 ;;
    --wait-port) [[ $# -ge 2 ]] || usage_err "--wait-port requires seconds"; WAIT_PORT="$2"; shift 2 ;;
    --wait-public) [[ $# -ge 2 ]] || usage_err "--wait-public requires seconds"; WAIT_PUBLIC="$2"; shift 2 ;;
    --wait-interval) [[ $# -ge 2 ]] || usage_err "--wait-interval requires seconds"; WAIT_INTERVAL="$2"; shift 2 ;;
    *) usage_err "unknown option: $1" ;;
  esac
done

validate_port "--old-port" "$OLD_PORT"
validate_port_list "--public-ports" "$PUBLIC_PORTS"
[[ -n "$BACKUP_DIR" ]] || usage_err "--backup-dir is required"
[[ -n "${PP_MCP_URL:-}" ]] || die "PP_MCP_URL is required (MCP verification, spec 12.3.6)"
[[ -n "${PP_MCP_TOKEN:-}" || -n "${PP_MCP_TOKEN_FILE:-}" ]] || die "PP_MCP_TOKEN or PP_MCP_TOKEN_FILE is required"

# pass MCP credentials through to the verify hook (never logged)
export PP_MCP_URL PP_MCP_TOKEN PP_MCP_TOKEN_FILE

EVIDENCE_DIR="${EVIDENCE_DIR:-$BACKUP_DIR/evidence}"
TS="$(date -u +%Y%m%d_%H%M%S)"
if (( DRY_RUN )); then
  # A dry-run is a pure validation/plan operation: it must not create an
  # evidence directory or log file before reporting the plan.
  LOG=""
  log() { printf '%s\n' "$*"; }
else
  mkdir -p "$EVIDENCE_DIR"
  LOG="$EVIDENCE_DIR/migrate-$TS.log"
  log() { printf '%s\n' "$*" | tee -a "$LOG"; }
fi

RB_START=0
write_rto() {
  local end rto
  end="$(date +%s)"
  rto=$((end - RB_START))
  printf '%d\n' "$rto" > "$EVIDENCE_DIR/rto.txt"
  if (( rto <= 60 )); then
    printf 'within_60s=yes\n' >> "$EVIDENCE_DIR/rto.txt"
  else
    printf 'within_60s=no\n' >> "$EVIDENCE_DIR/rto.txt"
  fi
  log "RTO elapsed for rollback: ${rto}s"
}

finalize_failed() {
  log "ROLLBACK FAILED: $1"
  log "rollback_failed marker written: $EVIDENCE_DIR/rollback_failed"
  : > "$EVIDENCE_DIR/rollback_failed"
  printf '%s\n' "$1" > "$EVIDENCE_DIR/rollback_failed.reason"
  write_rto
}

adb_reachable() {
  [[ -n "$ADB_BIN" ]] || return 1
  local out
  out="$("$ADB_BIN" devices 2>/dev/null || true)"
  printf '%s\n' "$out" | grep -q 'device$'
}

# k30_backup_dir_ok <dir>: the ADB fallback target must be an absolute,
# SPECIFIC timestamped backup directory (backup-k30pro.sh layout). The
# /data/frp/backups container (and any trailing-slash form of it) is rejected
# because it holds no manifest; the remote path is never stat'ed locally.
k30_backup_dir_ok() {
  local d="$1" base
  [[ -n "$d" ]] || return 1
  [[ "$d" == /* ]] || return 1
  [[ "$d" != *$'\t'* && "$d" != *$'\n'* ]] || return 1
  d="${d%/}"
  base="${d##*/}"
  [[ "$base" =~ ^[0-9]{8}_[0-9]{6}$ ]] || return 1
  return 0
}

# ---- phase 0: preflight (exit 1 before any change) ---------------------------
phase_preflight() {
  log "phase 0/4 preflight"
  [[ -d "$BACKUP_DIR" ]] || die "backup directory not found: $BACKUP_DIR"
  # spec 12.3.5: the phone fallback needs an explicit timestamped backup dir
  [[ -n "$K30_BACKUP_DIR" ]] || die "--k30-backup-dir is required (phone-side fallback backup dir, spec 12.3.5)"
  k30_backup_dir_ok "$K30_BACKUP_DIR" || die "--k30-backup-dir must be an absolute timestamped backup dir (e.g. /data/frp/backups/20260805_000000), got: $K30_BACKUP_DIR"
  # spec 12.3.1: rollback must stop the new-stack units; --no-stop-units is a
  # test seam only
  if (( ! NO_STOP_UNITS )) && [[ -z "$STOP_UNITS" ]]; then
    die "--stop-units is required for migration (spec 12.3.1); --no-stop-units is a test seam only"
  fi
  verify_manifest "$BACKUP_DIR"
  parse_manifest "$BACKUP_DIR" ALIYUN_PREFIXES
  local entry i found
  for entry in $REQUIRED_ENTRIES; do
    found=0
    for ((i = 0; i < ${#RESTORE_ENTRIES[@]}; i += 4)); do
      if [[ "${RESTORE_ENTRIES[i]}" == "$entry" ]]; then
        found=1
        break
      fi
    done
    ((found == 1)) || die "backup lacks required entry: $entry"
  done
  command -v systemctl >/dev/null 2>&1 || die "systemctl not found on PATH"
  [[ -f "$ROLLBACK_ALIYUN" ]] || die "rollback-aliyun script missing: $ROLLBACK_ALIYUN"
  [[ -f "$VERIFY_ALIYUN" ]] || die "verify-aliyun hook missing: $VERIFY_ALIYUN"
  # evidence (spec 12.3.7): keep the preflight-verified manifest + one
  # sha256sum -c run (filenames + OK only; no key/token content)
  if (( ! DRY_RUN )); then
    cp -p "$BACKUP_DIR/manifest.sha256" "$BACKUP_DIR/manifest.meta" "$EVIDENCE_DIR/"
    (cd "$BACKUP_DIR" && sha256sum -c manifest.sha256) > "$EVIDENCE_DIR/manifest-check.txt" 2>&1
    log "evidence saved: manifest.sha256 / manifest.meta / manifest-check.txt"
  fi
  log "preflight OK (manifest verified, tools present)"
}

# ---- phase 1: cutover ----------------------------------------------------------
phase_cutover() {
  log "phase 1/4 cutover: stop $OLD_UNIT, confirm port $OLD_PORT release"
  if ! dry_or_run systemctl stop "$OLD_UNIT"; then
    log "ERROR: failed to stop $OLD_UNIT"
    dry_or_run systemctl start "$OLD_UNIT" || true
    exit 1
  fi
  if ! wait_port_free "$OLD_PORT" "$WAIT_PORT"; then
    log "ERROR: port $OLD_PORT still bound after ${WAIT_PORT}s; restarting old unit"
    dry_or_run systemctl start "$OLD_UNIT" || true
    exit 1
  fi
  log "port $OLD_PORT released"
}

# ---- phase 2: takeover hook (env-injected) --------------------------------------
phase_takeover() {
  log "phase 2/4 takeover hook: $TAKEOVER_HOOK"
  export MIGRATE_OLD_PORT="$OLD_PORT"
  export MIGRATE_PUBLIC_PORTS="$PUBLIC_PORTS"
  export MIGRATE_BACKUP_DIR="$BACKUP_DIR"
  if ! dry_or_run bash -c "$TAKEOVER_HOOK"; then
    log "ERROR: takeover hook failed"
    return 1
  fi
  log "takeover hook completed"
}

# ---- phase 3: verification --------------------------------------------------------
phase_verify() {
  log "phase 3/4 verify: public ports $PUBLIC_PORTS"
  if ! wait_ports_listening "$PUBLIC_PORTS" "$WAIT_PUBLIC"; then
    log "ERROR: public ports not listening: $PUBLIC_PORTS"
    return 1
  fi
  log "public ports up"
  log "phase 3/4 verify: ssh root + MCP (verify-aliyun hook, host $SSH_HOST)"
  if ! dry_or_run bash "$VERIFY_ALIYUN" --ssh-host "$SSH_HOST"; then
    log "ERROR: verify-aliyun hook failed"
    return 1
  fi
  log "ssh + mcp verified"
}

# ---- phase 4: auto-rollback (exits 3 or 4) ------------------------------------------
# rollback_verified: recovered ONLY when the old frpc reconnected (public
# ports listening) AND the verify-aliyun hook (ssh root + MCP 401/200, spec
# 12.3.6) passes. Ports-up-but-hook-failing is NOT recovered — a new-system
# listener must never be mistaken for the old service.
rollback_verified() {
  if ! wait_ports_listening "$PUBLIC_PORTS" "$WAIT_PUBLIC"; then
    log "ERROR: public ports not listening after rollback"
    return 1
  fi
  log "public ports listening after rollback"
  if bash "$VERIFY_ALIYUN" --ssh-host "$SSH_HOST"; then
    log "ssh root + MCP verified after rollback"
    return 0
  fi
  log "ERROR: verify-aliyun hook failed after rollback"
  return 1
}

phase_rollback() {
  log "phase 4/4 AUTO-ROLLBACK (from pre-migration backup $BACKUP_DIR)"
  RB_START="$(date +%s)"

  # spec 12.3.1: stop the new 24/8000 rules and CONFIRM the ports release.
  # Any stop failure or port-not-released is an immediate rollback_failed —
  # continuing would let a still-serving new listener fake a "reconnect".
  if (( NO_STOP_UNITS )); then
    log "skipping stop of new units (--no-stop-units, test seam only)"
  else
    local u
    for u in $STOP_UNITS; do
      if ! dry_or_run systemctl stop "$u"; then
        log "ERROR: failed to stop new unit: $u"
        finalize_failed "failed to stop new unit: $u"
        exit 4
      fi
    done
    if ! wait_ports_free "$PUBLIC_PORTS" "$WAIT_PORT"; then
      log "ERROR: public ports still held after stopping new units"
      finalize_failed "public ports not released after stopping new units"
      exit 4
    fi
    log "public ports released"
  fi

  # restore the Aliyun side from the PRE-migration backup
  log "running rollback-aliyun: $ROLLBACK_ALIYUN --backup-dir $BACKUP_DIR --service $OLD_UNIT --port $OLD_PORT"
  RB_LOG="$EVIDENCE_DIR/rollback-aliyun-$TS.log"
  if ! bash "$ROLLBACK_ALIYUN" --backup-dir "$BACKUP_DIR" --service "$OLD_UNIT" --port "$OLD_PORT" \
    --fs-root "$FS_ROOT" --wait-active "$WAIT_ACTIVE" --wait-port "$WAIT_PORT" --wait-interval "${WAIT_INTERVAL:-1}" \
    2>&1 | tee -a "$RB_LOG"; then
    log "ERROR: rollback-aliyun failed (evidence: $RB_LOG)"
    finalize_failed "rollback-aliyun failed"
    exit 4
  fi
  log "rollback-aliyun OK; verifying old frpc reconnect + ssh/mcp hook ($PUBLIC_PORTS)"
  if rollback_verified; then
    log "rollback verified; rollback complete"
    write_rto
    exit 3
  fi

  # Not recovered (ports down OR hook failed): fall back to the phone ONLY
  # when ADB is reachable AND an explicit timestamped K30 backup dir exists;
  # otherwise mark rollback_failed and never fake the RTO (spec 12.3.5).
  if [[ -n "$K30_BACKUP_DIR" ]] && adb_reachable; then
    log "adb reachable; invoking k30 rollback: $ADB_BIN shell sh $ROLLBACK_K30 --backup-dir $K30_BACKUP_DIR"
    K30_LOG="$EVIDENCE_DIR/rollback-k30-$TS.log"
    if "$ADB_BIN" shell sh "$ROLLBACK_K30" --backup-dir "$K30_BACKUP_DIR" \
      --wait-port "$WAIT_PORT" --wait-interval "${WAIT_INTERVAL:-1}" \
      2>&1 | tee -a "$K30_LOG"; then
      if rollback_verified; then
        log "k30 rollback recovered public ports; rollback complete"
        write_rto
        exit 3
      fi
      log "ERROR: ports/hook still failing after k30 rollback"
    else
      log "ERROR: k30 rollback via adb failed"
    fi
    finalize_failed "k30 rollback incomplete"
    exit 4
  fi

  finalize_failed "rollback not verified (adb unreachable or no k30 backup dir configured)"
  exit 4
}

# ---- main -----------------------------------------------------------------------
log "migrate start $(date -u +%Y-%m-%dT%H:%M:%SZ) backup=$BACKUP_DIR old_unit=$OLD_UNIT old_port=$OLD_PORT"
phase_preflight

if (( DRY_RUN )); then
  log "[dry-run] plan: cutover (stop $OLD_UNIT, release $OLD_PORT); takeover ($TAKEOVER_HOOK); verify ($PUBLIC_PORTS, ssh $SSH_HOST, MCP); on failure rollback via $ROLLBACK_ALIYUN, phone fallback $ADB_BIN shell sh $ROLLBACK_K30"
  log "migrate dry-run OK"
  exit 0
fi

phase_cutover

if ! phase_takeover; then
  log "takeover failed -> rollback"
  phase_rollback
fi
if ! phase_verify; then
  log "verification failed -> rollback"
  phase_rollback
fi

log "migrate complete: new system serving $PUBLIC_PORTS; old config retained in $BACKUP_DIR (spec 12.2 observe phase)"
exit 0
