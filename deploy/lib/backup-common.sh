#!/usr/bin/env bash
# backup-common.sh - shared helpers for the I2 backup / rollback / migration
# scripts (spec section 12). Sourced, never executed. Requires bash 4.3+.
#
# Safety contract:
#   * every script runs strict: set -euo pipefail
#   * manifest names are relative, [A-Za-z0-9._/-]+, no '..', no leading '/',
#     no tab/newline
#   * restore targets must be absolute and inside a hardcoded prefix whitelist
#     with boundary matching (prefix + '/' or exact), so /opt/frp-evil is
#     rejected even though /opt/frp is allowed
#   * manifest.sha256 is generated inside the backup directory with RELATIVE
#     names (so `sha256sum -c` works from any cwd) and includes manifest.meta
#     itself, forming a tamper chain: changing meta breaks `-c`
#   * --fs-root is an absolute chroot-style relocation used by offline tests
#     only; --dry-run validates and prints, performs zero side effects
# shellcheck disable=SC2034,SC2016,SC2206
set -euo pipefail

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}
usage_err() {
  printf 'ERROR: %s\n' "$*" >&2
  printf 'Try --help for usage.\n' >&2
  exit 2
}
# Each script overrides usage() with its own text after sourcing this file.
usage() { printf 'usage: see --help\n'; }

# ---- numeric validation ---------------------------------------------------
# validate_port <name> <value>: 1..65535 pure digits (usage error otherwise).
# Every --port / --old-port / --public-ports / hook port is validated through
# here BEFORE any command runs, so operator input can never be interpolated
# into a regex (e.g. '24|7002') or produce a nonsense port.
validate_port() {
  local name="$1" value="$2"
  [[ "$value" =~ ^[0-9]+$ ]] || usage_err "$name must be a number: $value"
  ((value >= 1 && value <= 65535)) || usage_err "$name must be in 1..65535: $value"
}
# validate_port_list <name> <"p1 p2 ...">: at least one valid port.
validate_port_list() {
  local name="$1" value="$2" p
  [[ -n "$value" ]] || usage_err "$name requires at least one port"
  for p in $value; do
    validate_port "$name" "$p"
  done
}

# ---- common flags ----------------------------------------------------------
DRY_RUN=0
FS_ROOT="/"
COMMON_REMAINING=()
# parse_common_flags "$@" consumes --dry-run/--fs-root/--help and leaves the
# remaining arguments in COMMON_REMAINING. Caller must then run:
#   set -- "${COMMON_REMAINING[@]}"
parse_common_flags() {
  COMMON_REMAINING=()
  while (($# > 0)); do
    case "$1" in
      --dry-run) DRY_RUN=1; shift ;;
      --fs-root)
        [[ $# -ge 2 ]] || usage_err "--fs-root requires an absolute directory"
        [[ "$2" == /* ]] || usage_err "--fs-root must be an absolute path: $2"
        [[ -d "$2" ]] || die "--fs-root does not exist: $2"
        FS_ROOT="$2"
        shift 2 ;;
      --help|-h) usage; exit 0 ;;
      --) shift; COMMON_REMAINING+=("$@"); break ;;
      *) COMMON_REMAINING+=("$1"); shift ;;
    esac
  done
}

# dry_or_run <cmd...>: prints '[dry-run] cmd' in dry-run mode, else executes.
dry_or_run() {
  if (( DRY_RUN )); then
    printf '[dry-run] %s\n' "$*"
    return 0
  fi
  "$@"
}

# ---- path safety ------------------------------------------------------------
# manifest_name_ok <name>: relative backup-file name must be safe.
manifest_name_ok() {
  local name="$1"
  [[ -n "$name" ]] || return 1
  [[ "$name" =~ ^[A-Za-z0-9._/-]+$ ]] || return 1
  [[ "$name" != *".."* ]] || return 1
  [[ "$name" != /* ]] || return 1
  [[ "$name" != *$'\t'* && "$name" != *$'\n'* ]] || return 1
  return 0
}
# abs_target_ok <target>: manifest restore target must be absolute, tab/newline free.
abs_target_ok() {
  local target="$1"
  [[ "$target" == /* ]] || return 1
  [[ "$target" != *$'\t'* && "$target" != *$'\n'* ]] || return 1
  # Reject non-canonical path components before prefix matching. Without
  # this, `/opt/frp/../../etc/...` lexically matches /opt/frp but escapes it
  # when mkdir/cp lets the filesystem normalize the path.
  [[ "$target" != *"//"* ]] || return 1
  [[ "$target" != *"/../"* && "$target" != */.. ]] || return 1
  [[ "$target" != *"/./"* && "$target" != */. ]] || return 1
  return 0
}
# target_ok <target> <prefix...>: byte-prefix whitelist with boundary match.
# A prefix without a trailing slash matches the prefix or prefix + '/' + rest
# (so /opt/frp never matches /opt/frp-evil); a prefix with a trailing slash
# matches any target starting with it.
target_ok() {
  local target="$1"
  shift
  local prefix
  for prefix in "$@"; do
    if [[ "$prefix" == */ ]]; then
      [[ "$target" == "$prefix"* ]] && return 0
    elif [[ "$target" == "$prefix" || "$target" == "$prefix/"* ]]; then
      return 0
    fi
  done
  return 1
}
# require_target_allowed <target> <prefixes-array-name>: the backup side of the
# same whitelist gate rollback enforces, so a backup is never produced whose
# restore targets rollback would refuse (e.g. --frps-binary under /usr/local).
require_target_allowed() {
  local target="$1"
  local -n prefixes="$2"
  abs_target_ok "$target" || die "unsafe target: $target"
  target_ok "$target" "${prefixes[@]}" || die "target outside whitelist: $target"
}
# resolve_target <abs-target>: joins FS_ROOT and the absolute target.
resolve_target() {
  printf '%s' "${FS_ROOT%/}${1}"
}

ALIYUN_PREFIXES=(/opt/frp /etc/systemd/system/ /opt/frp-panel/data/ /opt/frp-panel/secrets/ /etc/frp-panel/ /opt/frp-panel/bin/)
K30_PREFIXES=(/data/frp/ /data/adb/service.d/)

# ---- stat / hash helpers (GNU coreutils with toybox/BSD fallbacks) ----------
stat_mode() { stat -c '%a' "$1" 2>/dev/null || stat -f '%Lp' "$1" 2>/dev/null || printf '0'; }
stat_uid()  { stat -c '%u' "$1" 2>/dev/null || stat -f '%u' "$1" 2>/dev/null || printf '0'; }
stat_gid()  { stat -c '%g' "$1" 2>/dev/null || stat -f '%g' "$1" 2>/dev/null || printf '0'; }
sha256_of() { sha256sum "$1" 2>/dev/null | awk '{print $1}'; }

# ---- backup helpers ----------------------------------------------------------
BACKUP_DIR=""      # output directory for the current backup
BACKUP_NAMES=()    # manifest names (relative)
BACKUP_META=()     # manifest.meta lines: name<TAB>target<TAB>mode<TAB>uid:gid

# backup_file <fs-abs-src> <name> <abs-target> [force_mode]
# Copies the source into $BACKUP_DIR and records its metadata. Returns 1 when
# the source is missing (caller decides whether it is optional).
backup_file() {
  local src="$1" name="$2" target="$3" force_mode="${4:-}"
  local meta_mode meta_uid meta_gid
  [[ -e "$src" ]] || return 1
  [[ -n "$BACKUP_DIR" ]] || die "backup_file: BACKUP_DIR not set"
  manifest_name_ok "$name" || die "unsafe manifest name: $name"
  abs_target_ok "$target" || die "unsafe target: $target"
  meta_mode="$(stat_mode "$src")"
  meta_uid="$(stat_uid "$src")"
  meta_gid="$(stat_gid "$src")"
  dry_or_run mkdir -p "$BACKUP_DIR"
  dry_or_run cp -p "$src" "$BACKUP_DIR/$name"
  if [[ -n "$force_mode" ]]; then
    dry_or_run chmod "$force_mode" "$BACKUP_DIR/$name"
    meta_mode="$force_mode"
  fi
  BACKUP_NAMES+=("$name")
  BACKUP_META+=("$name"$'\t'"$target"$'\t'"$meta_mode"$'\t'"$meta_uid:$meta_gid")
}

# record_backup <name> <abs-target> <mode> <uid:gid>
# Registers a file that was generated into $BACKUP_DIR (e.g. sqlite3 .backup
# output) without copying.
record_backup() {
  local name="$1" target="$2" mode="$3" owner="$4"
  manifest_name_ok "$name" || die "unsafe manifest name: $name"
  abs_target_ok "$target" || die "unsafe target: $target"
  BACKUP_NAMES+=("$name")
  BACKUP_META+=("$name"$'\t'"$target"$'\t'"$mode"$'\t'"$owner")
}

# finalize_backup <out_dir> [comment...]
# Writes manifest.meta, manifest.sha256 (comment header + manifest.meta + every
# named file, relative names, sha256sum -c compatible) and self-verifies.
finalize_backup() {
  local out_dir="$1"
  shift
  local c
  printf '%s\n' "${BACKUP_META[@]}" > "$out_dir/manifest.meta"
  (
    cd "$out_dir" || die "cannot enter backup dir: $out_dir"
    printf '# frp-panel backup manifest %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    for c in "$@"; do printf '# %s\n' "$c"; done
    sha256sum manifest.meta "${BACKUP_NAMES[@]}" > manifest.sha256
  )
  verify_manifest "$out_dir"
}

# checksum_manifest_has_name <dir> <name>: exact membership test for the
# sha256sum manifest (never a substring/suffix match).
checksum_manifest_has_name() {
  local dir="$1" wanted="$2" line name
  while IFS= read -r line; do
    [[ -n "$line" && "$line" != \#* ]] || continue
    ((${#line} >= 67)) || continue
    name="${line:66}"
    [[ "$name" == "$wanted" ]] && return 0
  done < "$dir/manifest.sha256"
  return 1
}

# validate_checksum_manifest <dir>: validate every checksum path BEFORE
# sha256sum is allowed to open it. Entries must be safe relative names,
# unique regular files inside the backup directory, never symlinks.
validate_checksum_manifest() {
  local dir="$1" line hash marker name count=0
  local -A seen=()
  while IFS= read -r line; do
    [[ -n "$line" && "$line" != \#* ]] || continue
    ((${#line} >= 67)) || die "invalid checksum manifest line"
    hash="${line:0:64}"
    marker="${line:64:2}"
    name="${line:66}"
    [[ "$hash" =~ ^[0-9A-Fa-f]{64}$ ]] || die "invalid checksum digest"
    [[ "$marker" == "  " || "$marker" == " *" ]] || die "invalid checksum marker"
    manifest_name_ok "$name" || die "unsafe checksum manifest name: $name"
    [[ -z "${seen[$name]+x}" ]] || die "duplicate checksum manifest name: $name"
    seen["$name"]=1
    [[ -f "$dir/$name" && ! -L "$dir/$name" ]] || die "checksum entry is missing, non-regular, or a symlink: $name"
    count=$((count + 1))
  done < "$dir/manifest.sha256"
  ((count > 0)) || die "checksum manifest has no entries"
  [[ -n "${seen[manifest.meta]+x}" ]] || die "checksum manifest does not cover manifest.meta"
}

# verify_manifest <dir>: validate paths, then sha256sum -c inside the backup
# directory; dies on mismatch. Path validation must precede sha256sum so a
# malicious manifest cannot make the verifier read outside the backup tree.
verify_manifest() {
  local dir="$1"
  [[ -f "$dir/manifest.sha256" && ! -L "$dir/manifest.sha256" && -f "$dir/manifest.meta" && ! -L "$dir/manifest.meta" ]] || die "backup has no regular manifest files: $dir"
  validate_checksum_manifest "$dir"
  (cd "$dir" && sha256sum -c manifest.sha256 >/dev/null) || die "manifest verification failed: $dir"
}

# ---- restore helpers ----------------------------------------------------------
RESTORE_ENTRIES=() # flat array: name target mode owner (4 slots per entry)
# parse_manifest <dir> <prefixes-array-name>: validates every meta line against
# the whitelist, dies on any unsafe entry BEFORE any restore happens.
parse_manifest() {
  local dir="$1"
  local -n prefixes="$2"
  RESTORE_ENTRIES=()
  local line name="" target="" mode="" owner=""
  local -A seen_names=() seen_targets=()
  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    name="" target="" mode="" owner=""
    IFS=$'\t' read -r name target mode owner <<< "$line" || true
    manifest_name_ok "$name" || die "unsafe manifest name: $name"
    abs_target_ok "$target" || die "unsafe target: $target"
    target_ok "$target" "${prefixes[@]}" || die "target outside whitelist: $target"
    [[ "$mode" =~ ^[0-7]{3,4}$ ]] || die "invalid mode in manifest: $mode"
    [[ "$owner" =~ ^[0-9]+:[0-9]+$ ]] || die "invalid owner in manifest: $owner"
    [[ -z "${seen_names[$name]+x}" ]] || die "duplicate manifest name: $name"
    [[ -z "${seen_targets[$target]+x}" ]] || die "duplicate restore target: $target"
    checksum_manifest_has_name "$dir" "$name" || die "manifest entry is not covered by manifest.sha256: $name"
    [[ -f "$dir/$name" && ! -L "$dir/$name" ]] || die "manifest entry is missing, non-regular, or a symlink: $name"
    seen_names["$name"]=1
    seen_targets["$target"]=1
    RESTORE_ENTRIES+=("$name" "$target" "$mode" "$owner")
  done < "$dir/manifest.meta"
  ((${#RESTORE_ENTRIES[@]} > 0)) || die "manifest.meta has no restore entries"
}

# restore_entry <backup_dir> <name> <target> <mode> <owner>
# Copies one file back to its absolute target (joined under FS_ROOT). chmod and
# chown are best-effort by design (chown is a no-op on non-root hosts).
restore_entry() {
  local dir="$1" name="$2" target="$3" mode="$4" owner="$5"
  local resolved
  resolved="$(resolve_target "$target")"
  dry_or_run mkdir -p "$(dirname "$resolved")"
  dry_or_run cp -p "$dir/$name" "$resolved"
  if ! dry_or_run chmod "$mode" "$resolved"; then
    printf 'WARNING: chmod %s %s failed (best-effort)\n' "$mode" "$resolved" >&2
  fi
  if ! dry_or_run chown "$owner" "$resolved" 2>/dev/null; then
    printf 'WARNING: chown %s %s failed (best-effort)\n' "$owner" "$resolved" >&2
  fi
}

# ---- service / port wait helpers ----------------------------------------------
# WAIT_INTERVAL defaults to 1s; tests override with a fraction to stay fast.
wait_is_active() {
  local service="$1" timeout="${2:-15}" i
  for ((i = 0; i < timeout; i++)); do
    if [[ "$(systemctl is-active "$service" 2>/dev/null || true)" == "active" ]]; then
      return 0
    fi
    sleep "${WAIT_INTERVAL:-1}"
  done
  return 1
}
port_listening() { # port_listening <port>: 0 when ss -ltn shows it
  # Anchored boundary so :17002 never satisfies 7002, while wildcard
  # (*:PORT), bracket ([::]:PORT) and numeric-IP (0.0.0.0:PORT) listeners
  # all match. Ports are validated numeric before reaching this regex.
  local port="$1"
  ss -ltn 2>/dev/null | grep -qE "(^|[^0-9]):${port}([[:space:]]|$)|[0-9]:${port}([[:space:]]|$)"
}
wait_port_listening() {
  local port="$1" timeout="${2:-15}" i
  for ((i = 0; i < timeout; i++)); do
    if port_listening "$port"; then return 0; fi
    sleep "${WAIT_INTERVAL:-1}"
  done
  return 1
}
wait_port_free() {
  local port="$1" timeout="${2:-15}" i
  for ((i = 0; i < timeout; i++)); do
    if ! port_listening "$port"; then return 0; fi
    sleep "${WAIT_INTERVAL:-1}"
  done
  return 1
}
ports_listening_in() { # ports_listening_in <ss-output> <"p1 p2">
  local out="$1" ports="$2" p
  for p in $ports; do
    grep -qE "(^|[^0-9]):${p}([[:space:]]|$)|[0-9]:${p}([[:space:]]|$)" <<<"$out" || return 1
  done
  return 0
}
wait_ports_listening() { # one ss capture per iteration, all ports must show
  local ports="$1" timeout="${2:-15}" i out
  for ((i = 0; i < timeout; i++)); do
    out="$(ss -ltn 2>/dev/null || true)"
    if ports_listening_in "$out" "$ports"; then return 0; fi
    sleep "${WAIT_INTERVAL:-1}"
  done
  return 1
}
wait_ports_free() {
  local ports="$1" timeout="${2:-15}" i out
  for ((i = 0; i < timeout; i++)); do
    out="$(ss -ltn 2>/dev/null || true)"
    if ! ports_listening_in "$out" "$ports"; then return 0; fi
    sleep "${WAIT_INTERVAL:-1}"
  done
  return 1
}
# k30_port_listening <port>: Android side check via /proc/net/tcp (hex port).
k30_port_listening() {
  local port="$1" hex
  hex="$(printf '%04X' "$port")"
  [[ -f "${FS_ROOT}/proc/net/tcp" ]] || return 1
  grep -q ":${hex} " "${FS_ROOT}/proc/net/tcp" 2>/dev/null
}
wait_k30_port() {
  local port="$1" timeout="${2:-24}" i
  for ((i = 0; i < timeout; i++)); do
    if k30_port_listening "$port"; then return 0; fi
    sleep "${WAIT_INTERVAL:-1}"
  done
  return 1
}
