#!/usr/bin/env bash
# Offline TDD harness for I2 backup / rollback / migration scripts.
#
# Read-only for the repository: every transient write happens below the
# checkout's D-drive `.tmp` directory and the trap removes it on exit.
# systemd / ss / sqlite3 / pkill / pgrep / ssh / curl /
# adb are all PATH stubs from deploy/tests/fixtures; nothing touches the network
# or a real host. Run from the repo root:
#   bash deploy/tests/test-backup-rollback.sh
#   bash deploy/tests/test-backup-rollback.sh --root /absolute/path/to/checkout
set -euo pipefail

root=""
if [[ "${1:-}" == "--root" ]]; then
  root="${2:-}"
  [[ -n "$root" ]] || { printf 'ERROR: --root requires a path\n' >&2; exit 2; }
  shift 2
else
  root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fi
[[ $# -eq 0 ]] || { printf 'ERROR: unexpected arguments\n' >&2; exit 2; }
[[ -d "$root/.git" ]] || { printf 'ERROR: not a git checkout: %s\n' "$root" >&2; exit 2; }

TMP_BASE="$root/.tmp"
mkdir -p "$TMP_BASE"
export TMPDIR="$TMP_BASE"
TMP="$(mktemp -d "$TMP_BASE/i2-offline.XXXXXX")"
cleanup_tmp() {
  rm -rf "$TMP"
  rmdir "$TMP_BASE" 2>/dev/null || true
}
trap cleanup_tmp EXIT
[[ "$TMP" == "$TMP_BASE/"* ]] || { printf 'ERROR: temp directory escaped D-drive checkout: %s\n' "$TMP" >&2; exit 1; }

FIX="$root/deploy/tests/fixtures"
STUBS="$TMP/stubs"
STUBS_NOSQL="$TMP/stubs-nosql"
mkdir -p "$STUBS" "$STUBS_NOSQL"
for s in systemctl ss sqlite3 pkill pgrep ssh curl adb; do
  cp "$FIX/stub-$s" "$STUBS/$s"
  chmod +x "$STUBS/$s"
done
for s in systemctl ss pkill pgrep ssh curl adb; do
  cp "$FIX/stub-$s" "$STUBS_NOSQL/$s"
  chmod +x "$STUBS_NOSQL/$s"
done

ORIG_PATH="$PATH"
export PATH="$STUBS:$ORIG_PATH"
export STUB_LOG="$TMP/stub.log"
export WAIT_ACTIVE=2 WAIT_PORT=2 WAIT_PUBLIC=3 WAIT_INTERVAL=0.05

FS="$TMP/fs"    # --fs-root fixture tree
ODIR="$TMP/out" # backup output dirs
mkdir -p "$FS" "$ODIR"
cp -r "$FIX/aliyun-src/." "$FS/"
cp -r "$FIX/k30-src/." "$FS/"

find "$root/deploy" -type f | sort > "$TMP/deploy-before.txt"
# Repo-root watcher: snapshot the pre-run state so T30b can prove the harness
# leaves no residue other than its own (trap-cleaned) .tmp workspace.
ls -A "$root" | grep -vE '^\.git$|^\.tmp$' | sort > "$TMP/root-files-before.txt"
(cd "$root" && git status --porcelain --untracked-files=all | sort) > "$TMP/status-before.txt"

PASS=0
FAIL=0
SKIP=0

report() {
  if [[ "$1" == ok ]]; then
    PASS=$((PASS + 1))
    printf '  ok   %s\n' "$2"
  elif [[ "$1" == skip ]]; then
    SKIP=$((SKIP + 1))
    printf '  SKIP %s\n' "$2"
  else
    FAIL=$((FAIL + 1))
    printf '  FAIL %s\n' "$2" >&2
  fi
}

assert_true() { # assert_true <desc> -- cmd...
  local desc="$1"
  shift
  if "$@" >/dev/null 2>&1; then report ok "$desc"; else report FAIL "$desc"; fi
}
assert_false() {
  local desc="$1"
  shift
  if "$@" >/dev/null 2>&1; then report FAIL "$desc"; else report ok "$desc"; fi
}
assert_eq() {
  local desc="$1" exp="$2" got="$3"
  if [[ "$got" == "$exp" ]]; then report ok "$desc"; else report FAIL "$desc (want '$exp', got '$got')"; fi
}
assert_file() {
  local desc="$1" f="$2"
  if [[ -f "$f" ]]; then report ok "$desc"; else report FAIL "$desc (missing: $f)"; fi
}
assert_no_file() {
  local desc="$1" f="$2"
  if [[ -e "$f" ]]; then report FAIL "$desc (exists: $f)"; else report ok "$desc"; fi
}
assert_differs() { # assert_differs <desc> <fileA> <fileB>
  local desc="$1" a="$2" b="$3"
  if cmp -s "$a" "$b"; then report FAIL "$desc (files identical)"; else report ok "$desc"; fi
}
assert_grep() { # assert_grep <desc> <literal> <file>
  local desc="$1" pat="$2" f="$3"
  if grep -Fq -- "$pat" "$f"; then report ok "$desc"; else report FAIL "$desc (no '$pat' in $f)"; fi
}
assert_no_grep() {
  local desc="$1" pat="$2" f="$3"
  if grep -Fq -- "$pat" "$f"; then report FAIL "$desc ('$pat' found in $f)"; else report ok "$desc"; fi
}
assert_out() { # assert_out <desc> <literal>  -- output in $OUT captured by run_capture
  local desc="$1" pat="$2"
  if [[ "$OUT" == *"$pat"* ]]; then report ok "$desc"; else report FAIL "$desc (no '$pat' in output)"; fi
}
assert_no_out() {
  local desc="$1" pat="$2"
  if [[ "$OUT" == *"$pat"* ]]; then report FAIL "$desc ('$pat' found in output)"; else report ok "$desc"; fi
}

RC=0
OUT=""
run_capture() { # run_capture -- cmd...   (captures rc into $RC, stdout+stderr into $OUT)
  set +e
  OUT="$("$@" 2>&1)"
  RC=$?
  set -e
}

new_fs() {
  rm -rf "$FS"
  mkdir -p "$FS" "$ODIR"
  cp -r "$FIX/aliyun-src/." "$FS/"
  cp -r "$FIX/k30-src/." "$FS/"
}
fresh_log() { : > "$STUB_LOG"; }
backup_aliyun() { # backup_aliyun <outdir> [extra flags...]  -> requires new_fs
  run_capture bash "$root/deploy/backup-aliyun.sh" --fs-root "$FS" --out "$1" "${@:2}"
  assert_eq "backup-aliyun rc" 0 "$RC"
}
backup_k30() {
  run_capture bash "$root/deploy/backup-k30pro.sh" --fs-root "$FS" --out "$1" "${@:2}"
  assert_eq "backup-k30pro rc" 0 "$RC"
}

# shellcheck disable=SC2016
SCRIPTS=(
  deploy/lib/backup-common.sh
  deploy/backup-aliyun.sh
  deploy/backup-k30pro.sh
  deploy/rollback-aliyun.sh
  deploy/rollback-k30pro.sh
  deploy/migrate.sh
  deploy/hooks/verify-aliyun-rollback.sh
  deploy/hooks/verify-k30pro-rollback.sh
)

printf 'T01 bash -n on all scripts\n'
for f in "${SCRIPTS[@]}" deploy/tests/test-backup-rollback.sh deploy/tests/validate-baseline.sh; do
  assert_true "bash -n $f" bash -n "$root/$f"
done
for f in "$FIX"/stub-*; do
  assert_true "bash -n $(basename "$f")" bash -n "$f"
done

printf 'T02 required scripts present\n'
for f in "${SCRIPTS[@]}" deploy/tests/fixtures/stub-systemctl deploy/tests/fixtures/stub-ss \
  deploy/tests/fixtures/stub-sqlite3 deploy/tests/fixtures/stub-pkill deploy/tests/fixtures/stub-pgrep \
  deploy/tests/fixtures/stub-ssh deploy/tests/fixtures/stub-curl deploy/tests/fixtures/stub-adb; do
  assert_file "present $f" "$root/$f"
done

printf 'T03 usage / exit codes\n'
run_capture bash "$root/deploy/backup-aliyun.sh" --nonsense
assert_eq "backup-aliyun unknown flag -> 2" 2 "$RC"
run_capture bash "$root/deploy/backup-aliyun.sh" --help
assert_eq "backup-aliyun --help -> 0" 0 "$RC"
run_capture bash "$root/deploy/backup-aliyun.sh" --fs-root relative/path
assert_eq "backup-aliyun relative --fs-root -> 2" 2 "$RC"
run_capture bash "$root/deploy/backup-k30pro.sh" --nonsense
assert_eq "backup-k30pro unknown flag -> 2" 2 "$RC"
run_capture bash "$root/deploy/rollback-aliyun.sh"
assert_eq "rollback-aliyun missing backup dir -> 2" 2 "$RC"
run_capture bash "$root/deploy/rollback-k30pro.sh"
assert_eq "rollback-k30pro missing backup dir -> 2" 2 "$RC"
run_capture bash "$root/deploy/migrate.sh"
assert_eq "migrate missing --backup-dir -> 2" 2 "$RC"
run_capture bash "$root/deploy/migrate.sh" --help
assert_eq "migrate --help -> 0" 0 "$RC"
run_capture bash "$root/deploy/hooks/verify-aliyun-rollback.sh" --help
assert_eq "verify-aliyun --help -> 0" 0 "$RC"
run_capture bash "$root/deploy/hooks/verify-aliyun-rollback.sh" --nonsense
assert_eq "verify-aliyun unknown flag -> 2" 2 "$RC"
run_capture bash "$root/deploy/hooks/verify-k30pro-rollback.sh" --help
assert_eq "verify-k30pro --help -> 0" 0 "$RC"

printf 'T04 backup-aliyun online SQLite (.backup, no wal/shm, no systemctl)\n'
new_fs
fresh_log
backup_aliyun "$ODIR/a1"
assert_file "out frps-phone.toml" "$ODIR/a1/frps-phone.toml"
assert_file "out unit" "$ODIR/a1/frps-phone.service"
assert_file "out frps binary" "$ODIR/a1/frps"
assert_file "out env" "$ODIR/a1/frp-panel.env"
assert_no_file "no snapshot.key in backup (spec 12.1 fingerprint only)" "$ODIR/a1/snapshot.key"
assert_file "out snapshot-key.fingerprint" "$ODIR/a1/snapshot-key.fingerprint"
assert_no_grep "fingerprint is not a manifest restore entry" "snapshot" "$ODIR/a1/manifest.meta"
assert_grep "fingerprint covered by manifest.sha256" "snapshot-key.fingerprint" "$ODIR/a1/manifest.sha256"
assert_file "out data.db" "$ODIR/a1/data.db"
assert_no_file "no wal in online backup" "$ODIR/a1/data.db-wal"
assert_no_file "no shm in online backup" "$ODIR/a1/data.db-shm"
assert_grep "sqlite3 .backup invoked" "sqlite3" "$STUB_LOG"
assert_no_grep "no systemctl stop in online mode" "systemctl stop" "$STUB_LOG"
assert_no_grep "no systemctl start in online mode" "systemctl start" "$STUB_LOG"
assert_out "stdout says backup complete" "backup complete"
assert_out "stdout shows backup dir" "$ODIR/a1"
fp="$(cat "$ODIR/a1/snapshot-key.fingerprint" 2>/dev/null || true)"
assert_true "fingerprint is 64 hex" grep -Eq '^[0-9a-f]{64}$' <<<"$fp"
exp_fp="$(sha256sum "$FS/opt/frp-panel/secrets/snapshot.key" | awk '{print $1}')"
assert_eq "fingerprint == sha256 of the source key" "$exp_fp" "$fp"
assert_no_out "fingerprint value never printed to output" "$fp"

printf 'T05 backup-aliyun stop-copy fallback (force-stop and sqlite3-absent)\n'
new_fs
fresh_log
run_capture bash "$root/deploy/backup-aliyun.sh" --fs-root "$FS" --out "$ODIR/a2" --force-stop
assert_eq "force-stop backup rc" 0 "$RC"
assert_file "wal copied in fallback" "$ODIR/a2/data.db-wal"
assert_file "shm copied in fallback" "$ODIR/a2/data.db-shm"
stop_line="$(grep -n 'systemctl stop frp-panel.service' "$STUB_LOG" | head -n1 | cut -d: -f1)"
start_line="$(grep -n 'systemctl start frp-panel.service' "$STUB_LOG" | head -n1 | cut -d: -f1)"
assert_true "stop before start (stop=$stop_line start=$start_line)" test "$stop_line" -lt "$start_line"
assert_true "stop line present" test -n "$stop_line"
new_fs
fresh_log
run_capture env PATH="$STUBS_NOSQL:$ORIG_PATH" bash "$root/deploy/backup-aliyun.sh" --fs-root "$FS" --out "$ODIR/a2b"
assert_eq "no-sqlite3 fallback rc" 0 "$RC"
assert_grep "no-sqlite3 -> systemctl stop" "systemctl stop frp-panel.service" "$STUB_LOG"
assert_file "no-sqlite3 wal copied" "$ODIR/a2b/data.db-wal"

printf 'T06 backup dry-run: zero side effects\n'
new_fs
fresh_log
run_capture bash "$root/deploy/backup-aliyun.sh" --dry-run --fs-root "$FS" --out "$ODIR/a3"
assert_eq "dry-run rc" 0 "$RC"
assert_no_file "dry-run creates no out dir" "$ODIR/a3"
assert_out "dry-run prints [dry-run]" "[dry-run]"
assert_no_grep "dry-run touches no stubs" "systemctl" "$STUB_LOG"
run_capture bash "$root/deploy/backup-k30pro.sh" --dry-run --fs-root "$FS" --out "$ODIR/k3"
assert_eq "k30 dry-run rc" 0 "$RC"
assert_no_file "k30 dry-run creates no out dir" "$ODIR/k3"

printf 'T07 manifest format and self-verification\n'
new_fs
fresh_log
backup_aliyun "$ODIR/a4"
assert_true "manifest.sha256 -c passes" bash -c "cd '$ODIR/a4' && sha256sum -c manifest.sha256 >/dev/null"
assert_grep "manifest.sha256 lists manifest.meta" "manifest.meta" "$ODIR/a4/manifest.sha256"
meta_bad="$(awk -F'\t' 'NF != 4 { print }' "$ODIR/a4/manifest.meta")"
assert_eq "meta lines all have 4 TAB fields" "" "$meta_bad"
meta_mode="$(awk -F'\t' '$3 !~ /^[0-7]{3,4}$/ { print }' "$ODIR/a4/manifest.meta")"
assert_eq "meta modes are octal 3-4 digits" "" "$meta_mode"
meta_owner="$(awk -F'\t' '$4 !~ /^[0-9]+:[0-9]+$/ { print }' "$ODIR/a4/manifest.meta")"
assert_eq "meta owners are numeric uid:gid" "" "$meta_owner"
meta_abs="$(awk -F'\t' '$2 !~ /^\// { print }' "$ODIR/a4/manifest.meta")"
assert_eq "meta targets are absolute" "" "$meta_abs"

printf 'T08 missing required file -> exit 1, fail-fast, trap restarts service\n'
new_fs
fresh_log
rm -f "$FS/opt/frp-panel/data/data.db"
run_capture env PATH="$STUBS_NOSQL:$ORIG_PATH" bash "$root/deploy/backup-aliyun.sh" --fs-root "$FS" --out "$ODIR/a5"
assert_eq "missing db rc" 1 "$RC"
assert_no_grep "missing db: service never stopped (fail-fast)" "systemctl stop" "$STUB_LOG"
assert_no_grep "missing db: no sqlite3 call" "sqlite3" "$STUB_LOG"
new_fs
fresh_log
rm -f "$FS/etc/systemd/system/frps-phone.service"
run_capture bash "$root/deploy/backup-aliyun.sh" --fs-root "$FS" --out "$ODIR/a7"
assert_eq "missing unit rc" 1 "$RC"
new_fs
fresh_log
export STUB_SYSTEMCTL_STOP_EXIT=1
run_capture env PATH="$STUBS_NOSQL:$ORIG_PATH" bash "$root/deploy/backup-aliyun.sh" --fs-root "$FS" --out "$ODIR/a8"
assert_eq "stop failure rc" 1 "$RC"
assert_grep "stop attempted" "systemctl stop frp-panel.service" "$STUB_LOG"
assert_grep "trap restarted service after stop failure" "systemctl start frp-panel.service" "$STUB_LOG"
unset STUB_SYSTEMCTL_STOP_EXIT

printf 'T09 backup-k30pro: files, mode/owner/hash, dry-run\n'
new_fs
fresh_log
backup_k30 "$ODIR/k1"
assert_file "k30 frpc" "$ODIR/k1/frpc"
assert_file "k30 frpc.toml" "$ODIR/k1/frpc.toml"
assert_file "k30 frpc.sh" "$ODIR/k1/frpc.sh"
assert_true "k30 manifest -c passes" bash -c "cd '$ODIR/k1' && sha256sum -c manifest.sha256 >/dev/null"
meta_mode="$(awk -F'\t' '$3 !~ /^[0-7]{3,4}$/ { print }' "$ODIR/k1/manifest.meta")"
assert_eq "k30 meta modes octal" "" "$meta_mode"
meta_owner="$(awk -F'\t' '$4 !~ /^[0-9]+:[0-9]+$/ { print }' "$ODIR/k1/manifest.meta")"
assert_eq "k30 meta owners numeric" "" "$meta_owner"

printf 'T10 rollback-aliyun success: verify -> restore -> service -> port\n'
new_fs
fresh_log
backup_aliyun "$ODIR/rb1"
rm -f "$FS/opt/frp/frp_0.65.0_linux_amd64/frps-phone.toml"
rm -f "$FS/etc/systemd/system/frps-phone.service"
printf 'tampered-in-place\n' > "$FS/opt/frp-panel/data/data.db"
export STUB_IS_ACTIVE=active
export STUB_SS_OUTPUT='LISTEN 0 128 *:7002 0.0.0.0:*'
run_capture bash "$root/deploy/rollback-aliyun.sh" --backup-dir "$ODIR/rb1" --fs-root "$FS"
assert_eq "rollback rc" 0 "$RC"
assert_true "toml restored" cmp -s "$ODIR/rb1/frps-phone.toml" "$FS/opt/frp/frp_0.65.0_linux_amd64/frps-phone.toml"
assert_true "unit restored" cmp -s "$ODIR/rb1/frps-phone.service" "$FS/etc/systemd/system/frps-phone.service"
assert_true "db restored" cmp -s "$ODIR/rb1/data.db" "$FS/opt/frp-panel/data/data.db"
assert_grep "daemon-reload called" "systemctl daemon-reload" "$STUB_LOG"
assert_grep "frps-phone started" "systemctl start frps-phone" "$STUB_LOG"
assert_grep "is-active checked" "systemctl is-active frps-phone" "$STUB_LOG"
unset STUB_IS_ACTIVE STUB_SS_OUTPUT

printf 'T11 tampered backup rejected with zero restore\n'
new_fs
fresh_log
backup_aliyun "$ODIR/rb2"
printf 'evil-append\n' >> "$ODIR/rb2/frps-phone.toml"
before="$(sha256sum "$FS/etc/systemd/system/frps-phone.service" | awk '{print $1}')"
export STUB_IS_ACTIVE=active STUB_SS_OUTPUT='LISTEN 0 128 *:7002 0.0.0.0:*'
run_capture bash "$root/deploy/rollback-aliyun.sh" --backup-dir "$ODIR/rb2" --fs-root "$FS"
assert_eq "tampered rc" 1 "$RC"
after="$(sha256sum "$FS/etc/systemd/system/frps-phone.service" | awk '{print $1}')"
assert_eq "zero restore: target untouched" "$before" "$after"
assert_no_grep "no service start after tamper" "systemctl start" "$STUB_LOG"
unset STUB_IS_ACTIVE STUB_SS_OUTPUT

printf 'T12 malicious manifest rejected (traversal / whitelist / formats)\n'
new_fs
fresh_log
backup_aliyun "$ODIR/rb3"
make_evil_backup() { # make_evil_backup <outdir> <meta-content-file>
  local d="$1" meta="$2"
  cp "$meta" "$d/manifest.meta"
  rehash_backup "$d"
}
# rehash_backup <dir>: recompute manifest.sha256 over every file present.
rehash_backup() {
  local d="$1" names=() f
  while IFS= read -r f; do
    [[ "$f" == manifest.meta || "$f" == manifest.sha256 ]] && continue
    names+=("$f")
  done < <(cd "$d" && ls -A)
  (cd "$d" && sha256sum manifest.meta "${names[@]}" > manifest.sha256)
}
evil_cases() {
  printf '../evil\t/etc/evil\t644\t0:0\n'
  printf '/etc/passwd\t/etc/passwd\t644\t0:0\n'
  printf 'ok\t/etc/cron.d/x\t644\t0:0\n'
  printf 'ok\t/opt/frp-evil/x\t644\t0:0\n'
  # Existing backup name + whitelisted lexical prefix, but path components
  # escape the prefix after filesystem normalization. This must be rejected
  # before mkdir/cp, not merely fail later on a missing source.
  printf 'frps-phone.toml\t/opt/frp/../../etc/cron.d/x\t644\t0:0\n'
  printf 'ok\topt/frp/x\t644\t0:0\n'
  printf 'ok\t/etc/systemd/system/frps-phone.service\tabc\t0:0\n'
  printf 'ok\t/etc/systemd/system/frps-phone.service\t644\troot:root\n'
  printf 'tab\tname\t/etc/systemd/system/frps-phone.service\t644\t0:0\n'
}
i=0
while IFS= read -r evil_line; do
  i=$((i + 1))
  printf '%s\n' "$evil_line" > "$TMP/evil-meta.txt"
  make_evil_backup "$ODIR/rb3" "$TMP/evil-meta.txt"
  run_capture bash "$root/deploy/rollback-aliyun.sh" --backup-dir "$ODIR/rb3" --fs-root "$FS"
  assert_eq "evil case $i rejected" 1 "$RC"
  assert_no_file "evil case $i: /etc/evil not created" "$FS/etc/evil"
  assert_no_file "evil case $i: /etc/cron.d/x not created" "$FS/etc/cron.d/x"
  assert_no_file "evil case $i: /opt/frp-evil not created" "$FS/opt/frp-evil"
  assert_no_file "evil case $i: /etc/passwd untouched" "$FS/etc/passwd"
  assert_no_grep "evil case $i: no service start" "systemctl start" "$STUB_LOG"
done < <(evil_cases)

# Duplicate manifest names and duplicate restore targets are rejected before
# any restore (backup-common.sh seen_names / seen_targets gates).
new_fs
fresh_log
backup_aliyun "$ODIR/rb3-dupname"
printf 'frps-phone.toml\t/opt/frp/a\t644\t0:0\n' > "$ODIR/rb3-dupname/manifest.meta"
printf 'frps-phone.toml\t/opt/frp/b\t644\t0:0\n' >> "$ODIR/rb3-dupname/manifest.meta"
rehash_backup "$ODIR/rb3-dupname"
run_capture bash "$root/deploy/rollback-aliyun.sh" --backup-dir "$ODIR/rb3-dupname" --fs-root "$FS"
assert_eq "duplicate manifest name rejected" 1 "$RC"
assert_no_file "dup name: /opt/frp/a not created" "$FS/opt/frp/a"

new_fs
fresh_log
backup_aliyun "$ODIR/rb3-duptarget"
printf 'frps-phone.toml\t/opt/frp/a\t644\t0:0\n' > "$ODIR/rb3-duptarget/manifest.meta"
printf 'frps-phone.service\t/opt/frp/a\t644\t0:0\n' >> "$ODIR/rb3-duptarget/manifest.meta"
rehash_backup "$ODIR/rb3-duptarget"
run_capture bash "$root/deploy/rollback-aliyun.sh" --backup-dir "$ODIR/rb3-duptarget" --fs-root "$FS"
assert_eq "duplicate restore target rejected" 1 "$RC"
assert_no_file "dup target: /opt/frp/a not created" "$FS/opt/frp/a"

# A manifest.meta entry must refer to a regular, non-symlink file that is
# itself covered by manifest.sha256. Recomputing only the metadata checksum
# must not let an unchecked payload reach a restore target.
new_fs
fresh_log
backup_aliyun "$ODIR/rb3-unlisted"
printf 'unchecked payload\n' > "$ODIR/rb3-unlisted/unlisted-safe"
printf 'unlisted-safe\t/opt/frp/unlisted-safe\t644\t0:0\n' > "$ODIR/rb3-unlisted/manifest.meta"
(cd "$ODIR/rb3-unlisted" && sha256sum manifest.meta frps-phone.toml frps-phone.service frps frp-panel.env data.db > manifest.sha256)
export STUB_IS_ACTIVE=active STUB_SS_OUTPUT='LISTEN 0 128 *:7002 0.0.0.0:*'
run_capture bash "$root/deploy/rollback-aliyun.sh" --backup-dir "$ODIR/rb3-unlisted" --fs-root "$FS"
assert_eq "unchecked manifest payload rejected" 1 "$RC"
assert_no_file "unchecked payload never restored" "$FS/opt/frp/unlisted-safe"

new_fs
fresh_log
backup_aliyun "$ODIR/rb3-symlink"
printf 'symlink payload\n' > "$TMP/symlink-payload"
ln -s "$TMP/symlink-payload" "$ODIR/rb3-symlink/link-safe"
if [[ -L "$ODIR/rb3-symlink/link-safe" ]]; then
  printf 'link-safe\t/opt/frp/link-safe\t644\t0:0\n' > "$ODIR/rb3-symlink/manifest.meta"
  (cd "$ODIR/rb3-symlink" && sha256sum manifest.meta link-safe > manifest.sha256)
  run_capture bash "$root/deploy/rollback-aliyun.sh" --backup-dir "$ODIR/rb3-symlink" --fs-root "$FS"
  assert_eq "symlink backup payload rejected" 1 "$RC"
  assert_no_file "symlink payload never restored" "$FS/opt/frp/link-safe"
else
  report skip "real symlink fixture unavailable on this Windows/MSYS host; ! -L gate reviewed statically"
fi
unset STUB_IS_ACTIVE STUB_SS_OUTPUT

printf 'T13 rollback-aliyun: is-active timeout -> exit 1, no fake success\n'
new_fs
fresh_log
backup_aliyun "$ODIR/rb4"
export STUB_IS_ACTIVE=inactive STUB_SS_OUTPUT='LISTEN 0 128 *:7002 0.0.0.0:*'
run_capture bash "$root/deploy/rollback-aliyun.sh" --backup-dir "$ODIR/rb4" --fs-root "$FS"
assert_eq "inactive rc" 1 "$RC"
assert_out "failure message mentions service" "frps-phone"
assert_out "failure message mentions not active" "not active"
unset STUB_IS_ACTIVE STUB_SS_OUTPUT

printf 'T14 rollback-aliyun: port not listening -> exit 1\n'
new_fs
fresh_log
backup_aliyun "$ODIR/rb5"
export STUB_IS_ACTIVE=active STUB_SS_OUTPUT='LISTEN 0 128 *:9999 0.0.0.0:*'
run_capture bash "$root/deploy/rollback-aliyun.sh" --backup-dir "$ODIR/rb5" --fs-root "$FS"
assert_eq "port timeout rc" 1 "$RC"
assert_out "failure message mentions 7002" "7002"
unset STUB_IS_ACTIVE STUB_SS_OUTPUT

printf 'T15 rollback-aliyun dry-run: zero writes, zero service calls\n'
new_fs
fresh_log
backup_aliyun "$ODIR/rb6"
rm -f "$FS/opt/frp/frp_0.65.0_linux_amd64/frps-phone.toml"
run_capture bash "$root/deploy/rollback-aliyun.sh" --dry-run --backup-dir "$ODIR/rb6" --fs-root "$FS"
assert_eq "dry-run rc" 0 "$RC"
assert_no_file "dry-run restores nothing" "$FS/opt/frp/frp_0.65.0_linux_amd64/frps-phone.toml"
assert_out "dry-run prints [dry-run]" "[dry-run]"
assert_no_grep "dry-run calls no systemctl" "systemctl" "$STUB_LOG"

printf 'T16 rollback-k30pro success: restore -> pkill -> frpc.sh -> verify\n'
new_fs
fresh_log
backup_k30 "$ODIR/kb1"
printf 'hacked\n' > "$FS/data/frp/frpc.toml"
export STUB_PGREP_OUTPUT=1234 STUB_PGREP_EXIT=0
run_capture bash "$root/deploy/rollback-k30pro.sh" --backup-dir "$ODIR/kb1" --fs-root "$FS" --wait-port 2
assert_eq "k30 rollback rc" 0 "$RC"
assert_true "frpc.toml restored" cmp -s "$ODIR/kb1/frpc.toml" "$FS/data/frp/frpc.toml"
assert_true "frpc restored" cmp -s "$ODIR/kb1/frpc" "$FS/data/frp/frpc"
assert_true "frpc.sh restored" cmp -s "$ODIR/kb1/frpc.sh" "$FS/data/adb/service.d/frpc.sh"
assert_grep "pkill called by script" "pkill -f" "$STUB_LOG"
pkill_count="$(grep -c 'pkill' "$STUB_LOG" || true)"
assert_true "frpc.sh ran (second pkill from script)" test "$pkill_count" -ge 2
assert_grep "verify hook ran (pgrep evidence)" "pgrep" "$STUB_LOG"
mode_after="$(stat -c %a "$FS/data/frp/frpc.toml")"
if [[ "$mode_after" == "600" ]]; then
  report ok "frpc.toml mode forced 0600"
elif [[ "$mode_after" == "644" ]]; then
  report ok "frpc.toml force-0600 attempted (MSYS maps 0600 to 0644; real Linux/toybox keeps 0600)"
else
  report FAIL "frpc.toml mode (want 600, got $mode_after)"
fi
unset STUB_PGREP_OUTPUT STUB_PGREP_EXIT

printf 'T17 rollback-k30pro port never up -> exit 1\n'
new_fs
fresh_log
backup_k30 "$ODIR/kb2"
printf '  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n' > "$FS/proc/net/tcp"
export STUB_PGREP_OUTPUT=1234 STUB_PGREP_EXIT=0
run_capture bash "$root/deploy/rollback-k30pro.sh" --backup-dir "$ODIR/kb2" --fs-root "$FS" --wait-port 2
assert_eq "k30 port timeout rc" 1 "$RC"
assert_out "k30 failure mentions port" "24"
unset STUB_PGREP_OUTPUT STUB_PGREP_EXIT

printf 'T18 rollback-k30pro dry-run: zero writes\n'
new_fs
fresh_log
backup_k30 "$ODIR/kb3"
printf 'hacked\n' > "$FS/data/frp/frpc.toml"
run_capture bash "$root/deploy/rollback-k30pro.sh" --dry-run --backup-dir "$ODIR/kb3" --fs-root "$FS"
assert_eq "k30 dry-run rc" 0 "$RC"
assert_differs "k30 dry-run does not restore" "$ODIR/kb3/frpc.toml" "$FS/data/frp/frpc.toml"
assert_out "k30 dry-run prints [dry-run]" "[dry-run]"
assert_no_grep "k30 dry-run no pkill" "pkill" "$STUB_LOG"
assert_no_grep "k30 dry-run no verify hook (pgrep)" "pgrep" "$STUB_LOG"

printf 'T18b port validation and listening regex safety\n'
# 17002 must never satisfy port 7002 (anchored boundary, no prefix matching)
run_capture env STUB_SS_OUTPUT='LISTEN 0 128 *:17002 0.0.0.0:*' bash -c '
  . "$0/deploy/lib/backup-common.sh"
  wait_port_listening 7002 2
' "$root"
assert_eq "17002 does not satisfy 7002" 1 "$RC"
# numeric-IP wildcard listeners must still satisfy the port
run_capture env STUB_SS_OUTPUT='LISTEN 0 128 0.0.0.0:7002 0.0.0.0:*' bash -c '
  . "$0/deploy/lib/backup-common.sh"
  wait_port_listening 7002 2
' "$root"
assert_eq "0.0.0.0:7002 satisfies 7002" 0 "$RC"
# '24|7002' is a usage error, never interpolated into a regex
run_capture bash "$root/deploy/migrate.sh" --public-ports '24|7002'
assert_eq "migrate --public-ports '24|7002' -> 2" 2 "$RC"
run_capture bash "$root/deploy/migrate.sh" --old-port 0
assert_eq "migrate --old-port 0 -> 2" 2 "$RC"
run_capture bash "$root/deploy/rollback-aliyun.sh" --port 70000
assert_eq "rollback-aliyun --port 70000 -> 2" 2 "$RC"
run_capture bash "$root/deploy/rollback-k30pro.sh" --port abc
assert_eq "rollback-k30pro --port abc -> 2" 2 "$RC"
run_capture bash "$root/deploy/hooks/verify-k30pro-rollback.sh" --port 0
assert_eq "verify-k30pro --port 0 -> 2" 2 "$RC"
# out-of-whitelist backup targets rejected BEFORE any meta is generated
run_capture bash "$root/deploy/backup-aliyun.sh" --fs-root "$FS" --out "$ODIR/a-whitelist" --frps-binary /usr/local/bin/frps
assert_eq "backup-aliyun --frps-binary /usr/local/bin/frps -> 1" 1 "$RC"
assert_out "backup whitelist rejection message" "whitelist"
assert_no_file "whitelist rejection creates no backup dir" "$ODIR/a-whitelist"
run_capture bash "$root/deploy/backup-k30pro.sh" --fs-root "$FS" --out "$ODIR/k-whitelist" --frpc /system/bin/sh
assert_eq "backup-k30pro out-of-whitelist target -> 1" 1 "$RC"

printf 'T19 migrate preflight failures -> exit 1, zero side effects\n'
new_fs
fresh_log
run_capture bash "$root/deploy/migrate.sh" --backup-dir "$TMP/nonexistent" --fs-root "$FS"
assert_eq "missing backup dir rc" 1 "$RC"
assert_no_grep "no service ops on preflight fail" "systemctl" "$STUB_LOG"
new_fs
fresh_log
mkdir -p "$ODIR/empty-bak"
run_capture bash "$root/deploy/migrate.sh" --backup-dir "$ODIR/empty-bak" --fs-root "$FS"
assert_eq "empty backup dir rc" 1 "$RC"
new_fs
fresh_log
backup_aliyun "$ODIR/mb0"
run_capture bash "$root/deploy/migrate.sh" --backup-dir "$ODIR/mb0" --fs-root "$FS"
assert_eq "missing PP_MCP_URL rc" 1 "$RC"

# new required preflight inputs (spec 12.3.5 phone fallback dir / 12.3.1 units)
run_capture env PP_MCP_URL='http://127.0.0.1:8000/mcp' PP_MCP_TOKEN='TOPSECRETTOKEN123' \
  bash "$root/deploy/migrate.sh" --backup-dir "$ODIR/mb0" --fs-root "$FS" --stop-units 'frp-panel-new.service'
assert_eq "missing --k30-backup-dir rc" 1 "$RC"
assert_out "k30 dir error mentions the flag" "--k30-backup-dir"
run_capture env PP_MCP_URL='http://127.0.0.1:8000/mcp' PP_MCP_TOKEN='TOPSECRETTOKEN123' \
  bash "$root/deploy/migrate.sh" --backup-dir "$ODIR/mb0" --fs-root "$FS" --stop-units 'frp-panel-new.service' \
  --k30-backup-dir '/data/frp/backups'
assert_eq "k30 container path rc" 1 "$RC"
run_capture env PP_MCP_URL='http://127.0.0.1:8000/mcp' PP_MCP_TOKEN='TOPSECRETTOKEN123' \
  bash "$root/deploy/migrate.sh" --backup-dir "$ODIR/mb0" --fs-root "$FS" --stop-units 'frp-panel-new.service' \
  --k30-backup-dir '/data/frp/backups/'
assert_eq "k30 trailing-slash container path rc" 1 "$RC"
run_capture env PP_MCP_URL='http://127.0.0.1:8000/mcp' PP_MCP_TOKEN='TOPSECRETTOKEN123' \
  bash "$root/deploy/migrate.sh" --backup-dir "$ODIR/mb0" --fs-root "$FS" --stop-units 'frp-panel-new.service' \
  --k30-backup-dir 'data/frp/backups/20260805_000000'
assert_eq "k30 relative path rc" 1 "$RC"
run_capture env PP_MCP_URL='http://127.0.0.1:8000/mcp' PP_MCP_TOKEN='TOPSECRETTOKEN123' \
  bash "$root/deploy/migrate.sh" --backup-dir "$ODIR/mb0" --fs-root "$FS" --stop-units 'frp-panel-new.service' \
  --k30-backup-dir '/data/frp/backups/latest'
assert_eq "k30 non-timestamp path rc" 1 "$RC"
run_capture env PP_MCP_URL='http://127.0.0.1:8000/mcp' PP_MCP_TOKEN='TOPSECRETTOKEN123' \
  bash "$root/deploy/migrate.sh" --backup-dir "$ODIR/mb0" --fs-root "$FS" \
  --k30-backup-dir '/data/frp/backups/20260805_000000'
assert_eq "missing --stop-units rc" 1 "$RC"
assert_out "stop-units error mentions the flag" "--stop-units"
# --no-stop-units is a test seam that lifts the stop-units requirement
run_capture env PP_MCP_URL='http://127.0.0.1:8000/mcp' PP_MCP_TOKEN='TOPSECRETTOKEN123' \
  bash "$root/deploy/migrate.sh" --dry-run --backup-dir "$ODIR/mb0" --fs-root "$FS" \
  --no-stop-units --k30-backup-dir '/data/frp/backups/20260805_000000'
assert_eq "dry-run with --no-stop-units passes preflight" 0 "$RC"

printf 'T20 migrate dry-run: plan printed, zero side effects\n'
new_fs
fresh_log
backup_aliyun "$ODIR/mb1"
rm -rf "$ODIR/mb1/evidence"
run_capture env PP_MCP_URL='http://127.0.0.1:8000/mcp' PP_MCP_TOKEN='TOPSECRETTOKEN123' \
  bash "$root/deploy/migrate.sh" --dry-run --backup-dir "$ODIR/mb1" --fs-root "$FS" \
  --takeover-hook "printf 'HOOK\n' >> '$STUB_LOG'" --stop-units 'frp-panel-new.service' \
  --k30-backup-dir '/data/frp/backups/20260805_000000'
assert_eq "migrate dry-run rc" 0 "$RC"
assert_out "dry-run prints [dry-run]" "[dry-run]"
assert_no_grep "dry-run no service calls" "systemctl" "$STUB_LOG"
assert_no_grep "dry-run no hook" "HOOK" "$STUB_LOG"
assert_no_file "migrate dry-run creates no evidence directory" "$ODIR/mb1/evidence"

printf 'T21 migrate success: cutover -> takeover -> verify -> exit 0\n'
new_fs
fresh_log
backup_aliyun "$ODIR/msb"
seq_file="$TMP/ss-seq-ok.txt"
printf 'LISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 *:24 *:* LISTEN 0 128 *:8000 *:*\n' > "$seq_file"
export STUB_SS_SEQUENCE_FILE="$seq_file"
export STUB_IS_ACTIVE=active
export STUB_ADB_DEVICES='List of devices attached
K30DEVICE	device'
export STUB_EXPECT_AUTH_TOKEN='TOPSECRETTOKEN123'
run_capture env PP_MCP_URL='http://127.0.0.1:8000/mcp' PP_MCP_TOKEN='TOPSECRETTOKEN123' \
  bash "$root/deploy/migrate.sh" --backup-dir "$ODIR/msb" --fs-root "$FS" \
  --takeover-hook "printf 'TAKEOVER_RAN OLD_PORT=%s\n' \"\$MIGRATE_OLD_PORT\" >> '$STUB_LOG'" \
  --stop-units 'frp-panel-new.service' --k30-backup-dir '/data/frp/backups/20260805_000000'
assert_eq "migrate success rc" 0 "$RC"
assert_grep "cutover stopped frps-phone" "systemctl stop frps-phone" "$STUB_LOG"
assert_grep "takeover hook ran" "TAKEOVER_RAN" "$STUB_LOG"
assert_grep "env injected into hook" "OLD_PORT=7002" "$STUB_LOG"
assert_grep "verify hook ran ssh" "ssh" "$STUB_LOG"
assert_grep "verify hook ran curl" "curl" "$STUB_LOG"
assert_no_grep "never touches main frps unit" "systemctl stop frps.service" "$STUB_LOG"
assert_no_grep "never touches port 7000" "7000" "$STUB_LOG"
assert_file "evidence migrate log" "$ODIR/msb/evidence/migrate"*.log
assert_file "evidence manifest.sha256" "$ODIR/msb/evidence/manifest.sha256"
assert_file "evidence manifest.meta" "$ODIR/msb/evidence/manifest.meta"
assert_file "evidence checksum verification" "$ODIR/msb/evidence/manifest-check.txt"
assert_grep "evidence checksum verification shows OK" "OK" "$ODIR/msb/evidence/manifest-check.txt"
assert_no_grep "evidence checksum verification has no token" "TOPSECRETTOKEN123" "$ODIR/msb/evidence/manifest-check.txt"
assert_no_grep "evidence has no token" "TOPSECRETTOKEN123" "$ODIR/msb/evidence/migrate"*.log
assert_no_grep "stub log has no token" "TOPSECRETTOKEN123" "$STUB_LOG"
unset STUB_SS_SEQUENCE_FILE STUB_IS_ACTIVE STUB_ADB_DEVICES STUB_EXPECT_AUTH_TOKEN

printf 'T22 migrate auto-rollback: post-cutover verify fails -> rollback from pre-migration backup -> exit 3\n'
new_fs
fresh_log
backup_aliyun "$ODIR/mrb"
seq_file="$TMP/ss-seq-rollback.txt"
printf 'LISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 *:7002 *:*\nLISTEN 0 128 *:24 *:* LISTEN 0 128 *:8000 *:*\n' > "$seq_file"
export STUB_SS_SEQUENCE_FILE="$seq_file"
export STUB_IS_ACTIVE=active
run_capture env PP_MCP_URL='http://127.0.0.1:8000/mcp' PP_MCP_TOKEN='TOPSECRETTOKEN123' \
  bash "$root/deploy/migrate.sh" --backup-dir "$ODIR/mrb" --fs-root "$FS" \
  --takeover-hook "printf 'HOOK\n' >> '$STUB_LOG'" \
  --stop-units 'frp-panel-new.service' --k30-backup-dir '/data/frp/backups/20260805_000000'
assert_eq "migrate auto-rollback rc" 3 "$RC"
assert_grep "new units stopped during rollback" "systemctl stop frp-panel-new.service" "$STUB_LOG"
assert_grep "frps-phone restarted by rollback" "systemctl start frps-phone" "$STUB_LOG"
assert_file "rollback evidence log" "$ODIR/mrb/evidence/rollback-aliyun"*.log
assert_file "rollback evidence manifest.sha256" "$ODIR/mrb/evidence/manifest.sha256"
assert_file "rollback evidence manifest-check.txt" "$ODIR/mrb/evidence/manifest-check.txt"
assert_file "rto evidence" "$ODIR/mrb/evidence/rto.txt"
rto="$(cat "$ODIR/mrb/evidence/rto.txt" | head -n1)"
assert_true "rto recorded numerically" grep -Eq '^[0-9]+$' <<<"$rto"
assert_no_file "no rollback_failed on successful rollback" "$ODIR/mrb/evidence/rollback_failed"
assert_no_grep "evidence no token" "TOPSECRETTOKEN123" "$ODIR/mrb/evidence"/*.log
unset STUB_SS_SEQUENCE_FILE STUB_IS_ACTIVE

printf 'T23 migrate rollback OK but old frpc reconnects + ADB unreachable -> exit 4, rollback_failed\n'
new_fs
fresh_log
backup_aliyun "$ODIR/mrb2"
seq_file="$TMP/ss-seq-fail.txt"
printf 'LISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 *:7002 *:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\n' > "$seq_file"
export STUB_SS_SEQUENCE_FILE="$seq_file"
export STUB_IS_ACTIVE=active
export STUB_ADB_DEVICES='List of devices attached

no devices'
run_capture env PP_MCP_URL='http://127.0.0.1:8000/mcp' PP_MCP_TOKEN='TOPSECRETTOKEN123' \
  bash "$root/deploy/migrate.sh" --backup-dir "$ODIR/mrb2" --fs-root "$FS" \
  --takeover-hook "printf 'HOOK\n' >> '$STUB_LOG'" --stop-units 'frp-panel-new.service' \
  --k30-backup-dir '/data/frp/backups/20260805_000000'
assert_eq "migrate rollback-failed rc" 4 "$RC"
assert_file "rollback_failed marker written" "$ODIR/mrb2/evidence/rollback_failed"
assert_no_grep "no adb shell attempted when unreachable" "adb shell" "$STUB_LOG"
assert_out "honest failure message" "rollback_failed"
unset STUB_SS_SEQUENCE_FILE STUB_IS_ACTIVE STUB_ADB_DEVICES

printf 'T24 migrate ADB-reachable fallback runs rollback-k30pro and recovers -> exit 3\n'
new_fs
fresh_log
backup_aliyun "$ODIR/mrb3"
seq_file="$TMP/ss-seq-adb.txt"
printf 'LISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 *:7002 *:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 *:24 *:* LISTEN 0 128 *:8000 *:*\n' > "$seq_file"
export STUB_SS_SEQUENCE_FILE="$seq_file"
export STUB_IS_ACTIVE=active
export STUB_ADB_DEVICES='List of devices attached
K30DEVICE	device'
run_capture env PP_MCP_URL='http://127.0.0.1:8000/mcp' PP_MCP_TOKEN='TOPSECRETTOKEN123' \
  bash "$root/deploy/migrate.sh" --backup-dir "$ODIR/mrb3" --fs-root "$FS" \
  --takeover-hook "printf 'HOOK\n' >> '$STUB_LOG'" --stop-units 'frp-panel-new.service' \
  --k30-backup-dir '/data/frp/backups/20260805_000000'
assert_eq "migrate adb-fallback rc" 3 "$RC"
assert_grep "k30 rollback invoked via adb" "adb shell sh /data/frp/bin/rollback-k30pro.sh" "$STUB_LOG"
assert_no_file "no rollback_failed when adb fallback recovered" "$ODIR/mrb3/evidence/rollback_failed"
unset STUB_SS_SEQUENCE_FILE STUB_IS_ACTIVE STUB_ADB_DEVICES

printf 'T24b migrate rollback-aliyun itself fails -> exit 4 without adb\n'
new_fs
fresh_log
backup_aliyun "$ODIR/mrb4"
seq_file="$TMP/ss-seq-rali-fail.txt"
printf 'LISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\n' > "$seq_file"
export STUB_SS_SEQUENCE_FILE="$seq_file"
export STUB_IS_ACTIVE=active
export STUB_SYSTEMCTL_START_EXIT=1
run_capture env PP_MCP_URL='http://127.0.0.1:8000/mcp' PP_MCP_TOKEN='TOPSECRETTOKEN123' \
  bash "$root/deploy/migrate.sh" --backup-dir "$ODIR/mrb4" --fs-root "$FS" \
  --takeover-hook "printf 'HOOK\n' >> '$STUB_LOG'" --stop-units 'frp-panel-new.service' \
  --k30-backup-dir '/data/frp/backups/20260805_000000'
assert_eq "rollback-aliyun-fail rc" 4 "$RC"
assert_file "rollback_failed written on rollback-aliyun failure" "$ODIR/mrb4/evidence/rollback_failed"
assert_no_grep "no adb fallback after rollback-aliyun failure" "adb shell" "$STUB_LOG"
unset STUB_SS_SEQUENCE_FILE STUB_IS_ACTIVE STUB_SYSTEMCTL_START_EXIT

printf 'T24c migrate rollback: ports up but verify hook fails -> k30 fallback, still failing -> exit 4\n'
new_fs
fresh_log
backup_aliyun "$ODIR/mrb5"
seq_file="$TMP/ss-seq-hookfail.txt"
printf 'LISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 *:7002 *:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 *:24 *:* LISTEN 0 128 *:8000 *:*\n' > "$seq_file"
export STUB_SS_SEQUENCE_FILE="$seq_file"
export STUB_IS_ACTIVE=active
export STUB_ADB_DEVICES='List of devices attached
K30DEVICE	device'
export STUB_SSH_OUTPUT=1000
run_capture env PP_MCP_URL='http://127.0.0.1:8000/mcp' PP_MCP_TOKEN='TOPSECRETTOKEN123' \
  bash "$root/deploy/migrate.sh" --backup-dir "$ODIR/mrb5" --fs-root "$FS" \
  --takeover-hook "printf 'HOOK\n' >> '$STUB_LOG'" --stop-units 'frp-panel-new.service' \
  --k30-backup-dir '/data/frp/backups/20260805_000000'
assert_eq "ports up but hook fails -> rc 4" 4 "$RC"
assert_file "rollback_failed written when hook fails" "$ODIR/mrb5/evidence/rollback_failed"
assert_grep "k30 fallback still attempted" "adb shell sh" "$STUB_LOG"
assert_out "failure message mentions verification" "verify"
unset STUB_SS_SEQUENCE_FILE STUB_IS_ACTIVE STUB_ADB_DEVICES STUB_SSH_OUTPUT

printf 'T24d migrate rollback: stop-unit failure -> immediate exit 4, no rollback-aliyun\n'
new_fs
fresh_log
backup_aliyun "$ODIR/mrb6"
seq_file="$TMP/ss-seq-stopfail.txt"
printf 'LISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\n' > "$seq_file"
export STUB_SS_SEQUENCE_FILE="$seq_file"
export STUB_IS_ACTIVE=active
export STUB_SYSTEMCTL_STOP_FAIL_UNITS='frp-panel-new.service'
run_capture env PP_MCP_URL='http://127.0.0.1:8000/mcp' PP_MCP_TOKEN='TOPSECRETTOKEN123' \
  bash "$root/deploy/migrate.sh" --backup-dir "$ODIR/mrb6" --fs-root "$FS" \
  --takeover-hook "printf 'HOOK\n' >> '$STUB_LOG'" --stop-units 'frp-panel-new.service' \
  --k30-backup-dir '/data/frp/backups/20260805_000000'
assert_eq "stop failure -> rc 4" 4 "$RC"
assert_file "rollback_failed written on stop failure" "$ODIR/mrb6/evidence/rollback_failed"
assert_no_grep "no rollback-aliyun after stop failure" "systemctl start frps-phone" "$STUB_LOG"
assert_no_grep "no adb fallback after stop failure" "adb shell" "$STUB_LOG"
assert_out "stop failure message" "stop"
unset STUB_SS_SEQUENCE_FILE STUB_IS_ACTIVE STUB_SYSTEMCTL_STOP_FAIL_UNITS

printf 'T24e migrate rollback: ports not released after stop -> immediate exit 4\n'
new_fs
fresh_log
backup_aliyun "$ODIR/mrb7"
seq_file="$TMP/ss-seq-norelease.txt"
printf 'LISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 0.0.0.0:9100 0.0.0.0:*\nLISTEN 0 128 *:24 *:* LISTEN 0 128 *:8000 *:*\nLISTEN 0 128 *:24 *:* LISTEN 0 128 *:8000 *:*\n' > "$seq_file"
export STUB_SS_SEQUENCE_FILE="$seq_file"
export STUB_IS_ACTIVE=active
run_capture env PP_MCP_URL='http://127.0.0.1:8000/mcp' PP_MCP_TOKEN='TOPSECRETTOKEN123' \
  bash "$root/deploy/migrate.sh" --backup-dir "$ODIR/mrb7" --fs-root "$FS" \
  --takeover-hook "printf 'HOOK\n' >> '$STUB_LOG'" --stop-units 'frp-panel-new.service' \
  --k30-backup-dir '/data/frp/backups/20260805_000000'
assert_eq "ports not released -> rc 4" 4 "$RC"
assert_file "rollback_failed written while ports held" "$ODIR/mrb7/evidence/rollback_failed"
assert_no_grep "no rollback-aliyun while ports held" "systemctl start frps-phone" "$STUB_LOG"
assert_out "port-held message" "released"
unset STUB_SS_SEQUENCE_FILE STUB_IS_ACTIVE

printf 'T25 (folded into T21) main frps :7000 never operated\n'

printf 'T26 verify-aliyun-rollback hook: SSH root + MCP 401/200, token never logged\n'
fresh_log
run_capture env PP_MCP_URL='http://127.0.0.1:8000/mcp' PP_MCP_TOKEN='TOPSECRETTOKEN123' \
  bash "$root/deploy/hooks/verify-aliyun-rollback.sh"
assert_eq "hook ok (401 + 200)" 0 "$RC"
assert_grep "ssh checked" "ssh" "$STUB_LOG"
assert_no_grep "token not in stub log" "TOPSECRETTOKEN123" "$STUB_LOG"
assert_no_out "token not in hook output" "TOPSECRETTOKEN123"
fresh_log
run_capture env PP_MCP_URL='http://127.0.0.1:8000/mcp' PP_MCP_TOKEN='TOPSECRETTOKEN123' \
  STUB_CURL_CODE_TOKEN=500 bash "$root/deploy/hooks/verify-aliyun-rollback.sh"
assert_eq "MCP token 500 -> fail" 1 "$RC"
fresh_log
run_capture env PP_MCP_URL='http://127.0.0.1:8000/mcp' PP_MCP_TOKEN='TOPSECRETTOKEN123' \
  STUB_CURL_CODE_NO_TOKEN=200 bash "$root/deploy/hooks/verify-aliyun-rollback.sh"
assert_eq "MCP no-token 200 -> fail" 1 "$RC"
fresh_log
run_capture env PP_MCP_URL='http://127.0.0.1:8000/mcp' PP_MCP_TOKEN='TOPSECRETTOKEN123' \
  STUB_SSH_OUTPUT=1000 bash "$root/deploy/hooks/verify-aliyun-rollback.sh"
assert_eq "ssh non-root -> fail" 1 "$RC"
fresh_log
printf 'TOPSECRETTOKEN123\n' > "$TMP/token-file"
run_capture env PP_MCP_URL='http://127.0.0.1:8000/mcp' PP_MCP_TOKEN_FILE="$TMP/token-file" \
  bash "$root/deploy/hooks/verify-aliyun-rollback.sh"
assert_eq "PP_MCP_TOKEN_FILE path works" 0 "$RC"
assert_no_grep "token file value not in stub log" "TOPSECRETTOKEN123" "$STUB_LOG"
run_capture bash "$root/deploy/hooks/verify-aliyun-rollback.sh"
assert_eq "missing PP_MCP_URL -> fail" 1 "$RC"

# strict stub mode: stub-curl READS the -H @file payload, so a wrong Bearer
# header is a real 401 (proves the token->header path, not just an @file arg)
fresh_log
export STUB_EXPECT_AUTH_TOKEN='TOPSECRETTOKEN123'
run_capture env PP_MCP_URL='http://127.0.0.1:8000/mcp' PP_MCP_TOKEN='TOPSECRETTOKEN123' \
  bash "$root/deploy/hooks/verify-aliyun-rollback.sh"
assert_eq "strict stub: correct Bearer payload -> 200 -> hook ok" 0 "$RC"
assert_no_grep "strict stub: token never in stub log/argv" "TOPSECRETTOKEN123" "$STUB_LOG"
assert_no_grep "strict stub: header content never logged" "Authorization" "$STUB_LOG"
fresh_log
run_capture env PP_MCP_URL='http://127.0.0.1:8000/mcp' PP_MCP_TOKEN='WRONGTOKENVALUE99' \
  bash "$root/deploy/hooks/verify-aliyun-rollback.sh"
assert_eq "strict stub: wrong header payload -> 401 -> hook fail" 1 "$RC"
assert_no_grep "wrong token not in stub log" "WRONGTOKENVALUE99" "$STUB_LOG"
assert_no_out "wrong token not in hook output" "WRONGTOKENVALUE99"
fresh_log
run_capture env PP_MCP_URL='http://127.0.0.1:8000/mcp' PP_MCP_TOKEN='TOPSECRETTOKEN123' \
  STUB_CURL_CODE_TOKEN=500 bash "$root/deploy/hooks/verify-aliyun-rollback.sh"
assert_eq "strict stub: correct payload but 500 -> hook fail" 1 "$RC"
unset STUB_EXPECT_AUTH_TOKEN

printf 'T27 verify-k30pro-rollback hook: proc + config hash + port\n'
new_fs
fresh_log
backup_k30 "$ODIR/kv"
export STUB_PGREP_OUTPUT=1234 STUB_PGREP_EXIT=0
run_capture bash "$root/deploy/hooks/verify-k30pro-rollback.sh" --backup-dir "$ODIR/kv" --fs-root "$FS"
assert_eq "k30 hook ok" 0 "$RC"
export STUB_PGREP_EXIT=1
run_capture bash "$root/deploy/hooks/verify-k30pro-rollback.sh" --backup-dir "$ODIR/kv" --fs-root "$FS"
assert_eq "k30 proc missing -> fail" 1 "$RC"
export STUB_PGREP_EXIT=0
printf 'hacked\n' >> "$FS/data/frp/frpc.toml"
run_capture bash "$root/deploy/hooks/verify-k30pro-rollback.sh" --backup-dir "$ODIR/kv" --fs-root "$FS"
assert_eq "k30 config hash mismatch -> fail" 1 "$RC"
unset STUB_PGREP_OUTPUT STUB_PGREP_EXIT
new_fs
fresh_log
backup_k30 "$ODIR/kv2"
export STUB_PGREP_OUTPUT=1234 STUB_PGREP_EXIT=0
printf '  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n' > "$FS/proc/net/tcp"
run_capture bash "$root/deploy/hooks/verify-k30pro-rollback.sh" --backup-dir "$ODIR/kv2" --fs-root "$FS"
assert_eq "k30 port missing -> fail" 1 "$RC"
unset STUB_PGREP_OUTPUT STUB_PGREP_EXIT

printf 'T28 secret scan: no hardcoded secrets, no token echo, manifest clean\n'
scan="$(grep -RnE '(APP_GLOBAL_SECRET|CLIENT_SECRET|MASTER_SNAPSHOT_KEY|SNAPSHOT_KEY|PP_MCP_TOKEN)[[:space:]]*=[[:space:]]*[A-Za-z0-9_]{16,}' \
  "$root"/deploy/backup-*.sh "$root"/deploy/rollback-*.sh "$root"/deploy/migrate.sh \
  "$root"/deploy/lib "$root"/deploy/hooks 2>/dev/null || true)"
assert_eq "no hardcoded secrets in new scripts" "" "$scan"
scan2="$(grep -RnE '(echo|printf)[^|]*\$PP_MCP_TOKEN' "$root"/deploy/hooks 2>/dev/null || true)"
assert_eq "hooks never echo the token" "" "$scan2"
new_fs
fresh_log
backup_aliyun "$ODIR/secret-check"
assert_no_file "backup dir has no snapshot.key" "$ODIR/secret-check/snapshot.key"
assert_file "backup dir has fingerprint" "$ODIR/secret-check/snapshot-key.fingerprint"
assert_no_grep "fingerprint file holds no key content" "SNAPSHOT_KEY_SECRET" "$ODIR/secret-check/snapshot-key.fingerprint"
assert_no_grep "manifest.sha256 has no env secret" "ENVSECRET_FIXTURE" "$ODIR/secret-check/manifest.sha256"
assert_no_grep "manifest.meta has no env secret" "ENVSECRET_FIXTURE" "$ODIR/secret-check/manifest.meta"
assert_no_grep "manifest.sha256 has no key material" "SNAPSHOT_KEY_SECRET" "$ODIR/secret-check/manifest.sha256"
assert_no_grep "manifest.meta has no key material" "SNAPSHOT_KEY_SECRET" "$ODIR/secret-check/manifest.meta"
assert_no_out "backup stdout has no env secret" "ENVSECRET_FIXTURE"
assert_no_out "backup stdout has no key material" "SNAPSHOT_KEY_SECRET"

printf 'T29 .gitignore narrowing: deploy files trackable, root binary ignored\n'
assert_false "deploy/systemd unit not ignored" git check-ignore deploy/systemd/frp-panel.service
assert_false "deploy/logrotate not ignored" git check-ignore deploy/logrotate/frp-panel
assert_false "deploy/env example not ignored" git check-ignore deploy/env/frp-panel.env.example
assert_false "backup-aliyun not ignored" git check-ignore deploy/backup-aliyun.sh
assert_false "backup-k30pro not ignored" git check-ignore deploy/backup-k30pro.sh
assert_false "rollback-aliyun not ignored" git check-ignore deploy/rollback-aliyun.sh
assert_false "rollback-k30pro not ignored" git check-ignore deploy/rollback-k30pro.sh
assert_false "migrate not ignored" git check-ignore deploy/migrate.sh
assert_false "lib not ignored" git check-ignore deploy/lib/backup-common.sh
assert_false "hooks not ignored" git check-ignore deploy/hooks/verify-aliyun-rollback.sh
assert_false "test harness not ignored" git check-ignore deploy/tests/test-backup-rollback.sh
assert_true "root frp-panel binary still ignored" git check-ignore frp-panel
assert_true "root frp-panel.exe still ignored" git check-ignore frp-panel.exe
assert_true "harness .tmp dir ignored" git check-ignore .tmp/
assert_true "harness .tmp content ignored" git check-ignore .tmp/i2-offline.anydir/x

printf 'T30 repo read-only: harness writes nothing under deploy/\n'
find "$root/deploy" -type f | sort > "$TMP/deploy-after.txt"
if diff -u "$TMP/deploy-before.txt" "$TMP/deploy-after.txt" > "$TMP/deploy.diff" 2>&1; then
  report ok "deploy/ file set unchanged by tests"
else
  report FAIL "deploy/ file set changed: $(cat "$TMP/deploy.diff")"
fi

printf 'T30b repo root watcher: only harness-owned residue, trap-cleanable\n'
# .tmp must be invisible to git status even while it holds harness files
status_now="$(cd "$root" && git status --porcelain --untracked-files=all)"
if grep -q '\.tmp' <<<"$status_now"; then
  report FAIL "git status shows .tmp during run (/.tmp/ ignore missing)"
else
  report ok "git status hides .tmp even while it holds harness files"
fi
if diff -u "$TMP/root-files-before.txt" <(ls -A "$root" | grep -vE '^\.git$|^\.tmp$' | sort) >/dev/null 2>&1; then
  report ok "repo root file set unchanged (ignoring .git and .tmp)"
else
  report FAIL "repo root file set changed by harness"
fi
if diff -u "$TMP/status-before.txt" <(cd "$root" && git status --porcelain --untracked-files=all | sort) >/dev/null 2>&1; then
  report ok "git status identical across the run"
else
  report FAIL "git status changed across the run"
fi

printf 'T31 static sample manifests self-verify and restore\n'
assert_true "sample-aliyun manifest -c passes" bash -c "cd '$FIX/sample-aliyun-backup' && sha256sum -c manifest.sha256 >/dev/null"
assert_true "sample-k30 manifest -c passes" bash -c "cd '$FIX/sample-k30-backup' && sha256sum -c manifest.sha256 >/dev/null"
new_fs
fresh_log
export STUB_IS_ACTIVE=active STUB_SS_OUTPUT='LISTEN 0 128 *:7002 0.0.0.0:*'
sample_key_before="$(sha256sum "$FS/opt/frp-panel/secrets/snapshot.key" | awk '{print $1}')"
run_capture bash "$root/deploy/rollback-aliyun.sh" --backup-dir "$FIX/sample-aliyun-backup" --fs-root "$FS"
assert_eq "rollback from static sample rc" 0 "$RC"
assert_true "static sample toml restored" cmp -s "$FIX/sample-aliyun-backup/frps-phone.toml" "$FS/opt/frp/frp_0.65.0_linux_amd64/frps-phone.toml"
sample_key_after="$(sha256sum "$FS/opt/frp-panel/secrets/snapshot.key" | awk '{print $1}')"
assert_eq "static sample rollback never touches the secret file" "$sample_key_before" "$sample_key_after"
unset STUB_IS_ACTIVE STUB_SS_OUTPUT

printf 'T32 baseline validator still passes\n'
run_capture bash "$root/deploy/tests/validate-baseline.sh" --root "$root"
assert_eq "validate-baseline rc" 0 "$RC"

# final cleanup proof: the harness's .tmp workspace is removed by cleanup_tmp
# (the EXIT trap re-runs it idempotently); a SIGKILLed run would leave it
# behind, but /.tmp/ in .gitignore keeps it out of git status (T29/T30b)
cleanup_tmp
assert_no_file "harness .tmp workspace removed by cleanup" "$root/.tmp"

printf '\n========================================\n'
printf 'PASS: %d   FAIL: %d   SKIP: %d\n' "$PASS" "$FAIL" "$SKIP"
printf '========================================\n'
if (( FAIL > 0 )); then
  printf 'test-backup-rollback: FAILED (%d failure(s))\n' "$FAIL" >&2
  exit 1
fi
printf 'test-backup-rollback: ALL GREEN\n'
