#!/bin/sh
# verify-port-reservations.sh - post-deploy health + consistency check for the
# same-name-overwrite / port-reservation-leak fix.
#
# Run this ON the master host (the layout deploy/ci-deploy-master.sh installs:
# binary /opt/frp-panel/bin/frp-panel, env /etc/frp-panel/frp-panel.env,
# sqlite /opt/frp-panel/data/data.db, master API on 127.0.0.1:9100, RPC on
# 127.0.0.1:9101).
#
# READ-ONLY: no UPDATE, no DELETE, no writes of any kind. Anything reported as
# an orphan is released by the operator with explicit per-row SQL, never
# silently.
#
# Overrides: DB, ENV_FILE, BIN, API_PORT, RPC_PORT, EXPECTED_AGENTS.
# Exit code: 0 = PASS, 1 = deployed but inconsistent (or a check failed).
#
# The DB checks run through python3 on purpose: the sqlite3 CLI shipped on
# CentOS 7 is 3.7.17 (no `-readonly`, no json1), and proxy_config.content is
# JSON, so the physical port is parsed with json instead of SQL.
set -u

DB=${DB:-}
ENV_FILE=${ENV_FILE:-/etc/frp-panel/frp-panel.env}
BIN=${BIN:-/opt/frp-panel/bin/frp-panel}
API_PORT=${API_PORT:-9100}
RPC_PORT=${RPC_PORT:-9101}
EXPECTED_AGENTS=${EXPECTED_AGENTS:-1}

fail=0
say()  { printf '%s\n' "$*"; }
bad()  { printf 'FAIL  %s\n' "$*"; fail=1; }
ok()   { printf 'ok    %s\n' "$*"; }
warn() { printf 'warn  %s\n' "$*"; }

# ---------------------------------------------------------------- service ----
say "== 1. service =="
if systemctl is-active --quiet frp-panel 2>/dev/null; then
  ok "systemd unit frp-panel active"
  started=$(systemctl show frp-panel -p ActiveEnterTimestamp --value 2>/dev/null || true)
  [ -n "$started" ] && say "      active since: $started"
elif command -v rc-service >/dev/null 2>&1 && rc-service frp-panel status >/dev/null 2>&1; then
  ok "openrc service frp-panel running"
else
  bad "frp-panel service is not active"
fi

if [ -f "$BIN" ]; then
  say "      binary: $(ls -l --time-style=+%F_%T "$BIN" 2>/dev/null | awk '{print $6, $7}')  ($BIN)"
  if grep -qa 'PROXY_NAME_CONFLICT' "$BIN" 2>/dev/null; then
    ok "binary contains the PROXY_NAME_CONFLICT fix"
  else
    bad "binary does NOT contain the same-name-create fix (stale build?)"
  fi
else
  bad "binary not found at $BIN"
fi

# -------------------------------------------------------------- listeners ----
say "== 2. listeners =="
for port in "$API_PORT" "$RPC_PORT"; do
  if ss -tln 2>/dev/null | grep -qE "[:.]${port}\b"; then
    ok "listening on :$port"
  else
    bad "nothing listening on :$port"
  fi
done

say "== 3. master API =="
code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 --noproxy '*' \
  -X POST -H 'Content-Type: application/json' -d '{}' \
  "http://127.0.0.1:${API_PORT}/api/v1/proxy/list_configs" 2>/dev/null || true)
case "$code" in
  ''|000) bad "master API did not answer on 127.0.0.1:$API_PORT" ;;
  401)    ok "master API answered 401 (alive, auth enforced)" ;;
  *)      ok "master API answered http_code=$code" ;;
esac

say "== 4. agents connected =="
peers=$(ss -tn state established "( sport = :${RPC_PORT} )" 2>/dev/null | tail -n +2 | grep -c . || true)
say "      established RPC sessions on :$RPC_PORT = ${peers:-0}"
if [ "${peers:-0}" -ge "$EXPECTED_AGENTS" ]; then
  ok "at least $EXPECTED_AGENTS agent(s) connected"
else
  bad "expected >= $EXPECTED_AGENTS agent(s), saw ${peers:-0}"
fi

# --------------------------------------------------------------------- db ----
# Resolve the sqlite file from the service env when DB was not passed in.
if [ -z "$DB" ] && [ -r "$ENV_FILE" ]; then
  DB=$(grep -E '^DB_DSN=' "$ENV_FILE" 2>/dev/null | head -1 | cut -d= -f2- \
       | sed 's/?.*$//' | tr -d '"' | tr -d "'")
fi
DB=${DB:-/opt/frp-panel/data/data.db}

say "== 5. port_reservation <-> proxy_config consistency =="
say "      db: $DB"
if ! command -v python3 >/dev/null 2>&1; then
  bad "python3 not available - DB consistency checks skipped"
elif [ ! -f "$DB" ]; then
  bad "database not found at $DB"
else
  python3 - "$DB" <<'PY'
import json, sqlite3, sys

db_path = sys.argv[1]
con = sqlite3.connect("file:%s?mode=ro" % db_path, uri=True)
cur = con.cursor()

def query(sql):
    try:
        cur.execute(sql)
        return cur.fetchall()
    except sqlite3.Error as exc:
        print("FAIL  query failed: %s" % exc)
        sys.exit(1)

proxies = {}
for pid, name, stopped, content in query(
        "select id, name, ifnull(stopped,0), ifnull(content,'') from proxy_config"):
    try:
        port = json.loads(content).get("remotePort")
    except Exception:
        port = None
    proxies[int(pid)] = (name, int(stopped), port)

leases = [(int(a), b, c, int(d), e, int(f)) for a, b, c, d, e, f in query(
    "select id, server_id, protocol, remote_port, status, ifnull(owner_proxy_id,-1)"
    " from port_reservation")]

by_status = {}
for _, _, _, _, status, _ in leases:
    by_status[status] = by_status.get(status, 0) + 1
print("      reservations: " + (", ".join("%s=%d" % kv for kv in sorted(by_status.items())) or "none"))
print("      proxy rows:   %d" % len(proxies))

problems = []
owners = {}
for lid, sid, proto, port, status, owner in leases:
    if status != "committed":
        continue
    if owner < 0:
        problems.append("lease %s port %s: committed but owned by no proxy row" % (lid, port))
        continue
    owners[owner] = owners.get(owner, 0) + 1
    if owner not in proxies:
        problems.append("lease %s port %s: DANGLING owner row %s (row deleted)" % (lid, port, owner))
        continue
    name, stopped, row_port = proxies[owner]
    if row_port is not None and int(row_port) != port:
        problems.append("lease %s port %s: owner row %s (%s) actually runs port %s"
                        % (lid, port, owner, name, row_port))

for owner, count in sorted(owners.items()):
    if count > 1:
        ids = ",".join(str(l[0]) for l in leases if l[5] == owner and l[4] == "committed")
        problems.append("proxy row %s (%s) holds %d committed leases (ids %s): a row owns exactly one port"
                        % (owner, proxies[owner][0], count, ids))

unleased = ["%s (%s) port %s" % (pid, name, port)
            for pid, (name, stopped, port) in proxies.items()
            if not stopped and port is not None and owners.get(pid, 0) == 0]
if unleased:
    print("warn  active rule(s) without a committed lease (legacy rows are expected here):")
    for item in sorted(unleased):
        print("        " + item)
else:
    print("ok    every active rule holds a committed lease")

if problems:
    print("FAIL  %d reservation inconsistency(ies):" % len(problems))
    for item in problems:
        print("        " + item)
    sys.exit(1)

print("ok    every committed lease matches a live row's port, one lease per row")
PY
  [ $? -ne 0 ] && fail=1
fi

say ""
if [ $fail -ne 0 ]; then
  say "RESULT: FAIL - inspect the rows above."
  say ""
  say "Cleanup is manual and PER ROW (never a blanket DELETE):"
  say "  1. back up first:"
  say "     sqlite3 <db> \".backup <db>.bak-\$(date +%Y%m%d-%H%M)-precleanup\""
  say "  2. delete only the ids listed above, one statement each:"
  say "     DELETE FROM port_reservation WHERE id = <id> AND remote_port = <port> AND status='committed';"
  say "  3. re-run this script; it must print PASS."
  exit 1
fi

say "RESULT: PASS"
