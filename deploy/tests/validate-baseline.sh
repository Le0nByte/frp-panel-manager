#!/usr/bin/env bash
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

failures=0
require_file() {
  local path="$1"
  if [[ ! -f "$root/$path" ]]; then
    printf 'MISSING: %s\n' "$path" >&2
    failures=$((failures + 1))
  fi
}
require_text() {
  local path="$1" text="$2"
  if [[ -f "$root/$path" ]] && ! grep -Fq -- "$text" "$root/$path"; then
    printf 'INVALID: %s lacks %s\n' "$path" "$text" >&2
    failures=$((failures + 1))
  fi
}

files=(
  deploy/nginx/frp-panel.conf
  deploy/certbot/auth-hook.sh
  deploy/certbot/cleanup-hook.sh
  deploy/certbot/renew-frp-panel.sh
  deploy/certbot/check-expiry.sh
  deploy/systemd/frp-panel.service
  deploy/systemd/frp-panel-cert-renew.service
  deploy/systemd/frp-panel-cert-renew.timer
  deploy/systemd/frp-panel-memory-alert.service
  deploy/logrotate/frp-panel
  deploy/env/frp-panel.env.example
  docs/ops-runbook.md
  deploy/lib/backup-common.sh
  deploy/backup-aliyun.sh
  deploy/backup-k30pro.sh
  deploy/rollback-aliyun.sh
  deploy/rollback-k30pro.sh
  deploy/migrate.sh
  deploy/hooks/verify-aliyun-rollback.sh
  deploy/hooks/verify-k30pro-rollback.sh
  deploy/tests/test-backup-rollback.sh
)
for file in "${files[@]}"; do require_file "$file"; done

require_text deploy/nginx/frp-panel.conf 'listen 9443 ssl'
require_text deploy/nginx/frp-panel.conf 'proxy_pass http://127.0.0.1:9100'
require_text deploy/nginx/frp-panel.conf '/.well-known/acme-challenge/'
require_text deploy/systemd/frp-panel.service 'EnvironmentFile=/etc/frp-panel/frp-panel.env'
require_text deploy/systemd/frp-panel.service 'MemoryHigh=300M'
require_text deploy/systemd/frp-panel.service 'MemoryMax=512M'
require_text deploy/systemd/frp-panel-cert-renew.timer 'OnUnitActiveSec=12h'
require_text deploy/systemd/frp-panel-cert-renew.service 'ExecStartPost=/opt/frp-panel/deploy/certbot/check-expiry.sh'
require_text deploy/certbot/check-expiry.sh '259200'
require_text deploy/logrotate/frp-panel '/opt/frp-panel/data/log/*.log'
require_text deploy/env/frp-panel.env.example 'APP_COOKIE_SECURE=true'
require_text deploy/env/frp-panel.env.example 'CLIENT_TLS_INSECURE_SKIP_VERIFY=false'
require_text deploy/env/frp-panel.env.example 'DB_DSN=/opt/frp-panel/data/data.db?_pragma=journal_mode(WAL)'
require_text docs/ops-runbook.md 'Do not open the public management endpoint'
require_text deploy/lib/backup-common.sh 'manifest.sha256'
require_text deploy/lib/backup-common.sh '--fs-root'
require_text deploy/backup-aliyun.sh 'sqlite3'
require_text deploy/backup-aliyun.sh 'manifest verified'
require_text deploy/rollback-aliyun.sh 'sha256sum -c'
require_text deploy/rollback-aliyun.sh 'not active'
require_text deploy/migrate.sh 'rollback_failed'
require_text deploy/migrate.sh 'exit 4'
require_text deploy/migrate.sh '--k30-backup-dir'
require_text deploy/migrate.sh '--stop-units'
require_text deploy/lib/backup-common.sh 'validate_port'
require_text deploy/lib/backup-common.sh 'require_target_allowed'
require_text deploy/backup-aliyun.sh 'snapshot-key.fingerprint'
require_text deploy/rollback-k30pro.sh '--verify-hook'
require_text deploy/hooks/verify-k30pro-rollback.sh '--wait-port'
require_text deploy/hooks/verify-aliyun-rollback.sh 'PP_MCP_TOKEN'
require_text deploy/hooks/verify-k30pro-rollback.sh 'frpc.toml'
require_text deploy/tests/test-backup-rollback.sh 'ALL GREEN'

if (( failures > 0 )); then
  printf 'Deployment baseline validation failed: %d issue(s).\n' "$failures" >&2
  exit 1
fi
printf 'Deployment baseline validation passed.\n'
