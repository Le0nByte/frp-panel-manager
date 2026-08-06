#!/usr/bin/env bash
set -euo pipefail
args=(renew --quiet)
if [[ "${1:-}" == "--dry-run" ]]; then
  args+=(--dry-run)
elif [[ $# -ne 0 ]]; then
  printf 'usage: %s [--dry-run]\n' "$0" >&2
  exit 2
fi
certbot "${args[@]}"
systemctl reload nginx
