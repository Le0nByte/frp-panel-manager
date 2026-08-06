#!/usr/bin/env bash
set -euo pipefail

cert_file="${FRP_PANEL_CERT_FILE:-/etc/letsencrypt/live/frp-panel/fullchain.pem}"
threshold_seconds="${FRP_PANEL_CERT_ALERT_SECONDS:-259200}" # 72 hours

[[ "$threshold_seconds" =~ ^[0-9]+$ ]] || {
  printf 'ERROR: FRP_PANEL_CERT_ALERT_SECONDS must be a non-negative integer\n' >&2
  exit 2
}
[[ -r "$cert_file" ]] || {
  printf 'ERROR: certificate file is not readable: %s\n' "$cert_file" >&2
  exit 2
}

if openssl x509 -checkend "$threshold_seconds" -noout -in "$cert_file" >/dev/null 2>&1; then
  printf 'Certificate validity exceeds the configured alert threshold.\n'
  exit 0
fi

message="FRP Panel certificate expires within 72 hours or is already expired"
if command -v logger >/dev/null 2>&1; then
  logger -p daemon.err -t frp-panel-cert -- "$message"
fi
printf 'ALERT: %s\n' "$message" >&2
exit 1
