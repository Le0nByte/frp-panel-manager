#!/usr/bin/env bash
set -euo pipefail
: "${CERTBOT_TOKEN:?CERTBOT_TOKEN is required}"
: "${CERTBOT_VALIDATION:?CERTBOT_VALIDATION is required}"
webroot="${CERTBOT_WEBROOT:-/var/www/frp-panel}"
challenge_dir="$webroot/.well-known/acme-challenge"
install -d -m 0755 "$challenge_dir"
printf '%s' "$CERTBOT_VALIDATION" > "$challenge_dir/$CERTBOT_TOKEN"
chmod 0644 "$challenge_dir/$CERTBOT_TOKEN"
