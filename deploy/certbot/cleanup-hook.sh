#!/usr/bin/env bash
set -euo pipefail
: "${CERTBOT_TOKEN:?CERTBOT_TOKEN is required}"
webroot="${CERTBOT_WEBROOT:-/var/www/frp-panel}"
challenge_dir="$webroot/.well-known/acme-challenge"
rm -f -- "$challenge_dir/$CERTBOT_TOKEN"
