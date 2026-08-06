#!/usr/bin/env bash
# verify-aliyun-rollback.sh - post-rollback verification on the Aliyun side
# (spec section 12.3.6):
#   1. `ssh <host> id -u` returns root (0)
#   2. MCP probe without token returns HTTP 401
#   3. MCP probe with token (initialize) returns HTTP 200
#
# The MCP URL and token are injected at runtime via environment:
#   PP_MCP_URL        e.g. http://127.0.0.1:8000/mcp
#   PP_MCP_TOKEN      the token value
#   PP_MCP_TOKEN_FILE path to a file containing the token (alternative)
# The token is passed to curl through a header file (-H @file) so it never
# appears on a command line, in process listings, or in any log/evidence.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/backup-common.sh"

usage() {
  cat <<'EOF'
verify-aliyun-rollback.sh - verify SSH root and MCP auth after rollback

Usage:
  PP_MCP_URL=... [PP_MCP_TOKEN=... | PP_MCP_TOKEN_FILE=...] verify-aliyun-rollback.sh [--ssh-host HOST]

Options:
  --ssh-host HOST   SSH host for `id -u` check (default k30pro)
  --help            show this help

Exits 0 only when ssh returns root AND MCP returns 401 without token AND 200
with token. The token is never printed or logged.
EOF
}

SSH_HOST="k30pro"
PP_MCP_URL="${PP_MCP_URL:-}"
PP_MCP_TOKEN="${PP_MCP_TOKEN:-}"
PP_MCP_TOKEN_FILE="${PP_MCP_TOKEN_FILE:-}"

parse_common_flags "$@"
set -- "${COMMON_REMAINING[@]}"
while (($# > 0)); do
  case "$1" in
    --ssh-host) [[ $# -ge 2 ]] || usage_err "--ssh-host requires a host"; SSH_HOST="$2"; shift 2 ;;
    *) usage_err "unknown option: $1" ;;
  esac
done

# 1. SSH root check
ssh_out="$(ssh -o BatchMode=yes -o ConnectTimeout=10 "$SSH_HOST" id -u 2>/dev/null || true)"
if [[ "$ssh_out" != "0" ]]; then
  printf 'ERROR: ssh %s id -u returned %s (expected 0)\n' "$SSH_HOST" "$ssh_out" >&2
  exit 1
fi

# 2. MCP without token must be 401
[[ -n "$PP_MCP_URL" ]] || { printf 'ERROR: PP_MCP_URL is required\n' >&2; exit 1; }
code_no_token="$(curl -s -o /dev/null -w '%{http_code}' "$PP_MCP_URL" 2>/dev/null || true)"
if [[ "$code_no_token" != "401" ]]; then
  printf 'ERROR: MCP without token expected 401, got %s\n' "$code_no_token" >&2
  exit 1
fi

# 3. MCP with token must be 200. Token stays in a 0600 temp file; only the
#    header file path reaches the curl command line.
token="$PP_MCP_TOKEN"
if [[ -n "$PP_MCP_TOKEN_FILE" ]]; then
  [[ -r "$PP_MCP_TOKEN_FILE" ]] || { printf 'ERROR: PP_MCP_TOKEN_FILE not readable\n' >&2; exit 1; }
  token="$(<"$PP_MCP_TOKEN_FILE")"
fi
[[ -n "$token" ]] || { printf 'ERROR: PP_MCP_TOKEN or PP_MCP_TOKEN_FILE required\n' >&2; exit 1; }

tokfile="$(mktemp)"
trap 'rm -f "$tokfile"' EXIT
chmod 600 "$tokfile"
printf 'Authorization: Bearer %s\n' "$token" > "$tokfile"

code_token="$(curl -s -o /dev/null -w '%{http_code}' -H "@$tokfile" "$PP_MCP_URL" 2>/dev/null || true)"
if [[ "$code_token" != "200" ]]; then
  printf 'ERROR: MCP with token expected 200, got %s\n' "$code_token" >&2
  exit 1
fi

printf 'verify-aliyun-rollback OK: ssh %s is root, MCP 401/200\n' "$SSH_HOST"
