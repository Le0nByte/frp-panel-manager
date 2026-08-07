#!/usr/bin/env bash
# CI 部署入口：由 GitHub Actions 在目标服务器（阿里云 Master）上执行。
# 用法: bash ci-deploy-master.sh <版本标签>
# 幂等：首次部署自动准备目录 / snapshot key / env / systemd unit；
# 增量部署只做二进制备份、原子替换、重启与健康检查。
set -euo pipefail

VERSION="${1:-ci}"
BIN_DIR=/opt/frp-panel/bin
DATA_DIR=/opt/frp-panel/data
SECRETS_DIR=/opt/frp-panel/secrets
ENV_FILE=/etc/frp-panel/frp-panel.env
UNIT_FILE=/etc/systemd/system/frp-panel.service
NEW_BIN="$BIN_DIR/frp-panel.new"

echo "==> ci-deploy-master.sh version=$VERSION"

# 1. 基座目录（首次与增量均幂等）
install -d -o root -g root "$BIN_DIR" "$DATA_DIR" "$SECRETS_DIR" /etc/frp-panel

# 2. snapshot key：缺失才生成（32 字节随机，0600）
if [ ! -s "$SECRETS_DIR/snapshot.key" ]; then
  umask 077
  head -c 32 /dev/urandom > "$SECRETS_DIR/snapshot.key"
  chmod 0600 "$SECRETS_DIR/snapshot.key"
  echo "generated snapshot.key"
fi

# 3. env：缺失才生成（后续保留服务器上已有配置）
if [ ! -f "$ENV_FILE" ]; then
  : "${MASTER_GLOBAL_SECRET:?MASTER_GLOBAL_SECRET 环境变量必须提供（GitHub Secret）}"
  if [ -z "${MASTER_API_HOST:-}" ]; then
    MASTER_API_HOST="$(curl -fsS --max-time 10 https://ifconfig.me 2>/dev/null || echo PUBLIC_IP_OR_DNS)"
  fi
  cat > "$ENV_FILE" <<EOF
# 由 GitHub Actions 首次部署生成；之后请在本文件维护
APP_GLOBAL_SECRET=${MASTER_GLOBAL_SECRET}
APP_ENABLE_REGISTER=false
APP_COOKIE_SECURE=true
APP_COOKIE_HTTP_ONLY=true
APP_COOKIE_AGE=86400
MASTER_API_HOST=${MASTER_API_HOST}
MASTER_API_PORT=9100
MASTER_API_SCHEME=http
MASTER_RPC_HOST=127.0.0.1
MASTER_RPC_PORT=9101
MASTER_INTERNAL_FRP_SERVER_HOST=
CLIENT_TLS_INSECURE_SKIP_VERIFY=true
CLIENT_API_URL=https://${MASTER_API_HOST}:9443
CLIENT_RPC_URL=grpc://${MASTER_API_HOST}:9101
DB_TYPE=sqlite3
DB_DSN=/opt/frp-panel/data/data.db?_pragma=journal_mode(WAL)
SNAPSHOT_KEY_FILE=/opt/frp-panel/secrets/snapshot.key
MASTER_FEATURES_ENABLE_PTY=false
MASTER_FEATURES_ENABLE_WORKER=false
MASTER_FEATURES_ENABLE_WIREGUARD=false
MASTER_RPC_GUARD_ENABLE=true
EOF
  chmod 0600 "$ENV_FILE"
  echo "generated $ENV_FILE"
fi

# 4. systemd unit：缺失才安装
if [ ! -f "$UNIT_FILE" ]; then
  cat > "$UNIT_FILE" <<'UNITEOF'
[Unit]
Description=FRP Panel Master
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
EnvironmentFile=/etc/frp-panel/frp-panel.env
ExecStart=/opt/frp-panel/bin/frp-panel master
WorkingDirectory=/opt/frp-panel
Restart=on-failure
RestartSec=5s
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/opt/frp-panel/data /opt/frp-panel/secrets
MemoryHigh=300M
MemoryMax=512M

[Install]
WantedBy=multi-user.target
UNITEOF
  echo "installed $UNIT_FILE"
fi

# 5. 二进制原子替换
if [ ! -f "$NEW_BIN" ]; then
  echo "!! $NEW_BIN 不存在（Actions 上传失败？）"
  exit 1
fi
if [ -f "$BIN_DIR/frp-panel" ]; then
  cp -f "$BIN_DIR/frp-panel" "$BIN_DIR/frp-panel.prev"
fi
mv -f "$NEW_BIN" "$BIN_DIR/frp-panel"
chmod 0755 "$BIN_DIR/frp-panel"

# 6. 启动 / 重启
systemctl daemon-reload
systemctl enable frp-panel >/dev/null 2>&1 || true
systemctl restart frp-panel

# 7. 健康检查：HTTP 探测 9100（Master API 监听 127.0.0.1:9100）
ok=""
for i in $(seq 1 12); do
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://127.0.0.1:9100/ 2>/dev/null || true)"
  echo "health check $i/12: http_code=$code"
  if [ -n "$code" ] && [ "$code" != "000" ]; then
    ok=1
    break
  fi
  sleep 5
done
if [ -z "$ok" ]; then
  echo "::error::Master 健康检查失败"
  journalctl -u frp-panel --no-pager -n 80 2>/dev/null || true
  exit 1
fi
echo "==> Master 部署成功（version=$VERSION）"
