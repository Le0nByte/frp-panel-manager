#!/usr/bin/env bash
# 统一构建入口。
#
# 版本号的唯一来源是 git tag：WebUI 顶部显示的版本必须与产出该二进制的
# tag 完全一致，否则用户看到一个不存在的版本号，排障时会被误导。
#
# 取值优先级：
#   1. $VERSION —— 仅当它形如 v1.2.3 时采用（CI 把 github.ref_name 传进来）
#   2. git describe --tags --exact-match —— HEAD 正好打在某 tag 上
#   3. dev-<短 sha> —— 其余情况（分支构建、detached、未打 tag 的提交）
#
# 用法：
#   scripts/build.sh <输出文件>
#   GOOS=linux GOARCH=arm64 scripts/build.sh frp-panel-linux-arm64
set -euo pipefail

OUT="${1:-}"
if [ -z "$OUT" ]; then
  echo "用法: $0 <输出文件>" >&2
  exit 2
fi

cd "$(dirname "$0")/.."

# 1) 外部传入的版本号必须是合法 tag 形态，否则忽略
if [ -n "${VERSION:-}" ] && ! printf '%s' "$VERSION" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+'; then
  echo "  忽略非 tag 形态的 VERSION=$VERSION，回退到 git 推断" >&2
  VERSION=""
fi

# 2) HEAD 恰好是个 tag
if [ -z "${VERSION:-}" ]; then
  VERSION="$(git describe --tags --exact-match HEAD 2>/dev/null || true)"
fi

# 3) 兜底：不是发布构建
if [ -z "${VERSION:-}" ]; then
  VERSION="dev-$(git rev-parse --short HEAD)"
fi

COMMIT="$(git rev-parse HEAD)"
REPO="github.com/VaalaCat/frp-panel"
LDFLAGS="-s -w"
LDFLAGS="$LDFLAGS -X $REPO/conf.gitVersion=$VERSION"
LDFLAGS="$LDFLAGS -X $REPO/conf.gitCommit=$COMMIT"
LDFLAGS="$LDFLAGS -X $REPO/conf.buildDate=$(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo "构建版本: $VERSION"
echo "提交:     $COMMIT"
echo "目标:     ${GOOS:-$(go env GOOS)}/${GOARCH:-$(go env GOARCH)} -> $OUT"

go build -trimpath -ldflags="$LDFLAGS" -o "$OUT" ./cmd/frpp
