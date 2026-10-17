#!/usr/bin/env bash
# 前端部署 = 安全构建（build-safe.sh：锁 + 内存预检）→ 原子同步到 build/ → 重载 Nginx
set -euo pipefail
cd "$(dirname "$0")/.."

bash scripts/build-safe.sh

rsync -a --delete build-staging/ build/
rm -rf build-staging

if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx nginx; then
  docker exec nginx nginx -s reload
  echo "✅ 部署完成：build/ 已更新，Nginx 已重载"
else
  echo "✅ 构建已同步到 build/（Nginx 容器未运行，已跳过重载；容器启动后自动生效）"
fi
