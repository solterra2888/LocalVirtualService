#!/usr/bin/env bash
# 安全构建：并发锁 + 残留构建进程检查 + 内存预检 + 限内存构建到 build-staging/
# 背景：服务器仅 7.4GB 内存，裸构建曾两次触发 OOM 整机死机（2026-10-15 / 2026-10-17）。
set -euo pipefail
cd "$(dirname "$0")/.."

LOCK_FILE=/tmp/guanhe-frontend-build.lock
MIN_AVAIL_MB=2500   # 构建峰值约需 2~3.5GB 内存余量（node 堆已限 2GB）

# 1) 并发锁：同一时刻只允许一个前端构建
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "❌ 已有另一个前端构建/部署正在进行，本次退出。" >&2
  echo "   （确认没有构建在跑时可删除锁文件重试: rm -f $LOCK_FILE）" >&2
  exit 1
fi

# 2) 残留构建进程检查（之前卡死的构建进程会吃掉 1~2GB 内存且不释放）
if pgrep -f 'react-scripts' >/dev/null; then
  echo "❌ 检测到残留的 react-scripts 构建进程（可能为之前卡死的构建，占用大量内存）：" >&2
  pgrep -af 'react-scripts' >&2 || true
  echo "   请先执行: pkill -f react-scripts ，然后重试。" >&2
  exit 1
fi

# 3) 可用内存预检
AVAIL_MB=$(awk '/^MemAvailable/ {printf "%d", $2/1024}' /proc/meminfo)
if [ "$AVAIL_MB" -lt "$MIN_AVAIL_MB" ]; then
  echo "❌ 可用内存仅 ${AVAIL_MB}MB（需 ≥ ${MIN_AVAIL_MB}MB），构建可能触发 OOM。" >&2
  echo "   请等待 AI 任务结束或降低系统内存占用后重试。" >&2
  exit 1
fi
echo "✅ 内存检查通过（可用 ${AVAIL_MB}MB），开始构建..."

# 4) 构建：每进程堆内存限 2GB、不生成 sourcemap、产物写入 build-staging/（不碰线上 build/）
export CI=false
export GENERATE_SOURCEMAP=false
export NODE_OPTIONS=--max-old-space-size=2048
export BUILD_PATH=build-staging
./node_modules/.bin/react-scripts build
echo "✅ 构建完成（产物在 build-staging/）"
