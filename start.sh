#!/bin/bash
# ============================================================
# YouTube Transcription Worker — 启动脚本
#
# 前台运行（直接查看日志）:
#   bash start.sh
#
# 后台运行（日志重定向到文件）:
#   nohup bash start.sh > logs/worker.log 2>&1 &
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONDA_ENV_NAME="yt_service"
CONDA_BASE=$(conda info --base 2>/dev/null || echo "$HOME/miniconda3")
CELERY="$CONDA_BASE/envs/$CONDA_ENV_NAME/bin/celery"

# 验证 conda 环境
if [ ! -f "$CELERY" ]; then
    echo "❌ 找不到 celery 可执行文件: $CELERY"
    echo "   请先运行 setup.sh 创建 conda 环境 ($CONDA_ENV_NAME)"
    exit 1
fi

# 加载环境变量
if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a
    source "$SCRIPT_DIR/.env"
    set +a
else
    echo "❌ 找不到 .env 文件: $SCRIPT_DIR/.env"
    echo "   请先复制并填写 .env.template"
    exit 1
fi

export PYTHONPATH="$SCRIPT_DIR"

# Worker 资源（可通过 .env 覆盖，未配置时使用这里的默认）
: "${WORKER_MAIN_CONCURRENCY:=2}"
: "${WORKER_LONG_CONCURRENCY:=1}"
: "${WORKER_MAIN_MAX_TASKS_PER_CHILD:=20}"
: "${WORKER_LONG_MAX_TASKS_PER_CHILD:=10}"
: "${WORKER_LOG_LEVEL:=info}"

echo "=========================================="
echo "  YouTube Transcription Worker 启动"
echo "  Conda 环境: $CONDA_ENV_NAME"
echo "  Celery:     $CELERY"
echo "  工作目录:   $SCRIPT_DIR"
echo "  队列拓扑:"
echo "    - Main Worker(concurrency=${WORKER_MAIN_CONCURRENCY}):"
echo "        youtube_fetching, youtube_transcription"
echo "    - Long Worker(concurrency=${WORKER_LONG_CONCURRENCY}):"
echo "        youtube_transcription_long"
echo "=========================================="

# 退出时回收后台的长视频 worker，避免残留进程
cleanup() {
    if [ -n "${LONG_PID:-}" ] && kill -0 "$LONG_PID" 2>/dev/null; then
        echo "停止长视频 worker (PID=$LONG_PID)..."
        kill "$LONG_PID" 2>/dev/null
    fi
}
trap cleanup EXIT INT TERM

# 1) 长视频 worker：后台运行，仅消费 youtube_transcription_long
"$CELERY" -A worker.celery_app worker \
    --queues=youtube_transcription_long \
    --concurrency="${WORKER_LONG_CONCURRENCY}" \
    --max-tasks-per-child="${WORKER_LONG_MAX_TASKS_PER_CHILD}" \
    --hostname=youtube-long-worker@$(hostname) \
    --loglevel="${WORKER_LOG_LEVEL}" &
LONG_PID=$!

# 2) 主 worker：前台运行（exec），消费字幕抓取 + 短视频转录
exec "$CELERY" -A worker.celery_app worker \
    --queues=youtube_fetching,youtube_transcription \
    --concurrency="${WORKER_MAIN_CONCURRENCY}" \
    --max-tasks-per-child="${WORKER_MAIN_MAX_TASKS_PER_CHILD}" \
    --hostname=youtube-transcription-worker@$(hostname) \
    --loglevel="${WORKER_LOG_LEVEL}"
