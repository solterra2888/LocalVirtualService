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

echo "=========================================="
echo "  YouTube Transcription Worker 启动"
echo "  Conda 环境: $CONDA_ENV_NAME"
echo "  Celery:     $CELERY"
echo "  工作目录:   $SCRIPT_DIR"
echo "=========================================="

exec "$CELERY" -A worker.celery_app worker \
    --queues=youtube_fetching,youtube_transcription \
    --concurrency=2 \
    --max-tasks-per-child=20 \
    --hostname=youtube-transcription-worker@$(hostname) \
    --loglevel=info
