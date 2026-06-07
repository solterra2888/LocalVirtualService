#!/bin/bash
# ============================================================
# YouTube Transcription Worker — 单 Worker 启动脚本
#
# 用法（通常由 systemd unit 调用）:
#   bash run_worker.sh <role>
#
# role 取值:
#   fetch                — youtube_fetching（Feed 抓取 + 字幕，双节点「字幕机」）
#   asr                  — youtube_transcription（Feed 短视频 ASR，双节点「下载机」）
#   main                 — youtube_fetching + youtube_transcription（单机全栈，向后兼容）
#   long                 — youtube_transcription_long（长视频 ASR）
#   priority-transcript  — youtube_priority（Upload Link 字幕，插队）
#   priority-asr         — youtube_transcription_priority（Upload Link ASR，插队）
#
# 双节点分工见 DEPLOY_DUAL_NODE.md；单机部署仍用 yt-worker.target（main + long + priority*）
#
# 调试（前台运行，直接看日志）:
#   bash run_worker.sh fetch
# ============================================================

set -euo pipefail

ROLE="${1:-}"
if [[ -z "$ROLE" ]]; then
    echo "❌ 用法: $0 <fetch|asr|main|long|priority-transcript|priority-asr>" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONDA_BASE=$(conda info --base 2>/dev/null || echo "$HOME/miniconda3")
CONDA_ENV_NAME="yt_service"
CELERY="$CONDA_BASE/envs/$CONDA_ENV_NAME/bin/celery"

if [ ! -f "$CELERY" ]; then
    echo "❌ 找不到 celery 可执行文件: $CELERY" >&2
    echo "   请先运行 setup.sh 创建 conda 环境 ($CONDA_ENV_NAME)" >&2
    exit 1
fi

# 加载 .env
if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a
    source "$SCRIPT_DIR/.env"
    set +a
else
    echo "❌ 找不到 .env 文件: $SCRIPT_DIR/.env" >&2
    echo "   请先复制并填写 .env.template" >&2
    exit 1
fi

export PYTHONPATH="$SCRIPT_DIR"

# 默认并发与任务数（可通过 .env 覆盖）
: "${WORKER_MAIN_CONCURRENCY:=1}"
: "${WORKER_FETCH_CONCURRENCY:=${WORKER_MAIN_CONCURRENCY}}"
: "${WORKER_ASR_CONCURRENCY:=${WORKER_MAIN_CONCURRENCY}}"
: "${WORKER_LONG_CONCURRENCY:=1}"
: "${WORKER_PRIORITY_TRANSCRIPT_CONCURRENCY:=1}"
: "${WORKER_PRIORITY_ASR_CONCURRENCY:=1}"
: "${WORKER_MAIN_MAX_TASKS_PER_CHILD:=20}"
: "${WORKER_FETCH_MAX_TASKS_PER_CHILD:=${WORKER_MAIN_MAX_TASKS_PER_CHILD}}"
: "${WORKER_ASR_MAX_TASKS_PER_CHILD:=${WORKER_MAIN_MAX_TASKS_PER_CHILD}}"
: "${WORKER_LONG_MAX_TASKS_PER_CHILD:=10}"
: "${WORKER_PRIORITY_MAX_TASKS_PER_CHILD:=20}"
: "${WORKER_LOG_LEVEL:=info}"

HOSTNAME_SUFFIX="@$(hostname)"

case "$ROLE" in
    fetch)
        echo "[run_worker] 启动 Fetch Worker (youtube_fetching, concurrency=${WORKER_FETCH_CONCURRENCY})"
        exec "$CELERY" -A worker.celery_app worker \
            --queues=youtube_fetching \
            --concurrency="${WORKER_FETCH_CONCURRENCY}" \
            --max-tasks-per-child="${WORKER_FETCH_MAX_TASKS_PER_CHILD}" \
            --hostname="youtube-fetch-worker${HOSTNAME_SUFFIX}" \
            --loglevel="${WORKER_LOG_LEVEL}"
        ;;
    asr)
        echo "[run_worker] 启动 ASR Worker (youtube_transcription, concurrency=${WORKER_ASR_CONCURRENCY})"
        exec "$CELERY" -A worker.celery_app worker \
            --queues=youtube_transcription \
            --concurrency="${WORKER_ASR_CONCURRENCY}" \
            --max-tasks-per-child="${WORKER_ASR_MAX_TASKS_PER_CHILD}" \
            --hostname="youtube-asr-worker${HOSTNAME_SUFFIX}" \
            --loglevel="${WORKER_LOG_LEVEL}"
        ;;
    main)
        echo "[run_worker] 启动 Main Worker (youtube_fetching + youtube_transcription, concurrency=${WORKER_MAIN_CONCURRENCY})"
        exec "$CELERY" -A worker.celery_app worker \
            --queues=youtube_fetching,youtube_transcription \
            --concurrency="${WORKER_MAIN_CONCURRENCY}" \
            --max-tasks-per-child="${WORKER_MAIN_MAX_TASKS_PER_CHILD}" \
            --hostname="youtube-transcription-worker${HOSTNAME_SUFFIX}" \
            --loglevel="${WORKER_LOG_LEVEL}"
        ;;
    long)
        echo "[run_worker] 启动 Long Worker (youtube_transcription_long, concurrency=${WORKER_LONG_CONCURRENCY})"
        exec "$CELERY" -A worker.celery_app worker \
            --queues=youtube_transcription_long \
            --concurrency="${WORKER_LONG_CONCURRENCY}" \
            --max-tasks-per-child="${WORKER_LONG_MAX_TASKS_PER_CHILD}" \
            --hostname="youtube-long-worker${HOSTNAME_SUFFIX}" \
            --loglevel="${WORKER_LOG_LEVEL}"
        ;;
    priority-transcript)
        echo "[run_worker] 启动 Priority Transcript Worker (youtube_priority, concurrency=${WORKER_PRIORITY_TRANSCRIPT_CONCURRENCY})"
        exec "$CELERY" -A worker.celery_app worker \
            --queues=youtube_priority \
            --concurrency="${WORKER_PRIORITY_TRANSCRIPT_CONCURRENCY}" \
            --max-tasks-per-child="${WORKER_PRIORITY_MAX_TASKS_PER_CHILD}" \
            --hostname="youtube-priority-worker${HOSTNAME_SUFFIX}" \
            --loglevel="${WORKER_LOG_LEVEL}"
        ;;
    priority-asr)
        # 并发始终至少为 1；若要"关闭"这档，改用 systemctl disable --now yt-worker-priority-asr
        CONCURRENCY="${WORKER_PRIORITY_ASR_CONCURRENCY}"
        if [[ "$CONCURRENCY" -lt 1 ]]; then
            CONCURRENCY=1
        fi
        echo "[run_worker] 启动 Priority ASR Worker (youtube_transcription_priority, concurrency=${CONCURRENCY})"
        exec "$CELERY" -A worker.celery_app worker \
            --queues=youtube_transcription_priority \
            --concurrency="${CONCURRENCY}" \
            --max-tasks-per-child="${WORKER_PRIORITY_MAX_TASKS_PER_CHILD}" \
            --hostname="youtube-priority-asr-worker${HOSTNAME_SUFFIX}" \
            --loglevel="${WORKER_LOG_LEVEL}"
        ;;
    *)
        echo "❌ 未知 role: $ROLE（可选值: fetch / asr / main / long / priority-transcript / priority-asr）" >&2
        exit 1
        ;;
esac
