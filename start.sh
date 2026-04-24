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
# 默认 MAIN_CONCURRENCY=1：家用宽带上行普遍只有 10–50 Mbps，2 个并发任务
# 同时往 OSS 上传音频会互相挤压带宽（在日志里表现为 ReadTimeout / 上传卡几分钟），
# 反而比串行还慢。带宽充足或迁移到云 VPS 后可重新调到 2–4。
: "${WORKER_MAIN_CONCURRENCY:=1}"
: "${WORKER_LONG_CONCURRENCY:=1}"
# Priority worker 同样默认 concurrency=1：
#   - PRIORITY_TRANSCRIPT 只跑用户上传的字幕抓取（轻量，~KB 网络流量），
#     1 个进程足以满足"秒级响应单条上传"的体验目标。
#   - PRIORITY_ASR 跑用户上传的 Fun-ASR 转录。这一档**会**和 main worker 的
#     批量 ASR 同时下载音频 + 上传 OSS，可能短暂挤占上行带宽。但用户上传通常
#     是低频事件（单条/几分钟），换来的"上传不被批量挤一小时"的体验提升是
#     值得的。如果以后家用上行真的撑不住，把 PRIORITY_ASR 设成 0 关掉这档即可
#     (会 fallback 回原行为：批量任务期间用户上传的 ASR 排队等待)。
: "${WORKER_PRIORITY_TRANSCRIPT_CONCURRENCY:=1}"
: "${WORKER_PRIORITY_ASR_CONCURRENCY:=1}"
: "${WORKER_MAIN_MAX_TASKS_PER_CHILD:=20}"
: "${WORKER_LONG_MAX_TASKS_PER_CHILD:=10}"
: "${WORKER_PRIORITY_MAX_TASKS_PER_CHILD:=20}"
: "${WORKER_LOG_LEVEL:=info}"

echo "=========================================="
echo "  YouTube Transcription Worker 启动"
echo "  Conda 环境: $CONDA_ENV_NAME"
echo "  Celery:     $CELERY"
echo "  工作目录:   $SCRIPT_DIR"
echo "  队列拓扑:"
echo "    - Priority Transcript Worker(concurrency=${WORKER_PRIORITY_TRANSCRIPT_CONCURRENCY}):"
echo "        youtube_priority                    # Upload Link 用户上传字幕"
echo "    - Priority ASR Worker(concurrency=${WORKER_PRIORITY_ASR_CONCURRENCY}):"
echo "        youtube_transcription_priority      # Upload Link 用户上传 ASR"
echo "    - Main Worker(concurrency=${WORKER_MAIN_CONCURRENCY}):"
echo "        youtube_fetching, youtube_transcription   # Feed 批量"
echo "    - Long Worker(concurrency=${WORKER_LONG_CONCURRENCY}):"
echo "        youtube_transcription_long           # 长视频 ASR"
echo "=========================================="

# 退出时回收所有后台 worker，避免残留进程
cleanup() {
    for var in LONG_PID PRIORITY_TRANSCRIPT_PID PRIORITY_ASR_PID; do
        pid="${!var:-}"
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            echo "停止 worker $var (PID=$pid)..."
            kill "$pid" 2>/dev/null
        fi
    done
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

# 2) Priority Transcript worker：后台运行，仅消费 youtube_priority
#    专门服务 Upload Link 链路的字幕抓取，让用户上传不被批量 Feed 任务挤压。
#    即使批量 fetch_all_youtube_subscriptions 跑 60 min，这个 worker 也始终空闲待命，
#    用户一上传就秒级响应。
if [ "${WORKER_PRIORITY_TRANSCRIPT_CONCURRENCY}" -gt 0 ]; then
    "$CELERY" -A worker.celery_app worker \
        --queues=youtube_priority \
        --concurrency="${WORKER_PRIORITY_TRANSCRIPT_CONCURRENCY}" \
        --max-tasks-per-child="${WORKER_PRIORITY_MAX_TASKS_PER_CHILD}" \
        --hostname=youtube-priority-worker@$(hostname) \
        --loglevel="${WORKER_LOG_LEVEL}" &
    PRIORITY_TRANSCRIPT_PID=$!
fi

# 3) Priority ASR worker：后台运行，仅消费 youtube_transcription_priority
#    专门服务 Upload Link 链路的 Fun-ASR。注意它会和 main worker 的 ASR 并发占用
#    家用上行带宽（音频下载 + OSS 上传），但用户上传是低频事件，换体验值得。
#    带宽吃紧时可设 WORKER_PRIORITY_ASR_CONCURRENCY=0 关掉这档（用户上传的 ASR
#    会回退到主 worker 排队，但字幕抓取依然走 priority 队列保持插队能力）。
if [ "${WORKER_PRIORITY_ASR_CONCURRENCY}" -gt 0 ]; then
    "$CELERY" -A worker.celery_app worker \
        --queues=youtube_transcription_priority \
        --concurrency="${WORKER_PRIORITY_ASR_CONCURRENCY}" \
        --max-tasks-per-child="${WORKER_PRIORITY_MAX_TASKS_PER_CHILD}" \
        --hostname=youtube-priority-asr-worker@$(hostname) \
        --loglevel="${WORKER_LOG_LEVEL}" &
    PRIORITY_ASR_PID=$!
fi

# 4) 主 worker：前台运行（exec），消费 Feed 批量字幕抓取 + Feed 短视频转录
#    注意：这里不再消费 youtube_priority / youtube_transcription_priority，
#    确保它和上面两个 priority worker 完全解耦，绝对不会因为批量任务卡死而
#    阻塞用户上传。
exec "$CELERY" -A worker.celery_app worker \
    --queues=youtube_fetching,youtube_transcription \
    --concurrency="${WORKER_MAIN_CONCURRENCY}" \
    --max-tasks-per-child="${WORKER_MAIN_MAX_TASKS_PER_CHILD}" \
    --hostname=youtube-transcription-worker@$(hostname) \
    --loglevel="${WORKER_LOG_LEVEL}"
