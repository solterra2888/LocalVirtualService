#!/bin/bash
# ============================================================
# YouTube Transcription Worker — 调试启动脚本
#
# 此脚本供**本地前台调试**使用（或临时后台运行），
# 正式部署请改用 systemd target，详见 DEPLOY_DUAL_NODE.md / README。
#
# 前台运行（直接查看日志）:
#   bash start.sh
#
# 后台运行（日志重定向到文件）:
#   nohup bash start.sh > logs/worker.log 2>&1 &
#
# 按 .env 中 WORKER_NODE_ROLE 启动对应进程组合：
#   all     → main + long + priority*（单机）
#   caption → fetch + priority-transcript（字幕节点）
#   asr     → asr + long + priority-asr（ASR 节点）
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_WORKER="$SCRIPT_DIR/run_worker.sh"

if [ ! -f "$RUN_WORKER" ]; then
    echo "❌ 找不到 run_worker.sh: $RUN_WORKER" >&2
    exit 1
fi

if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a
    source "$SCRIPT_DIR/.env"
    set +a
fi
WORKER_NODE_ROLE="${WORKER_NODE_ROLE:-all}"

echo "=========================================="
echo "  YouTube Transcription Worker 启动（调试模式）"
echo "  WORKER_NODE_ROLE=${WORKER_NODE_ROLE}"
echo "  注意: 生产环境请使用 systemd target"
echo "=========================================="

PIDS=()

cleanup() {
    echo "停止所有后台 worker..."
    for pid in "${PIDS[@]}"; do
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            echo "  停止 PID=$pid"
            kill "$pid" 2>/dev/null
        fi
    done
}
trap cleanup EXIT INT TERM

start_bg() {
    local role="$1"
    bash "$RUN_WORKER" "$role" &
    PIDS+=("$!")
}

case "$WORKER_NODE_ROLE" in
    caption)
        start_bg priority-transcript
        exec bash "$RUN_WORKER" fetch
        ;;
    asr)
        start_bg long
        start_bg priority-asr
        exec bash "$RUN_WORKER" asr
        ;;
    all|*)
        start_bg long
        start_bg priority-transcript
        start_bg priority-asr
        exec bash "$RUN_WORKER" main
        ;;
esac
