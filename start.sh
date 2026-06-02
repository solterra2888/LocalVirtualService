#!/bin/bash
# ============================================================
# YouTube Transcription Worker — 调试启动脚本
#
# 此脚本供**本地前台调试**使用（或临时后台运行），
# 正式部署请改用 systemd（yt-worker.target + 4 个独立 unit），
# 详见 README §一"首次部署"和 §三"日常重启"。
#
# 前台运行（直接查看日志）:
#   bash start.sh
#
# 后台运行（日志重定向到文件）:
#   nohup bash start.sh > logs/worker.log 2>&1 &
#
# 所有 celery 命令定义在 run_worker.sh，本脚本只负责编排 4 个角色进程。
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_WORKER="$SCRIPT_DIR/run_worker.sh"

if [ ! -f "$RUN_WORKER" ]; then
    echo "❌ 找不到 run_worker.sh: $RUN_WORKER" >&2
    exit 1
fi

echo "=========================================="
echo "  YouTube Transcription Worker 启动（调试模式）"
echo "  注意: 生产环境请使用 systemd yt-worker.target"
echo "=========================================="

# 退出时回收所有后台 worker，避免残留进程
cleanup() {
    echo "停止所有后台 worker..."
    for var in LONG_PID PRIORITY_TRANSCRIPT_PID PRIORITY_ASR_PID; do
        pid="${!var:-}"
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            echo "  停止 $var (PID=$pid)"
            kill "$pid" 2>/dev/null
        fi
    done
}
trap cleanup EXIT INT TERM

# 1) 长视频 worker（后台）
bash "$RUN_WORKER" long &
LONG_PID=$!

# 2) Priority Transcript worker（后台）
bash "$RUN_WORKER" priority-transcript &
PRIORITY_TRANSCRIPT_PID=$!

# 3) Priority ASR worker（后台）
bash "$RUN_WORKER" priority-asr &
PRIORITY_ASR_PID=$!

# 4) 主 worker（前台，exec 替换当前进程，接收所有信号）
exec bash "$RUN_WORKER" main
