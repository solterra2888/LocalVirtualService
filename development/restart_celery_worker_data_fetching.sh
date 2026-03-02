#!/bin/bash
# 重启数据爬取 Celery Worker（停止旧进程 → 备份日志 → 后台启动新进程）

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

echo -e "${BLUE}🔄 重启 Celery Data Fetching Worker...${NC}"
echo "=============================================="

dev_load_conf
dev_load_env
dev_detect_python
dev_ensure_logs

cd "$PROJECT_ROOT"

LOG_FILE="$LOGS_DIR/celery_worker_data_fetching.log"
PID_FILE="$LOGS_DIR/celery_worker_data_fetching.pid"

# ── 1. 停止现有 worker ───────────────────────────────────────
echo -e "${YELLOW}⏹️  停止现有 worker...${NC}"
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    dev_stop_process "$OLD_PID" "Data Fetching Worker"
    rm -f "$PID_FILE"
else
    dev_stop_by_pattern "celery.*data_fetching" "Data Fetching Worker"
fi
sleep 2

# ── 2. 备份旧日志 ────────────────────────────────────────────
if [ -f "$LOG_FILE" ]; then
    BACKUP_LOG="${LOG_FILE}.$(date +%Y%m%d_%H%M%S)"
    mv "$LOG_FILE" "$BACKUP_LOG"
    echo -e "${CYAN}   旧日志已备份: $BACKUP_LOG${NC}"
fi

# ── 3. 后台启动新 worker ─────────────────────────────────────
echo -e "${GREEN}🚀 启动新 worker...${NC}"
"$PYTHON_CMD" -m celery -A backend.celery_app worker \
    -Q data_fetching \
    --concurrency=2 \
    --max-tasks-per-child=50 \
    --loglevel=INFO \
    --logfile="$LOG_FILE" \
    --pidfile="$PID_FILE" \
    --detach

# ── 4. 验证启动 ──────────────────────────────────────────────
sleep 3
if [ -f "$PID_FILE" ]; then
    NEW_PID=$(cat "$PID_FILE")
    if ps -p "$NEW_PID" >/dev/null 2>&1; then
        echo -e "${GREEN}✅ Worker 启动成功 (PID: $NEW_PID)${NC}"
    else
        echo -e "${RED}❌ Worker 启动失败，请查看日志: tail -f $LOG_FILE${NC}"
        exit 1
    fi
else
    echo -e "${RED}❌ Worker 启动失败（未找到 PID 文件）${NC}"
    echo "   请查看日志: tail -f $LOG_FILE"
    exit 1
fi

echo ""
echo -e "${CYAN}📊 Worker 信息:${NC}"
echo "   队列: data_fetching"
echo "   并发: 2"
echo "   日志: $LOG_FILE"
echo ""
echo -e "${CYAN}💡 提示:${NC}"
echo "   - 查看日志: tail -f $LOG_FILE"
echo "   - 查看状态: $PYTHON_CMD -m celery -A backend.celery_app inspect active"
