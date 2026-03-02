#!/bin/bash
# 数据爬取 Celery Worker 启动脚本（data_fetching 队列，低并发防封 IP）

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

echo -e "${BLUE}🔄 Celery Worker (Data Fetching) 启动${NC}"
echo "=============================================="

dev_load_conf
dev_load_env
dev_detect_python
dev_ensure_logs

cd "$PROJECT_ROOT"

dev_check_redis || exit 1
dev_check_celery || exit 1

echo ""
echo -e "${CYAN}🚀 启动配置:${NC}"
echo "   - 队列: data_fetching"
echo "   - 并发数: 1（避免并发爬取导致 IP 被封）"
echo "   - 最大任务数: 10（每个子进程处理 10 个任务后重启）"
echo "   - 日志: $LOGS_DIR/celery_worker_data_fetching.log"
echo ""
echo "   按 Ctrl+C 停止服务"
echo ""

exec "$PYTHON_CMD" -m celery -A backend.celery_app worker \
    -Q data_fetching \
    --concurrency=1 \
    --max-tasks-per-child=10 \
    --loglevel="${CELERY_LOG_LEVEL:-info}" \
    --logfile="$LOGS_DIR/celery_worker_data_fetching.log" \
    --pidfile="$LOGS_DIR/celery_worker_data_fetching.pid" \
    -n data_fetching_worker@%h
