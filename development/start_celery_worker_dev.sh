#!/bin/bash
# Celery Worker 开发启动脚本（file_processing + ai_processing 队列）

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

echo -e "${BLUE}🔄 Celery Worker 开发模式启动${NC}"
echo "=============================================="

dev_load_conf
dev_load_env
dev_detect_python
dev_ensure_logs

cd "$PROJECT_ROOT"

dev_check_redis || exit 1
dev_check_celery || exit 1
dev_check_celery_tasks "process_files_async" || exit 1

echo ""
echo -e "${CYAN}🚀 启动配置:${NC}"
echo "   - 并发数: ${CELERY_CONCURRENCY:-2}"
echo "   - 队列: file_processing, ai_processing"
echo "   - 日志: $LOGS_DIR/celery_worker.log"
echo ""
echo "   按 Ctrl+C 停止服务"
echo ""

exec "$PYTHON_CMD" -m celery -A backend.celery_app worker \
    --loglevel="${CELERY_LOG_LEVEL:-info}" \
    --concurrency="${CELERY_CONCURRENCY:-2}" \
    --queues=file_processing,ai_processing \
    --hostname=dev-worker@%h \
    --logfile="$LOGS_DIR/celery_worker.log" \
    --pidfile="$LOGS_DIR/celery_worker.pid"
