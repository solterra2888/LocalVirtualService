#!/bin/bash
# 新闻分析 Celery Worker 启动脚本（ai_processing 队列，低并发）

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

echo -e "${BLUE}🔄 新闻分析 Celery Worker 启动${NC}"
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
echo "   - 并发数: ${CELERY_CONCURRENCY:-1}"
echo "   - 队列: ai_processing"
echo "   - 预取: 1（逐条处理）"
echo "   - 日志: $LOGS_DIR/celery_news_worker.log"
echo ""
echo "   按 Ctrl+C 停止服务"
echo ""

exec "$PYTHON_CMD" -m celery -A backend.celery_app worker \
    --loglevel="${CELERY_LOG_LEVEL:-info}" \
    --concurrency="${CELERY_CONCURRENCY:-1}" \
    --queues=ai_processing \
    --hostname=news-worker@%h \
    --prefetch-multiplier=1 \
    --logfile="$LOGS_DIR/celery_news_worker.log" \
    --pidfile="$LOGS_DIR/celery_news_worker.pid"
