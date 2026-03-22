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
if [[ "$(uname -s)" == "Darwin" ]]; then
  echo "   - 进程池: solo（macOS 开发推荐，避免 SIGSEGV）"
else
  echo "   - 并发数: ${CELERY_CONCURRENCY:-2}"
fi
echo "   - 队列: file_processing, ai_processing"
echo "   - 日志: $LOGS_DIR/celery_worker.log"
echo ""
echo "   按 Ctrl+C 停止服务"
echo ""

# macOS: prefork 子进程在已加载 libpq/OpenSSL 等库后再跑任务时可能 SIGSEGV（日志里表现为
# WorkerLostError / signal 11）。OBJC_DISABLE_INITIALIZE_FORK_SAFETY 可缓解但不总能避免。
# 开发环境在 Darwin 上默认使用 solo 池（单进程、不 fork），与 Linux 生产仍可用 prefork。
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

CELERY_EXTRA_ARGS=()
if [[ "$(uname -s)" == "Darwin" ]]; then
  CELERY_EXTRA_ARGS=(--pool=solo)
else
  CELERY_EXTRA_ARGS=(--concurrency="${CELERY_CONCURRENCY:-2}")
fi

exec "$PYTHON_CMD" -m celery -A backend.celery_app worker \
    "${CELERY_EXTRA_ARGS[@]}" \
    --loglevel="${CELERY_LOG_LEVEL:-info}" \
    --queues=file_processing,ai_processing \
    --hostname=dev-worker@%h \
    --logfile="$LOGS_DIR/celery_worker.log" \
    --pidfile="$LOGS_DIR/celery_worker.pid"
