#!/bin/bash
# Celery Worker 开发启动脚本（file_processing + ai_processing 队列）
#
# 改进 (2026-04-29): 加 supervisor 循环, worker 崩溃后自动重启, 不再卡死
# 整套上传/AI 处理流程. 起因: file_processing worker 因 Redis 队列里的"毒消息"
# (反序列化失败的 task result meta) 启动即崩, 没有自愈机制 → 上传卡 0%.
#
# 行为:
#   - 收到 SIGINT/SIGTERM: 转发给 worker, 等 10s 优雅退出, supervisor 也退出
#   - worker 任意退出 (无论 exit code): 退避 2s 后重启
#     注: Celery worker 即使被外部 pkill SIGTERM 也会 "warm shutdown" 报 exit 0,
#         所以不能靠 exit code 判断是否人为停止. 唯一可靠信号是 supervisor 自己
#         捕获到的 SIGTERM/SIGINT.
#   - 60s 内崩溃 ≥ 5 次: supervisor 退出, 提示用户排查/purge 队列
#     (毒消息死循环防护: 即使每次 exit 0, 5 次连崩也会停止, 避免日志爆炸)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

echo -e "${BLUE}🔄 Celery Worker 开发模式启动 (with supervisor)${NC}"
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
echo "   - Supervisor: 崩溃后自动重启 (60s 内 5 次后停止)"
echo ""
echo "   按 Ctrl+C 优雅停止"
echo ""

# macOS prefork 子进程在加载 libpq/OpenSSL 后跑任务可能 SIGSEGV; solo 池规避
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

CELERY_EXTRA_ARGS=()
if [[ "$(uname -s)" == "Darwin" ]]; then
  CELERY_EXTRA_ARGS=(--pool=solo)
else
  CELERY_EXTRA_ARGS=(--concurrency="${CELERY_CONCURRENCY:-2}")
fi

PIDFILE="$LOGS_DIR/celery_worker.pid"
SUPERVISOR_PIDFILE="$LOGS_DIR/celery_worker_supervisor.pid"
echo $$ > "$SUPERVISOR_PIDFILE"

CHILD_PID=""
SHUTDOWN_REQUESTED=0

cleanup_and_exit() {
    SHUTDOWN_REQUESTED=1
    if [ -n "$CHILD_PID" ] && kill -0 "$CHILD_PID" 2>/dev/null; then
        echo ""
        echo -e "${YELLOW}🛑 收到停止信号, 转发给 worker (PID: $CHILD_PID)...${NC}"
        kill -TERM "$CHILD_PID" 2>/dev/null || true
        for _ in $(seq 1 10); do
            if ! kill -0 "$CHILD_PID" 2>/dev/null; then
                break
            fi
            sleep 1
        done
        kill -9 "$CHILD_PID" 2>/dev/null || true
    fi
    rm -f "$SUPERVISOR_PIDFILE" "$PIDFILE"
    echo -e "${BLUE}👋 Celery worker supervisor 已退出${NC}"
    exit 0
}

trap cleanup_and_exit SIGINT SIGTERM

# 崩溃频率控制
MAX_CRASHES_IN_WINDOW=5
CRASH_WINDOW_SEC=60
recent_crashes=()

run_iteration=0
while [ $SHUTDOWN_REQUESTED -eq 0 ]; do
    run_iteration=$((run_iteration + 1))

    # 清理上次的 pidfile, 否则 celery 报"pidfile already exists"
    rm -f "$PIDFILE"

    if [ $run_iteration -gt 1 ]; then
        echo -e "${CYAN}🔁 第 $run_iteration 次启动 worker...${NC}"
    fi

    "$PYTHON_CMD" -m celery -A backend.celery_app worker \
        "${CELERY_EXTRA_ARGS[@]}" \
        --loglevel="${CELERY_LOG_LEVEL:-info}" \
        --queues=file_processing,ai_processing \
        --hostname=dev-worker@%h \
        --logfile="$LOGS_DIR/celery_worker.log" \
        --pidfile="$PIDFILE" &
    CHILD_PID=$!

    # wait 子进程; trap 触发时 wait 会被中断, 然后循环顶部重新检查 SHUTDOWN_REQUESTED
    set +e
    wait "$CHILD_PID"
    EXIT_CODE=$?
    set -e
    CHILD_PID=""

    if [ $SHUTDOWN_REQUESTED -eq 1 ]; then
        break
    fi

    # 注意: 不再用 exit 0 判断"主动退出". Celery worker 被 pkill SIGTERM 也是 exit 0,
    # 用户上报的"毒消息"场景同样是 worker 内部 warm shutdown → exit 0. 一律重启,
    # 靠下面的 60s 5 次频率限制兜底.

    NOW=$(date +%s)
    recent_crashes+=("$NOW")
    THRESHOLD=$((NOW - CRASH_WINDOW_SEC))
    new_crashes=()
    for ts in "${recent_crashes[@]}"; do
        if [ "$ts" -ge "$THRESHOLD" ]; then
            new_crashes+=("$ts")
        fi
    done
    recent_crashes=("${new_crashes[@]}")

    if [ ${#recent_crashes[@]} -ge $MAX_CRASHES_IN_WINDOW ]; then
        echo -e "${RED}❌ Worker 在 ${CRASH_WINDOW_SEC}s 内崩溃 ${MAX_CRASHES_IN_WINDOW} 次, supervisor 停止重启.${NC}"
        echo -e "${YELLOW}   排查日志: tail -50 $LOGS_DIR/celery_worker.log${NC}"
        echo -e "${YELLOW}   常见原因: Redis 队列里有损坏消息. 清空命令:${NC}"
        echo -e "${YELLOW}     $PYTHON_CMD -m celery -A backend.celery_app purge -Q file_processing,ai_processing -f${NC}"
        break
    fi

    BACKOFF=2
    echo -e "${YELLOW}⚠️  Worker 退出 (exit $EXIT_CODE), ${BACKOFF}s 后重启 (近 ${CRASH_WINDOW_SEC}s 内 ${#recent_crashes[@]} 次)...${NC}"
    sleep $BACKOFF
done

rm -f "$SUPERVISOR_PIDFILE"
echo -e "${BLUE}👋 Celery worker supervisor 已退出${NC}"
