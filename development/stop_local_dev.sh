#!/bin/bash
# 本地开发环境停止脚本

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

echo -e "${RED}🛑 本地开发环境停止脚本${NC}"
echo "=============================================="
echo ""

# ── 从 PID 文件停止 ──────────────────────────────────────────
PID_FILE="$SCRIPT_DIR/.dev_pids"
if [ -f "$PID_FILE" ]; then
    echo -e "${BLUE}📖 从 PID 文件读取进程信息...${NC}"
    source "$PID_FILE"
    dev_stop_process "${FRONTEND_PID:-}" "前端应用"
    dev_stop_process "${WEBAPP_PID:-}" "Web 应用"
    dev_stop_process "${CELERY_PID:-}" "Celery file/ai Worker"
    dev_stop_process "${DATA_CELERY_PID:-}" "Celery data Worker"
    rm -f "$PID_FILE"
    echo -e "${GREEN}✅ PID 文件已清理${NC}"
else
    echo -e "${YELLOW}⚠️  PID 文件不存在，将通过端口/进程模式查找${NC}"
fi

# ── 清理 Redis Celery 队列与任务元数据（必须在停 Redis 之前！）──
# 每次停止时清空，防止损坏的旧任务元数据在下次启动时导致 worker 崩溃
echo ""
echo -e "${BLUE}🧹 清理 Redis Celery 队列...${NC}"

# 读取 Redis 连接信息（优先从 .env，否则默认本地无密码）
REDIS_HOST="127.0.0.1"
REDIS_PORT="6379"
REDIS_DB="0"
ENV_FILE="$SCRIPT_DIR/../.env"
if [ -f "$ENV_FILE" ]; then
    _redis_url=$(grep -E '^REDIS_URL=' "$ENV_FILE" 2>/dev/null | head -1 | cut -d= -f2- | tr -d '"'"'" | tr -d ' ')
    if [ -n "$_redis_url" ]; then
        _redis_host=$(echo "$_redis_url" | sed -E 's|redis://([^@]*@)?([^:/]+).*|\2|')
        _redis_port=$(echo "$_redis_url" | sed -E 's|.*:([0-9]+)(/.*)?$|\1|')
        _redis_db=$(echo "$_redis_url" | sed -E 's|.*/([0-9]+)$|\1|')
        [ -n "$_redis_host" ] && REDIS_HOST="$_redis_host"
        [ -n "$_redis_port" ] && REDIS_PORT="$_redis_port"
        # 修正：=~ 必须用 [[ ]]，不能用 [ ]
        [[ "$_redis_db" =~ ^[0-9]+$ ]] && REDIS_DB="$_redis_db"
    fi
fi

PYTHON_BIN="${PYTHON_BIN:-$(command -v python3 || command -v python)}"
if [ -n "$PYTHON_BIN" ] && "$PYTHON_BIN" -c "import redis" 2>/dev/null; then
    "$PYTHON_BIN" - <<PYEOF
import redis, sys
try:
    r = redis.Redis(host='$REDIS_HOST', port=int('$REDIS_PORT'), db=int('$REDIS_DB'), socket_connect_timeout=3)
    r.ping()
except Exception as e:
    print(f"  ⚠️  Redis 连接失败，跳过队列清理: {e}")
    sys.exit(0)

queues = ['file_processing', 'ai_processing', 'default',
          'youtube_fetching', 'youtube_transcription']
cleaned_queues = 0
for q in queues:
    length = r.llen(q)
    if length > 0:
        r.delete(q)
        print(f"  ✅ 清空队列 {q}（{length} 条任务）")
        cleaned_queues += 1

meta_keys = r.keys('celery-task-meta-*')
deleted_meta = len(meta_keys)
for k in meta_keys:
    r.delete(k)

unacked_keys = r.keys('unacked*')
for k in unacked_keys:
    r.delete(k)

if cleaned_queues == 0 and deleted_meta == 0:
    print("  ✅ Redis 队列已是干净状态，无需清理")
else:
    print(f"  ✅ 共删除 {deleted_meta} 条任务元数据，清空 {cleaned_queues} 个队列")
PYEOF
else
    if command -v redis-cli &>/dev/null; then
        for q in file_processing ai_processing default youtube_fetching youtube_transcription; do
            count=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB" LLEN "$q" 2>/dev/null || echo 0)
            if [ "$count" -gt 0 ] 2>/dev/null; then
                redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB" DEL "$q" >/dev/null
                echo "  ✅ 清空队列 $q（$count 条任务）"
            fi
        done
        # 清理 task meta
        redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB" \
            EVAL "local ks=redis.call('keys','celery-task-meta-*') for _,k in ipairs(ks) do redis.call('del',k) end return #ks" 0 2>/dev/null | \
            xargs -I{} echo "  ✅ 删除 {} 条任务元数据（via redis-cli）"
        echo "  ✅ Redis 队列清理完成（via redis-cli）"
    else
        echo "  ⚠️  未找到 Python redis 模块或 redis-cli，跳过 Redis 队列清理"
    fi
fi

# ── 停止由 start_local_dev 启动的 Redis（在清理完队列之后）──────
if [ -f "$DEV_REDIS_PID_FILE" ]; then
    REDIS_PID=$(cat "$DEV_REDIS_PID_FILE")
    dev_stop_process "$REDIS_PID" "Redis (本地)"
    rm -f "$DEV_REDIS_PID_FILE" "$DEV_DIR/.env.redis.local"
fi

# ── 按端口停止 ────────────────────────────────────────────────
echo ""
echo -e "${BLUE}🔍 检查并停止端口上的进程...${NC}"
dev_stop_by_port 3000 "前端应用"
dev_stop_by_port 8000 "Web 应用"
dev_stop_by_port 5555 "Flower 监控"

# ── 按进程模式清理 ────────────────────────────────────────────
echo ""
echo -e "${BLUE}🧹 清理相关进程...${NC}"
dev_stop_by_pattern "npm start"          "Node.js (npm start)"
dev_stop_by_pattern "uvicorn.*main:app"  "Python (uvicorn)"
# 先停 supervisor (start_celery_worker_dev.sh) 否则它会立刻重启被杀的 worker
dev_stop_by_pattern "start_celery_worker_dev.sh" "Celery Worker Supervisor"
dev_stop_by_pattern "celery.*worker"     "Celery Worker"
dev_stop_by_pattern "celery.*flower"     "Flower 监控"

# ── 清理日志（可选）──────────────────────────────────────────
DEV_LOGS_DIR="$DEV_DIR/logs"
if [ -d "$DEV_LOGS_DIR" ]; then
    echo ""
    read -p "是否清理开发日志文件？(y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm -f "$DEV_LOGS_DIR"/*.log
        echo -e "${GREEN}✅ 日志文件已清理${NC}"
    fi
fi

# ── 最终检查 ─────────────────────────────────────────────────
echo ""
echo -e "${BLUE}🔍 最终端口检查...${NC}"
for entry in "3000:前端应用" "8000:Web应用" "5555:Flower监控"; do
    port="${entry%%:*}"
    name="${entry##*:}"
    if lsof -Pi :"$port" -sTCP:LISTEN -t >/dev/null 2>&1; then
        echo -e "${RED}❌ 端口 $port ($name) 仍有进程${NC}"
    else
        echo -e "${GREEN}✅ 端口 $port ($name) 已释放${NC}"
    fi
done

echo ""
echo -e "${GREEN}🎉 开发服务已停止!${NC}"
echo "=============================================="
echo -e "${CYAN}💡 重新启动: ./development/start_local_dev.sh${NC}"
echo "=============================================="
