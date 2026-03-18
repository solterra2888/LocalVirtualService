#!/bin/bash
# ===========================================
# 本地开发脚本公共库
# 被 development/ 下的各启动/停止脚本 source 引用
# ===========================================

# ── 颜色 ──────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m'

# ── 路径 ──────────────────────────────────────────────────────
_COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$_COMMON_DIR")"
DEV_DIR="$_COMMON_DIR"
LOGS_DIR="$PROJECT_ROOT/logs"
DEV_REDIS_PID_FILE="$DEV_DIR/.dev_redis_pid"

# ── 加载 dev.conf（本地开发机配置）────────────────────────────
dev_load_conf() {
    local conf_file="$DEV_DIR/dev.conf"
    if [ ! -f "$conf_file" ]; then
        echo -e "${RED}❌ 开发配置文件不存在: $conf_file${NC}"
        echo "   请先创建: cp development/dev.conf.example development/dev.conf"
        echo "   然后根据本地环境修改 dev.conf"
        exit 1
    fi
    source "$conf_file"
}

# ── 加载 .env（运行时环境变量）────────────────────────────────
# 若存在 .env.redis.local（由 start_local_dev 自动启动 Redis 时生成），会覆盖 REDIS_URL 为无密码本地连接
dev_load_env() {
    local env_file="$PROJECT_ROOT/.env"
    if [ -f "$env_file" ]; then
        echo -e "${CYAN}📝 加载环境配置: $env_file${NC}"
        set -a
        source "$env_file"
        [ -f "$DEV_DIR/.env.redis.local" ] && source "$DEV_DIR/.env.redis.local"
        set +a
    else
        echo -e "${RED}❌ 环境配置文件不存在: $env_file${NC}"
        echo "   请确保项目根目录有 .env 文件"
        echo "   可以从 example.env 复制: cp example.env .env"
        exit 1
    fi
}

# ── 检测 Python 解释器 ───────────────────────────────────────
dev_detect_python() {
    local conda_python="${DEV_CONDA_ENV:-}/bin/python"
    if [ -n "${DEV_CONDA_ENV:-}" ] && [ -f "$conda_python" ]; then
        PYTHON_CMD="$conda_python"
        echo -e "${GREEN}✅ 使用 conda 环境: $PYTHON_CMD${NC}"
    elif command -v python3 &>/dev/null; then
        PYTHON_CMD="python3"
        echo -e "${YELLOW}⚠️  conda 环境未找到，使用系统 Python: $PYTHON_CMD${NC}"
    elif command -v python &>/dev/null; then
        PYTHON_CMD="python"
        echo -e "${YELLOW}⚠️  使用系统 Python: $PYTHON_CMD${NC}"
    else
        echo -e "${RED}❌ Python 未安装${NC}"
        exit 1
    fi
    echo -e "${GREEN}   Python 版本: $($PYTHON_CMD --version 2>&1)${NC}"
    export PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}"
}

# ── 校验后端核心依赖是否已安装 ───────────────────────────────
dev_verify_backend_python_deps() {
    echo -e "${BLUE}🔍 检查后端 Python 依赖...${NC}"
    local err
    err="$($PYTHON_CMD -c "
import sys
mods = ('fastapi', 'celery', 'redis', 'uvicorn', 'sqlalchemy')
missing = []
for m in mods:
    try:
        __import__(m)
    except ImportError:
        missing.append(m)
if missing:
    print(' '.join(missing))
    sys.exit(1)
" 2>&1)" || {
        echo -e "${RED}❌ 缺少 Python 包: ${err}${NC}"
        echo -e "${YELLOW}   请安装: pip install -r requirements.txt${NC}"
        return 1
    }
    echo -e "${GREEN}✅ 后端 Python 依赖检查通过${NC}"
    return 0
}

# ── 检查 Redis ────────────────────────────────────────────────
dev_check_redis() {
    local redis_url="${REDIS_URL:-redis://localhost:6379/0}"
    echo -e "${BLUE}🔍 检查 Redis 服务... (${redis_url})${NC}"
    if $PYTHON_CMD -c "
import redis, sys
try:
    r = redis.from_url('$redis_url')
    r.ping()
except Exception as e:
    print(f'❌ Redis 连接失败: {e}')
    sys.exit(1)
" 2>/dev/null; then
        echo -e "${GREEN}✅ Redis 服务正常${NC}"
        return 0
    else
        echo -e "${RED}❌ Redis 服务不可用，请先启动 Redis${NC}"
        return 1
    fi
}

# ── 若 Redis 未运行则自动启动（本地无密码实例，并写入 .dev_redis_pid）──
dev_start_redis_if_needed() {
    if dev_check_redis; then
        return 0
    fi
    if lsof -Pi :6379 -sTCP:LISTEN -t >/dev/null 2>&1; then
        echo -e "${YELLOW}ℹ️  端口 6379 已被占用，但连接失败（请检查 .env 中 REDIS_URL 是否与本地 Redis 密码一致）${NC}"
        return 1
    fi
    if ! command -v redis-server &>/dev/null; then
        echo -e "${RED}❌ 未找到 redis-server，请安装 Redis 或手动启动后重试${NC}"
        return 1
    fi
    echo -e "${BLUE}🔄 正在启动本地 Redis (无密码)...${NC}"
    local redis_log="${DEV_DIR}/logs/redis.log"
    mkdir -p "$(dirname "$redis_log")"
    nohup redis-server > "$redis_log" 2>&1 &
    local pid=$!
    echo $pid > "$DEV_REDIS_PID_FILE"
    sleep 2
    local url="redis://127.0.0.1:6379/0"
    if REDIS_URL="$url" dev_check_redis; then
        export REDIS_URL="$url"
        echo "REDIS_URL=$url" > "$DEV_DIR/.env.redis.local"
        echo -e "${GREEN}✅ 已启动本地 Redis (PID: $pid)，并使用无密码连接${NC}"
        return 0
    fi
    dev_stop_process "$pid" "Redis"
    rm -f "$DEV_REDIS_PID_FILE"
    echo -e "${RED}❌ 启动 Redis 失败，请查看 $redis_log${NC}"
    return 1
}

# ── 检查 Celery 配置 ─────────────────────────────────────────
dev_check_celery() {
    echo -e "${BLUE}🔍 检查 Celery 配置...${NC}"
    if $PYTHON_CMD -c "
import sys
sys.path.insert(0, '$PROJECT_ROOT')
from backend.celery_app import app as celery_app
from backend.celery_config import broker_url, result_backend
print(f'Broker: {broker_url}')
" 2>/dev/null; then
        echo -e "${GREEN}✅ Celery 配置正常${NC}"
        return 0
    else
        echo -e "${RED}❌ Celery 配置错误${NC}"
        return 1
    fi
}

# ── 检查 Celery 任务注册 ─────────────────────────────────────
dev_check_celery_tasks() {
    local required_task="${1:-}"
    echo -e "${BLUE}🔍 检查 Celery 任务注册...${NC}"
    $PYTHON_CMD -c "
import sys
sys.path.insert(0, '$PROJECT_ROOT')
from backend.celery_app import app as celery_app
registered = celery_app.tasks.keys()
custom = [t for t in registered if not t.startswith('celery.')]
print(f'已注册 {len(custom)} 个自定义任务')
required = '$required_task'
if required and required not in registered:
    print(f'❌ 必需任务 {required} 未注册')
    sys.exit(1)
" 2>/dev/null
    local rc=$?
    if [ $rc -eq 0 ]; then
        echo -e "${GREEN}✅ 任务注册正常${NC}"
    else
        echo -e "${RED}❌ 任务注册异常${NC}"
    fi
    return $rc
}

# ── 检查端口是否被占用 ────────────────────────────────────────
dev_check_port() {
    local port=$1
    local name=$2
    if lsof -Pi :"$port" -sTCP:LISTEN -t >/dev/null 2>&1; then
        echo -e "${YELLOW}⚠️  端口 $port 已被占用 ($name)${NC}"
        return 1
    fi
    return 0
}

# ── 停止单个进程（先 SIGTERM，超时后 SIGKILL）────────────────
dev_stop_process() {
    local pid=$1
    local name=$2
    if [ -z "$pid" ] || ! ps -p "$pid" >/dev/null 2>&1; then
        [ -n "$pid" ] && echo -e "${CYAN}ℹ️  $name (PID: $pid) 已经停止${NC}"
        return 0
    fi
    echo -e "${YELLOW}🔄 停止 $name (PID: $pid)...${NC}"
    kill "$pid" 2>/dev/null || true
    for _ in {1..10}; do
        if ! ps -p "$pid" >/dev/null 2>&1; then
            echo -e "${GREEN}✅ $name 已停止${NC}"
            return 0
        fi
        sleep 1
    done
    kill -9 "$pid" 2>/dev/null || true
    echo -e "${GREEN}✅ $name 已强制停止${NC}"
}

# ── 停止监听指定端口的进程 ────────────────────────────────────
dev_stop_by_port() {
    local port=$1
    local name=$2
    local pids
    pids=$(lsof -ti:"$port" 2>/dev/null || true)
    if [ -n "$pids" ]; then
        echo -e "${YELLOW}🔍 发现端口 $port ($name) 上的进程: $pids${NC}"
        for pid in $pids; do
            dev_stop_process "$pid" "$name"
        done
    else
        echo -e "${CYAN}ℹ️  端口 $port ($name) 无活动进程${NC}"
    fi
}

# ── 停止匹配模式的进程 ───────────────────────────────────────
dev_stop_by_pattern() {
    local pattern=$1
    local name=$2
    local pids
    pids=$(pgrep -f "$pattern" 2>/dev/null || true)
    if [ -n "$pids" ]; then
        echo -e "${YELLOW}🔄 停止 $name 进程...${NC}"
        echo "$pids" | xargs kill 2>/dev/null || true
        sleep 2
        pids=$(pgrep -f "$pattern" 2>/dev/null || true)
        [ -n "$pids" ] && echo "$pids" | xargs kill -9 2>/dev/null || true
        echo -e "${GREEN}✅ $name 进程已清理${NC}"
    fi
}

# ── 确保日志目录存在 ──────────────────────────────────────────
dev_ensure_logs() {
    mkdir -p "$LOGS_DIR"
}

# ── 检查数据库连接 ────────────────────────────────────────────
dev_check_db() {
    echo -e "${BLUE}🔍 检查数据库连接... (${DB_HOST:-localhost}:${DB_PORT:-5432}/${DB_NAME:-rc_database})${NC}"
    if $PYTHON_CMD -c "
import sys, asyncio
sys.path.insert(0, '$PROJECT_ROOT')
async def test():
    from backend.database.db_manager import DatabaseManager
    from config.settings import config
    db = DatabaseManager({
        'db_host': config.get('DB_HOST', 'localhost'),
        'db_port': config.get('DB_PORT', '5432'),
        'db_name': config.get('DB_NAME', 'rc_database'),
        'db_user': config.get('DB_USER', 'postgres'),
        'db_password': config.get('DB_PASSWORD', '')
    })
    if not db.ensure_schema():
        sys.exit(1)
asyncio.run(test())
" 2>/dev/null; then
        echo -e "${GREEN}✅ 数据库连接正常${NC}"
        return 0
    else
        echo -e "${RED}❌ 数据库连接失败${NC}"
        return 1
    fi
}

# ── 检查远程调度器状态（可选）─────────────────────────────────
dev_check_remote_scheduler() {
    local url="${DEV_REMOTE_SCHEDULER_URL:-}"
    if [ -z "$url" ]; then
        echo -e "${CYAN}ℹ️  未配置远程调度器地址，跳过检查${NC}"
        return 0
    fi
    echo -e "${BLUE}🔍 检查远程调度器... ($url)${NC}"
    if curl -s --connect-timeout 5 --max-time 10 "$url/api/health" >/dev/null 2>&1; then
        echo -e "${GREEN}✅ 远程调度器服务正在运行${NC}"
        curl -s --connect-timeout 5 --max-time 10 "$url/api/health" 2>/dev/null | \
            $PYTHON_CMD -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(f'   运行状态: {\"运行中\" if d.get(\"scheduler_running\") else \"已停止\"}')
    print(f'   总任务数: {d.get(\"total_tasks\", 0)}, 启用: {d.get(\"enabled_tasks\", 0)}')
except: pass
" 2>/dev/null || true
    else
        echo -e "${YELLOW}⚠️  远程调度器未运行或无法访问（不影响本地开发）${NC}"
    fi
}
