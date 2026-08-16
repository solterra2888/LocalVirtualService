#!/bin/bash
# GuanHe Project 服务停止脚本
# 注意：PostgreSQL 和 Redis 由 Docker 管理（restart=always），本脚本不停止它们

set -euo pipefail

APP_NAME="$(grep -s '^APP_NAME=' "$(cd "$(dirname "$0")" && pwd)/.env" 2>/dev/null | cut -d= -f2)"
APP_NAME="${APP_NAME:-GuanHe Project}"

echo "🛑 $APP_NAME 服务停止脚本"
echo "================================"

if [ "$EUID" -ne 0 ]; then
    echo "❌ 请以root权限运行此脚本: sudo $0"
    exit 1
fi

echo "📋 停止服务列表:"
echo "   - Nginx代理 (Docker)"
echo "   - 调度器服务"
echo "   - Celery Worker (AI/文件处理)"
echo "   - Celery Worker (数据获取)"
echo "   - Web应用"
echo "   (PostgreSQL/Redis 由 Docker 管理，不在此处停止)"
echo ""

# 停止服务并验证状态的辅助函数
stop_service() {
    local service=$1
    systemctl stop "$service" 2>/dev/null || true
    if systemctl is-active --quiet "$service"; then
        echo "   ⚠️  $service 可能未完全停止"
    else
        echo "   ✅ $service 已停止"
    fi
}

# 停止 Docker Nginx
echo "   🔄 停止 Nginx (Docker)..."
docker stop nginx 2>/dev/null || true
if docker inspect -f '{{.State.Running}}' nginx 2>/dev/null | grep -q "^false$"; then
    echo "   ✅ nginx (Docker) 已停止"
else
    echo "   ⚠️  nginx (Docker) 可能未完全停止"
fi

# ── 停止 Celery 前清理 Redis 任务元数据 ──────────────────────
# Redis 由 Docker 管理不会随 stop 停止，旧 task meta 会一直积累。
# 在此处（Worker 还在接收 ACK 的最后时机）清理，避免下次启动时
# worker 碰到损坏 meta 而进入无限崩溃重启循环。
echo ""
echo "🧹 清理 Redis Celery 任务元数据..."
ENV_FILE="$(cd "$(dirname "$0")" && pwd)/.env"
REDIS_URL_VAL="redis://localhost:6379/0"
[ -f "$ENV_FILE" ] && REDIS_URL_VAL=$(grep -E '^REDIS_URL=' "$ENV_FILE" | cut -d= -f2- | tr -d '"'"'" | head -1 || echo "$REDIS_URL_VAL")

# 优先用 Python（conda env 中已有 redis 包）
PYTHON_BIN="$(grep -E '^CONDA_ENV=' "$ENV_FILE" 2>/dev/null | cut -d= -f2 || true)/bin/python"
[ ! -x "$PYTHON_BIN" ] && PYTHON_BIN="$(command -v python3 2>/dev/null || echo "")"

if [ -n "$PYTHON_BIN" ] && [ -x "$PYTHON_BIN" ] && "$PYTHON_BIN" -c "import redis" 2>/dev/null; then
    "$PYTHON_BIN" - <<PYEOF 2>/dev/null
import redis, os, sys
url = os.environ.get('REDIS_URL', '$REDIS_URL_VAL')
try:
    r = redis.from_url(url, socket_connect_timeout=3)
    r.ping()
except Exception as e:
    print(f"  ⚠️  Redis 连接失败，跳过清理: {e}")
    sys.exit(0)
keys = r.keys('celery-task-meta-*') + r.keys('unacked*')
for k in keys:
    r.delete(k)
print(f"  ✅ 已清除 {len(keys)} 条旧任务元数据" if keys else "  ✅ Redis 无残留任务元数据")
PYEOF
elif command -v redis-cli &>/dev/null; then
    # fallback: redis-cli（解析 REDIS_URL 取 host/port/password）
    _host=$(echo "$REDIS_URL_VAL" | sed -E 's|redis://([^:@]*:[^@]*@)?([^:/]+).*|\2|')
    _port=$(echo "$REDIS_URL_VAL" | sed -E 's|.*:([0-9]+)(/.*)?$|\1|')
    _auth=$(echo "$REDIS_URL_VAL" | sed -En 's|redis://:([^@]+)@.*|\1|p')
    _cli_args="-h ${_host:-127.0.0.1} -p ${_port:-6379}"
    [ -n "$_auth" ] && _cli_args="$_cli_args -a $_auth"
    redis-cli $_cli_args EVAL \
        "local ks=redis.call('keys','celery-task-meta-*') for _,k in ipairs(ks) do redis.call('del',k) end return #ks" 0 2>/dev/null | \
        xargs -I{} echo "  ✅ 已清除 {} 条任务元数据（via redis-cli）"
    echo "  ✅ Redis 清理完成（via redis-cli）"
else
    echo "  ⚠️  未找到 Python redis 模块或 redis-cli，跳过清理"
fi

# 按依赖顺序停止 systemd 服务（先停外层，再停内层）
stop_service rc-scheduler
stop_service rc-celery-ai
stop_service rc-celery-data
stop_service rc-celery-zsxq-files
stop_service rc-webapp

# ── 清理残留进程 ──────────────────────────────────────────────
echo ""
echo "🧹 清理残留进程..."

cleanup_port() {
    local port=$1
    local pids
    pids=$(lsof -t -i:"$port" 2>/dev/null || true)
    if [ -n "$pids" ]; then
        echo "   🔄 发现 $port 端口残留进程 (PID: $pids)，正在清理..."
        kill -15 $pids 2>/dev/null || true
        sleep 2
        pids=$(lsof -t -i:"$port" 2>/dev/null || true)
        if [ -n "$pids" ]; then
            kill -9 $pids 2>/dev/null || true
        fi
        echo "   ✅ $port 端口已清理"
    fi
}

cleanup_port 8000   # Web应用
cleanup_port 8001   # 调度器

echo ""
echo "🎉 应用服务已全部停止!"
echo "================================"
echo "💡 PostgreSQL (Docker: postgres) 和 Redis (Docker: redis) 仍在运行"
echo "   如需停止 Docker 容器: docker stop redis postgres"
echo "💡 要重新启动服务，请运行: sudo ./start_services.sh"
