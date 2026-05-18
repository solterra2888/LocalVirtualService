#!/bin/bash
# GuanHe Project 服务启动脚本

set -euo pipefail

APP_NAME="$(grep -s '^APP_NAME=' "$(cd "$(dirname "$0")" && pwd)/.env" 2>/dev/null | cut -d= -f2)"
APP_NAME="${APP_NAME:-GuanHe Project}"

echo "🚀 $APP_NAME 服务启动脚本"
echo "================================"

if [ "$EUID" -ne 0 ]; then
    echo "❌ 请以root权限运行此脚本: sudo $0"
    exit 1
fi

echo "📋 启动服务列表:"
echo "   - PostgreSQL数据库 (Docker)"
echo "   - Redis缓存 (Docker)"
echo "   - Web应用"
echo "   - Celery Worker (AI/文件处理)"
echo "   - Celery Worker (数据获取)"
echo "   - 调度器服务"
echo "   - Nginx代理 (Docker)"
echo ""

# ── 辅助函数 ──────────────────────────────────────────────────

# 等待 systemd 服务就绪
wait_for_service() {
    local service=$1
    local max_wait=${2:-15}
    local count=0
    while ! systemctl is-active --quiet "$service"; do
        if [ $count -ge $max_wait ]; then
            return 1
        fi
        sleep 1
        count=$((count + 1))
    done
    return 0
}

# 检查 Docker 容器是否正在运行
docker_running() {
    local container=$1
    docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null | grep -q "^true$"
}

# 确保 Docker 容器已运行（不存在则报错，已停止则尝试启动）
ensure_docker_container() {
    local container=$1
    local display_name=$2
    local max_wait=${3:-15}

    if docker_running "$container"; then
        echo "✅ $display_name (Docker: $container) 已在运行"
        return 0
    fi

    if ! docker inspect "$container" &>/dev/null; then
        echo "❌ Docker容器 '$container' 不存在，请先创建该容器"
        return 1
    fi

    echo "🔄 Docker容器 $container 未运行，尝试启动..."
    docker start "$container"

    local count=0
    while ! docker_running "$container"; do
        if [ $count -ge $max_wait ]; then
            echo "❌ $display_name 启动超时"
            return 1
        fi
        sleep 1
        count=$((count + 1))
    done

    echo "✅ $display_name (Docker: $container) 已启动"
    return 0
}

# 获取显示用的访问地址（优先读 .env 中的 SERVER_HOST，否则取第一块网卡 IP）
get_server_ip() {
    local env_file="$(cd "$(dirname "$0")" && pwd)/.env"
    if [ -f "$env_file" ]; then
        local host
        host=$(grep -E '^SERVER_HOST=' "$env_file" | cut -d'=' -f2 | tr -d '"' | tr -d "'" | head -1)
        [ -n "$host" ] && echo "$host" && return
    fi
    hostname -I | awk '{print $1}'
}

# ── 1. PostgreSQL (Docker) ────────────────────────────────────
echo "🗄️  检查PostgreSQL数据库..."
if ! ensure_docker_container "postgres" "PostgreSQL"; then
    echo "❌ PostgreSQL不可用，中止启动"
    exit 1
fi

# ── 2. Redis (Docker) ─────────────────────────────────────────
echo "🔴 检查Redis缓存..."
if ! ensure_docker_container "redis" "Redis"; then
    echo "❌ Redis不可用，Celery和调度器无法工作，中止启动"
    exit 1
fi

# ── 3. Web应用 ───────────────────────────────────────────────
echo "🌐 启动Web应用..."
systemctl start rc-webapp
if wait_for_service rc-webapp 20; then
    echo "✅ Web应用已启动"
else
    echo "❌ Web应用启动失败，中止启动"
    exit 1
fi

# ── 4. Celery AI Worker (file_processing + ai_processing) ────
# 启动前清理 Redis 旧任务元数据，防止损坏记录导致 worker 启动即崩
echo "🧹 清理 Redis 旧任务元数据..."
SCRIPT_DIR_SS="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE_SS="$SCRIPT_DIR_SS/.env"
REDIS_URL_SS="redis://localhost:6379/0"
[ -f "$ENV_FILE_SS" ] && REDIS_URL_SS=$(grep -E '^REDIS_URL=' "$ENV_FILE_SS" | cut -d= -f2- | tr -d '"'"'" | head -1 || echo "$REDIS_URL_SS")
PYTHON_BIN_SS="$(grep -E '^CONDA_ENV=' "$ENV_FILE_SS" 2>/dev/null | cut -d= -f2 || true)/bin/python"
[ ! -x "$PYTHON_BIN_SS" ] && PYTHON_BIN_SS="$(command -v python3 2>/dev/null || echo "")"
if [ -n "$PYTHON_BIN_SS" ] && [ -x "$PYTHON_BIN_SS" ] && "$PYTHON_BIN_SS" -c "import redis" 2>/dev/null; then
    "$PYTHON_BIN_SS" - <<PYEOF 2>/dev/null
import redis, sys
try:
    r = redis.from_url('$REDIS_URL_SS', socket_connect_timeout=3)
    r.ping()
except Exception as e:
    print(f"  ⚠️  Redis 连接失败，跳过清理: {e}"); sys.exit(0)
keys = r.keys('celery-task-meta-*') + r.keys('unacked*')
for k in keys: r.delete(k)
print(f"  ✅ 已清除 {len(keys)} 条旧任务元数据" if keys else "  ✅ Redis 无残留任务元数据")
PYEOF
fi

echo "🤖 启动Celery AI Worker..."
systemctl start rc-celery-ai
if wait_for_service rc-celery-ai 15; then
    echo "✅ Celery AI Worker已启动"
else
    echo "⚠️  Celery AI Worker启动失败（文件上传和AI分析将无法执行）"
fi

# ── 5. Celery Data Worker (data_fetching) ────────────────────
echo "📊 启动Celery Data Worker..."
systemctl start rc-celery-data
if wait_for_service rc-celery-data 15; then
    echo "✅ Celery Data Worker已启动"
else
    echo "⚠️  Celery Data Worker启动失败（数据获取任务将无法执行）"
fi

# ── 6. 调度器服务 ─────────────────────────────────────────────
echo "🕒 启动调度器服务..."
systemctl start rc-scheduler
if wait_for_service rc-scheduler 15; then
    echo "✅ 调度器服务已启动"
else
    echo "⚠️  调度器服务启动失败（定时任务将无法执行）"
fi

# ── 7. Nginx (Docker) ─────────────────────────────────────────
echo "🌐 启动Nginx代理 (Docker)..."
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -f "$SCRIPT_DIR/web.conf" ]; then
    cp "$SCRIPT_DIR/web.conf" /opt/nginx/conf/vhosts/web.conf
fi
if ! ensure_docker_container "nginx" "Nginx"; then
    echo "❌ Nginx不可用"
    exit 1
fi

SERVER_IP=$(get_server_ip)

echo ""
echo "🎉 所有服务启动完成!"
echo "================================"
echo "🌐 访问地址 (通过Nginx):"
echo "   - 前端应用: http://${SERVER_IP}"
echo "   - API文档:  http://${SERVER_IP}/api/docs  (ENV=production 时默认不暴露，需 ENABLE_API_DOCS=true)"
echo ""
echo "🔧 服务管理命令:"
echo "   - 查看所有服务: systemctl status 'rc-*'"
echo "   - 停止所有服务: sudo ./stop_services.sh"
echo "   - 重启所有服务: sudo ./restart_services.sh"
echo "   - 仅重启后端:   sudo ./restart_backend.sh"
echo ""
echo "📊 查看服务日志:"
echo "   - Web应用:    journalctl -u rc-webapp -f"
echo "   - 调度器:     journalctl -u rc-scheduler -f"
echo "   - Celery AI:  journalctl -u rc-celery-ai -f"
echo "   - Celery Data:journalctl -u rc-celery-data -f"
