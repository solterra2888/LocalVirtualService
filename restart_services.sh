#!/bin/bash
# GuanHe Project 全量重启脚本（不重启 PostgreSQL / Redis Docker 容器）

set -euo pipefail

APP_NAME="$(grep -s '^APP_NAME=' "$(cd "$(dirname "$0")" && pwd)/.env" 2>/dev/null | cut -d= -f2)"
APP_NAME="${APP_NAME:-GuanHe Project}"

echo "🔄 $APP_NAME 服务重启脚本"
echo "================================"

if [ "$EUID" -ne 0 ]; then
    echo "❌ 请以root权限运行此脚本: sudo $0"
    exit 1
fi

# ── 辅助函数 ──────────────────────────────────────────────────

wait_for_service() {
    local service=$1
    local max_wait=${2:-20}
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

docker_running() {
    local container=$1
    docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null | grep -q "^true$"
}

get_server_ip() {
    local env_file="$(cd "$(dirname "$0")" && pwd)/.env"
    if [ -f "$env_file" ]; then
        local host
        host=$(grep -E '^SERVER_HOST=' "$env_file" | cut -d'=' -f2 | tr -d '"' | tr -d "'" | head -1)
        [ -n "$host" ] && echo "$host" && return
    fi
    hostname -I | awk '{print $1}'
}

# ── 前置检查：Docker 容器健康状态 ────────────────────────────
echo "🔍 检查基础服务 (Docker 容器) 状态..."

DOCKER_OK=true

if docker_running "postgres"; then
    echo "   ✅ PostgreSQL (Docker) 运行中"
else
    echo "   ❌ PostgreSQL (Docker) 未运行 — 请先执行: docker start postgres"
    DOCKER_OK=false
fi

if docker_running "redis"; then
    echo "   ✅ Redis (Docker) 运行中"
else
    echo "   ❌ Redis (Docker) 未运行 — 请先执行: docker start redis"
    DOCKER_OK=false
fi

if [ "$DOCKER_OK" = false ]; then
    echo ""
    echo "❌ 基础服务不可用，Celery/调度器无法正常工作，中止重启"
    echo "   修复方法: docker start postgres redis"
    exit 1
fi

echo ""
echo "📋 重启服务列表（保留 PostgreSQL/Redis Docker 容器不重启）:"
echo "   - Web应用"
echo "   - Celery Worker (AI/文件处理)"
echo "   - Celery Worker (数据获取)"
echo "   - 调度器服务"
echo "   - Nginx代理 (Docker)"
echo ""

# 同步 web.conf 到 Docker Nginx 配置卷
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -f "$SCRIPT_DIR/web.conf" ]; then
    cp "$SCRIPT_DIR/web.conf" /opt/nginx/conf/vhosts/web.conf
    echo "   ✅ nginx 配置 (web.conf) 已同步到 Docker 卷"
fi

# ── 重启前清理 Redis 任务元数据 ───────────────────────────────
# 防止上次异常停止留下的损坏 meta 导致 Celery worker 重启后立即崩溃
echo "🧹 清理 Redis 旧任务元数据..."
SCRIPT_DIR_RS="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE_RS="$SCRIPT_DIR_RS/.env"
REDIS_URL_RS="redis://localhost:6379/0"
[ -f "$ENV_FILE_RS" ] && REDIS_URL_RS=$(grep -E '^REDIS_URL=' "$ENV_FILE_RS" | cut -d= -f2- | tr -d '"'"'" | head -1 || echo "$REDIS_URL_RS")
PYTHON_BIN_RS="$(grep -E '^CONDA_ENV=' "$ENV_FILE_RS" 2>/dev/null | cut -d= -f2)/bin/python"
[ ! -x "$PYTHON_BIN_RS" ] && PYTHON_BIN_RS="$(command -v python3 2>/dev/null || echo "")"
if [ -n "$PYTHON_BIN_RS" ] && [ -x "$PYTHON_BIN_RS" ] && "$PYTHON_BIN_RS" -c "import redis" 2>/dev/null; then
    "$PYTHON_BIN_RS" - <<PYEOF 2>/dev/null
import redis, sys
try:
    r = redis.from_url('$REDIS_URL_RS', socket_connect_timeout=3)
    r.ping()
except Exception as e:
    print(f"  ⚠️  Redis 连接失败，跳过清理: {e}"); sys.exit(0)
keys = r.keys('celery-task-meta-*') + r.keys('unacked*')
for k in keys: r.delete(k)
print(f"  ✅ 已清除 {len(keys)} 条旧任务元数据" if keys else "  ✅ Redis 无残留任务元数据")
PYEOF
fi
echo ""

SERVICES=("rc-webapp" "rc-celery-ai" "rc-celery-data" "rc-scheduler")

for service in "${SERVICES[@]}"; do
    echo "🔄 重启 $service ..."
    systemctl restart "$service"
    if wait_for_service "$service" 20; then
        echo "   ✅ $service 已就绪"
    else
        echo "   ❌ $service 重启失败"
    fi
done

# 重启 Docker Nginx
echo "🔄 重启 nginx (Docker) ..."
docker restart nginx 2>/dev/null || docker start nginx 2>/dev/null
sleep 2
if docker_running "nginx"; then
    echo "   ✅ nginx (Docker) 已就绪"
else
    echo "   ❌ nginx (Docker) 重启失败"
fi

echo ""
echo "🔍 服务状态汇总:"
all_running=true
for service in "${SERVICES[@]}"; do
    if systemctl is-active --quiet "$service"; then
        echo "   ✅ $service: 运行中"
    else
        echo "   ❌ $service: 未运行"
        all_running=false
    fi
done
if docker_running "nginx"; then
    echo "   ✅ nginx (Docker): 运行中"
else
    echo "   ❌ nginx (Docker): 未运行"
    all_running=false
fi

SERVER_IP=$(get_server_ip)

echo ""
if [ "$all_running" = true ]; then
    echo "🎉 所有服务重启成功!"
    echo "================================"
    echo "🌐 访问地址 (通过Nginx):"
    echo "   - 前端应用: http://${SERVER_IP}"
    echo "   - API文档:  http://${SERVER_IP}/api/docs"
else
    echo "⚠️  部分服务重启失败，请查看日志:"
    echo "   journalctl -u rc-webapp -f"
    echo "   journalctl -u rc-celery-ai -f"
    echo "   journalctl -u rc-celery-data -f"
    echo "   journalctl -u rc-scheduler -f"
    echo "   docker logs nginx --tail 50"
fi
