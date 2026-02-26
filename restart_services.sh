#!/bin/bash
# RC Project 全量重启脚本（不重启 PostgreSQL / Redis Docker 容器）

echo "🔄 RC Project 服务重启脚本"
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
    local env_file="/opt/rcproject/.env"
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

if docker_running "postgresql"; then
    echo "   ✅ PostgreSQL (Docker) 运行中"
else
    echo "   ❌ PostgreSQL (Docker) 未运行 — 请先执行: docker start postgresql"
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
    echo "   修复方法: docker start postgresql redis"
    exit 1
fi

echo ""
echo "📋 重启服务列表（保留 PostgreSQL/Redis Docker 容器不重启）:"
echo "   - Web应用"
echo "   - 前端应用"
echo "   - Celery Worker (AI/文件处理)"
echo "   - Celery Worker (数据获取)"
echo "   - 调度器服务"
echo "   - Nginx代理"
echo ""

# 同步 nginx 配置到系统目录（项目中的 nginx.conf 是主配置源）
NGINX_SRC="/opt/rcproject/nginx.conf"
NGINX_DST="/etc/nginx/nginx.conf"
if [ -f "$NGINX_SRC" ]; then
    cp "$NGINX_SRC" "$NGINX_DST"
    if nginx -t -q 2>/dev/null; then
        echo "   ✅ nginx 配置已同步并验证通过"
    else
        echo "   ⚠️  nginx 配置验证失败，已还原"
        git -C /opt/rcproject show HEAD:nginx.conf > "$NGINX_DST" 2>/dev/null || true
    fi
fi

SERVICES=("rc-webapp" "rc-frontend" "rc-celery-ai" "rc-celery-data" "rc-scheduler" "nginx")

for service in "${SERVICES[@]}"; do
    echo "🔄 重启 $service ..."
    systemctl restart "$service"
    if wait_for_service "$service" 20; then
        echo "   ✅ $service 已就绪"
    else
        echo "   ❌ $service 重启失败"
    fi
done

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
    echo "   journalctl -u nginx -f"
fi
