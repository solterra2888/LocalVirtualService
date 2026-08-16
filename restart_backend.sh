#!/bin/bash
# GuanHe Project 后端快速重启脚本
# 仅重启后端相关服务（不重启前端、数据库、Redis）
# 适用场景：部署了新代码后快速生效

set -euo pipefail

APP_NAME="$(grep -s '^APP_NAME=' "$(cd "$(dirname "$0")" && pwd)/.env" 2>/dev/null | cut -d= -f2)"
APP_NAME="${APP_NAME:-GuanHe Project}"

echo "🔄 $APP_NAME 后端快速重启"
echo "================================"

# 检查是否以root权限运行
if [ "$EUID" -ne 0 ]; then
    echo "❌ 请以root权限运行此脚本: sudo $0"
    exit 1
fi

# 等待服务就绪的辅助函数
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

echo "📋 重启范围（后端相关服务）:"
echo "   - Web应用 (rc-webapp)"
echo "   - Celery AI Worker (rc-celery-ai)"
echo "   - Celery Data Worker (rc-celery-data)"
echo "   - Celery ZSXQ Files Worker (rc-celery-zsxq-files)"
echo "   - 调度器服务 (rc-scheduler)"
echo ""
echo "   ℹ️  以下服务保持不动: PostgreSQL / Redis / 前端 / Nginx"
echo ""

BACKEND_SERVICES=("rc-webapp" "rc-celery-ai" "rc-celery-data" "rc-celery-zsxq-files" "rc-scheduler")

for service in "${BACKEND_SERVICES[@]}"; do
    echo "🔄 重启 $service ..."
    systemctl restart "$service"
    if wait_for_service "$service" 20; then
        echo "   ✅ $service 已就绪"
    else
        echo "   ❌ $service 重启失败，请检查日志: journalctl -u $service -f"
    fi
done

echo ""
echo "🔍 后端服务状态汇总:"
all_running=true
for service in "${BACKEND_SERVICES[@]}"; do
    if systemctl is-active --quiet "$service"; then
        echo "   ✅ $service: 运行中"
    else
        echo "   ❌ $service: 未运行"
        all_running=false
    fi
done

SERVER_IP=$(
    host=$(grep -E '^SERVER_HOST=' "$(cd "$(dirname "$0")" && pwd)/.env" 2>/dev/null | cut -d'=' -f2 | tr -d '"' | tr -d "'" | head -1)
    [ -n "$host" ] && echo "$host" || hostname -I | awk '{print $1}'
)

echo ""
if [ "$all_running" = true ]; then
    echo "🎉 后端服务重启成功!"
    echo "================================"
    echo "🌐 访问地址: http://${SERVER_IP}"
    echo "📄 API文档:  http://${SERVER_IP}/api/docs  (ENV=production 时默认不暴露，需 ENABLE_API_DOCS=true)"
else
    echo "⚠️  部分服务重启失败，请查看日志排查原因"
fi
