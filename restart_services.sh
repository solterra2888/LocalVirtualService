#!/bin/bash
# RC Project 全量重启脚本（不重启 PostgreSQL / Redis）

echo "🔄 RC Project 服务重启脚本"
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

echo "📋 重启服务列表（保留 PostgreSQL/Redis 不重启）:"
echo "   - Web应用"
echo "   - 前端应用"
echo "   - Celery Worker (AI/文件处理)"
echo "   - Celery Worker (数据获取)"
echo "   - 调度器服务"
echo "   - Nginx代理"
echo ""

# 逐个重启以便及时发现问题
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

echo ""
if [ "$all_running" = true ]; then
    echo "🎉 所有服务重启成功!"
    echo "================================"
    echo "🌐 访问地址 (通过Nginx):"
    echo "   - 前端应用: http://111.230.48.119"
    echo "   - API文档:  http://111.230.48.119/api/docs"
else
    echo "⚠️  部分服务重启失败，请查看日志:"
    echo "   journalctl -u rc-webapp -f"
    echo "   journalctl -u rc-celery-ai -f"
    echo "   journalctl -u rc-celery-data -f"
    echo "   journalctl -u rc-scheduler -f"
    echo "   journalctl -u nginx -f"
fi
