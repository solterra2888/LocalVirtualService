#!/bin/bash
# RC Project 服务重启脚本
# 重启所有服务

echo "🔄 RC Project 服务重启脚本"
echo "================================"

# 检查是否以root权限运行
if [ "$EUID" -ne 0 ]; then
    echo "❌ 请以root权限运行此脚本: sudo $0"
    exit 1
fi

echo "📋 重启服务列表:"
echo "   - Web应用"
echo "   - 前端应用"
echo "   - Nginx代理"
echo ""
echo "ℹ️  注意: 调度器服务需要单独重启"
echo "   重启调度器: sudo ./scheduler/restart_scheduler.sh"
echo ""

# 重启RC Project服务（不包括调度器）
echo "🔄 重启服务..."
systemctl restart rc-webapp rc-frontend nginx

# 等待服务启动
sleep 5

# 检查服务状态
echo "🔍 检查服务状态..."
services=("rc-webapp" "rc-frontend" "nginx")
all_running=true

for service in "${services[@]}"; do
    if systemctl is-active --quiet $service; then
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
    echo "🌐 访问地址:"
    echo "   - 前端应用: http://111.230.48.119:3000"
    echo "   - Web应用: http://111.230.48.119:8000"
    echo "   - API文档: http://111.230.48.119:8000/api/docs"
    echo ""
    echo "🕒 调度器管理:"
    echo "   - 启动调度器: sudo ./scheduler/start_scheduler.sh"
    echo "   - 重启调度器: sudo ./scheduler/restart_scheduler.sh"
    echo "   - 查看状态: sudo ./scheduler/status_scheduler.sh"
else
    echo "⚠️ 部分服务重启失败，请检查日志:"
    echo "   journalctl -u rc-webapp -f"
    echo "   journalctl -u rc-frontend -f"
    echo "   journalctl -u nginx -f"
fi
